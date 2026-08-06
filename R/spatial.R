# spatial helper functions for workflow outputs (pure DuckDB SQL, no sf dependency)

#' Add Point Geometry Column to a DuckDB Table
#'
#' Uses DuckDB spatial SQL to add a GEOMETRY column from longitude/latitude
#' columns. No sf dependency needed.
#'
#' @param con DBI connection to DuckDB
#' @param table Character. Table name.
#' @param lon_col Character. Longitude column name (default: "lon_dec").
#' @param lat_col Character. Latitude column name (default: "lat_dec").
#' @param geom_col Character. Geometry column name to create (default: "geom").
#'
#' @return Invisible NULL. Side effect: adds geometry column to table.
#' @export
#' @concept spatial
#'
#' @examples
#' \dontrun{
#' add_point_geom(con, "casts")
#' add_point_geom(con, "site", lon_col = "longitude", lat_col = "latitude")
#' }
#' @importFrom DBI dbExecute
#' @importFrom glue glue
add_point_geom <- function(
    con,
    table,
    lon_col  = "lon_dec",
    lat_col  = "lat_dec",
    geom_col = "geom") {

  DBI::dbExecute(con, glue::glue(
    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {geom_col} GEOMETRY"))
  DBI::dbExecute(con, glue::glue(
    "UPDATE {table} SET {geom_col} = ST_Point({lon_col}, {lat_col})"))
  message(glue::glue("Added {geom_col} column to {table} table"))
  invisible(NULL)
}

#' Assign Grid Key via Spatial Join
#'
#' Uses DuckDB spatial SQL to add a `grid_key` column to a table by
#' intersecting its geometry with a grid table. Returns a summary of
#' how many rows fell inside vs outside the grid.
#'
#' @param con DBI connection to DuckDB
#' @param table Character. Table name to update.
#' @param geom_col Character. Geometry column in `table` (default: "geom").
#' @param grid_table Character. Grid table name (default: "grid").
#'
#' @return Data frame with columns `status` (in_grid / not_in_grid) and `n`.
#' @export
#' @concept spatial
#'
#' @examples
#' \dontrun{
#' grid_stats <- assign_grid_key(con, "casts")
#' grid_stats |> datatable()
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
assign_grid_key <- function(
    con,
    table,
    geom_col   = "geom",
    grid_table = "grid") {

  DBI::dbExecute(con, paste0(
    'ALTER TABLE "', table, '" ADD COLUMN IF NOT EXISTS grid_key TEXT'))

  DBI::dbExecute(con, paste0(
    'UPDATE "', table, '" SET grid_key = (',
    'SELECT g.grid_key FROM ', grid_table, ' g',
    ' WHERE ST_Intersects("', table, '".', geom_col, ', g.geom) LIMIT 1)'))

  grid_stats <- DBI::dbGetQuery(con, paste0(
    "SELECT CASE WHEN grid_key IS NULL THEN 'not_in_grid' ELSE 'in_grid' END AS status,",
    " COUNT(*) AS n FROM ", table, " GROUP BY status"))

  message(glue::glue(
    "Assigned grid_key to {table}: ",
    "{paste(grid_stats$status, '=', grid_stats$n, collapse = ', ')}"))

  grid_stats
}

#' Load a GCS Parquet File into DuckDB
#'
#' Downloads a parquet file from Google Cloud Storage via [get_gcs_file()],
#' creates a DuckDB table from it, and converts any WKB BLOB geometry
#' columns back to native GEOMETRY type.
#'
#' @param con DBI connection to DuckDB
#' @param gcs_path Character. Full `gs://` path to the parquet file.
#' @param table_name Character. Name for the DuckDB table.
#' @param geom_cols Character vector. Column names containing WKB geometry
#'   BLOBs to convert. If NULL (default), auto-detects BLOB columns with
#'   "geom" in their name.
#'
#' @return Invisible NULL. Side effect: creates table in DuckDB.
#' @export
#' @concept spatial
#'
#' @examples
#' \dontrun{
#' load_gcs_parquet_to_duckdb(
#'   con        = con,
#'   gcs_path   = "gs://calcofi-db/ingest/swfsc_ichthyo/grid.parquet",
#'   table_name = "grid")
#' }
#' @importFrom DBI dbExecute dbGetQuery dbListFields
#' @importFrom glue glue
load_gcs_parquet_to_duckdb <- function(
    con,
    gcs_path,
    table_name,
    geom_cols = NULL) {

  # download parquet from GCS
  local_path <- get_gcs_file(gcs_path)

  # create table from parquet
  DBI::dbExecute(con, glue::glue(
    "CREATE OR REPLACE TABLE {table_name} AS
     SELECT * FROM read_parquet('{local_path}')"))

  # auto-detect geometry BLOB columns if not specified
  if (is.null(geom_cols)) {
    col_info <- DBI::dbGetQuery(con, glue::glue(
      "SELECT column_name, data_type FROM information_schema.columns
       WHERE table_name = '{table_name}'
         AND data_type = 'BLOB'
         AND column_name LIKE '%geom%'"))
    geom_cols <- col_info$column_name
  }

  # convert WKB BLOB -> GEOMETRY for each geometry column
  for (gc in geom_cols) {
    tmp_col <- paste0(gc, "_tmp")
    DBI::dbExecute(con, glue::glue(
      'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {tmp_col} GEOMETRY'))
    DBI::dbExecute(con, glue::glue(
      'UPDATE {table_name} SET {tmp_col} = ST_GeomFromWKB({gc})'))
    DBI::dbExecute(con, glue::glue(
      'ALTER TABLE {table_name} DROP COLUMN {gc}'))
    DBI::dbExecute(con, glue::glue(
      'ALTER TABLE {table_name} RENAME COLUMN {tmp_col} TO {gc}'))
    message(glue::glue("Converted {gc} from WKB BLOB to GEOMETRY in {table_name}"))
  }

  n_rows <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {table_name}"))$n
  message(glue::glue(
    "Loaded {table_name} from GCS: {n_rows} rows, {length(DBI::dbListFields(con, table_name))} columns"))

  invisible(NULL)
}

# look up a column's DuckDB data type from information_schema (helper, not exported)
.column_type <- function(con, table, column) {
  t <- DBI::dbGetQuery(con, glue::glue(
    "SELECT data_type FROM information_schema.columns
     WHERE table_name = '{table}' AND column_name = '{column}'
     LIMIT 1"))$data_type
  if (length(t) == 0 || is.na(t)) "INTEGER" else t
}

#' Match Records to a Reference Table by Key + Datetime Window
#'
#' Adds a foreign-key column to `data_tbl` and populates it with the primary
#' key of the nearest-in-time row of `ref_tbl` that shares the same key column
#' (e.g. `site_key`) and falls within `window_days` of the data row's datetime.
#' This is the standard cross-dataset bridge for datasets that lack an explicit
#' cast/cruise FK (issue #47). Extracted from `ingest_calcofi_dic.qmd` so every
#' ingest reuses the same logic rather than re-writing inline SQL.
#'
#' @param con DBI connection to DuckDB.
#' @param data_tbl Character. Table to add the FK column to (e.g. "dic_sample").
#' @param ref_tbl Character. Reference table to match against (e.g. "casts").
#' @param fk_col Character. FK column to create on `data_tbl` (default "cast_id").
#' @param ref_pk Character. Primary-key column on `ref_tbl` (default "cast_id").
#' @param key_col Character. Shared key matched exactly on both tables
#'   (default "site_key").
#' @param datetime_col Character. Timestamp column on `data_tbl`, compared by
#'   date (default "datetime_start_utc").
#' @param ref_key_col Character. Key column on `ref_tbl` (default: same as
#'   `key_col`). Set when the reference table names the key differently.
#' @param ref_datetime_col Character. Timestamp column on `ref_tbl` (default:
#'   same as `datetime_col`). Set when the two tables name their timestamp
#'   differently — e.g. matching a dataset on `datetime_start_utc` to a not-yet-
#'   normalized reference still on `datetime_utc`.
#' @param window_days Numeric. Maximum absolute date difference, in days
#'   (default 3).
#' @param return_stats Logical. If TRUE (default), return a stats list;
#'   otherwise return invisible NULL.
#'
#' @return If `return_stats`, a list with `matched`, `total`, `pct`, and an
#'   `unmatched` data frame of distinct unmatched key/date rows. Side effect:
#'   adds and populates `fk_col` on `data_tbl`.
#' @export
#' @concept spatial
#'
#' @examples
#' \dontrun{
#' s <- match_by_site_datetime(con, "dic_sample", "casts")
#' cat(glue::glue("matched {s$matched}/{s$total} ({s$pct}%)"), "\n")
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
match_by_site_datetime <- function(
    con,
    data_tbl,
    ref_tbl,
    fk_col       = "cast_id",
    ref_pk       = "cast_id",
    key_col          = "site_key",
    datetime_col     = "datetime_start_utc",
    ref_key_col      = NULL,
    ref_datetime_col = NULL,
    window_days      = 3,
    return_stats     = TRUE) {

  if (is.null(ref_key_col))      ref_key_col      <- key_col
  if (is.null(ref_datetime_col)) ref_datetime_col <- datetime_col

  fk_type <- .column_type(con, ref_tbl, ref_pk)

  DBI::dbExecute(con, glue::glue(
    "ALTER TABLE {data_tbl} ADD COLUMN IF NOT EXISTS {fk_col} {fk_type}"))

  DBI::dbExecute(con, glue::glue(
    "UPDATE {data_tbl} d
     SET {fk_col} = (
       SELECT r.{ref_pk}
       FROM {ref_tbl} r
       WHERE r.{ref_key_col} = d.{key_col}
         AND ABS(r.{ref_datetime_col}::DATE - d.{datetime_col}::DATE) <= {window_days}
       ORDER BY ABS(r.{ref_datetime_col}::DATE - d.{datetime_col}::DATE)
       LIMIT 1
     )"))

  matched <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {data_tbl} WHERE {fk_col} IS NOT NULL"))$n
  total <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {data_tbl}"))$n
  pct <- if (total > 0) round(100 * matched / total, 1) else NA_real_

  message(glue::glue(
    "match_by_site_datetime: {matched}/{total} {data_tbl} rows matched to ",
    "{ref_tbl} ({pct}%) within {window_days} d"))

  if (!return_stats) return(invisible(NULL))

  unmatched <- DBI::dbGetQuery(con, glue::glue(
    "SELECT DISTINCT {key_col}, {datetime_col}::DATE AS date
     FROM {data_tbl} WHERE {fk_col} IS NULL ORDER BY {key_col}, date"))

  list(matched = matched, total = total, pct = pct, unmatched = unmatched)
}

#' Match Records to the Nearest Reference Row Along a Continuous Axis
#'
#' Adds a foreign-key column to `data_tbl` and populates it with the primary
#' key of the `ref_tbl` row that is nearest along a continuous axis (e.g.
#' `depth_m`) within `tolerance`, restricted to rows sharing an already-matched
#' parent FK (e.g. `cast_id`). Use after [match_by_site_datetime()] to descend
#' from a parent match (cast) to a child match (Niskin bottle). Extracted from
#' `ingest_calcofi_dic.qmd`.
#'
#' @param con DBI connection to DuckDB.
#' @param data_tbl Character. Table to add the FK column to (e.g. "dic_sample").
#' @param ref_tbl Character. Reference table to match against (e.g. "bottle").
#' @param fk_col Character. FK column to create on `data_tbl` (default
#'   "bottle_id").
#' @param ref_pk Character. Primary-key column on `ref_tbl` (default
#'   "bottle_id").
#' @param parent_fk Character. Parent FK present on both tables that scopes the
#'   match (default "cast_id"); only rows with a non-NULL parent are matched.
#' @param axis_col Character. Continuous column minimized in absolute difference
#'   (default "depth_m").
#' @param tolerance Numeric. Maximum absolute difference on `axis_col`
#'   (default 1.0).
#' @param return_stats Logical. If TRUE (default), return a stats list;
#'   otherwise return invisible NULL.
#'
#' @return If `return_stats`, a list with `matched`, `eligible` (rows with a
#'   non-NULL parent), and `pct`. Side effect: adds and populates `fk_col`.
#' @export
#' @concept spatial
#'
#' @examples
#' \dontrun{
#' match_by_site_datetime(con, "dic_sample", "casts")
#' match_nearest_by_depth(con, "dic_sample", "bottle")
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
match_nearest_by_depth <- function(
    con,
    data_tbl,
    ref_tbl,
    fk_col       = "bottle_id",
    ref_pk       = "bottle_id",
    parent_fk    = "cast_id",
    axis_col     = "depth_m",
    tolerance    = 1.0,
    return_stats = TRUE) {

  fk_type <- .column_type(con, ref_tbl, ref_pk)

  DBI::dbExecute(con, glue::glue(
    "ALTER TABLE {data_tbl} ADD COLUMN IF NOT EXISTS {fk_col} {fk_type}"))

  DBI::dbExecute(con, glue::glue(
    "UPDATE {data_tbl} d
     SET {fk_col} = (
       SELECT r.{ref_pk}
       FROM {ref_tbl} r
       WHERE r.{parent_fk} = d.{parent_fk}
         AND ABS(r.{axis_col} - d.{axis_col}) <= {tolerance}
       ORDER BY ABS(r.{axis_col} - d.{axis_col})
       LIMIT 1
     )
     WHERE d.{parent_fk} IS NOT NULL"))

  matched <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {data_tbl} WHERE {fk_col} IS NOT NULL"))$n
  eligible <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {data_tbl} WHERE {parent_fk} IS NOT NULL"))$n
  pct <- if (eligible > 0) round(100 * matched / eligible, 1) else NA_real_

  message(glue::glue(
    "match_nearest_by_depth: {matched}/{eligible} {data_tbl} rows matched to ",
    "{ref_tbl} ({pct}%) within {tolerance} {axis_col}"))

  if (!return_stats) return(invisible(NULL))
  list(matched = matched, eligible = eligible, pct = pct)
}

#' Match Records to a Cruise by Space-Time Proximity to an Occupied-Station Track
#'
#' Assigns `cruise_key` to rows that carry a date and a position but no cruise
#' FK, by finding the nearest station occupation in a reference *track* (any
#' table of `cruise_key` + datetime + lon/lat, e.g. the `sample` shard of an
#' already-ingested dataset) on the same day, within `max_km`.
#'
#' Datasets whose observers ride a CalCOFI ship — the bird/mammal census
#' (issue #74) is the motivating case — record a survey label rather than a
#' cruise, so the cruise can only be recovered from where and when the platform
#' actually was. Year-month parsed from a survey label is *not* sufficient: it
#' is ambiguous whenever several ships sailed in one month, and it is wrong
#' outright for a cruise that straddles a month boundary.
#'
#' With `group_col` set, the match becomes a **consensus** rather than a
#' per-row assignment: every row of a group (one survey = one cruise) votes
#' with its own nearest-station match, the modal `cruise_key` wins if it holds
#' at least `min_share` of the votes, and that winner is written to *all* rows
#' of the group — including rows too far from any station to have voted. This is
#' both more robust (a single transect that strays near another ship's station
#' cannot mis-assign itself) and higher-yield (a group resolves as a whole).
#' A group whose votes are too split, or that has no vote at all, is left NULL
#' rather than guessed.
#'
#' Only cruises present in the reference track can be assigned, so pointing
#' `ref_tbl` at a track whose `cruise_key`s are all present in the `cruise`
#' reference table guarantees the emitted FK resolves. Rows that cannot be
#' matched keep `NULL`, which is the honest answer for a survey that rode a
#' non-CalCOFI cruise.
#'
#' Distance uses the cosine-corrected equirectangular approximation, which is
#' well under 1% error at the sub-degree separations that matter here and is
#' far cheaper than `ST_Distance_Sphere()` over the candidate join. Candidates
#' are pre-filtered to a bounding box of `max_km`, so an absurd antipodal
#' "nearest" row can never be considered.
#'
#' @param con DBI connection to DuckDB.
#' @param data_tbl Character. Table to populate `cruise_key` on.
#' @param ref_tbl Character. Reference track table carrying `cruise_key_col`,
#'   `ref_datetime_col` and `ref_lon_col`/`ref_lat_col`.
#' @param cruise_key_col Character. Cruise key column, on both tables
#'   (default "cruise_key").
#' @param datetime_col,lon_col,lat_col Character. Timestamp (compared by date)
#'   and position columns on `data_tbl`.
#' @param ref_datetime_col,ref_lon_col,ref_lat_col Character. The same on
#'   `ref_tbl`; each defaults to its `data_tbl` counterpart.
#' @param group_col Character or NULL. Column naming the survey/cruise grouping
#'   (e.g. "cruise_label"). When NULL (default) each row is assigned its own
#'   nearest match; when set, the group consensus described above is used.
#' @param window_days Numeric. Maximum absolute date difference in days
#'   (default 0, i.e. same day — the observer is aboard on the day).
#' @param max_km Numeric. Maximum distance for a row to match, in km
#'   (default 25).
#' @param min_share Numeric. Minimum share of a group's votes the winning
#'   `cruise_key` must hold, in 0-1 (default 0.5). Ignored when `group_col` is
#'   NULL.
#' @param return_stats Logical. If TRUE (default) return a stats list.
#'
#' @return If `return_stats`, a list with `matched`, `total`, `pct` and
#'   `groups` — a data frame of one row per group with the winning
#'   `cruise_key`, `votes`, `votes_total`, `share` and `n_rows` (NULL
#'   `cruise_key` for an unresolved group; absent when `group_col` is NULL).
#'   Side effect: adds and populates `cruise_key_col` on `data_tbl`.
#' @export
#' @concept spatial
#'
#' @examples
#' \dontrun{
#' # reference track = the ichthyo sample shard (its cruise_keys are exactly
#' # the `cruise` reference table's, so the emitted FK always resolves)
#' s <- match_cruise_by_track(
#'   con, "bird_mammal_transect", "cruise_track",
#'   datetime_col = "date", lon_col = "longitude", lat_col = "latitude",
#'   group_col    = "cruise_label")
#' cat(glue::glue("{s$matched}/{s$total} ({s$pct}%)"), "\n")
#' s$groups[is.na(s$groups$cruise_key), ]  # surveys with no CalCOFI cruise
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
match_cruise_by_track <- function(
    con,
    data_tbl,
    ref_tbl,
    cruise_key_col   = "cruise_key",
    datetime_col     = "datetime_start_utc",
    lon_col          = "longitude",
    lat_col          = "latitude",
    ref_datetime_col = NULL,
    ref_lon_col      = NULL,
    ref_lat_col      = NULL,
    group_col        = NULL,
    window_days      = 0,
    max_km           = 25,
    min_share        = 0.5,
    return_stats     = TRUE) {

  if (is.null(ref_datetime_col)) ref_datetime_col <- datetime_col
  if (is.null(ref_lon_col))      ref_lon_col      <- lon_col
  if (is.null(ref_lat_col))      ref_lat_col      <- lat_col

  stopifnot(
    "min_share must be in (0, 1]" = min_share > 0 && min_share <= 1,
    "max_km must be positive"     = max_km > 0,
    "window_days must be >= 0"    = window_days >= 0)

  # check the columns up front: the ref_* arguments default to their data_tbl
  # counterparts, so a reference track naming its timestamp differently (a
  # `sample` shard calls it `datetime`, a source table `date`) otherwise fails
  # deep inside DuckDB as a binder error whose message is swallowed by cli
  # formatting of the surrounding glue braces
  .need <- function(tbl, cols, arg) {
    have <- DBI::dbListFields(con, tbl)
    miss <- setdiff(cols, have)
    if (length(miss))
      stop(glue::glue(
        "match_cruise_by_track: {tbl} has no column ",
        "{paste(sQuote(miss), collapse = ', ')} (set {arg}). ",
        "Available: {paste(have, collapse = ', ')}"), call. = FALSE)
  }
  .need(data_tbl, c(datetime_col, lon_col, lat_col),
        "datetime_col / lon_col / lat_col")
  .need(ref_tbl, c(cruise_key_col, ref_datetime_col, ref_lon_col, ref_lat_col),
        "cruise_key_col / ref_datetime_col / ref_lon_col / ref_lat_col")
  if (!is.null(group_col)) .need(data_tbl, group_col, "group_col")

  # squared distance in degrees, longitude scaled by cos(latitude); ranking and
  # thresholding share one expression so the winner is always the reported one
  d2 <- glue::glue(
    "(d.{lat_col} - r.{ref_lat_col}) * (d.{lat_col} - r.{ref_lat_col}) + ",
    "(d.{lon_col} - r.{ref_lon_col}) * (d.{lon_col} - r.{ref_lon_col}) * ",
    "cos(radians(d.{lat_col})) * cos(radians(d.{lat_col}))")
  max_deg <- max_km / 111.32

  # NaN survives IS NOT NULL and would poison both the join and the ranking
  finite <- function(a, lon, lat) glue::glue(
    "{a}.{lon} IS NOT NULL AND {a}.{lat} IS NOT NULL AND ",
    "NOT isnan({a}.{lon}) AND NOT isnan({a}.{lat}) AND ",
    "NOT isinf({a}.{lon}) AND NOT isinf({a}.{lat})")

  DBI::dbExecute(con, glue::glue(
    "ALTER TABLE {data_tbl} ADD COLUMN IF NOT EXISTS {cruise_key_col} VARCHAR"))

  # one candidate per data row: nearest same-window station occupation, bounded
  # by a max_km box so a far-away row is never even considered
  DBI::dbExecute(con, "DROP TABLE IF EXISTS tmp_cruise_track_vote")
  DBI::dbExecute(con, glue::glue("
    CREATE TEMP TABLE tmp_cruise_track_vote AS
    SELECT rowid_, {cruise_key_col}, km FROM (
      SELECT d.rowid AS rowid_, r.{cruise_key_col} AS {cruise_key_col},
             sqrt({d2}) * 111.32 AS km,
             row_number() OVER (PARTITION BY d.rowid ORDER BY {d2}) AS rn
      FROM {data_tbl} d
      JOIN {ref_tbl} r
        ON abs(date_diff('day', d.{datetime_col}::DATE,
                                r.{ref_datetime_col}::DATE)) <= {window_days}
       AND abs(d.{lat_col} - r.{ref_lat_col}) <= {max_deg}
       AND abs(d.{lon_col} - r.{ref_lon_col}) <= {max_deg} /
             GREATEST(cos(radians(d.{lat_col})), 0.01)
      WHERE d.{datetime_col} IS NOT NULL
        AND r.{cruise_key_col} IS NOT NULL
        AND {finite('d', lon_col, lat_col)}
        AND {finite('r', ref_lon_col, ref_lat_col)}
    ) q WHERE rn = 1 AND km <= {max_km}"))

  groups <- NULL

  if (is.null(group_col)) {
    DBI::dbExecute(con, glue::glue(
      "UPDATE {data_tbl} d SET {cruise_key_col} = (
         SELECT v.{cruise_key_col} FROM tmp_cruise_track_vote v
         WHERE v.rowid_ = d.rowid)"))

  } else {
    # consensus: modal cruise_key per group, applied to every row of the group
    DBI::dbExecute(con, "DROP TABLE IF EXISTS tmp_cruise_track_win")
    DBI::dbExecute(con, glue::glue("
      CREATE TEMP TABLE tmp_cruise_track_win AS
      WITH tally AS (
        SELECT d.{group_col} AS grp_, v.{cruise_key_col} AS ck_, count(*) AS votes
        FROM {data_tbl} d JOIN tmp_cruise_track_vote v ON v.rowid_ = d.rowid
        WHERE d.{group_col} IS NOT NULL
        GROUP BY 1, 2),
      ranked AS (
        SELECT grp_, ck_, votes, sum(votes) OVER (PARTITION BY grp_) AS votes_total,
               row_number() OVER (PARTITION BY grp_ ORDER BY votes DESC, ck_) AS rn
        FROM tally)
      SELECT grp_, ck_, votes, votes_total,
             CAST(votes AS DOUBLE) / votes_total AS share
      FROM ranked
      WHERE rn = 1 AND CAST(votes AS DOUBLE) / votes_total >= {min_share}"))

    DBI::dbExecute(con, glue::glue(
      "UPDATE {data_tbl} d SET {cruise_key_col} = (
         SELECT w.ck_ FROM tmp_cruise_track_win w WHERE w.grp_ = d.{group_col})"))

    groups <- DBI::dbGetQuery(con, glue::glue("
      SELECT g.{group_col} AS {group_col}, w.ck_ AS {cruise_key_col},
             w.votes, w.votes_total, round(w.share, 4) AS share, g.n_rows
      FROM (SELECT {group_col}, count(*) AS n_rows FROM {data_tbl}
            WHERE {group_col} IS NOT NULL GROUP BY 1) g
      LEFT JOIN tmp_cruise_track_win w ON w.grp_ = g.{group_col}
      ORDER BY (w.ck_ IS NULL) DESC, share, g.{group_col}"))
    DBI::dbExecute(con, "DROP TABLE IF EXISTS tmp_cruise_track_win")
  }

  DBI::dbExecute(con, "DROP TABLE IF EXISTS tmp_cruise_track_vote")

  matched <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {data_tbl} WHERE {cruise_key_col} IS NOT NULL"))$n
  total <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {data_tbl}"))$n
  pct <- if (total > 0) round(100 * matched / total, 1) else NA_real_

  message(glue::glue(
    "match_cruise_by_track: {matched}/{total} {data_tbl} rows got a cruise_key ",
    "({pct}%) within {max_km} km / {window_days} d",
    if (is.null(group_col)) "" else glue::glue(
      ", by {group_col} consensus (min_share {min_share})")))

  if (!return_stats) return(invisible(NULL))
  out <- list(matched = matched, total = total, pct = pct)
  if (!is.null(groups)) out$groups <- groups
  out
}
