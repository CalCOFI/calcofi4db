# depth plausibility: absolute ceiling + seafloor ---------------------------------
#
# `valid_min`/`valid_max` bound a MEASUREMENT VALUE. A depth is a coordinate, and
# nothing bounded it: v2026.08.14 shipped a CTD cast with scans at 14,671 m over a
# 101 m seafloor. The guard that already knew 17,964 dbar was impossible deleted
# the `pressure` value — and left the depth that pressure had been converted to,
# because `drop_out_of_bounds()` cannot see a coordinate column.
#
# Two checks, deliberately different in consequence:
# - an ABSOLUTE ceiling, `CC_DEPTH_MAX_M`: nothing in the region is deeper, so a
#   violation is an error and fails a release outright;
# - the SEAFLOOR at the sample's position: a sample below the bottom is also
#   impossible, but the bottom we know is a ~460 m GEBCO cell and the positions
#   we hold for 1949–1975 are rounded to the minute, so on a slope or a canyon a
#   real cast can read deeper than the cell. Measured at v2026.08.14 with the
#   deepest cell in the 3x3 neighbourhood + 10 m: 695 of 412,640 root samples,
#   all but one within 1.2 km of the bottom. Those are position-precision findings
#   to report and ratchet, not rows to delete.

#' Deepest plausible sample depth in the CalCOFI region, metres
#'
#' The abyssal plain under the westernmost historical stations (~158°W) is
#' ~5,500 m and the region has no trench; 6,500 m matches the ceiling already
#' declared on `pressure` (6,500 dbar). A depth beyond it is an error, not an
#' observation.
#' @export
#' @concept validation
CC_DEPTH_MAX_M <- 6500

#' Seafloor depth at each sample position from a GEBCO GeoTIFF
#'
#' Extracts, for every distinct position in `sample_tbl`, the bilinear seafloor
#' depth (positive down, land clamped to 0 — the same convention as
#' `calcofi4r::cc_bathy_depth()`) and the deepest cell in the 3x3 neighbourhood
#' around it, which is what a plausibility check should compare against: on a
#' slope the neighbourhood is deeper than the cell, and by the amount the slope
#' warrants.
#'
#' @param con DBI connection holding `sample_tbl`.
#' @param gebco_tif Path to a GEBCO GeoTIFF (elevation, metres, negative below
#'   sea level). Any extent works; positions outside it return NA. A
#'   `/vsicurl/...` (or `http(s)://`) source streams over GDAL's range reads —
#'   the release's fallback when no local tile is present (D29).
#' @param sample_tbl,key_col,lon_col,lat_col Table and columns to read.
#' @return A data.frame: `<key_col>`, `seafloor_depth_m`, `seafloor_max3x3_m`.
#' @export
#' @concept validation
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue
sample_seafloor <- function(con, gebco_tif,
                            sample_tbl = "sample", key_col = "sample_key",
                            lon_col = "longitude", lat_col = "latitude") {
  if (!requireNamespace("terra", quietly = TRUE))
    stop("sample_seafloor() needs the `terra` package", call. = FALSE)
  if (grepl("^https?://", gebco_tif)) gebco_tif <- paste0("/vsicurl/", gebco_tif)
  if (!grepl("^/vsi", gebco_tif) && !file.exists(gebco_tif))
    stop("GEBCO tif not found: ", gebco_tif, call. = FALSE)
  pos <- DBI::dbGetQuery(con, glue::glue(
    "SELECT {key_col} AS key, {lon_col} AS lon, {lat_col} AS lat
     FROM {sample_tbl}
     WHERE {lon_col} IS NOT NULL AND {lat_col} IS NOT NULL
       AND NOT isnan({lon_col}) AND NOT isnan({lat_col})"))
  # one extraction per distinct position, mapped back by an exact index — NOT
  # merge(): merge() compares double keys through their character form (15
  # significant digits), while unique() compares the doubles themselves, so two
  # positions differing past the 15th digit stayed distinct here and both
  # matched every sample at either — 4,855 samples were stamped twice and
  # `sample` shipped them twice in v2026.08.25 (obs joined 76,320 rows twice)
  pkey <- paste(sprintf("%.17g", pos$lon), sprintf("%.17g", pos$lat))
  ukey <- unique(pkey)
  u <- pos[match(ukey, pkey), c("lon", "lat")]
  r <- terra::rast(gebco_tif)
  xy <- as.matrix(u)
  # elevation -> depth: negate, clamp land to 0, keep NA off-extent
  to_depth <- function(v) ifelse(is.na(v), NA_real_, pmax(-v, 0))
  u$seafloor_depth_m <- to_depth(terra::extract(r, xy, method = "bilinear")[, 1])
  rs <- terra::res(r)
  nb <- sapply(seq_len(9), function(i) {
    dx <- ((i - 1) %% 3) - 1; dy <- ((i - 1) %/% 3) - 1
    terra::extract(r, cbind(xy[, 1] + dx * rs[1], xy[, 2] + dy * rs[2]))[, 1]
  })
  nb <- matrix(nb, ncol = 9)
  mx <- suppressWarnings(apply(nb, 1, function(v) if (all(is.na(v))) NA_real_ else max(-v, na.rm = TRUE)))
  u$seafloor_max3x3_m <- to_depth(-mx)
  idx <- match(pkey, ukey)
  out <- data.frame(key = pos$key,
                    seafloor_depth_m  = u$seafloor_depth_m[idx],
                    seafloor_max3x3_m = u$seafloor_max3x3_m[idx],
                    stringsAsFactors = FALSE)
  names(out)[1] <- key_col
  if (anyDuplicated(out[[key_col]]))
    stop("sample_seafloor(): ", sum(duplicated(out[[key_col]])), " duplicated ", key_col,
         " — the sample table itself is not unique on its key", call. = FALSE)
  out
}

#' Fail unless every core table is unique on its primary key
#'
#' The release gate that was missing when v2026.08.25 shipped `sample` with
#' 4,855 keys twice: the `validate` chunk only *warned* on `ship`/`cruise`.
#' Primary keys come from [core_relationships()].
#'
#' @param con DuckDB connection holding the assembled release
#' @param tables core tables to check (those present in `con` are checked)
#' @return invisibly, a data.frame `table`, `pk`, `n_rows`, `n_dup`
#' @export
#' @concept validation
check_core_pk_unique <- function(con, tables) {
  pk <- core_relationships(tables)$primary_keys
  have <- intersect(names(pk), DBI::dbListTables(con))
  res <- do.call(rbind, lapply(have, function(tb) {
    cols <- paste0('"', pk[[tb]], '"', collapse = ", ")
    q <- DBI::dbGetQuery(con, glue::glue(
      'SELECT COUNT(*) AS n_rows, COUNT(*) - COUNT(DISTINCT ({cols})) AS n_dup FROM "{tb}"'))
    data.frame(table = tb, pk = paste(pk[[tb]], collapse = ","),
               n_rows = q$n_rows, n_dup = q$n_dup, stringsAsFactors = FALSE)
  }))
  bad <- res[res$n_dup > 0, , drop = FALSE]
  if (nrow(bad))
    stop("primary key not unique in the release: ",
         paste(sprintf("%s(%s): %d duplicate row(s)", bad$table, bad$pk, bad$n_dup), collapse = "; "),
         call. = FALSE)
  invisible(res)
}


#' Stamp `seafloor_depth_m` onto the sample table
#'
#' Rebuilds `sample_tbl` with a trailing `seafloor_depth_m` column (bilinear
#' GEBCO depth, see [sample_seafloor()]). The table is recreated rather than
#' `UPDATE`d because DuckDB cannot update a table carrying a CRS-tagged
#' `GEOMETRY` column (the `geom` on `sample`).
#'
#' @inheritParams sample_seafloor
#' @param seafloor Optional result of [sample_seafloor()] to reuse instead of
#'   extracting again.
#' @return Invisibly, the [sample_seafloor()] data.frame used.
#' @export
#' @concept validation
#' @importFrom DBI dbExecute dbListFields dbWriteTable
#' @importFrom glue glue
add_sample_seafloor <- function(con, gebco_tif, sample_tbl = "sample",
                                key_col = "sample_key", seafloor = NULL) {
  if (is.null(seafloor))
    seafloor <- sample_seafloor(con, gebco_tif, sample_tbl, key_col)
  flds <- setdiff(DBI::dbListFields(con, sample_tbl), "seafloor_depth_m")
  DBI::dbWriteTable(con, "_sample_seafloor",
                    seafloor[, c(key_col, "seafloor_depth_m")], overwrite = TRUE)
  cols <- paste(DBI::dbQuoteIdentifier(con, flds), collapse = ", ")
  DBI::dbExecute(con, glue::glue("
    CREATE OR REPLACE TABLE _sample_new AS
    SELECT s.{gsub(', ', ', s.', cols)}, f.seafloor_depth_m
    FROM {sample_tbl} s LEFT JOIN _sample_seafloor f USING ({key_col})"))
  DBI::dbExecute(con, glue::glue("DROP TABLE {sample_tbl}"))
  DBI::dbExecute(con, glue::glue("ALTER TABLE _sample_new RENAME TO {sample_tbl}"))
  DBI::dbExecute(con, "DROP TABLE _sample_seafloor")
  message(glue::glue(
    "seafloor_depth_m on {sample_tbl}: {sum(!is.na(seafloor$seafloor_depth_m))} of ",
    "{nrow(seafloor)} positioned rows (NA = outside the raster)"))
  invisible(seafloor)
}

#' Check depth coordinates against an absolute range
#'
#' One row per (table, dataset, depth column): how many depths are NaN, below
#' `min_depth_m` or above `max_depth_m`. A non-`ok` row is an error in the data
#' — assert `all(status == "ok")` at ingest and at release.
#'
#' @param con DBI connection.
#' @param tbls Tables to check (each needs the depth columns it has of
#'   `depth_cols`; a table lacking all of them is skipped with a message).
#' @param depth_cols Depth columns to check where present.
#' @param max_depth_m,min_depth_m The plausible range.
#' @param by Grouping column (default `dataset_key`; NULL for none).
#' @return A tibble: `table`, `dataset_key`, `depth_col`, `n_total`, `n_nan`,
#'   `n_below`, `n_above`, `v_min`, `v_max`, `status` (`ok` | `out_of_range`).
#' @export
#' @concept validation
#' @importFrom DBI dbGetQuery dbListFields dbListTables
#' @importFrom glue glue
check_depth_bounds <- function(con, tbls = c("sample", "obs"),
                               depth_cols = c("depth_min_m", "depth_max_m"),
                               max_depth_m = CC_DEPTH_MAX_M, min_depth_m = 0,
                               by = "dataset_key") {
  have <- intersect(tbls, DBI::dbListTables(con))
  out <- list()
  for (tb in have) {
    flds <- DBI::dbListFields(con, tb)
    cols <- intersect(depth_cols, flds)
    if (!length(cols)) { message("check_depth_bounds: `", tb, "` has no depth column; skipped"); next }
    grp <- if (!is.null(by) && by %in% flds) by else "NULL"
    for (cl in cols) {
      out[[length(out) + 1]] <- DBI::dbGetQuery(con, glue::glue("
        SELECT '{tb}' AS \"table\", {grp} AS dataset_key, '{cl}' AS depth_col,
               COUNT({cl})                                   AS n_total,
               COUNT(*) FILTER (WHERE isnan({cl}))           AS n_nan,
               COUNT(*) FILTER (WHERE {cl} < {min_depth_m})  AS n_below,
               COUNT(*) FILTER (WHERE {cl} > {max_depth_m})  AS n_above,
               MIN({cl}) FILTER (WHERE NOT isnan({cl}))      AS v_min,
               MAX({cl}) FILTER (WHERE NOT isnan({cl}))      AS v_max
        FROM {tb} GROUP BY 2"))
    }
  }
  if (!length(out))
    return(tibble::tibble(table = character(), dataset_key = character(),
                          depth_col = character(), n_total = numeric(), n_nan = numeric(),
                          n_below = numeric(), n_above = numeric(), v_min = numeric(),
                          v_max = numeric(), status = character()))
  d <- do.call(rbind, out)
  for (cl in c("n_total", "n_nan", "n_below", "n_above")) d[[cl]] <- as.numeric(d[[cl]])
  d$status <- ifelse(d$n_nan + d$n_below + d$n_above > 0, "out_of_range", "ok")
  d <- d[order(d$status != "out_of_range", -(d$n_nan + d$n_below + d$n_above)), ]
  tibble::as_tibble(d)
}

#' Find samples deeper than the seafloor at their position
#'
#' For every root sample (no parent) takes the deepest depth attributed to it —
#' its own `depth_max_m`/`depth_min_m`, its descendants' and its observations' —
#' and compares it with the deepest GEBCO cell in the 3x3 neighbourhood of its
#' position plus `tolerance_m`. Positions outside the raster are `unknown`, not
#' violations.
#'
#' @param con DBI connection holding `sample_tbl` (with `parent_sample_key`,
#'   `root_sample_key`) and optionally `obs_tbl`.
#' @param seafloor Result of [sample_seafloor()] (or a GEBCO tif path, in which
#'   case it is computed).
#' @param sample_tbl,obs_tbl Table names; `obs_tbl` may be absent.
#' @param tolerance_m Metres a sample may exceed the neighbourhood-deepest cell
#'   before it is a finding (default 10).
#' @return A tibble of violators — `sample_key`, `dataset_key`, `sample_type`,
#'   `cruise_key`, `longitude`, `latitude`, `depth_m`, `seafloor_depth_m`,
#'   `seafloor_max3x3_m`, `excess_m`, `on_land` — worst first, with attribute
#'   `summary`: per-dataset `n_root`, `n_unknown`, `n_over` and `max_excess_m`.
#' @export
#' @concept validation
#' @importFrom DBI dbGetQuery dbListTables dbWriteTable dbExecute
#' @importFrom glue glue
check_depth_vs_seafloor <- function(con, seafloor, sample_tbl = "sample",
                                    obs_tbl = "obs", tolerance_m = 10) {
  if (is.character(seafloor)) seafloor <- sample_seafloor(con, seafloor, sample_tbl)
  stopifnot(all(c("sample_key", "seafloor_depth_m", "seafloor_max3x3_m") %in% names(seafloor)))
  DBI::dbWriteTable(con, "_sf", seafloor, overwrite = TRUE)
  has_obs <- !is.null(obs_tbl) && obs_tbl %in% DBI::dbListTables(con)
  obs_cte <- if (has_obs) glue::glue("
    ob AS (
      SELECT s.root_sample_key AS sample_key, MAX(GREATEST(COALESCE(o.depth_max_m, 0), COALESCE(o.depth_min_m, 0))) AS d_obs
      FROM {obs_tbl} o JOIN {sample_tbl} s USING (sample_key) GROUP BY 1),") else
    "ob AS (SELECT NULL::VARCHAR AS sample_key, NULL::DOUBLE AS d_obs WHERE FALSE),"
  d <- DBI::dbGetQuery(con, glue::glue("
    WITH root AS (
      SELECT sample_key, dataset_key, sample_type, cruise_key, longitude, latitude,
             GREATEST(COALESCE(depth_max_m, 0), COALESCE(depth_min_m, 0)) AS d_self
      FROM {sample_tbl} WHERE parent_sample_key IS NULL),
    kid AS (
      SELECT root_sample_key AS sample_key,
             MAX(GREATEST(COALESCE(depth_max_m, 0), COALESCE(depth_min_m, 0))) AS d_kid
      FROM {sample_tbl} WHERE parent_sample_key IS NOT NULL GROUP BY 1),
    {obs_cte}
    all_d AS (
      SELECT r.*, GREATEST(r.d_self, COALESCE(k.d_kid, 0), COALESCE(o.d_obs, 0)) AS depth_m
      FROM root r LEFT JOIN kid k USING (sample_key) LEFT JOIN ob o USING (sample_key))
    SELECT a.sample_key, a.dataset_key, a.sample_type, a.cruise_key, a.longitude, a.latitude,
           a.depth_m, f.seafloor_depth_m, f.seafloor_max3x3_m,
           a.depth_m - f.seafloor_max3x3_m AS excess_m,
           f.seafloor_max3x3_m = 0 AS on_land
    FROM all_d a LEFT JOIN _sf f USING (sample_key)"))
  DBI::dbExecute(con, "DROP TABLE _sf")
  unknown <- is.na(d$seafloor_max3x3_m)
  over    <- !unknown & d$depth_m > 0 & d$excess_m > tolerance_m
  smry <- stats::aggregate(
    cbind(n_root = 1, n_unknown = unknown, n_over = over) ~ dataset_key, data = d, FUN = sum)
  mx <- stats::aggregate(excess_m ~ dataset_key, data = d[over, , drop = FALSE], FUN = max)
  names(mx)[2] <- "max_excess_m"
  smry <- merge(smry, mx, by = "dataset_key", all.x = TRUE)
  smry <- smry[order(-smry$n_over, smry$dataset_key), ]
  out <- d[over, , drop = FALSE]
  out <- out[order(-out$excess_m), , drop = FALSE]
  message(glue::glue(
    "depth vs seafloor (+{tolerance_m} m over the deepest 3x3 cell): ",
    "{sum(over)} of {nrow(d)} root samples exceed it, {sum(unknown)} outside the raster"))
  out <- tibble::as_tibble(out)
  attr(out, "summary") <- tibble::as_tibble(smry)
  out
}

#' Classify the samples whose `seafloor_depth_m` is NULL, by cause
#'
#' After [add_sample_seafloor()], a `NULL` seafloor can mean four different
#' things, and only one of them is acceptable to ship silently. This returns one
#' row per cause with its count — `no_coordinates` (lon or lat NULL),
#' `nan_coordinate`, `outside_source_tile` (the position genuinely falls off the
#' GEBCO tile that was sampled) — and `inside_tile_null`, a position **inside**
#' the tile that still reads NULL, which can only be a regression in the
#' sampling itself. Gate on that one:
#' `stopifnot(attr(x, "n_inside_null") == 0)` (D29, 2026-08-31).
#'
#' @param con DBI connection holding `sample_tbl` after [add_sample_seafloor()].
#' @param source_bbox `c(w, s, e, n)` of the GEBCO source that was sampled —
#'   the full sub-ice tile `c(-180, 0, -90, 90)` by default.
#' @param sample_tbl,lon_col,lat_col Table and columns to read.
#' @return data.frame `cause`, `n` (plus a `datasets` summary column for the
#'   inside-tile rows); attribute `n_inside_null` carries the gate value.
#' @export
#' @concept validation
check_seafloor_nulls <- function(con, source_bbox = c(-180, 0, -90, 90),
                                 sample_tbl = "sample",
                                 lon_col = "longitude", lat_col = "latitude") {
  q <- DBI::dbGetQuery(con, glue::glue(
    "SELECT
       CASE
         WHEN {lon_col} IS NULL OR {lat_col} IS NULL THEN 'no_coordinates'
         WHEN isnan({lon_col}) OR isnan({lat_col})   THEN 'nan_coordinate'
         WHEN {lon_col} < {source_bbox[1]} OR {lon_col} > {source_bbox[3]}
           OR {lat_col} < {source_bbox[2]} OR {lat_col} > {source_bbox[4]}
                                                     THEN 'outside_source_tile'
         ELSE 'inside_tile_null'
       END AS cause,
       COUNT(*) AS n,
       string_agg(DISTINCT dataset_key, ', ') AS datasets
     FROM {sample_tbl}
     WHERE seafloor_depth_m IS NULL
     GROUP BY 1 ORDER BY 1"))
  attr(q, "n_inside_null") <- sum(q$n[q$cause == "inside_tile_null"])
  q
}
