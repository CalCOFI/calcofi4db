# core consolidated data model -------------------------------------------------
# materialize + append the small "core" fact/dimension family that every
# cross-dataset consumer reads, replacing the ~40 per-dataset triples:
#   - sample              : event dimension (adjacency list: leaf/parent/root)
#   - obs                 : occurrence-headline long table (realm env|bio)
#   - obs_attribute       : sub-occurrence attribution (length/stage freq, behavior)
#   - sample_measurement  : event-level (effort) long table
#   - obs_ctd_full        : supplemental full-resolution CTD (same shape as obs)
# plus the shared, dataset-independent reference builder build_grid_reference().
#
# Every projection here is a GENERIC SHAPE, never a per-dataset arm: each dataset's
# projection SQL lives in the ingest notebook that owns the dataset
# (ingest_{provider}_{dataset}.qmd, "Emit Core Tables"), and release_database.qmd
# is a pure union of the resulting parquet shards. That is deliberate — while the
# arms lived here they were duplicated by an inline copy on the release side, the
# two drifted, and each divergence was a silent data error (euphausiids flattened
# to family, bird_mammal merged distinct species, phytoplankton emitted no
# observations at all). One projection, in one place, owned by one notebook.
#
# See design_env-bio-consolidation.md (CalCOFI/workflows) for the target model.
#
# sample_key convention: every sample_key is namespaced "<dataset_key>:<sample_type>:<id>"
# so it is globally unique across datasets AND across event levels within a dataset
# (bottle cast_id=5 and bottle_id=5 would otherwise collide). obs.sample_key then
# joins sample on a single column. This also makes the DIC->bottle dedup fall out: a
# DIC observation sharing a physical Niskin points at "calcofi_bottle:bottle:<id>".

# finest H3 resolution stored in obs.hex_id / obs_ctd_full.hex_id; coarser
# aggregations are a query-time function h3_cell_to_parent(hex_id, res) - no
# per-resolution columns are stored (retires the hex_h3res0..N ladder).
CC_H3_RES_MAX <- 10L

# extensions -------------------------------------------------------------------

.load_h3 <- function(con) {
  # community H3 extension: h3_latlng_to_cell() returns UBIGINT, and
  # h3_cell_to_parent(hex_id, res) climbs the hierarchy at query time.
  load_duckdb_extension(con, "h3", from = "community")
  invisible(con)
}

.load_spatial <- function(con) {
  load_duckdb_extension(con, "spatial")
  invisible(con)
}

# SQL fragment computing hex_id from lat/lng columns of the wrapped SELECT
.hex_expr <- function(res = CC_H3_RES_MAX, lat = "latitude", lng = "longitude") {
  glue::glue(
    "CASE WHEN {lat} IS NULL OR {lng} IS NULL THEN NULL::UBIGINT
          ELSE h3_latlng_to_cell({lat}, {lng}, {res})::UBIGINT END")
}

# namespaced sample_key: '<dataset_key>:<sample_type>:' || CAST(<id_sql> AS VARCHAR)
#' Namespaced `sample_key` expression: `dataset_key:sample_type:id`
#' @param dataset_key,sample_type,id_sql components
#' @return a SQL expression string
#' @export
#' @concept model
ns_key <- function(dataset_key, sample_type, id_sql) {
  glue::glue("'{dataset_key}:{sample_type}:' || CAST({id_sql} AS VARCHAR)")
}

# typed schema DDL (shared by both callers so parquet types stay stable) --------

.ensure_obs_schema <- function(con, obs_tbl = "obs") {
  DBI::dbExecute(con, glue::glue(
    "CREATE TABLE IF NOT EXISTS {obs_tbl} (
       obs_id            BIGINT,
       realm             VARCHAR,
       dataset_key       VARCHAR,
       sample_key        VARCHAR,
       grid_key          VARCHAR,
       cruise_key        VARCHAR,
       latitude          DOUBLE,
       longitude         DOUBLE,
       datetime          TIMESTAMP,
       depth_min_m       DOUBLE,
       depth_max_m       DOUBLE,
       taxon_key         VARCHAR,
       life_stage        VARCHAR,
       measurement_type  VARCHAR,
       measurement_value DOUBLE,
       measurement_qual  VARCHAR,
       measurement_prec  DOUBLE,
       hex_id            UBIGINT)"))
  invisible(obs_tbl)
}

# obs_attribute: generalized sub-occurrence attribution — length-/stage-frequency
# AND categorical breakdowns (e.g. seabird behavior). `bin_value` = the numeric
# attribute (length mm, stage no.; NULL for categorical), `bin_label` = its
# category label (preflexion, Flying), `count` = individuals. Supersedes the old
# `obs_freq` (same columns; adds behavior rows + the taxon_key rename).
.ensure_obs_attribute_schema <- function(con, tbl = "obs_attribute") {
  DBI::dbExecute(con, glue::glue(
    "CREATE TABLE IF NOT EXISTS {tbl} (
       obs_attribute_id  BIGINT,
       dataset_key       VARCHAR,
       sample_key        VARCHAR,
       taxon_key         VARCHAR,
       life_stage        VARCHAR,
       measurement_type  VARCHAR,
       bin_value         DOUBLE,
       bin_label         VARCHAR,
       count             INTEGER,
       measurement_qual  VARCHAR)"))
  invisible(tbl)
}

.ensure_sample_measurement_schema <- function(con, tbl = "sample_measurement") {
  DBI::dbExecute(con, glue::glue(
    "CREATE TABLE IF NOT EXISTS {tbl} (
       sample_measurement_id BIGINT,
       sample_key            VARCHAR,
       dataset_key           VARCHAR,
       measurement_type      VARCHAR,
       measurement_value     DOUBLE,
       measurement_qual      VARCHAR)"))
  invisible(tbl)
}

.ensure_sample_schema <- function(con, tbl = "sample") {
  DBI::dbExecute(con, glue::glue(
    "CREATE TABLE IF NOT EXISTS {tbl} (
       sample_key        VARCHAR,
       sample_type       VARCHAR,
       parent_sample_key VARCHAR,
       root_sample_key   VARCHAR,
       dataset_key       VARCHAR,
       grid_key          VARCHAR,
       site_key          VARCHAR,
       cruise_key        VARCHAR,
       order_occ         INTEGER,
       latitude          DOUBLE,
       longitude         DOUBLE,
       datetime          TIMESTAMP,
       depth_min_m       DOUBLE,
       depth_max_m       DOUBLE,
       tow_type          VARCHAR,
       data_stage        VARCHAR,
       geom              GEOMETRY)"))
  # `data_stage` was added in 3.4.0. The wrangling DB survives across runs (each
  # ingest restores from a checkpoint), so CREATE TABLE IF NOT EXISTS alone would
  # leave a pre-3.4.0 `sample` a column short and the INSERT below would fail on
  # a stale DB rather than on anything the caller did.
  DBI::dbExecute(con, glue::glue(
    "ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS data_stage VARCHAR"))
  invisible(tbl)
}

# append_* primitives ----------------------------------------------------------

#' Append occurrence-headline rows into the core `obs` table
#'
#' Wraps a caller-supplied projection `select_sql` (which must yield the canonical
#' `obs` columns *by name* — `realm`, `dataset_key`, `sample_key`, `grid_key`,
#' `cruise_key`, `latitude`, `longitude`, `datetime`, `depth_min_m`, `depth_max_m`,
#' `taxon_key`, `life_stage`, `measurement_type`, `measurement_value`,
#' `measurement_qual`, `measurement_prec`), mints a surrogate `obs_id` (offset from
#' the current max so repeated calls stay unique within one connection) and computes
#' `hex_id` at H3 resolution `res_max`. The same helper serves the central Phase-2
#' materialization (`release_database.qmd`) and each per-dataset ingest (Phase 3);
#' release assembly renumbers `obs_id` globally across the reassembled shards.
#'
#' @param con a DuckDB connection (open via [get_duckdb_con()])
#' @param select_sql a SELECT producing the canonical `obs` columns by name
#' @param obs_tbl target table name (`"obs"`, or `"obs_ctd_full"` for full CTD)
#' @param res_max finest H3 resolution stored in `hex_id`
#' @return (invisibly) the total row count of `obs_tbl` after the append
#' @export
#' @concept model
append_obs <- function(con, select_sql, obs_tbl = "obs", res_max = CC_H3_RES_MAX) {
  .load_h3(con)
  .ensure_obs_schema(con, obs_tbl)
  off <- DBI::dbGetQuery(
    con, glue::glue("SELECT COALESCE(MAX(obs_id), 0) AS m FROM {obs_tbl}"))$m
  hex <- .hex_expr(res_max)
  DBI::dbExecute(con, glue::glue(
    "INSERT INTO {obs_tbl}
       (obs_id, realm, dataset_key, sample_key, grid_key, cruise_key,
        latitude, longitude, datetime, depth_min_m, depth_max_m,
        taxon_key, life_stage, measurement_type, measurement_value,
        measurement_qual, measurement_prec, hex_id)
     SELECT {off} + ROW_NUMBER() OVER () AS obs_id,
            realm, dataset_key, sample_key, grid_key, cruise_key,
            latitude, longitude, datetime, depth_min_m, depth_max_m,
            taxon_key, life_stage, measurement_type, measurement_value,
            measurement_qual, measurement_prec,
            {hex} AS hex_id
     FROM ( {select_sql} ) AS src(realm, dataset_key, sample_key, grid_key, cruise_key,
            latitude, longitude, datetime, depth_min_m, depth_max_m, taxon_key, life_stage,
            measurement_type, measurement_value, measurement_qual, measurement_prec)"))
  invisible(DBI::dbGetQuery(
    con, glue::glue("SELECT COUNT(*) AS n FROM {obs_tbl}"))$n)
}

#' Append sub-occurrence attribute rows into the core `obs_attribute` table
#'
#' Generalizes the former `obs_freq`: holds any within-occurrence attribution —
#' length-frequency, stage-frequency, and categorical breakdowns like seabird
#' behavior. `select_sql` must yield `dataset_key`, `sample_key`, `taxon_key`,
#' `life_stage`, `measurement_type` (the attribute, e.g. `body_length`/`stage`/
#' `behavior`), `bin_value` (numeric bin / stage no.), `bin_label` (category
#' label), `count`, `measurement_qual` by name.
#' @inheritParams append_obs
#' @param tbl target table (default `"obs_attribute"`)
#' @return (invisibly) the total row count of `tbl` after the append
#' @export
#' @concept model
append_obs_attribute <- function(con, select_sql, tbl = "obs_attribute") {
  .ensure_obs_attribute_schema(con, tbl)
  off <- DBI::dbGetQuery(
    con, glue::glue("SELECT COALESCE(MAX(obs_attribute_id), 0) AS m FROM {tbl}"))$m
  DBI::dbExecute(con, glue::glue(
    "INSERT INTO {tbl}
       (obs_attribute_id, dataset_key, sample_key, taxon_key, life_stage,
        measurement_type, bin_value, bin_label, count, measurement_qual)
     SELECT {off} + ROW_NUMBER() OVER () AS obs_attribute_id,
            dataset_key, sample_key, taxon_key, life_stage,
            measurement_type, bin_value, bin_label, count, measurement_qual
     FROM ( {select_sql} ) AS src(dataset_key, sample_key, taxon_key, life_stage,
            measurement_type, bin_value, bin_label, count, measurement_qual)"))
  invisible(DBI::dbGetQuery(
    con, glue::glue("SELECT COUNT(*) AS n FROM {tbl}"))$n)
}

#' Append event-level (effort) rows into the core `sample_measurement` table
#'
#' `select_sql` must yield `sample_key`, `dataset_key`, `measurement_type`,
#' `measurement_value`, `measurement_qual` by name.
#' @inheritParams append_obs
#' @param tbl target table (default `"sample_measurement"`)
#' @return (invisibly) the total row count of `tbl` after the append
#' @export
#' @concept model
append_sample_measurement <- function(con, select_sql, tbl = "sample_measurement") {
  .ensure_sample_measurement_schema(con, tbl)
  off <- DBI::dbGetQuery(
    con, glue::glue("SELECT COALESCE(MAX(sample_measurement_id), 0) AS m FROM {tbl}"))$m
  DBI::dbExecute(con, glue::glue(
    "INSERT INTO {tbl}
       (sample_measurement_id, sample_key, dataset_key,
        measurement_type, measurement_value, measurement_qual)
     SELECT {off} + ROW_NUMBER() OVER () AS sample_measurement_id,
            sample_key, dataset_key, measurement_type, measurement_value, measurement_qual
     FROM ( {select_sql} ) AS src(sample_key, dataset_key,
            measurement_type, measurement_value, measurement_qual)"))
  invisible(DBI::dbGetQuery(
    con, glue::glue("SELECT COUNT(*) AS n FROM {tbl}"))$n)
}

#' Append event rows into the core `sample` dimension
#'
#' `select_sql` is bound **positionally**, so it must yield either the 15 columns
#' of the base contract — `sample_key`, `sample_type`, `parent_sample_key`,
#' `root_sample_key`, `dataset_key`, `grid_key`, `site_key`, `cruise_key`,
#' `order_occ`, `latitude`, `longitude`, `datetime`, `depth_min_m`, `depth_max_m`,
#' `tow_type` — or those 15 plus a trailing 16th, `data_stage`. `geom` is minted
#' here as `ST_Point(longitude, latitude)`. `tow_type` is the net gear code
#' (ichthyo tow/net grains: C1/CB/CV/PV oblique/vertical, MT manta), NULL for
#' gears/datasets without one. Call it once per event level — a multi-level
#' dataset (ichthyo `site`->`tow`->`net`, bottle `cast`->`bottle`) appends one arm
#' per level, and [sample_arm_self()] writes the single-level case for you.
#'
#' `data_stage` is **optional and trailing** on purpose: it records the source's
#' own processing state for the event (`final` vs `preliminary` for CTD casts, per
#' question `calcofi_ctd-cast_14`), which most datasets do not distinguish. Making
#' it positional column 16 rather than inserting it into the contract lets a
#' dataset opt in when it has a meaningful stage without touching the other arms —
#' a 15-column arm gets `NULL` and keeps working unchanged.
#' @inheritParams append_obs
#' @param sample_tbl target table (default `"sample"`)
#' @return (invisibly) the total row count of `sample_tbl` after the append
#' @export
#' @concept model
append_sample <- function(con, select_sql, sample_tbl = "sample") {
  .load_spatial(con)
  .ensure_sample_schema(con, sample_tbl)

  src_cols <- c(
    "sample_key", "sample_type", "parent_sample_key", "root_sample_key",
    "dataset_key", "grid_key", "site_key", "cruise_key", "order_occ",
    "latitude", "longitude", "datetime", "depth_min_m", "depth_max_m", "tow_type")
  # DESCRIBE, not a LIMIT 0 scan: the arity has to be known before the positional
  # alias list is written, and a 15-vs-16 mismatch must be a named error rather
  # than DuckDB's "table function has N columns but M names were given".
  n_col <- nrow(DBI::dbGetQuery(con, glue::glue("DESCRIBE ({select_sql})")))
  if (!n_col %in% c(15L, 16L))
    stop("append_sample(): `select_sql` must yield 15 columns (the base contract) ",
         "or 16 (with `data_stage` trailing); got ", n_col, ".", call. = FALSE)
  has_stage <- n_col == 16L
  if (has_stage) src_cols <- c(src_cols, "data_stage")
  stage_sel <- if (has_stage) "data_stage" else "NULL::VARCHAR AS data_stage"
  src_alias <- paste(src_cols, collapse = ", ")

  # NaN/Inf coordinates are normalised to NULL before anything is minted from
  # them. `NaN` is not `NULL`: it survives an IS NOT NULL check, so it passed
  # validation and reached the release, where ST_Point(NaN, NaN) produced a real
  # non-NULL GEOMETRY — meaning `WHERE geom IS NOT NULL` did not filter it either,
  # and any consumer doing a spatial join silently carried a point that is
  # nowhere. It also poisons aggregates: one NaN makes MAX(longitude) NaN for the
  # whole column, which is how this was found. v2026.08.02 shipped 1,590 such rows
  # (swfsc_cufes 1,583, calcofi_mets 7), all sample_type = 'underway'.
  #
  # Normalising here rather than in each ingest fixes every dataset at once and
  # puts the guard where the geometry is created. It is reported, not silent — a
  # coordinate quietly becoming NULL is its own kind of surprise.
  n_bad <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM ( {select_sql} ) AS src({src_alias})
      WHERE isnan(latitude) OR isnan(longitude)
         OR isinf(latitude) OR isinf(longitude)"))$n
  if (n_bad > 0)
    message(glue::glue(
      "append_sample(): {n_bad} row(s) had a non-finite coordinate ",
      "(NaN/Inf) — normalised to NULL, so no geometry is minted for them"))

  DBI::dbExecute(con, glue::glue(
    "INSERT INTO {sample_tbl}
       (sample_key, sample_type, parent_sample_key, root_sample_key,
        dataset_key, grid_key, site_key, cruise_key, order_occ, latitude, longitude, datetime,
        depth_min_m, depth_max_m, tow_type, data_stage, geom)
     WITH src AS (SELECT * FROM ( {select_sql} ) AS s({src_alias})),
          fin AS (
            SELECT * REPLACE (
              CASE WHEN isnan(latitude)  OR isinf(latitude)  THEN NULL
                   ELSE latitude  END AS latitude,
              CASE WHEN isnan(longitude) OR isinf(longitude) THEN NULL
                   ELSE longitude END AS longitude)
            FROM src)
     SELECT sample_key, sample_type, parent_sample_key, root_sample_key,
            dataset_key, grid_key, site_key, cruise_key, order_occ, latitude, longitude, datetime,
            depth_min_m, depth_max_m, tow_type, {stage_sel},
            CASE WHEN latitude IS NULL OR longitude IS NULL THEN NULL
                 ELSE ST_Point(longitude, latitude) END AS geom
     FROM fin"))
  invisible(DBI::dbGetQuery(
    con, glue::glue("SELECT COUNT(*) AS n FROM {sample_tbl}"))$n)
}

# build_grid_reference ---------------------------------------------------------

#' Build the shared `grid` reference table (deterministic, dataset-independent)
#'
#' Materializes the CalCOFI station grid from `calcofi4r::cc_grid` +
#' `calcofi4r::cc_grid_ctrs` — the exact build previously embedded in
#' `ingest_swfsc_ichthyo.qmd` (`mk_grid_v2` + `grid_to_db`). Because it is a pure
#' deterministic function of the bundled `cc_grid`/`cc_grid_ctrs`, `grid_key` values
#' are byte-identical wherever it runs, so promoting the build out of the ichthyo
#' ingest into a shared reference is non-destructive. Requires the DuckDB connection
#' to allow native GEOMETRY (open via [get_duckdb_con()], which sets
#' `storage_compatibility_version = 'latest'`).
#'
#' @param con a DuckDB connection
#' @param grid_tbl target table name (default `"grid"`)
#' @return (invisibly) the row count of the created `grid` table
#' @export
#' @concept model
build_grid_reference <- function(con, grid_tbl = "grid") {
  for (pkg in c("calcofi4r", "sf", "units", "dplyr", "tidyr"))
    if (!requireNamespace(pkg, quietly = TRUE))
      stop("build_grid_reference() requires the '", pkg, "' package.", call. = FALSE)
  .load_spatial(con)  # ST_GeomFromHEXWKB / native GEOMETRY

  cc_grid_v2 <- calcofi4r::cc_grid |>
    dplyr::rename(dplyr::any_of(c(site_key = "sta_key"))) |>
    dplyr::select(
      "site_key",
      shore   = "sta_shore",
      pattern = "sta_pattern",
      spacing = "sta_dpos") |>
    tidyr::separate_wider_delim(
      "site_key", ",", names = c("line", "station"), cols_remove = FALSE) |>
    dplyr::mutate(
      line     = as.double(.data$line),
      station  = as.double(.data$station),
      grid_key = ifelse(
        .data$pattern == "historical",
        glue::glue("st{station}-ln{line}_hist"),
        glue::glue("st{station}-ln{line}")),
      zone     = glue::glue("{shore}-{pattern}")) |>
    dplyr::relocate("grid_key", "station") |>
    sf::st_as_sf() |>
    dplyr::mutate(
      area_km2 = as.numeric(units::set_units(sf::st_area(.data$geom), "km^2")))

  cc_grid_ctrs_v2 <- calcofi4r::cc_grid_ctrs |>
    dplyr::rename(dplyr::any_of(c(site_key = "sta_key"))) |>
    dplyr::select("site_key", pattern = "sta_pattern") |>
    dplyr::left_join(sf::st_drop_geometry(cc_grid_v2), by = c("site_key", "pattern")) |>
    dplyr::select(-"site_key") |>
    dplyr::relocate("grid_key")

  cc_grid_v2 <- dplyr::select(cc_grid_v2, -"site_key")

  grid <- cc_grid_v2 |>
    as.data.frame() |>
    dplyr::left_join(
      as.data.frame(cc_grid_ctrs_v2) |> dplyr::select("grid_key", geom_ctr = "geom"),
      by = "grid_key") |>
    sf::st_as_sf(sf_column_name = "geom")

  grid_df <- grid |>
    dplyr::mutate(
      geom_wkb     = sf::st_as_binary(.data$geom, hex = TRUE),
      geom_ctr_wkb = sf::st_as_binary(.data$geom_ctr, hex = TRUE)) |>
    sf::st_drop_geometry() |>
    dplyr::select(-"geom_ctr")

  DBI::dbWriteTable(con, grid_tbl, grid_df, overwrite = TRUE)
  # WKB -> native GEOMETRY (fresh, untagged columns: safe UPDATE, unlike the
  # CRS-tagged geom checkpoint bug)
  for (g in c("geom", "geom_ctr")) {
    DBI::dbExecute(con, glue::glue("ALTER TABLE {grid_tbl} ADD COLUMN IF NOT EXISTS {g} GEOMETRY"))
    DBI::dbExecute(con, glue::glue("UPDATE {grid_tbl} SET {g} = ST_GeomFromHEXWKB({g}_wkb)"))
    DBI::dbExecute(con, glue::glue("ALTER TABLE {grid_tbl} DROP COLUMN {g}_wkb"))
  }
  n <- DBI::dbGetQuery(con, glue::glue("SELECT COUNT(*) AS n FROM {grid_tbl}"))$n
  invisible(n)
}

# sample_arm_self --------------------------------------------------------------

#' Build a `sample` arm for a single self-contained event table
#'
#' The declarative shape most datasets need: one `sample` row per row of one event
#' table, keyed `dataset_key:sample_type:id`, with no parent. Exported because a
#' dataset's projection belongs in its own ingest notebook — this is what keeps that
#' a one-line declaration rather than copied SQL.
#'
#' @param dataset_key provider_dataset
#' @param tbl the event table
#' @param id_col its id column
#' @param sample_type core `sample_type` value
#' @param dt_col datetime column, or `"NULL"` for none
#' @param grid_expr,site_expr,ord_expr,depth_min,depth_max SQL expressions or bare
#'   column names (bare names are alias-qualified for you)
#' @return a SQL SELECT string for [append_sample()]
#' @export
#' @concept model
sample_arm_self <- function(dataset_key, tbl, id_col, sample_type,
                             dt_col = "datetime_start_utc",
                             grid_expr = "grid_key",
                             site_expr = "NULL::VARCHAR",
                             ord_expr  = "NULL::INTEGER",
                             depth_min = "0::DOUBLE", depth_max = "0::DOUBLE") {
  key <- ns_key(dataset_key, sample_type, id_col)
  # qualify bare column references with the table alias: DuckDB resolves an
  # unqualified `site_key AS site_key` against the alias being defined in the
  # same SELECT (lateral column alias) and errors rather than reading the column.
  # Applies to every caller-supplied expression, since `depth_min_m AS
  # depth_min_m` and `datetime AS datetime` hit the same trap.
  q <- function(expr) if (grepl("^[A-Za-z_][A-Za-z0-9_]*$", expr))
    paste0("_src.", expr) else expr
  dt <- if (identical(dt_col, "NULL")) "NULL::TIMESTAMP" else
    glue::glue("CAST({q(dt_col)} AS TIMESTAMP)")
  glue::glue(
    "SELECT {key} AS sample_key, '{sample_type}' AS sample_type,
            NULL::VARCHAR AS parent_sample_key, {key} AS root_sample_key,
            '{dataset_key}' AS dataset_key, {q(grid_expr)} AS grid_key,
            {q(site_expr)} AS site_key, _src.cruise_key, {q(ord_expr)} AS order_occ,
            _src.latitude, _src.longitude, {dt} AS datetime,
            {q(depth_min)} AS depth_min_m, {q(depth_max)} AS depth_max_m,
            NULL::VARCHAR AS tow_type
     FROM {tbl} AS _src")
}

# `_measurement_taxon`: the composite-decomposition registry the cufes /
# phyllosoma arms INNER JOIN to split a taxon-bearing measurement_type name
# ("sardine_eggs", "phyllosoma_stage_3") into (taxon_key, canonical type,
# life_stage, bin_value). Restricted to `dataset_key` so an ingest never emits
# another dataset's rows. Always created — an absent registry yields an empty
# table (arms project zero rows) rather than a catalog error.
#' Stage the `_measurement_taxon` crosswalk in a connection
#'
#' Materializes `metadata/measurement_taxon.csv` as the `_measurement_taxon` table
#' an `obs`/`obs_attribute` projection INNER JOINs to split a taxon-bearing
#' measurement_type name (`sardine_eggs`, `phyllosoma_stage_3`) into
#' `(taxon_key, canonical type, life_stage, bin_value)`.
#'
#' Exported because a dataset's projection lives in its own ingest notebook, and
#' the derived `taxon_key` is the part you must not hand-roll: it is
#' [taxon_key_of()] over `worms_id`/`itis_id`, so a `'worms:' || worms_id` string
#' built inline silently mis-keys any ITIS-resolved taxon.
#'
#' @param con a DuckDB connection
#' @param measurement_taxon the crosswalk data.frame (or NULL for an empty table)
#' @param dataset_key restrict to this dataset, so an ingest never stages another
#'   dataset's rows
#' @param tbl target table name
#' @return (invisibly) `tbl`
#' @export
#' @concept model
ensure_measurement_taxon <- function(con, measurement_taxon = NULL,
                                      dataset_key = NULL, tbl = "_measurement_taxon") {
  cols <- c("dataset_key", "raw_measurement_type", "target", "measurement_type",
            "taxon_scientific_name", "worms_id", "itis_id", "life_stage", "bin_value")
  mt <- measurement_taxon
  if (is.null(mt) || !nrow(mt)) {
    mt <- data.frame(
      dataset_key = character(), raw_measurement_type = character(),
      target = character(), measurement_type = character(),
      taxon_scientific_name = character(), worms_id = integer(),
      itis_id = integer(), life_stage = character(), bin_value = double(),
      stringsAsFactors = FALSE)
  } else {
    for (cl in setdiff(cols, names(mt))) mt[[cl]] <- NA
    mt <- mt[, cols, drop = FALSE]
    if (!is.null(dataset_key))
      mt <- mt[mt$dataset_key %in% dataset_key, , drop = FALSE]
  }
  # composites are non-bird, so worms: wins where an AphiaID resolves, itis: else.
  # guard the empty case: taxon_key_of() recycles to the length of its longest
  # argument, and the scalar `is_bird` would make a 1-row key for a 0-row frame.
  mt$taxon_key <- if (nrow(mt)) taxon_key_of(mt$worms_id, mt$itis_id, FALSE) else character()
  .replace_table(con, tbl, as.data.frame(mt))
  invisible(tbl)
}

# core output tables ----------------------------------------------------------

#' Core tables an ingest writes to parquet
#'
#' The core shard set, filtered to those actually present and non-empty in `con`.
#' Use it to drive `write_parquet_outputs()` so a dataset with no `obs_attribute`
#' (most of them) does not emit an empty file.
#'
#' @param con a DuckDB connection after the notebook's core projection has run
#' @param extra additional table names to append (shared refs the ingest owns,
#'   e.g. `c("grid", "cruise", "ship", "lookup")` for `swfsc_ichthyo`)
#' @return character vector of table names, core family first
#' @export
#' @concept model
core_output_tables <- function(con, extra = NULL) {
  core <- c("sample", "obs", "obs_attribute", "sample_measurement",
            "taxon", "dataset_taxon", "taxon_group")
  present <- DBI::dbListTables(con)
  keep <- vapply(intersect(core, present), function(t)
    DBI::dbGetQuery(con, glue::glue("SELECT COUNT(*) AS n FROM {t}"))$n > 0,
    logical(1))
  c(names(keep)[keep], intersect(extra, present))
}

# compat VIEWs ----------------------------------------------------------------

#' Rebuild a per-dataset event table as a VIEW over the core `sample`
#'
#' The source id is recovered from the namespaced `sample_key`
#' (`'<dataset_key>:<sample_type>:<id>'` -> field 3), the containment FK from
#' `parent_sample_key`, and the event-level effort columns by pivoting
#' `sample_measurement` back out of long form.
#'
#' Exported because each dataset's compat VIEWs are declared in its own ingest
#' notebook — this is the reusable *shape*, not a per-dataset projection.
#'
#' @param dataset_key provider_dataset the rows carry
#' @param sample_type the `sample_type` the rows carry (`site`, `tow`, `net`, …)
#' @param id_col name for the recovered source id column
#' @param parent_col optional name for the recovered parent FK column
#' @param cols named character vector `c(<out name> = <sample column>)` of
#'   straight passthrough columns
#' @param measures named character vector `c(<out name> = <measurement_type>)` of
#'   effort columns to pivot back out of `sample_measurement`
#' @param sample_tbl name of the core `sample` table to read
#' @return a SQL SELECT string
#' @export
#' @concept model
#' @examples
#' compat_event_sql("swfsc_ichthyo", "tow", "tow_uuid", "site_uuid",
#'                  c(tow_type_key = "tow_type", datetime_start_utc = "datetime"))
compat_event_sql <- function(dataset_key, sample_type, id_col, parent_col = NULL,
                             cols = character(), measures = character(),
                             sample_tbl = "sample") {
  sel <- c(
    glue::glue("split_part(s.sample_key, ':', 3) AS {id_col}"),
    if (!is.null(parent_col))
      glue::glue("split_part(s.parent_sample_key, ':', 3) AS {parent_col}"),
    unname(vapply(seq_along(cols), function(i)
      glue::glue("s.{cols[[i]]} AS {names(cols)[i]}"), "")),
    unname(vapply(seq_along(measures), function(i) glue::glue(
      "MAX(m.measurement_value) FILTER (WHERE m.measurement_type = '{measures[[i]]}')",
      " AS {names(measures)[i]}"), "")))
  grp <- seq_len(length(sel) - length(measures))
  glue::glue(
    "SELECT {paste(sel, collapse = ',\n         ')}
     FROM {if (length(measures)) paste0(sample_tbl, ' s LEFT JOIN sample_measurement m USING (sample_key)') else paste0(sample_tbl, ' s')}
     WHERE s.dataset_key = '{dataset_key}' AND s.sample_type = '{sample_type}'",
    if (length(measures)) glue::glue("\n     GROUP BY {paste(grp, collapse = ', ')}") else "")
}

# per-dataset long measurement table, rebuilt from obs
#' Rebuild a per-dataset measurement table as a VIEW over `obs`
#'
#' @param dataset_key provider_dataset
#' @param sample_type the `sample_type` its rows carry
#' @param fk_col name for the recovered event FK column
#' @param id_col name for the recovered measurement id column
#' @return a SQL SELECT string
#' @export
#' @concept model
compat_measurement_sql <- function(dataset_key, sample_type, fk_col, id_col) {
  glue::glue(
    "SELECT obs_id AS {id_col},
            split_part(sample_key, ':', 3) AS {fk_col},
            measurement_type, measurement_value, measurement_qual
     FROM obs WHERE dataset_key = '{dataset_key}'")
}

#' PK/FK spec for the consolidated core tables
#'
#' Every ingest now emits the same core shape, so every ingest declares the same
#' relationships. This returns that one spec in [build_relationships_json()]'s
#' `rels` form, restricted to the tables actually present in `tables` — so an
#' ingest that emits no `obs_attribute` does not advertise an edge to it.
#'
#' @param tables character vector of tables the ingest writes (typically the
#'   result of [core_output_tables()])
#' @return a list with `primary_keys` and `foreign_keys`
#' @export
#' @concept model
core_relationships <- function(tables) {
  pk <- list(
    sample             = "sample_key",
    obs                = "obs_id",
    obs_attribute      = "obs_attribute_id",
    sample_measurement = "sample_measurement_id",
    taxon              = "taxon_key",
    dataset_taxon      = "ds_taxon_key",
    grid               = "grid_key",
    cruise             = "cruise_key",
    ship               = "ship_key",
    measurement_type   = "measurement_type",
    region             = "region_key")
  fk <- list(
    list(table = "sample",             column = "parent_sample_key", ref_table = "sample",           ref_column = "sample_key"),
    list(table = "sample",             column = "root_sample_key",   ref_table = "sample",           ref_column = "sample_key"),
    list(table = "sample",             column = "grid_key",          ref_table = "grid",             ref_column = "grid_key"),
    list(table = "sample",             column = "cruise_key",        ref_table = "cruise",           ref_column = "cruise_key"),
    list(table = "obs",                column = "sample_key",        ref_table = "sample",           ref_column = "sample_key"),
    list(table = "obs",                column = "taxon_key",         ref_table = "taxon",            ref_column = "taxon_key"),
    list(table = "obs",                column = "grid_key",          ref_table = "grid",             ref_column = "grid_key"),
    list(table = "obs",                column = "cruise_key",        ref_table = "cruise",           ref_column = "cruise_key"),
    list(table = "obs",                column = "measurement_type",  ref_table = "measurement_type", ref_column = "measurement_type"),
    list(table = "obs_attribute",      column = "sample_key",        ref_table = "sample",           ref_column = "sample_key"),
    list(table = "obs_attribute",      column = "taxon_key",         ref_table = "taxon",            ref_column = "taxon_key"),
    list(table = "obs_attribute",      column = "measurement_type",  ref_table = "measurement_type", ref_column = "measurement_type"),
    list(table = "sample_measurement", column = "sample_key",        ref_table = "sample",           ref_column = "sample_key"),
    list(table = "sample_measurement", column = "measurement_type",  ref_table = "measurement_type", ref_column = "measurement_type"),
    list(table = "dataset_taxon",      column = "taxon_key",         ref_table = "taxon",            ref_column = "taxon_key"),
    list(table = "taxon_group",        column = "taxon_key",         ref_table = "taxon",            ref_column = "taxon_key"),
    list(table = "taxon",              column = "parent_taxon_key",  ref_table = "taxon",            ref_column = "taxon_key"))

  list(
    primary_keys = pk[intersect(names(pk), tables)],
    # keep an edge only when BOTH ends ship from this ingest; cross-ingest edges
    # (obs.grid_key -> grid when grid comes from ichthyo) belong in
    # metadata/relationships_cross.csv, which the release merges.
    foreign_keys = unname(Filter(
      function(e) e$table %in% tables && e$ref_table %in% tables, fk)))
}
