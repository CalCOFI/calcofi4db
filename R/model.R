# core consolidated data model -------------------------------------------------
# materialize + append the small "core" fact/dimension family that every
# cross-dataset consumer reads, replacing the ~40 per-dataset triples:
#   - sample              : event dimension (adjacency list: leaf/parent/root)
#   - obs                 : occurrence-headline long table (realm env|bio)
#   - obs_attribute       : sub-occurrence attribution (length/stage freq, behavior)
#   - sample_measurement  : event-level (effort) long table
#   - obs_ctd_full        : supplemental full-resolution CTD (same shape as obs)
# plus the shared reference builders build_grid_reference() / build_sample_reference().
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

.has_tables <- function(con, ...) {
  tbls <- DBI::dbGetQuery(
    con, "SELECT table_name FROM information_schema.tables")$table_name
  all(c(...) %in% tbls)
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
       geom              GEOMETRY)"))
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
#' `select_sql` must yield `sample_key`, `sample_type`, `parent_sample_key`,
#' `root_sample_key`, `dataset_key`, `grid_key`, `cruise_key`, `latitude`,
#' `longitude`, `datetime`, `depth_min_m`, `depth_max_m`, `tow_type` by name;
#' `geom` is minted here as `ST_Point(longitude, latitude)`. `tow_type` is the net
#' gear code (ichthyo tow/net grains: C1/CB/CV/PV oblique/vertical, MT manta), NULL
#' for gears/datasets without one. Prefer [build_sample_reference()] for the central
#' Phase-2 build; use this for per-dataset (Phase 3) appends.
#' @inheritParams append_obs
#' @param sample_tbl target table (default `"sample"`)
#' @return (invisibly) the total row count of `sample_tbl` after the append
#' @export
#' @concept model
append_sample <- function(con, select_sql, sample_tbl = "sample") {
  .load_spatial(con)
  .ensure_sample_schema(con, sample_tbl)
  DBI::dbExecute(con, glue::glue(
    "INSERT INTO {sample_tbl}
       (sample_key, sample_type, parent_sample_key, root_sample_key,
        dataset_key, grid_key, site_key, cruise_key, order_occ, latitude, longitude, datetime,
        depth_min_m, depth_max_m, tow_type, geom)
     SELECT sample_key, sample_type, parent_sample_key, root_sample_key,
            dataset_key, grid_key, site_key, cruise_key, order_occ, latitude, longitude, datetime,
            depth_min_m, depth_max_m, tow_type,
            CASE WHEN latitude IS NULL OR longitude IS NULL THEN NULL
                 ELSE ST_Point(longitude, latitude) END AS geom
     FROM ( {select_sql} ) AS src(sample_key, sample_type, parent_sample_key, root_sample_key,
            dataset_key, grid_key, site_key, cruise_key, order_occ, latitude, longitude, datetime,
            depth_min_m, depth_max_m, tow_type)"))
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

# build_sample_reference -------------------------------------------------------

# private: single-level (leaf = root = self) sample arm generator
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

#' Build the shared `sample` event dimension from the per-dataset event tables
#'
#' Materializes the adjacency-list `sample` dimension (one row per physical
#' sampling event, at its native grain) from whichever per-dataset event tables are
#' present in `con` — subsuming `site`/`tow`/`net`/`casts`/`ctd_cast`/`dic_sample`/
#' `cufes_sample`/`*_tow`/`*_sample`/`bird_mammal_transect`/`phyto_sample` into one
#' table. Every `sample_key` is namespaced `"<dataset_key>:<sample_type>:<id>"`;
#' `parent_sample_key`/`root_sample_key` encode the `site->tow->net` and
#' `cast->bottle` hierarchies (a flat adjacency list with no attribute inheritance).
#' `geom` is minted from `latitude`/`longitude`. Only arms whose source tables exist
#' are included. Errors if the resulting `sample_key` is not unique.
#'
#' @param con a DuckDB connection with the per-dataset event tables loaded
#' @param sample_tbl target table name (default `"sample"`)
#' @param datasets optional character vector of `dataset_key`s to restrict which
#'   arms build (default `NULL` = every dataset whose event tables are present).
#'   Use in an ingest that has other datasets' event tables loaded as references
#'   so only this dataset's `sample` rows are built.
#' @return (invisibly) the row count of the built `sample` table
#' @export
#' @concept model
build_sample_reference <- function(con, sample_tbl = "sample", datasets = NULL) {
  .load_spatial(con)
  has <- function(...) .has_tables(con, ...)

  # DIC natural key (columns shared with dic_measurement so obs aligns)
  dic_md5 <- "md5(concat_ws('|', d.expocode, CAST(d.datetime_start_utc AS VARCHAR),
                 CAST(d.latitude AS VARCHAR), CAST(d.longitude AS VARCHAR),
                 CAST(d.depth_m AS VARCHAR)))"

  arms <- list(
    # --- calcofi_bottle: cast (root) + bottle (leaf) -------------------------
    bottle_cast = if (has("casts")) glue::glue(
      "SELECT {ns_key('calcofi_bottle','cast','cast_id')} AS sample_key, 'cast' AS sample_type,
              NULL::VARCHAR AS parent_sample_key,
              {ns_key('calcofi_bottle','cast','cast_id')} AS root_sample_key,
              'calcofi_bottle' AS dataset_key, grid_key, site_key, cruise_key,
              CAST(order_occ AS INTEGER) AS order_occ,
              latitude, longitude, CAST(datetime_start_utc AS TIMESTAMP) AS datetime,
              NULL::DOUBLE AS depth_min_m, NULL::DOUBLE AS depth_max_m,
              NULL::VARCHAR AS tow_type
       FROM casts"),
    bottle_btl = if (has("bottle", "casts")) glue::glue(
      "SELECT {ns_key('calcofi_bottle','bottle','b.bottle_id')} AS sample_key, 'bottle' AS sample_type,
              {ns_key('calcofi_bottle','cast','b.cast_id')} AS parent_sample_key,
              {ns_key('calcofi_bottle','cast','b.cast_id')} AS root_sample_key,
              'calcofi_bottle' AS dataset_key, c.grid_key, b.site_key, c.cruise_key,
              CAST(c.order_occ AS INTEGER) AS order_occ, c.latitude, c.longitude,
              CAST(c.datetime_start_utc AS TIMESTAMP) AS datetime, b.depth_m AS depth_min_m, b.depth_m AS depth_max_m,
              NULL::VARCHAR AS tow_type
       FROM bottle b JOIN casts c USING (cast_id)"),

    # --- calcofi_ctd-cast: physical cast (leaf = root). ctd_cast is per-SCAN
    # (5.5M rows / ctd_cast_uuid); the physical cast is cast_key (~14k, globally
    # unique). Dedup to one sample row per cast_key; obs joins ctd_thin->ctd_cast
    # to map each scan's ctd_cast_uuid to its cast_key. ------------------------
    ctd = if (has("ctd_cast")) glue::glue(
      "SELECT * FROM (
         SELECT {ns_key('calcofi_ctd-cast','cast','cast_key')} AS sample_key, 'cast' AS sample_type,
                NULL::VARCHAR AS parent_sample_key,
                {ns_key('calcofi_ctd-cast','cast','cast_key')} AS root_sample_key,
                'calcofi_ctd-cast' AS dataset_key, grid_key, site_key, cruise_key,
                TRY_CAST(ord_occ AS INTEGER) AS order_occ, latitude, longitude,
                CAST(datetime_start_utc AS TIMESTAMP) AS datetime,
                NULL::DOUBLE AS depth_min_m, NULL::DOUBLE AS depth_max_m,
                NULL::VARCHAR AS tow_type
         FROM ctd_cast
       ) q QUALIFY row_number() OVER (PARTITION BY sample_key ORDER BY datetime) = 1"),

    # --- calcofi_dic: bottle-shared leaf; mint only the non-bottle events ----
    dic = if (has("dic_sample", "casts")) {
      btl_filter <- if (has("bottle"))
        "WHERE d.bottle_id IS NULL OR d.bottle_id NOT IN (SELECT bottle_id FROM bottle)" else ""
      glue::glue(
        "SELECT * FROM (
           SELECT 'calcofi_dic:bottle:' || {dic_md5} AS sample_key, 'bottle' AS sample_type,
                  CASE WHEN c.cast_id IS NULL THEN NULL
                       ELSE 'calcofi_bottle:cast:' || CAST(c.cast_id AS VARCHAR) END AS parent_sample_key,
                  COALESCE(
                    CASE WHEN c.cast_id IS NULL THEN NULL
                         ELSE 'calcofi_bottle:cast:' || CAST(c.cast_id AS VARCHAR) END,
                    'calcofi_dic:bottle:' || {dic_md5}) AS root_sample_key,
                  'calcofi_dic' AS dataset_key, c.grid_key, d.site_key, c.cruise_key,
                  CAST(c.order_occ AS INTEGER) AS order_occ,
                  d.latitude, d.longitude, CAST(d.datetime_start_utc AS TIMESTAMP) AS datetime,
                  d.depth_m AS depth_min_m, d.depth_m AS depth_max_m,
                  NULL::VARCHAR AS tow_type
           FROM dic_sample d LEFT JOIN casts c ON d.cast_id = c.cast_id
           {btl_filter}
         ) q QUALIFY row_number() OVER (PARTITION BY sample_key) = 1")
    },

    # --- swfsc_ichthyo: site (root) + tow (parent) + net (leaf) --------------
    # site has no datetime of its own -> earliest tow time
    ich_site = if (has("site")) glue::glue(
      "SELECT {ns_key('swfsc_ichthyo','site','s.site_uuid')} AS sample_key, 'site' AS sample_type,
              NULL::VARCHAR AS parent_sample_key,
              {ns_key('swfsc_ichthyo','site','s.site_uuid')} AS root_sample_key, 'swfsc_ichthyo' AS dataset_key,
              s.grid_key, s.site_key, s.cruise_key, CAST(s.order_occ AS INTEGER) AS order_occ,
              s.latitude, s.longitude,
              CAST(td.dt AS TIMESTAMP) AS datetime, NULL::DOUBLE AS depth_min_m, NULL::DOUBLE AS depth_max_m,
              NULL::VARCHAR AS tow_type
       FROM site s
       LEFT JOIN (SELECT site_uuid, min(datetime_start_utc) AS dt FROM tow GROUP BY 1) td
              ON td.site_uuid = s.site_uuid"),
    ich_tow = if (has("tow", "site")) glue::glue(
      "SELECT {ns_key('swfsc_ichthyo','tow','t.tow_uuid')} AS sample_key, 'tow' AS sample_type,
              {ns_key('swfsc_ichthyo','site','t.site_uuid')} AS parent_sample_key,
              {ns_key('swfsc_ichthyo','site','t.site_uuid')} AS root_sample_key,
              'swfsc_ichthyo' AS dataset_key, s.grid_key, s.site_key, s.cruise_key,
              CAST(s.order_occ AS INTEGER) AS order_occ, s.latitude, s.longitude,
              CAST(t.datetime_start_utc AS TIMESTAMP) AS datetime, 0::DOUBLE AS depth_min_m, NULL::DOUBLE AS depth_max_m,
              t.tow_type_key AS tow_type
       FROM tow t JOIN site s USING (site_uuid)"),
    ich_net = if (has("net", "tow", "site")) glue::glue(
      "SELECT {ns_key('swfsc_ichthyo','net','n.net_uuid')} AS sample_key, 'net' AS sample_type,
              {ns_key('swfsc_ichthyo','tow','n.tow_uuid')} AS parent_sample_key,
              {ns_key('swfsc_ichthyo','site','t.site_uuid')} AS root_sample_key,
              'swfsc_ichthyo' AS dataset_key, s.grid_key, s.site_key, s.cruise_key,
              CAST(s.order_occ AS INTEGER) AS order_occ, s.latitude, s.longitude,
              CAST(t.datetime_start_utc AS TIMESTAMP) AS datetime, 0::DOUBLE AS depth_min_m, NULL::DOUBLE AS depth_max_m,
              t.tow_type_key AS tow_type
       FROM net n JOIN tow t USING (tow_uuid) JOIN site s USING (site_uuid)"),

    # --- single-level datasets (leaf = root = self) --------------------------
    cufes = if (has("cufes_sample"))
      sample_arm_self("swfsc_cufes", "cufes_sample", "sample_id", "underway"),
    euph = if (has("euphausiids_tow"))
      sample_arm_self("cce-lter_euphausiids", "euphausiids_tow", "tow_id", "tow",
                       site_expr = "site_key",
                       depth_min = "NULL::DOUBLE", depth_max = "NULL::DOUBLE"),
    phyllosoma = if (has("phyllosoma_tow"))
      sample_arm_self("calcofi_phyllosoma", "phyllosoma_tow", "tow_id", "tow",
                       site_expr = "site_key",
                       depth_min = "0::DOUBLE", depth_max = "max_tow_depth_m"),
    zoodb = if (has("zoodb_sample"))
      sample_arm_self("cce-lter_zoodb", "zoodb_sample", "sample_id", "tow",
                       site_expr = "site_key",
                       depth_min = "min_depth_m", depth_max = "max_depth_m"),
    zooscan = if (has("zooscan_sample"))
      sample_arm_self("cce-lter_zooscan", "zooscan_sample", "sample_id", "tow",
                       dt_col = "station_date", site_expr = "site_key",
                       depth_min = "min_depth_m", depth_max = "max_depth_m"),
    bird = if (has("bird_mammal_transect"))
      sample_arm_self("calcofi_bird_mammal_census", "bird_mammal_transect",
                       "gis_key", "transect"),
    pic = if (has("zooplankton_tow"))
      sample_arm_self("pic_zooplankton", "zooplankton_tow", "tow_id", "tow",
                       site_expr = "site_key", ord_expr = "CAST(order_occ AS INTEGER)",
                       depth_min = "depth_min_m", depth_max = "depth_max_m"),
    # METS underway: one row per retained track sample. Restricted to samples
    # mets_thin references so `sample` stays proportionate to `obs` — the full
    # ~1-minute series is a supplemental output, not a core event dimension.
    mets = if (has("mets_sample", "mets_thin")) glue::glue(
      "SELECT {ns_key('calcofi_mets','underway','s.mets_sample_uuid')} AS sample_key,
              'underway' AS sample_type,
              NULL::VARCHAR AS parent_sample_key,
              {ns_key('calcofi_mets','underway','s.mets_sample_uuid')} AS root_sample_key,
              'calcofi_mets' AS dataset_key, s.grid_key, NULL::VARCHAR AS site_key, s.cruise_key,
              NULL::INTEGER AS order_occ, s.latitude, s.longitude,
              CAST(s.datetime_start_utc AS TIMESTAMP) AS datetime,
              0::DOUBLE AS depth_min_m, 0::DOUBLE AS depth_max_m,
              NULL::VARCHAR AS tow_type
       FROM mets_sample s
       WHERE EXISTS (SELECT 1 FROM mets_thin t
                     WHERE t.mets_sample_uuid = s.mets_sample_uuid)"),
    meso = if (has("mesopelagic_fish_tow"))
      sample_arm_self("ucsd_sio_mesopelagic-fish", "mesopelagic_fish_tow",
                       "tow_id", "tow", site_expr = "site_key",
                       depth_min = "0::DOUBLE", depth_max = "depth_m"),
    # picoplankton is bottle-shaped (one row per bottle/depth on a CTD cast) but
    # the export carries no cast-level event table, so the bottle is its own root
    pico = if (has("picoplankton_bacteria_bottle"))
      sample_arm_self("cce-lter_picoplankton-bacteria",
                       "picoplankton_bacteria_bottle", "bottle_id", "bottle",
                       dt_col = "datetime_utc", site_expr = "site_key",
                       depth_min = "depth_m", depth_max = "depth_m"),
    phyto = if (has("phyto_sample"))
      sample_arm_self("calcofi_phytoplankton", "phyto_sample", "phyto_sample_id",
                       "region_pool", dt_col = "NULL", grid_expr = "NULL::VARCHAR"))

  arms <- Filter(Negate(is.null), arms)
  # restrict to the requested datasets (arm name -> dataset_key)
  if (!is.null(datasets)) {
    arm_ds <- c(bottle_cast = "calcofi_bottle", bottle_btl = "calcofi_bottle",
                ctd = "calcofi_ctd-cast", dic = "calcofi_dic",
                ich_site = "swfsc_ichthyo", ich_tow = "swfsc_ichthyo", ich_net = "swfsc_ichthyo",
                cufes = "swfsc_cufes", euph = "cce-lter_euphausiids",
                phyllosoma = "calcofi_phyllosoma", zoodb = "cce-lter_zoodb",
                zooscan = "cce-lter_zooscan", bird = "calcofi_bird_mammal_census",
                pic = "pic_zooplankton", phyto = "calcofi_phytoplankton",
                mets = "calcofi_mets",
                meso = "ucsd_sio_mesopelagic-fish",
                pico = "cce-lter_picoplankton-bacteria")
    arms <- arms[names(arms) %in% names(arm_ds)[arm_ds %in% datasets]]
  }
  if (!length(arms)) stop("build_sample_reference(): no source event tables found.")

  DBI::dbExecute(con, glue::glue("DROP TABLE IF EXISTS {sample_tbl}"))
  DBI::dbExecute(con, glue::glue("DROP VIEW IF EXISTS {sample_tbl}"))
  append_sample(con, paste(arms, collapse = "\nUNION ALL\n"), sample_tbl = sample_tbl)

  # global sample_key uniqueness (namespacing bug guard)
  dup <- DBI::dbGetQuery(con, glue::glue(
    "SELECT sample_key, COUNT(*) n FROM {sample_tbl}
     GROUP BY 1 HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT 5"))
  if (nrow(dup))
    stop("build_sample_reference(): duplicate sample_key(s), e.g. ",
         paste(sprintf("%s (x%d)", dup$sample_key, dup$n), collapse = "; "))

  n <- DBI::dbGetQuery(con, glue::glue("SELECT COUNT(*) AS n FROM {sample_tbl}"))$n
  invisible(n)
}

# emit_core_tables ------------------------------------------------------------

# the bio obs arms LEFT JOIN dataset_taxon. Normally each ingest builds its own
# slice (see .build_taxa_slices()); this stub only covers the case where a caller
# projects a dataset whose taxon vocabulary is not present, so the LEFT JOIN
# resolves to NULL instead of raising a catalog error.
.ensure_dataset_taxon <- function(con) {
  if ("dataset_taxon" %in% DBI::dbListTables(con)) return(invisible(FALSE))
  DBI::dbExecute(con, "
    CREATE TABLE dataset_taxon (
      ds_taxon_key       VARCHAR,
      dataset_key        VARCHAR,
      taxon_key          VARCHAR,
      ds_scientific_name VARCHAR,
      ds_common_name     VARCHAR,
      ds_taxa_code       VARCHAR)")
  invisible(TRUE)
}

# `_measurement_taxon`: the composite-decomposition registry the cufes /
# phyllosoma arms INNER JOIN to split a taxon-bearing measurement_type name
# ("sardine_eggs", "phyllosoma_stage_3") into (taxon_key, canonical type,
# life_stage, bin_value). Restricted to `dataset_key` so an ingest never emits
# another dataset's rows. Always created — an absent registry yields an empty
# table (arms project zero rows) rather than a catalog error.
.ensure_measurement_taxon <- function(con, measurement_taxon = NULL,
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

# build this dataset's slice of the shared taxa references. The builders read
# whichever per-dataset taxon source tables are present in `con`, so inside an
# ingest they naturally yield just that dataset's vocabulary; `dataset_taxon` is
# then hard-filtered because an ingest may have loaded another dataset's tables
# as references (e.g. dic loads bottle/casts, several load ichthyo's site/tow).
# The release unions these shards and re-coalesces them (merge_taxon_shards()).
.build_taxa_slices <- function(con, dataset_key, measurement_taxon = NULL,
                               overrides = NULL) {
  out <- list()
  ok <- tryCatch({
    out$taxon         <- build_taxon_reference(con, measurement_taxon, overrides)
    out$dataset_taxon <- build_dataset_taxon(con,   measurement_taxon, overrides)
    out$taxon_group   <- build_taxon_group(con,     measurement_taxon, overrides)
    TRUE
  }, error = function(e) {
    # no taxon source tables at all (env-only datasets: bottle, ctd, dic, mets,
    # picoplankton) -> stub dataset_taxon and carry on
    message("emit_core_tables(): no taxon sources for ", dataset_key,
            " (", conditionMessage(e), "); emitting env-only core")
    FALSE
  })
  if (!ok) { .ensure_dataset_taxon(con); return(out) }

  DBI::dbExecute(con, glue::glue(
    "DELETE FROM dataset_taxon WHERE dataset_key <> '{dataset_key}'"))
  # keep only the taxon rows this dataset's vocabulary actually reaches, plus
  # their lineage ancestors (parent chain), so shards stay disjoint-ish and the
  # release union stays small. Ancestors matter: descendant expansion walks
  # parent_taxon_key, so dropping them would break the chain.
  DBI::dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE _tx_keep AS
    WITH RECURSIVE seed AS (
      SELECT taxon_key FROM dataset_taxon WHERE taxon_key IS NOT NULL
    ), chain AS (
      SELECT taxon_key FROM seed
      UNION
      SELECT t.parent_taxon_key FROM taxon t JOIN chain c ON t.taxon_key = c.taxon_key
      WHERE t.parent_taxon_key IS NOT NULL
    ) SELECT DISTINCT taxon_key FROM chain WHERE taxon_key IS NOT NULL")
  DBI::dbExecute(con,
    "DELETE FROM taxon WHERE taxon_key NOT IN (SELECT taxon_key FROM _tx_keep)")
  DBI::dbExecute(con,
    "DELETE FROM taxon_group WHERE taxon_key NOT IN (SELECT taxon_key FROM taxon)")
  DBI::dbExecute(con, "DROP TABLE IF EXISTS _tx_keep")

  for (t in c("taxon", "dataset_taxon", "taxon_group"))
    out[[t]] <- DBI::dbGetQuery(
      con, glue::glue("SELECT COUNT(*) AS n FROM {t}"))$n
  out
}


# per-dataset obs (occurrence-headline) projection SQL. The single source of
# truth for how each dataset maps into `obs`, reused by the release assembly and
# by each ingest's emit_core step. Mirrors the validated release core_tables arms.
.obs_arm_sql <- function(dataset_key) {
  switch(dataset_key,
    "calcofi_bottle" = "
      SELECT 'env' realm, 'calcofi_bottle' dataset_key,
             'calcofi_bottle:bottle:' || CAST(b.bottle_id AS VARCHAR) sample_key,
             c.grid_key, c.cruise_key, c.latitude, c.longitude,
             CAST(c.datetime_start_utc AS TIMESTAMP) datetime, b.depth_m depth_min_m, b.depth_m depth_max_m,
             NULL::VARCHAR taxon_key, NULL::VARCHAR life_stage,
             m.measurement_type, m.measurement_value, m.measurement_qual, m.measurement_prec
      FROM bottle_measurement m JOIN bottle b USING (bottle_id) JOIN casts c USING (cast_id)
      WHERE c.grid_key IS NOT NULL",
    "calcofi_ctd-cast" = "
      SELECT 'env', 'calcofi_ctd-cast',
             'calcofi_ctd-cast:cast:' || CAST(cc.cast_key AS VARCHAR),
             cc.grid_key, cc.cruise_key, cc.latitude, cc.longitude,
             CAST(cc.datetime_start_utc AS TIMESTAMP), t.depth_m, t.depth_m,
             NULL::VARCHAR, NULL::VARCHAR, t.measurement_type, t.measurement_value,
             t.measurement_qual, NULL::DOUBLE
      FROM ctd_thin t JOIN ctd_cast cc ON t.ctd_cast_uuid = cc.ctd_cast_uuid
      WHERE cc.grid_key IS NOT NULL",
    "calcofi_dic" = glue::glue("
      SELECT 'env', 'calcofi_dic',
             CASE WHEN dm.bottle_id IS NOT NULL AND dm.bottle_id IN (SELECT bottle_id FROM bottle)
                  THEN 'calcofi_bottle:bottle:' || CAST(dm.bottle_id AS VARCHAR)
                  ELSE 'calcofi_dic:bottle:' || md5(concat_ws('|', dm.expocode,
                    CAST(dm.datetime_start_utc AS VARCHAR), CAST(dm.latitude AS VARCHAR),
                    CAST(dm.longitude AS VARCHAR), CAST(dm.depth_m AS VARCHAR))) END,
             c.grid_key, c.cruise_key, dm.latitude, dm.longitude,
             CAST(dm.datetime_start_utc AS TIMESTAMP), dm.depth_m, dm.depth_m,
             NULL::VARCHAR, NULL::VARCHAR, dm.measurement_type, dm.measurement_value,
             dm.measurement_qual, NULL::DOUBLE
      FROM dic_measurement dm JOIN casts c USING (cast_id)
      WHERE c.grid_key IS NOT NULL"),
    # bio taxon_key resolves through dataset_taxon (built by build_dataset_taxon):
    # the global "worms:"/"itis:" key, not the dataset-local species_id/taxon_id.
    "swfsc_ichthyo" = "
      SELECT 'bio', 'swfsc_ichthyo', 'swfsc_ichthyo:net:' || CAST(i.net_uuid AS VARCHAR),
             s.grid_key, s.cruise_key, s.latitude, s.longitude,
             CAST(t.datetime_start_utc AS TIMESTAMP), NULL::DOUBLE, NULL::DOUBLE,
             dt.taxon_key, i.life_stage,
             'abundance', CAST(i.tally AS DOUBLE), NULL::VARCHAR, NULL::DOUBLE
      FROM ichthyo i JOIN net n USING (net_uuid) JOIN tow t USING (tow_uuid) JOIN site s USING (site_uuid)
      LEFT JOIN dataset_taxon dt ON dt.dataset_key = 'swfsc_ichthyo'
                                AND dt.ds_taxa_code = CAST(i.species_id AS VARCHAR)
      WHERE i.measurement_type IS NULL AND s.grid_key IS NOT NULL",
    # cufes: the taxon is baked into the type name (sardine_eggs, anchovy_eggs, …).
    # `_measurement_taxon` (target='obs') decomposes it into taxon_key + the
    # canonical type ('abundance') + life_stage ('egg') — an INNER join, so a raw
    # type absent from the registry is dropped rather than silently untaxoned.
    "swfsc_cufes" = "
      SELECT 'bio', 'swfsc_cufes', 'swfsc_cufes:underway:' || CAST(c.sample_id AS VARCHAR),
             c.grid_key, c.cruise_key, c.latitude, c.longitude,
             CAST(c.datetime_start_utc AS TIMESTAMP), 0::DOUBLE, 0::DOUBLE,
             mx.taxon_key, mx.life_stage, mx.measurement_type, m.measurement_value,
             m.measurement_qual, NULL::DOUBLE
      FROM cufes_measurement m JOIN cufes_sample c USING (sample_id)
      JOIN _measurement_taxon mx ON mx.dataset_key = 'swfsc_cufes'
                                AND mx.raw_measurement_type = m.measurement_type
                                AND mx.target = 'obs'
      WHERE c.grid_key IS NOT NULL",
    # euphausiids: BTEDB export is species- AND life-stage-resolved, so taxon_key
    # comes from dataset_taxon (like zoodb/zooscan) and life_stage is carried on
    # the headline. Before that export the measurement was one undifferentiated
    # value per tow with the taxon baked into measurement_type.
    "cce-lter_euphausiids" = "
      SELECT 'bio', 'cce-lter_euphausiids', 'cce-lter_euphausiids:tow:' || CAST(tw.tow_id AS VARCHAR),
             tw.grid_key, tw.cruise_key, tw.latitude, tw.longitude,
             CAST(tw.datetime_start_utc AS TIMESTAMP), NULL::DOUBLE, NULL::DOUBLE,
             dt.taxon_key, m.life_stage, m.measurement_type, m.measurement_value, m.measurement_qual, NULL::DOUBLE
      FROM euphausiids_measurement m JOIN euphausiids_tow tw USING (tow_id)
      LEFT JOIN dataset_taxon dt ON dt.dataset_key = 'cce-lter_euphausiids'
                                AND dt.ds_taxa_code = CAST(m.taxon_id AS VARCHAR)
      WHERE tw.grid_key IS NOT NULL",
    # phyllosoma: only the total is the occurrence headline (target='obs'); the
    # per-stage counts (phyllosoma_stage_N, target='attribute') are sub-occurrence
    # detail and go to obs_attribute — see .obs_attribute_arm_sql().
    "calcofi_phyllosoma" = "
      SELECT 'bio', 'calcofi_phyllosoma', 'calcofi_phyllosoma:tow:' || CAST(tw.tow_id AS VARCHAR),
             tw.grid_key, tw.cruise_key, tw.latitude, tw.longitude,
             CAST(tw.datetime_start_utc AS TIMESTAMP), 0::DOUBLE, tw.max_tow_depth_m,
             mx.taxon_key, mx.life_stage, mx.measurement_type, m.measurement_value,
             m.measurement_qual, NULL::DOUBLE
      FROM phyllosoma_measurement m JOIN phyllosoma_tow tw USING (tow_id)
      JOIN _measurement_taxon mx ON mx.dataset_key = 'calcofi_phyllosoma'
                                AND mx.raw_measurement_type = m.measurement_type
                                AND mx.target = 'obs'
      WHERE tw.grid_key IS NOT NULL",
    "cce-lter_zoodb" = "
      SELECT 'bio', 'cce-lter_zoodb', 'cce-lter_zoodb:tow:' || CAST(sp.sample_id AS VARCHAR),
             sp.grid_key, sp.cruise_key, sp.latitude, sp.longitude,
             CAST(sp.datetime_start_utc AS TIMESTAMP), sp.min_depth_m, sp.max_depth_m,
             dt.taxon_key, NULL::VARCHAR, m.measurement_type, m.measurement_value, NULL::VARCHAR, NULL::DOUBLE
      FROM zoodb_measurement m JOIN zoodb_sample sp USING (sample_id)
      LEFT JOIN dataset_taxon dt ON dt.dataset_key = 'cce-lter_zoodb'
                                AND dt.ds_taxa_code = CAST(m.taxon_id AS VARCHAR)
      WHERE sp.grid_key IS NOT NULL",
    "cce-lter_zooscan" = "
      SELECT 'bio', 'cce-lter_zooscan', 'cce-lter_zooscan:tow:' || CAST(sp.sample_id AS VARCHAR),
             sp.grid_key, sp.cruise_key, sp.latitude, sp.longitude,
             CAST(sp.station_date AS TIMESTAMP), sp.min_depth_m, sp.max_depth_m,
             dt.taxon_key, NULL::VARCHAR, m.measurement_type, m.measurement_value, NULL::VARCHAR, NULL::DOUBLE
      FROM zooscan_measurement m JOIN zooscan_sample sp USING (sample_id)
      LEFT JOIN dataset_taxon dt ON dt.dataset_key = 'cce-lter_zooscan'
                                AND dt.ds_taxa_code = CAST(m.taxon_id AS VARCHAR)
      WHERE sp.grid_key IS NOT NULL",
    # METS underway: env realm, fed by mets_thin — the same thinned-table pattern
    # `calcofi_ctd-cast` uses (obs carries ctd_thin, not the full scan set).
    # Underway seawater is drawn from a hull intake a few metres down; the exact
    # depth is undocumented per cruise (questions.csv mets_25), so depth is
    # recorded as surface, matching swfsc_cufes.
    "calcofi_mets" = "
      SELECT 'env', 'calcofi_mets', 'calcofi_mets:underway:' || CAST(t.mets_sample_uuid AS VARCHAR),
             s.grid_key, s.cruise_key, s.latitude, s.longitude,
             CAST(s.datetime_start_utc AS TIMESTAMP), 0::DOUBLE, 0::DOUBLE,
             NULL::VARCHAR, NULL::VARCHAR, t.measurement_type, t.measurement_value,
             NULL::VARCHAR, NULL::DOUBLE
      FROM mets_thin t JOIN mets_sample s USING (mets_sample_uuid)
      WHERE s.grid_key IS NOT NULL",
    # mesopelagic fish: species-as-columns pivoted to a per-tow tally; the source
    # names taxa by scientific name (no local code), so ds_taxa_code IS the name
    "ucsd_sio_mesopelagic-fish" = "
      SELECT 'bio', 'ucsd_sio_mesopelagic-fish', 'ucsd_sio_mesopelagic-fish:tow:' || CAST(tw.tow_id AS VARCHAR),
             tw.grid_key, tw.cruise_key, tw.latitude, tw.longitude,
             CAST(tw.datetime_start_utc AS TIMESTAMP), 0::DOUBLE, tw.depth_m,
             dt.taxon_key, NULL::VARCHAR, m.measurement_type, m.measurement_value, NULL::VARCHAR, NULL::DOUBLE
      FROM mesopelagic_fish_measurement m JOIN mesopelagic_fish_tow tw USING (tow_id)
      LEFT JOIN dataset_taxon dt ON dt.dataset_key = 'ucsd_sio_mesopelagic-fish'
                                AND dt.ds_taxa_code = m.scientific_name
      WHERE tw.grid_key IS NOT NULL",
    # picoplankton/bacteria: env realm — flow-cytometry cell counts per bottle,
    # no taxon_key (the four types are the measurement vocabulary, not taxa)
    "cce-lter_picoplankton-bacteria" = "
      SELECT 'env', 'cce-lter_picoplankton-bacteria', 'cce-lter_picoplankton-bacteria:bottle:' || CAST(b.bottle_id AS VARCHAR),
             b.grid_key, b.cruise_key, b.latitude, b.longitude,
             CAST(b.datetime_utc AS TIMESTAMP), b.depth_m, b.depth_m,
             NULL::VARCHAR, NULL::VARCHAR, m.measurement_type, m.measurement_value, NULL::VARCHAR, NULL::DOUBLE
      FROM picoplankton_bacteria_measurement m
      JOIN picoplankton_bacteria_bottle b USING (bottle_id)
      WHERE b.grid_key IS NOT NULL",
    # phytoplankton: region-pooled (cruise x region grain) — no grid_key and no
    # datetime of its own; taxon via dataset_taxon on the source species_code.
    "calcofi_phytoplankton" = "
      SELECT 'bio', 'calcofi_phytoplankton',
             'calcofi_phytoplankton:region_pool:' || CAST(ps.phyto_sample_id AS VARCHAR),
             NULL::VARCHAR, ps.cruise_key, ps.latitude, ps.longitude,
             NULL::TIMESTAMP, 0::DOUBLE, 0::DOUBLE,
             dt.taxon_key, NULL::VARCHAR, pm.measurement_type, pm.measurement_value,
             NULL::VARCHAR, NULL::DOUBLE
      FROM phyto_measurement pm JOIN phyto_sample ps USING (phyto_sample_id)
      LEFT JOIN dataset_taxon dt ON dt.dataset_key = 'calcofi_phytoplankton'
                                AND dt.ds_taxa_code = CAST(pm.species_code AS VARCHAR)",
    # bird_mammal headline: one row per (transect, SPECIES CODE) with count
    # SUMmed across behaviors. The behavior breakdown is sub-occurrence detail
    # and goes to obs_attribute — it must NOT ride on the headline's life_stage,
    # or the same bird is counted once per behavior code.
    #
    # Grouping is on the source species_code, NOT on taxon_key: only 156 of the
    # 207 observed codes resolve to a taxon (the rest are excluded by
    # include_flag or are coarse unidentified categories), so grouping by
    # taxon_key alone would sum every unresolved species into a single
    # NULL-taxon row per transect, silently merging distinct species. taxon_key
    # is functionally determined by species_code (dataset_taxon is unique on
    # ds_taxa_code within a dataset), so carrying both does not split the grain.
    "calcofi_bird_mammal_census" = "
      SELECT 'bio', 'calcofi_bird_mammal_census', 'calcofi_bird_mammal_census:transect:' || CAST(tr.gis_key AS VARCHAR),
             tr.grid_key, tr.cruise_key, tr.latitude, tr.longitude,
             CAST(tr.datetime_start_utc AS TIMESTAMP), 0::DOUBLE, 0::DOUBLE,
             dt.taxon_key, NULL::VARCHAR, 'count', CAST(SUM(o.count) AS DOUBLE),
             NULL::VARCHAR, NULL::DOUBLE
      FROM bird_mammal_observation o JOIN bird_mammal_transect tr USING (gis_key)
      LEFT JOIN dataset_taxon dt ON dt.dataset_key = 'calcofi_bird_mammal_census'
                                AND dt.ds_taxa_code = CAST(o.species_code AS VARCHAR)
      WHERE tr.grid_key IS NOT NULL
      GROUP BY tr.gis_key, tr.grid_key, tr.cruise_key, tr.latitude, tr.longitude,
               tr.datetime_start_utc, o.species_code, dt.taxon_key",
    NULL)  # pic_zooplankton (no measurements) -> sample only
}

# per-dataset obs_attribute (sub-occurrence attribution) projection SQL:
# length-/stage-frequency bins and categorical breakdowns that sit UNDER an `obs`
# headline row rather than beside it.
.obs_attribute_arm_sql <- function(dataset_key) {
  switch(dataset_key,
    # ichthyo size -> body_length bins + stage frequency (bin_label from `lookup`)
    "swfsc_ichthyo" = "
      SELECT 'swfsc_ichthyo' dataset_key, 'swfsc_ichthyo:net:' || CAST(i.net_uuid AS VARCHAR) sample_key,
             dt.taxon_key, i.life_stage,
             CASE i.measurement_type WHEN 'size' THEN 'body_length' ELSE i.measurement_type END measurement_type,
             i.measurement_value bin_value,
             CASE WHEN i.measurement_type = 'stage' THEN lk.description ELSE NULL END bin_label,
             i.tally count, NULL::VARCHAR measurement_qual
      FROM ichthyo i
      LEFT JOIN dataset_taxon dt ON dt.dataset_key = 'swfsc_ichthyo'
                                AND dt.ds_taxa_code = CAST(i.species_id AS VARCHAR)
      LEFT JOIN lookup lk ON lk.lookup_type = i.life_stage || '_stage'
                         AND lk.lookup_num = CAST(i.measurement_value AS INTEGER)
      WHERE i.measurement_type IN ('stage','size')",
    # phyllosoma stage frequency: phyllosoma_stage_N -> ('stage', bin_value=N, count)
    "calcofi_phyllosoma" = "
      SELECT 'calcofi_phyllosoma' dataset_key, 'calcofi_phyllosoma:tow:' || CAST(tw.tow_id AS VARCHAR) sample_key,
             mx.taxon_key, mx.life_stage, mx.measurement_type, mx.bin_value,
             NULL::VARCHAR bin_label, CAST(m.measurement_value AS INTEGER) count,
             NULL::VARCHAR measurement_qual
      FROM phyllosoma_measurement m JOIN phyllosoma_tow tw USING (tow_id)
      JOIN _measurement_taxon mx ON mx.dataset_key = 'calcofi_phyllosoma'
                                AND mx.raw_measurement_type = m.measurement_type
                                AND mx.target = 'attribute'
      WHERE tw.grid_key IS NOT NULL AND m.measurement_value > 0",
    # bird_mammal behavior breakdown -> ('behavior', bin_label = description)
    "calcofi_bird_mammal_census" = "
      SELECT 'calcofi_bird_mammal_census' dataset_key,
             'calcofi_bird_mammal_census:transect:' || CAST(tr.gis_key AS VARCHAR) sample_key,
             dt.taxon_key, NULL::VARCHAR life_stage, 'behavior' measurement_type,
             NULL::DOUBLE bin_value, bb.description bin_label,
             CAST(o.count AS INTEGER) count, NULL::VARCHAR measurement_qual
      FROM bird_mammal_observation o JOIN bird_mammal_transect tr USING (gis_key)
      LEFT JOIN dataset_taxon dt ON dt.dataset_key = 'calcofi_bird_mammal_census'
                                AND dt.ds_taxa_code = CAST(o.species_code AS VARCHAR)
      LEFT JOIN bird_mammal_behavior bb ON bb.behavior_code = o.behavior_code
      WHERE tr.grid_key IS NOT NULL",
    NULL)
}

.sample_measurement_arm_sql <- function(dataset_key) {
  switch(dataset_key,
    "swfsc_ichthyo" = "
      SELECT 'swfsc_ichthyo:net:' || CAST(net_uuid AS VARCHAR) sample_key, 'swfsc_ichthyo' dataset_key,
             mt measurement_type, mv measurement_value, NULL::VARCHAR measurement_qual
      FROM (
        SELECT net_uuid, 'volume_sampled' mt, volume_sampled mv FROM net UNION ALL
        SELECT net_uuid, 'std_haul_factor', standard_haul_factor FROM net UNION ALL
        SELECT net_uuid, 'prop_sorted', prop_sorted FROM net UNION ALL
        SELECT net_uuid, 'small_plankton_biomass', smallplankton FROM net UNION ALL
        SELECT net_uuid, 'total_plankton_biomass', totalplankton FROM net)
      WHERE mv IS NOT NULL",
    # bottle cast conditions. These already INCLUDE bottom_depth: the ingest
    # pivots the source `Bottom_D` column into cast_condition (33,363 rows) and
    # drops it from `casts`, so no separate arm is needed — an earlier attempt to
    # UNION `bottom_depth_m FROM casts` was both redundant and a binder error,
    # since that column no longer exists by the time this runs.
    "calcofi_bottle" = "
      SELECT 'calcofi_bottle:cast:' || CAST(CAST(cast_id AS BIGINT) AS VARCHAR), 'calcofi_bottle',
             condition_type, condition_value, NULL::VARCHAR
      FROM cast_condition",
    NULL)
}

#' Project one dataset into the consolidated core tables
#'
#' The per-ingest entry point, and the authoritative projection: after an ingest
#' has built its per-dataset tables, `emit_core_tables()` turns them into that
#' dataset's slice of the shared core family — `sample` (via
#' [build_sample_reference()], which auto-detects the dataset's event tables
#' present in `con`), its `obs` occurrence headline, `obs_attribute`
#' sub-occurrence detail, `sample_measurement` event-level effort, and its slice
#' of the taxa references (`taxon` / `dataset_taxon` / `taxon_group`). These
#' shards ARE the ingest's parquet output; `release_database.qmd` concatenates
#' them rather than re-deriving the core from per-dataset tables (which is how
#' the two projections drifted apart). `pic_zooplankton` (no measurements)
#' contributes `sample` only.
#'
#' `taxon_key` is resolved here, at ingest time. Datasets whose taxon lives in a
#' vocabulary table (ichthyo `species`, `zoodb_taxon`, `bird_mammal_species`, …)
#' resolve through `dataset_taxon`; datasets that bake the taxon into the
#' measurement type name (cufes `sardine_eggs`, phyllosoma `phyllosoma_stage_3`)
#' resolve through `measurement_taxon` — pass `metadata/measurement_taxon.csv`
#' and `metadata/taxon_override.csv`, or those arms project zero rows.
#'
#' @param con a DuckDB connection holding this dataset's per-dataset tables
#' @param dataset_key provider_dataset (e.g. `"swfsc_ichthyo"`, `"calcofi_bottle"`)
#' @param sample logical; also (re)build `sample` from the present event tables (default TRUE)
#' @param measurement_taxon optional data.frame of the composite-type crosswalk
#'   (`metadata/measurement_taxon.csv`); required for `swfsc_cufes` /
#'   `calcofi_phyllosoma`, ignored by every other dataset
#' @param overrides optional data.frame of manual id resolution
#'   (`metadata/taxon_override.csv`) for coarse taxa (phyto groups, mammals)
#' @param taxa logical; also build this dataset's `taxon` / `dataset_taxon` /
#'   `taxon_group` slices (default TRUE). Set FALSE to project against taxa
#'   references already present in `con`.
#' @return (invisibly) a named list of row counts for the core tables written
#' @export
#' @concept model
#' @examples
#' \dontrun{
#' mt <- readr::read_csv(here::here("metadata/measurement_taxon.csv"))
#' ov <- readr::read_csv(here::here("metadata/taxon_override.csv"))
#' core <- emit_core_tables(con, "swfsc_cufes", measurement_taxon = mt, overrides = ov)
#' }
emit_core_tables <- function(con, dataset_key, sample = TRUE,
                             measurement_taxon = NULL, overrides = NULL,
                             taxa = TRUE) {
  out <- list()
  .ensure_measurement_taxon(con, measurement_taxon, dataset_key = dataset_key)
  if (isTRUE(taxa)) {
    out <- c(out, .build_taxa_slices(con, dataset_key, measurement_taxon, overrides))
  } else {
    .ensure_dataset_taxon(con)
  }
  if (isTRUE(sample)) out$sample <- build_sample_reference(con, datasets = dataset_key)
  oa <- .obs_arm_sql(dataset_key)
  if (!is.null(oa)) out$obs <- append_obs(con, oa)
  fa <- .obs_attribute_arm_sql(dataset_key)
  if (!is.null(fa)) out$obs_attribute <- append_obs_attribute(con, fa)
  ma <- .sample_measurement_arm_sql(dataset_key)
  if (!is.null(ma)) out$sample_measurement <- append_sample_measurement(con, ma)
  invisible(out)
}

# core output tables ----------------------------------------------------------

#' Core tables an ingest writes to parquet
#'
#' The shard set [emit_core_tables()] produces, filtered to those actually
#' present and non-empty in `con`. Use it to drive `write_parquet_outputs()` so a
#' dataset with no `obs_attribute` (most of them) does not emit an empty file.
#'
#' @param con a DuckDB connection after [emit_core_tables()]
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

# Rebuild a per-dataset event table from the core. The source id is recovered
# from the namespaced sample_key ('<dataset_key>:<sample_type>:<id>' -> field 3),
# the containment FK from parent_sample_key, and the event-level effort columns
# by pivoting sample_measurement back out of long form.
.compat_event_sql <- function(dataset_key, sample_type, id_col, parent_col = NULL,
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

.compat_specs <- function(dataset_key, sample_tbl = "sample") {
  switch(dataset_key,
    "swfsc_ichthyo" = list(
      site = .compat_event_sql("swfsc_ichthyo", "site", "site_uuid", NULL,
        c(order_occ = "order_occ", longitude = "longitude", latitude = "latitude",
          cruise_key = "cruise_key", geom = "geom", grid_key = "grid_key",
          site_key = "site_key"), sample_tbl = sample_tbl),
      tow = .compat_event_sql("swfsc_ichthyo", "tow", "tow_uuid", "site_uuid",
        c(tow_type_key = "tow_type", datetime_start_utc = "datetime"),
        sample_tbl = sample_tbl),
      net = .compat_event_sql("swfsc_ichthyo", "net", "net_uuid", "tow_uuid",
        character(),
        c(standard_haul_factor = "std_haul_factor", volume_sampled = "volume_sampled",
          prop_sorted = "prop_sorted", smallplankton = "small_plankton_biomass",
          totalplankton = "total_plankton_biomass"), sample_tbl = sample_tbl)),
    "calcofi_bottle" = list(
      # cast/bottle event tables: downstream ingests (dic) match against these by
      # site_key + datetime, then by depth, so they must come back from the core.
      casts = glue::glue(
        "SELECT CAST(split_part(s.sample_key, ':', 3) AS BIGINT) AS cast_id,
                s.site_key, s.grid_key, s.cruise_key, s.order_occ,
                s.latitude, s.longitude, s.datetime AS datetime_start_utc, s.geom
         FROM {sample_tbl} s
         WHERE s.dataset_key = 'calcofi_bottle' AND s.sample_type = 'cast'"),
      bottle = glue::glue(
        "SELECT CAST(split_part(s.sample_key, ':', 3) AS BIGINT) AS bottle_id,
                CAST(split_part(s.parent_sample_key, ':', 3) AS BIGINT) AS cast_id,
                s.site_key, s.depth_min_m AS depth_m
         FROM {sample_tbl} s
         WHERE s.dataset_key = 'calcofi_bottle' AND s.sample_type = 'bottle'"),
      cast_condition = "
        SELECT sample_measurement_id AS cast_condition_id,
               CAST(split_part(sample_key, ':', 3) AS BIGINT) AS cast_id,
               measurement_type AS condition_type, measurement_value AS condition_value
        FROM sample_measurement
        WHERE dataset_key = 'calcofi_bottle'",
      bottle_measurement = "
        SELECT obs_id AS bottle_measurement_id,
               CAST(split_part(sample_key, ':', 3) AS BIGINT) AS bottle_id,
               measurement_type, measurement_value, measurement_qual, measurement_prec
        FROM obs WHERE dataset_key = 'calcofi_bottle'"),
    "cce-lter_zoodb" = list(
      zoodb_measurement = compat_measurement_sql(
        "cce-lter_zoodb", "tow", "sample_id", "measurement_id")),
    "cce-lter_zooscan" = list(
      zooscan_measurement = compat_measurement_sql(
        "cce-lter_zooscan", "tow", "sample_id", "zooscan_measurement_id")),
    "swfsc_cufes" = list(
      cufes_measurement = compat_measurement_sql(
        "swfsc_cufes", "underway", "sample_id", "cufes_measurement_id")),
    "calcofi_phyllosoma" = list(
      phyllosoma_measurement = compat_measurement_sql(
        "calcofi_phyllosoma", "tow", "tow_id", "phyllosoma_measurement_id")),
    "ucsd_sio_mesopelagic-fish" = list(
      mesopelagic_fish_measurement = compat_measurement_sql(
        "ucsd_sio_mesopelagic-fish", "tow", "tow_id", "mesopelagic_fish_measurement_id")),
    "cce-lter_picoplankton-bacteria" = list(
      picoplankton_bacteria_measurement = compat_measurement_sql(
        "cce-lter_picoplankton-bacteria", "bottle", "bottle_id", "measurement_id")),
    NULL)
}

#' Recreate per-dataset tables as VIEWs over the consolidated core
#'
#' Once an ingest publishes the core, the per-dataset event and measurement
#' tables it used to publish can be served as VIEWs instead of stored bytes: the
#' source id is recovered from the namespaced `sample_key`, the containment FK
#' from `parent_sample_key`, event-level effort by pivoting `sample_measurement`
#' back out of long form, and the measurement triples straight from `obs`.
#'
#' **This is exact for the columns the core models and lossy for the rest.**
#' Verified against the shipped data, `net` and `tow` round-trip identically
#' (76,512 / 75,506 rows, every value equal). What does NOT come back is the
#' columns the consolidated model never carried — `net.side`, `tow.tow_number`,
#' `site.order_occ`/`line`/`station`/`site_key`, most of the 33 legacy `casts`
#' columns (`rpt_line`, `ac_sta`, `int_chl`, …), `bottle.btl_num`/`depth_qual`,
#' and the CTD scan-grain columns (`ctd_cast_uuid`, `cast_dir`, `data_stage`),
#' since `sample` holds one row per physical cast. Those are dropped from the
#' release by `core_keep` regardless, so the VIEW is no thinner than what
#' consumers already get — but do not treat it as a lossless archive of the
#' source. Use [core_output_tables()] to publish; use this to keep in-notebook
#' consumers and ad-hoc queries working against the old names.
#'
#' @param con a DuckDB connection holding the core tables
#' @param dataset_key provider_dataset to rebuild views for
#' @param replace logical; drop an existing table/view of the same name first
#'   (default TRUE — the ingest still has the real tables in scope)
#' @param sample_tbl name of the core `sample` table to read. Override when a
#'   downstream ingest loads ANOTHER dataset's shard as a reference — e.g. dic
#'   loads bottle's `sample` as `_bottle_sample` so rebuilding `casts`/`bottle`
#'   does not collide with the `sample` dic builds for itself.
#' @return (invisibly) character vector of view names created
#' @export
#' @concept model
create_compat_views <- function(con, dataset_key, replace = TRUE,
                                sample_tbl = "sample") {
  specs <- .compat_specs(dataset_key, sample_tbl = sample_tbl)
  if (is.null(specs)) return(invisible(character()))
  made <- character()
  present <- DBI::dbListTables(con)
  for (nm in names(specs)) {
    # a view can only be built if the core tables it reads are present: the
    # effort pivot needs sample_measurement, the measurement triples need obs.
    need <- intersect(c(sample_tbl, "sample_measurement", "obs"),
                      unlist(regmatches(specs[[nm]], gregexpr(
                        paste0("sample_measurement|\\b", sample_tbl, "\\b|\\bobs\\b"),
                        specs[[nm]]))))
    missing <- setdiff(need, present)
    if (length(missing)) {
      message("create_compat_views(): skipping ", nm, " (needs ",
              paste(missing, collapse = ", "), ")")
      next
    }
    if (isTRUE(replace)) {
      t <- DBI::dbGetQuery(con, glue::glue(
        "SELECT table_type FROM information_schema.tables WHERE table_name = '{nm}'"))
      if (nrow(t)) {
        kind <- if (grepl("VIEW", t$table_type[1], ignore.case = TRUE)) "VIEW" else "TABLE"
        DBI::dbExecute(con, glue::glue('DROP {kind} IF EXISTS "{nm}"'))
      }
    }
    DBI::dbExecute(con, glue::glue("CREATE VIEW {nm} AS {specs[[nm]]}"))
    made <- c(made, nm)
  }
  invisible(made)
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
