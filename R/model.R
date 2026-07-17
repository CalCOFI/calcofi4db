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
.ns_key <- function(dataset_key, sample_type, id_sql) {
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
       cruise_key        VARCHAR,
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
        dataset_key, grid_key, cruise_key, latitude, longitude, datetime,
        depth_min_m, depth_max_m, tow_type, geom)
     SELECT sample_key, sample_type, parent_sample_key, root_sample_key,
            dataset_key, grid_key, cruise_key, latitude, longitude, datetime,
            depth_min_m, depth_max_m, tow_type,
            CASE WHEN latitude IS NULL OR longitude IS NULL THEN NULL
                 ELSE ST_Point(longitude, latitude) END AS geom
     FROM ( {select_sql} ) AS src(sample_key, sample_type, parent_sample_key, root_sample_key,
            dataset_key, grid_key, cruise_key, latitude, longitude, datetime, depth_min_m, depth_max_m, tow_type)"))
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
.sample_arm_self <- function(dataset_key, tbl, id_col, sample_type,
                             dt_col = "datetime_start_utc",
                             grid_expr = "grid_key",
                             depth_min = "0::DOUBLE", depth_max = "0::DOUBLE") {
  key <- .ns_key(dataset_key, sample_type, id_col)
  dt  <- if (identical(dt_col, "NULL")) "NULL::TIMESTAMP" else glue::glue("CAST({dt_col} AS TIMESTAMP)")
  glue::glue(
    "SELECT {key} AS sample_key, '{sample_type}' AS sample_type,
            NULL::VARCHAR AS parent_sample_key, {key} AS root_sample_key,
            '{dataset_key}' AS dataset_key, {grid_expr} AS grid_key, cruise_key,
            latitude, longitude, {dt} AS datetime,
            {depth_min} AS depth_min_m, {depth_max} AS depth_max_m,
            NULL::VARCHAR AS tow_type
     FROM {tbl}")
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
      "SELECT {.ns_key('calcofi_bottle','cast','cast_id')} AS sample_key, 'cast' AS sample_type,
              NULL::VARCHAR AS parent_sample_key,
              {.ns_key('calcofi_bottle','cast','cast_id')} AS root_sample_key,
              'calcofi_bottle' AS dataset_key, grid_key, cruise_key,
              latitude, longitude, CAST(datetime_start_utc AS TIMESTAMP) AS datetime,
              NULL::DOUBLE AS depth_min_m, NULL::DOUBLE AS depth_max_m,
              NULL::VARCHAR AS tow_type
       FROM casts"),
    bottle_btl = if (has("bottle", "casts")) glue::glue(
      "SELECT {.ns_key('calcofi_bottle','bottle','b.bottle_id')} AS sample_key, 'bottle' AS sample_type,
              {.ns_key('calcofi_bottle','cast','b.cast_id')} AS parent_sample_key,
              {.ns_key('calcofi_bottle','cast','b.cast_id')} AS root_sample_key,
              'calcofi_bottle' AS dataset_key, c.grid_key, c.cruise_key, c.latitude, c.longitude,
              CAST(c.datetime_start_utc AS TIMESTAMP) AS datetime, b.depth_m AS depth_min_m, b.depth_m AS depth_max_m,
              NULL::VARCHAR AS tow_type
       FROM bottle b JOIN casts c USING (cast_id)"),

    # --- calcofi_ctd-cast: physical cast (leaf = root). ctd_cast is per-SCAN
    # (5.5M rows / ctd_cast_uuid); the physical cast is cast_key (~14k, globally
    # unique). Dedup to one sample row per cast_key; obs joins ctd_thin->ctd_cast
    # to map each scan's ctd_cast_uuid to its cast_key. ------------------------
    ctd = if (has("ctd_cast")) glue::glue(
      "SELECT * FROM (
         SELECT {.ns_key('calcofi_ctd-cast','cast','cast_key')} AS sample_key, 'cast' AS sample_type,
                NULL::VARCHAR AS parent_sample_key,
                {.ns_key('calcofi_ctd-cast','cast','cast_key')} AS root_sample_key,
                'calcofi_ctd-cast' AS dataset_key, grid_key, cruise_key, latitude, longitude,
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
                  'calcofi_dic' AS dataset_key, c.grid_key, c.cruise_key,
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
      "SELECT {.ns_key('swfsc_ichthyo','site','s.site_uuid')} AS sample_key, 'site' AS sample_type,
              NULL::VARCHAR AS parent_sample_key,
              {.ns_key('swfsc_ichthyo','site','s.site_uuid')} AS root_sample_key, 'swfsc_ichthyo' AS dataset_key,
              s.grid_key, s.cruise_key, s.latitude, s.longitude,
              CAST(td.dt AS TIMESTAMP) AS datetime, NULL::DOUBLE AS depth_min_m, NULL::DOUBLE AS depth_max_m,
              NULL::VARCHAR AS tow_type
       FROM site s
       LEFT JOIN (SELECT site_uuid, min(datetime_start_utc) AS dt FROM tow GROUP BY 1) td
              ON td.site_uuid = s.site_uuid"),
    ich_tow = if (has("tow", "site")) glue::glue(
      "SELECT {.ns_key('swfsc_ichthyo','tow','t.tow_uuid')} AS sample_key, 'tow' AS sample_type,
              {.ns_key('swfsc_ichthyo','site','t.site_uuid')} AS parent_sample_key,
              {.ns_key('swfsc_ichthyo','site','t.site_uuid')} AS root_sample_key,
              'swfsc_ichthyo' AS dataset_key, s.grid_key, s.cruise_key, s.latitude, s.longitude,
              CAST(t.datetime_start_utc AS TIMESTAMP) AS datetime, 0::DOUBLE AS depth_min_m, NULL::DOUBLE AS depth_max_m,
              t.tow_type_key AS tow_type
       FROM tow t JOIN site s USING (site_uuid)"),
    ich_net = if (has("net", "tow", "site")) glue::glue(
      "SELECT {.ns_key('swfsc_ichthyo','net','n.net_uuid')} AS sample_key, 'net' AS sample_type,
              {.ns_key('swfsc_ichthyo','tow','n.tow_uuid')} AS parent_sample_key,
              {.ns_key('swfsc_ichthyo','site','t.site_uuid')} AS root_sample_key,
              'swfsc_ichthyo' AS dataset_key, s.grid_key, s.cruise_key, s.latitude, s.longitude,
              CAST(t.datetime_start_utc AS TIMESTAMP) AS datetime, 0::DOUBLE AS depth_min_m, NULL::DOUBLE AS depth_max_m,
              t.tow_type_key AS tow_type
       FROM net n JOIN tow t USING (tow_uuid) JOIN site s USING (site_uuid)"),

    # --- single-level datasets (leaf = root = self) --------------------------
    cufes = if (has("cufes_sample"))
      .sample_arm_self("swfsc_cufes", "cufes_sample", "sample_id", "underway"),
    euph = if (has("euphausiids_tow"))
      .sample_arm_self("cce-lter_euphausiids", "euphausiids_tow", "tow_id", "tow",
                       depth_min = "NULL::DOUBLE", depth_max = "NULL::DOUBLE"),
    phyllosoma = if (has("phyllosoma_tow"))
      .sample_arm_self("calcofi_phyllosoma", "phyllosoma_tow", "tow_id", "tow",
                       depth_min = "0::DOUBLE", depth_max = "max_tow_depth_m"),
    zoodb = if (has("zoodb_sample"))
      .sample_arm_self("cce-lter_zoodb", "zoodb_sample", "sample_id", "tow",
                       depth_min = "min_depth_m", depth_max = "max_depth_m"),
    zooscan = if (has("zooscan_sample"))
      .sample_arm_self("cce-lter_zooscan", "zooscan_sample", "sample_id", "tow",
                       dt_col = "station_date",
                       depth_min = "min_depth_m", depth_max = "max_depth_m"),
    bird = if (has("bird_mammal_transect"))
      .sample_arm_self("calcofi_bird_mammal_census", "bird_mammal_transect",
                       "gis_key", "transect"),
    pic = if (has("zooplankton_tow"))
      .sample_arm_self("pic_zooplankton", "zooplankton_tow", "tow_id", "tow",
                       depth_min = "depth_min_m", depth_max = "depth_max_m"),
    phyto = if (has("phyto_sample"))
      .sample_arm_self("calcofi_phytoplankton", "phyto_sample", "phyto_sample_id",
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
                pic = "pic_zooplankton", phyto = "calcofi_phytoplankton")
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
    "swfsc_cufes" = "
      SELECT 'bio', 'swfsc_cufes', 'swfsc_cufes:underway:' || CAST(c.sample_id AS VARCHAR),
             c.grid_key, c.cruise_key, c.latitude, c.longitude,
             CAST(c.datetime_start_utc AS TIMESTAMP), 0::DOUBLE, 0::DOUBLE,
             NULL::VARCHAR, NULL::VARCHAR, m.measurement_type, m.measurement_value, m.measurement_qual, NULL::DOUBLE
      FROM cufes_measurement m JOIN cufes_sample c USING (sample_id) WHERE c.grid_key IS NOT NULL",
    "cce-lter_euphausiids" = "
      SELECT 'bio', 'cce-lter_euphausiids', 'cce-lter_euphausiids:tow:' || CAST(tw.tow_id AS VARCHAR),
             tw.grid_key, tw.cruise_key, tw.latitude, tw.longitude,
             CAST(tw.datetime_start_utc AS TIMESTAMP), NULL::DOUBLE, NULL::DOUBLE,
             NULL::VARCHAR, NULL::VARCHAR, m.measurement_type, m.measurement_value, m.measurement_qual, NULL::DOUBLE
      FROM euphausiids_measurement m JOIN euphausiids_tow tw USING (tow_id) WHERE tw.grid_key IS NOT NULL",
    "calcofi_phyllosoma" = "
      SELECT 'bio', 'calcofi_phyllosoma', 'calcofi_phyllosoma:tow:' || CAST(tw.tow_id AS VARCHAR),
             tw.grid_key, tw.cruise_key, tw.latitude, tw.longitude,
             CAST(tw.datetime_start_utc AS TIMESTAMP), 0::DOUBLE, tw.max_tow_depth_m,
             NULL::VARCHAR, NULL::VARCHAR, m.measurement_type, m.measurement_value, m.measurement_qual, NULL::DOUBLE
      FROM phyllosoma_measurement m JOIN phyllosoma_tow tw USING (tow_id) WHERE tw.grid_key IS NOT NULL",
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
    # bird_mammal: taxon_key via dataset_taxon; behavior stays in life_stage on the
    # obs headline here (the release additionally splits behavior into obs_attribute).
    "calcofi_bird_mammal_census" = "
      SELECT 'bio', 'calcofi_bird_mammal_census', 'calcofi_bird_mammal_census:transect:' || CAST(tr.gis_key AS VARCHAR),
             tr.grid_key, tr.cruise_key, tr.latitude, tr.longitude,
             CAST(tr.datetime_start_utc AS TIMESTAMP), 0::DOUBLE, 0::DOUBLE,
             dt.taxon_key, o.behavior_code, 'count', CAST(o.count AS DOUBLE), NULL::VARCHAR, NULL::DOUBLE
      FROM bird_mammal_observation o JOIN bird_mammal_transect tr USING (gis_key)
      LEFT JOIN dataset_taxon dt ON dt.dataset_key = 'calcofi_bird_mammal_census'
                                AND dt.ds_taxa_code = CAST(o.species_code AS VARCHAR)
      WHERE tr.grid_key IS NOT NULL",
    # NOTE: cufes / euphausiids / phyllosoma bake the taxon into measurement_type;
    # their taxon_key + canonical-type decomposition lives in release_database.qmd
    # (Phase 2, via metadata/measurement_taxon.csv). This Phase-3 helper omits them.
    NULL)  # pic_zooplankton (no measurements) / calcofi_phytoplankton (no grid_key) -> sample only
}

.obs_attribute_arm_sql <- function(dataset_key) {
  if (!identical(dataset_key, "swfsc_ichthyo")) return(NULL)
  "SELECT 'swfsc_ichthyo' dataset_key, 'swfsc_ichthyo:net:' || CAST(i.net_uuid AS VARCHAR) sample_key,
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
   WHERE i.measurement_type IN ('stage','size')"
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
    "calcofi_bottle" = "
      SELECT 'calcofi_bottle:cast:' || CAST(CAST(cast_id AS BIGINT) AS VARCHAR), 'calcofi_bottle',
             condition_type, condition_value, NULL::VARCHAR
      FROM cast_condition",
    NULL)
}

#' Project one dataset into the consolidated core tables
#'
#' The per-ingest (Phase 3) entry point: after an ingest has built its
#' per-dataset tables, `emit_core_tables()` projects that dataset into the shared
#' core family — `sample` (via [build_sample_reference()], which auto-detects the
#' dataset's event tables present in `con`), plus its `obs` occurrence headline,
#' `obs_attribute` sub-occurrence detail, and `sample_measurement` effort — using the same
#' validated projection the release assembly uses. Idempotent per connection for a
#' single dataset's tables. Arms for `pic_zooplankton` (no measurements) and
#' `calcofi_phytoplankton` (region-pooled, no grid_key) contribute `sample` only.
#'
#' @param con a DuckDB connection holding this dataset's per-dataset tables
#' @param dataset_key provider_dataset (e.g. `"swfsc_ichthyo"`, `"calcofi_bottle"`)
#' @param sample logical; also (re)build `sample` from the present event tables (default TRUE)
#' @return (invisibly) a named list of row counts for the core tables written
#' @export
#' @concept model
emit_core_tables <- function(con, dataset_key, sample = TRUE) {
  out <- list()
  if (isTRUE(sample)) out$sample <- build_sample_reference(con, datasets = dataset_key)
  oa <- .obs_arm_sql(dataset_key)
  if (!is.null(oa)) out$obs <- append_obs(con, oa)
  fa <- .obs_attribute_arm_sql(dataset_key)
  if (!is.null(fa)) out$obs_attribute <- append_obs_attribute(con, fa)
  ma <- .sample_measurement_arm_sql(dataset_key)
  if (!is.null(ma)) out$sample_measurement <- append_sample_measurement(con, ma)
  invisible(out)
}
