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
    'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {geom_col} GEOMETRY'))
  DBI::dbExecute(con, glue::glue(
    'UPDATE {table} SET {geom_col} = ST_Point({lon_col}, {lat_col})'))
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

  DBI::dbExecute(con, glue::glue(
    'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS grid_key TEXT'))

  DBI::dbExecute(con, glue::glue(
    'UPDATE {table} SET grid_key = (
       SELECT g.grid_key FROM {grid_table} g
       WHERE ST_Intersects({table}.{geom_col}, g.geom) LIMIT 1)'))

  grid_stats <- DBI::dbGetQuery(con, glue::glue(
    "SELECT
       CASE WHEN grid_key IS NULL THEN 'not_in_grid' ELSE 'in_grid' END AS status,
       COUNT(*) AS n
     FROM {table}
     GROUP BY status"))

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
#'   gcs_path   = "gs://calcofi-db/ingest/swfsc.noaa.gov_calcofi-db/grid.parquet",
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
      'ALTER TABLE {table_name} ADD COLUMN {tmp_col} GEOMETRY'))
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
