# duckdb operations for calcofi data workflow

#' Get a DuckDB connection
#'
#' Creates a connection to a DuckDB database, either local file or in-memory.
#' Optionally downloads from GCS if path starts with gs://.
#'
#' @param path Path to DuckDB file, GCS path, or ":memory:" for in-memory
#' @param read_only Open database in read-only mode (default: FALSE)
#' @param config Named list of DuckDB configuration options (default: empty list)
#'
#' @return DuckDB connection object
#' @export
#' @concept duckdb
#'
#' @examples
#' \dontrun{
#' # in-memory database
#' con <- get_duckdb_con()
#'
#' # local file
#' con <- get_duckdb_con("data/calcofi.duckdb")
#'
#' # from GCS (downloads first)
#' con <- get_duckdb_con("gs://calcofi-db/duckdb/calcofi.duckdb")
#'
#' # read-only with custom config
#' con <- get_duckdb_con(
#'   "calcofi.duckdb",
#'   read_only = TRUE,
#'   config = list(threads = 4))
#' }
#' @importFrom DBI dbConnect
get_duckdb_con <- function(
    path      = ":memory:",
    read_only = FALSE,
    config    = list()) {

  if (!requireNamespace("duckdb", quietly = TRUE)) {
    stop("Package 'duckdb' is required. Install with: install.packages('duckdb')")
  }

  # handle GCS paths
  if (grepl("^gs://", path)) {
    local_path <- get_gcs_file(path)
    path       <- local_path
  }

  # ensure directory exists for file-based databases
  if (path != ":memory:") {
    db_dir <- dirname(path)
    if (!dir.exists(db_dir) && db_dir != ".") {
      dir.create(db_dir, recursive = TRUE)
    }
  }

  # ensure config is a list (duckdb requires this)
  if (is.null(config)) {
    config <- list()
  }

  # create connection
  con <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir     = path,
    read_only = read_only,
    config    = config)

  return(con)
}

#' Create DuckDB from Parquet files
#'
#' Creates or updates a DuckDB database from a collection of Parquet files.
#'
#' @param parquet_files Named list or vector of Parquet file paths
#' @param db_path Path for DuckDB file (default: ":memory:")
#' @param table_names Optional table names (default: derived from file names)
#'
#' @return DuckDB connection with loaded tables
#' @export
#' @concept duckdb
#'
#' @examples
#' \dontrun{
#' # create from multiple parquet files
#' con <- create_duckdb_from_parquet(
#'   c("parquet/bottle.parquet", "parquet/cast.parquet"),
#'   db_path = "calcofi.duckdb")
#'
#' # with explicit table names
#' con <- create_duckdb_from_parquet(
#'   parquet_files = c(
#'     bottle = "parquet/bottle.parquet",
#'     cast   = "parquet/cast.parquet"),
#'   db_path = "calcofi.duckdb")
#' }
#' @importFrom DBI dbExecute
#' @importFrom glue glue
create_duckdb_from_parquet <- function(
    parquet_files,
    db_path     = ":memory:",
    table_names = NULL) {

  con <- get_duckdb_con(db_path)

  # determine table names
  if (is.null(table_names)) {
    if (!is.null(names(parquet_files))) {
      table_names <- names(parquet_files)
    } else {
      table_names <- tools::file_path_sans_ext(basename(parquet_files))
    }
  }

  # load each parquet file as a table
  for (i in seq_along(parquet_files)) {
    pqt_path <- parquet_files[i]
    tbl_name <- table_names[i]

    # handle GCS paths
    if (grepl("^gs://", pqt_path)) {
      pqt_path <- get_gcs_file(pqt_path)
    }

    DBI::dbExecute(con, glue::glue(
      "CREATE OR REPLACE TABLE {tbl_name} AS SELECT * FROM read_parquet('{pqt_path}')"))

    message(glue::glue("Loaded table: {tbl_name}"))
  }

  return(con)
}

#' Create views from a manifest
#'
#' Creates DuckDB views pointing to Parquet files defined in a manifest.
#'
#' @param con DuckDB connection
#' @param manifest List or data frame with table definitions
#'
#' @return The connection object (invisibly)
#' @export
#' @concept duckdb
#'
#' @examples
#' \dontrun{
#' manifest <- list(
#'   tables = list(
#'     list(name = "bottle", path = "parquet/bottle.parquet"),
#'     list(name = "cast",   path = "parquet/cast.parquet")))
#'
#' con <- get_duckdb_con()
#' create_duckdb_views(con, manifest)
#' }
#' @importFrom DBI dbExecute
#' @importFrom glue glue
create_duckdb_views <- function(con, manifest) {
  # handle manifest as list or data frame
  if (is.data.frame(manifest)) {
    tables <- split(manifest, seq_len(nrow(manifest)))
  } else if ("tables" %in% names(manifest)) {
    tables <- manifest$tables
  } else {
    tables <- manifest
  }

  for (tbl in tables) {
    name <- if (is.list(tbl)) tbl$name else tbl["name"]
    path <- if (is.list(tbl)) tbl$path else tbl["path"]

    # handle GCS paths
    if (grepl("^gs://", path)) {
      local_path <- get_gcs_file(path)
      path       <- local_path
    }

    DBI::dbExecute(con, glue::glue(
      "CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{path}')"))

    message(glue::glue("Created view: {name}"))
  }

  invisible(con)
}

#' Set table and column comments in DuckDB
#'
#' Adds documentation comments to tables and columns in DuckDB.
#'
#' @param con DuckDB connection
#' @param table Table name
#' @param table_comment Optional table-level comment
#' @param column_comments Named list of column comments
#'
#' @return The connection (invisibly)
#' @export
#' @concept duckdb
#'
#' @examples
#' \dontrun{
#' set_duckdb_comments(
#'   con,
#'   table = "bottle",
#'   table_comment = "Bottle sample data from CalCOFI cruises",
#'   column_comments = list(
#'     cruise_id = "Unique cruise identifier",
#'     depth_m   = "Sample depth in meters",
#'     temp_c    = "Water temperature in Celsius"))
#' }
#' @importFrom DBI dbExecute
#' @importFrom glue glue
set_duckdb_comments <- function(
    con,
    table,
    table_comment   = NULL,
    column_comments = NULL) {

  # set table comment
  if (!is.null(table_comment)) {
    safe_comment <- gsub("'", "''", table_comment)
    DBI::dbExecute(con, glue::glue(
      "COMMENT ON TABLE {table} IS '{safe_comment}'"))
  }

  # set column comments
  if (!is.null(column_comments)) {
    for (col in names(column_comments)) {
      safe_comment <- gsub("'", "''", column_comments[[col]])
      DBI::dbExecute(con, glue::glue(
        "COMMENT ON COLUMN {table}.{col} IS '{safe_comment}'"))
    }
  }

  invisible(con)
}

#' Export DuckDB table to Parquet
#'
#' Exports a DuckDB table or query result to a Parquet file.
#'
#' @param con DuckDB connection
#' @param table_or_query Table name or SQL query
#' @param output Path for output Parquet file
#' @param compression Compression codec (default: "snappy")
#'
#' @return Path to created Parquet file
#' @export
#' @concept duckdb
#'
#' @examples
#' \dontrun{
#' # export table
#' duckdb_to_parquet(con, "bottle", "export/bottle.parquet")
#'
#' # export query result
#' duckdb_to_parquet(
#'   con,
#'   "SELECT * FROM bottle WHERE year >= 2020",
#'   "export/bottle_recent.parquet")
#' }
#' @importFrom DBI dbExecute
#' @importFrom glue glue
duckdb_to_parquet <- function(
    con,
    table_or_query,
    output,
    compression = "snappy") {

  # ensure output directory exists
  output_dir <- dirname(output)
  if (!dir.exists(output_dir) && output_dir != ".") {
    dir.create(output_dir, recursive = TRUE)
  }

  # determine if input is table name or query
  is_query <- grepl("\\s", table_or_query) || grepl("^SELECT", table_or_query, ignore.case = TRUE)

  if (is_query) {
    DBI::dbExecute(con, glue::glue(
      "COPY ({table_or_query}) TO '{output}' (FORMAT PARQUET, COMPRESSION '{compression}')"))
  } else {
    DBI::dbExecute(con, glue::glue(
      "COPY {table_or_query} TO '{output}' (FORMAT PARQUET, COMPRESSION '{compression}')"))
  }

  message(glue::glue("Exported to: {output}"))
  return(output)
}

#' Save DuckDB to GCS
#'
#' Saves a DuckDB database file to Google Cloud Storage.
#'
#' @param con DuckDB connection
#' @param gcs_path GCS path (e.g., "gs://calcofi-db/duckdb/calcofi.duckdb")
#' @param checkpoint Run checkpoint before saving to ensure data is flushed
#'
#' @return GCS URI of uploaded file
#' @export
#' @concept duckdb
#'
#' @examples
#' \dontrun{
#' save_duckdb_to_gcs(con, "gs://calcofi-db/duckdb/calcofi.duckdb")
#' }
#' @importFrom DBI dbExecute dbGetInfo
#' @importFrom glue glue
save_duckdb_to_gcs <- function(con, gcs_path, checkpoint = TRUE) {
  # get local database path
  db_info <- DBI::dbGetInfo(con)
  local_path <- db_info$dbname

  if (local_path == ":memory:") {
    stop("Cannot save in-memory database to GCS. Export to file first.")
  }

  # checkpoint to ensure all data is written
  if (checkpoint) {
    DBI::dbExecute(con, "CHECKPOINT")
  }

  # upload to GCS
  put_gcs_file(local_path, gcs_path)
}

#' Get table information from DuckDB
#'
#' Retrieves schema information for all tables in a DuckDB database.
#'
#' @param con DuckDB connection
#'
#' @return Data frame with table names, column info, and row counts
#' @export
#' @concept duckdb
#'
#' @examples
#' \dontrun{
#' tables <- get_duckdb_tables(con)
#' tables
#' }
#' @importFrom DBI dbGetQuery
get_duckdb_tables <- function(con) {
  # get table list with row counts
  tables <- DBI::dbGetQuery(con, "
    SELECT
      table_name,
      estimated_size as row_count
    FROM duckdb_tables()
    ORDER BY table_name")

  # get column info for each table
  columns <- DBI::dbGetQuery(con, "
    SELECT
      table_name,
      column_name,
      data_type,
      is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'main'
    ORDER BY table_name, ordinal_position")

  list(
    tables  = tables,
    columns = columns)
}

#' Install and load DuckDB extension
#'
#' Installs (if needed) and loads a DuckDB extension.
#'
#' @param con DuckDB connection
#' @param extension Extension name (e.g., "spatial", "h3", "httpfs")
#' @param from Optional source ("community" or specific URL)
#'
#' @return The connection (invisibly)
#' @export
#' @concept duckdb
#'
#' @examples
#' \dontrun{
#' # install community extension
#' load_duckdb_extension(con, "h3", from = "community")
#'
#' # install core extension
#' load_duckdb_extension(con, "spatial")
#' }
#' @importFrom DBI dbExecute
#' @importFrom glue glue
load_duckdb_extension <- function(con, extension, from = NULL) {
  if (!is.null(from)) {
    DBI::dbExecute(con, glue::glue("INSTALL {extension} FROM {from}"))
  } else {
    DBI::dbExecute(con, glue::glue("INSTALL {extension}"))
  }

  DBI::dbExecute(con, glue::glue("LOAD {extension}"))

  message(glue::glue("Loaded extension: {extension}"))
  invisible(con)
}

#' Disconnect from DuckDB and shutdown
#'
#' Properly closes a DuckDB connection and shuts down the database.
#'
#' @param con DuckDB connection
#' @param checkpoint Run checkpoint before closing (default: TRUE for writeable connections)
#'
#' @return NULL (invisibly)
#' @export
#' @concept duckdb
#'
#' @examples
#' \dontrun{
#' close_duckdb(con)
#' }
#' @importFrom DBI dbDisconnect dbExecute dbGetInfo
close_duckdb <- function(con, checkpoint = NULL) {
  # determine if we should checkpoint
  db_info <- DBI::dbGetInfo(con)

  if (is.null(checkpoint)) {
    checkpoint <- !isTRUE(db_info$read_only) && db_info$dbname != ":memory:"
  }

  if (checkpoint) {
    tryCatch(
      DBI::dbExecute(con, "CHECKPOINT"),
      error = function(e) NULL)
  }

  DBI::dbDisconnect(con, shutdown = TRUE)
  invisible(NULL)
}
