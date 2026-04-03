# ducklake operations for calcofi data workflow
# Working DuckLake with provenance tracking

#' Load Tables from a Prior Ingest's Parquet Directory
#'
#' Reads parquet files from another ingest workflow's output directory
#' into the current wrangling DuckDB. Handles WKB->GEOMETRY conversion for
#' spatial tables and hive-partitioned subdirectories automatically.
#'
#' When \code{tables = NULL} (default), auto-discovers all parquet sources
#' in the directory: single \code{.parquet} files and subdirectories containing
#' \code{.parquet} files (hive-partitioned tables).
#'
#' @param con DBI connection to DuckDB
#' @param tables Character vector of table names to load (without .parquet
#'   extension). If NULL (default), auto-discovers all parquet in the directory.
#' @param parquet_dir Path to directory containing parquet files
#' @param gcs_prefix Optional GCS prefix (e.g., "ingest/swfsc_ichthyo") to use
#'   as fallback when \code{parquet_dir} doesn't exist or is empty. Reads from
#'   \code{https://storage.googleapis.com/calcofi-db/{gcs_prefix}/}. Requires
#'   the httpfs DuckDB extension.
#' @param geom_tables Character vector of table names that contain WKB geometry
#'   columns needing conversion (default: c("grid", "site", "segment"))
#' @param overwrite If TRUE, replace existing tables (default: TRUE)
#' @param include_supplemental If FALSE (default), tables listed under
#'   `"supplemental"` in the directory's manifest.json are excluded from
#'   auto-discovery. Set to TRUE to load all tables including supplemental
#'   (e.g. wide-format ERDDAP outputs).
#' @param as_view If TRUE, create VIEWs instead of TABLEs (zero-copy,
#'   reads directly from parquet on disk). Use for dependency tables that
#'   don't need to be modified or re-exported. Default FALSE for backward
#'   compatibility.
#'
#' @return Tibble with columns: table, rows, has_geom
#' @export
#' @concept ducklake
#'
#' @examples
#' \dontrun{
#' # load grid from ichthyo workflow
#' load_prior_tables(
#'   con         = con,
#'   tables      = "grid",
#'   parquet_dir = here("data/parquet/swfsc_ichthyo"))
#'
#' # load all tables from a dataset (auto-discover)
#' load_prior_tables(
#'   con         = con,
#'   parquet_dir = here("data/parquet/swfsc_ichthyo"))
#'
#' # fallback to GCS if local dir doesn't exist
#' load_prior_tables(
#'   con         = con,
#'   parquet_dir = here("data/parquet/swfsc_ichthyo"),
#'   gcs_prefix  = "ingest/swfsc_ichthyo")
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
#' @importFrom jsonlite read_json
#' @importFrom tibble tibble
#' @importFrom purrr map_dfr
load_prior_tables <- function(
    con,
    parquet_dir,
    tables                = NULL,
    gcs_prefix            = NULL,
    geom_tables           = c("grid", "site", "segment"),
    overwrite             = TRUE,
    include_supplemental  = FALSE,
    as_view               = FALSE) {

  # determine if we should use local dir or GCS fallback
  use_gcs <- !dir.exists(parquet_dir) ||
    length(list.files(parquet_dir, pattern = "\\.parquet$", recursive = TRUE)) == 0

  if (use_gcs && is.null(gcs_prefix)) {
    stop(glue::glue(
      "Local parquet dir does not exist or is empty: {parquet_dir}\n",
      "Provide gcs_prefix for GCS fallback."))
  }

  if (use_gcs) {
    return(.load_prior_tables_gcs(
      con         = con,
      parquet_dir = parquet_dir,
      gcs_prefix  = gcs_prefix,
      tables      = tables,
      geom_tables = geom_tables,
      overwrite   = overwrite))
  }

  # discover parquet sources if tables not specified
  sources <- .discover_parquet_sources(parquet_dir, tables)

  # exclude supplemental tables unless requested

  if (!include_supplemental && is.null(tables)) {
    manifest_path <- file.path(parquet_dir, "manifest.json")
    if (file.exists(manifest_path)) {
      manifest <- jsonlite::read_json(manifest_path)
      supp <- unlist(manifest$supplemental)
      if (length(supp) > 0) {
        sources <- sources[
          !vapply(sources, function(s) s$name %in% supp, logical(1))]
        message(glue::glue(
          "Excluding {length(supp)} supplemental table(s): ",
          "{paste(supp, collapse = ', ')}"))
      }
    }
  }

  if (length(sources) == 0) {
    message(glue::glue("No parquet files found in: {parquet_dir}"))
    return(tibble::tibble(table = character(), rows = integer(), has_geom = logical()))
  }

  or_replace <- if (overwrite) "OR REPLACE " else ""
  obj_type   <- if (as_view) "VIEW" else "TABLE"

  purrr::map_dfr(sources, function(src) {
    tbl_name <- src$name

    # build read expression based on single file vs partitioned dir
    if (src$partitioned) {
      read_expr <- glue::glue(
        "read_parquet('{src$path}/**/*.parquet', hive_partitioning = true)")
    } else {
      read_expr <- glue::glue("read_parquet('{src$path}')")
    }

    has_geom <- FALSE

    if (as_view) {
      # VIEW: zero-copy reference to parquet on disk
      # geometry handling happens at query time, not at load time
      if (tbl_name %in% geom_tables) {
        # probe schema to build geometry-normalizing SELECT
        DBI::dbExecute(con, glue::glue(
          "CREATE OR REPLACE VIEW _lpt_probe AS SELECT * FROM {read_expr} LIMIT 0"))
        geom_cols <- DBI::dbGetQuery(con,
          "SELECT column_name FROM information_schema.columns
           WHERE table_name = '_lpt_probe'
             AND data_type LIKE 'GEOMETRY%'")$column_name
        all_col_names <- DBI::dbGetQuery(con,
          "SELECT column_name FROM information_schema.columns
           WHERE table_name = '_lpt_probe'
           ORDER BY ordinal_position")$column_name
        DBI::dbExecute(con, "DROP VIEW IF EXISTS _lpt_probe")

        if (length(geom_cols) > 0) {
          select_exprs <- sapply(all_col_names, function(col) {
            if (col %in% geom_cols) {
              glue::glue("ST_GeomFromWKB(ST_AsWKB({col})) AS {col}")
            } else {
              col
            }
          })
          DBI::dbExecute(con, glue::glue(
            "CREATE {or_replace}VIEW {tbl_name} AS
             SELECT {paste(select_exprs, collapse = ', ')}
             FROM {read_expr}"))
          has_geom <- TRUE
        } else {
          DBI::dbExecute(con, glue::glue(
            "CREATE {or_replace}VIEW {tbl_name} AS
             SELECT * FROM {read_expr}"))
        }
      } else {
        DBI::dbExecute(con, glue::glue(
          "CREATE {or_replace}VIEW {tbl_name} AS
           SELECT * FROM {read_expr}"))
      }
    } else {
      # TABLE: copy data into DuckDB (original behavior)
      if (tbl_name %in% geom_tables) {
        DBI::dbExecute(con, glue::glue(
          "CREATE OR REPLACE VIEW _lpt_probe AS SELECT * FROM {read_expr} LIMIT 0"))
        geom_cols <- DBI::dbGetQuery(con,
          "SELECT column_name, data_type FROM information_schema.columns
           WHERE table_name = '_lpt_probe'
             AND data_type LIKE 'GEOMETRY%'")
        all_col_names <- DBI::dbGetQuery(con,
          "SELECT column_name FROM information_schema.columns
           WHERE table_name = '_lpt_probe'
           ORDER BY ordinal_position")$column_name
        DBI::dbExecute(con, "DROP VIEW IF EXISTS _lpt_probe")

        if (nrow(geom_cols) > 0) {
          select_exprs <- sapply(all_col_names, function(col) {
            if (col %in% geom_cols$column_name) {
              glue::glue("ST_GeomFromWKB(ST_AsWKB({col})) AS {col}")
            } else {
              col
            }
          })
          DBI::dbExecute(con, glue::glue(
            "CREATE {or_replace}TABLE {tbl_name} AS
             SELECT {paste(select_exprs, collapse = ', ')}
             FROM {read_expr}"))
        } else {
          DBI::dbExecute(con, glue::glue(
            "CREATE {or_replace}TABLE {tbl_name} AS
             SELECT * FROM {read_expr}"))
        }
      } else {
        DBI::dbExecute(con, glue::glue(
          "CREATE {or_replace}TABLE {tbl_name} AS
           SELECT * FROM {read_expr}"))
      }

      # convert WKB BLOB columns to GEOMETRY for spatial tables
      has_geom <- .convert_wkb_to_geometry(con, tbl_name, geom_tables)
    }

    n <- DBI::dbGetQuery(con, glue::glue(
      "SELECT COUNT(*) AS n FROM {tbl_name}"))$n
    suffix <- if (src$partitioned) " (partitioned)" else ""
    suffix <- paste0(suffix, if (as_view) " (VIEW)" else "")
    suffix <- paste0(suffix, if (has_geom) " (GEOMETRY)" else "")
    message(glue::glue("Loaded {tbl_name}: {n} rows{suffix}"))

    tibble::tibble(table = tbl_name, rows = n, has_geom = has_geom)
  })
}

#' Discover parquet sources in a directory
#'
#' @param parquet_dir Path to directory
#' @param tables Optional character vector to filter by table name
#' @return List of list(name, path, partitioned)
#' @keywords internal
.discover_parquet_sources <- function(parquet_dir, tables = NULL) {
  # single .parquet files
  pqt_files <- list.files(parquet_dir, pattern = "\\.parquet$", full.names = TRUE)
  # subdirectories containing .parquet files (hive-partitioned)
  pqt_dirs <- list.dirs(parquet_dir, recursive = FALSE, full.names = TRUE)
  pqt_dirs <- pqt_dirs[
    vapply(pqt_dirs, function(d)
      length(list.files(d, pattern = "\\.parquet$", recursive = TRUE)) > 0,
      logical(1))
  ]

  sources <- c(
    lapply(pqt_files, function(f) list(
      name        = tools::file_path_sans_ext(basename(f)),
      path        = f,
      partitioned = FALSE)),
    lapply(pqt_dirs, function(d) list(
      name        = basename(d),
      path        = d,
      partitioned = TRUE))
  )

  # filter to requested tables
  if (!is.null(tables)) {
    sources <- sources[vapply(sources, function(s) s$name %in% tables, logical(1))]
  }

  sources
}

#' Convert WKB BLOB columns to GEOMETRY for spatial tables
#'
#' @param con DBI connection
#' @param tbl_name Table name
#' @param geom_tables Character vector of table names with geometry columns
#' @return logical TRUE if geometry conversion was performed
#' @keywords internal
.convert_wkb_to_geometry <- function(con, tbl_name, geom_tables) {
  if (!tbl_name %in% geom_tables) return(FALSE)

  blob_cols <- DBI::dbGetQuery(con, glue::glue(
    "SELECT column_name FROM information_schema.columns
     WHERE table_name = '{tbl_name}'
       AND data_type = 'BLOB'
       AND column_name LIKE '%geom%'"))$column_name

  if (length(blob_cols) == 0) return(FALSE)

  for (gc in blob_cols) {
    tmp_col <- paste0(gc, "_tmp")
    # check if column already has GEOMETRY type with CRS (e.g., from parquet)
    col_type <- DBI::dbGetQuery(con, glue::glue(
      "SELECT data_type FROM information_schema.columns
       WHERE table_name = '{tbl_name}' AND column_name = '{gc}'"))$data_type
    if (grepl("^GEOMETRY", col_type)) {
      # already GEOMETRY (possibly with non-4326 CRS); normalize to EPSG:4326
      DBI::dbExecute(con, glue::glue(
        "ALTER TABLE {tbl_name} ADD COLUMN {tmp_col} GEOMETRY"))
      DBI::dbExecute(con, glue::glue(
        "UPDATE {tbl_name} SET {tmp_col} = ST_GeomFromWKB(ST_AsWKB({gc}))"))
      DBI::dbExecute(con, glue::glue(
        "ALTER TABLE {tbl_name} DROP COLUMN {gc}"))
      DBI::dbExecute(con, glue::glue(
        "ALTER TABLE {tbl_name} RENAME COLUMN {tmp_col} TO {gc}"))
    } else {
      # WKB BLOB -> GEOMETRY (EPSG:4326)
      DBI::dbExecute(con, glue::glue(
        "ALTER TABLE {tbl_name} ADD COLUMN {tmp_col} GEOMETRY"))
      DBI::dbExecute(con, glue::glue(
        "UPDATE {tbl_name} SET {tmp_col} = ST_GeomFromWKB({gc})"))
      DBI::dbExecute(con, glue::glue(
        "ALTER TABLE {tbl_name} DROP COLUMN {gc}"))
      DBI::dbExecute(con, glue::glue(
        "ALTER TABLE {tbl_name} RENAME COLUMN {tmp_col} TO {gc}"))
    }
  }

  TRUE
}

#' Load prior tables from GCS (fallback when local parquet missing)
#'
#' @param con DBI connection
#' @param parquet_dir Local parquet dir (used to find manifest.json)
#' @param gcs_prefix GCS prefix under calcofi-db bucket
#' @param tables Optional table filter
#' @param geom_tables Spatial table names
#' @param overwrite Replace existing tables
#' @return Tibble with table, rows, has_geom
#' @keywords internal
#' @importFrom jsonlite read_json
.load_prior_tables_gcs <- function(
    con,
    parquet_dir,
    gcs_prefix,
    tables      = NULL,
    geom_tables = c("grid", "site", "segment"),
    overwrite   = TRUE) {

  gcs_base <- glue::glue(
    "https://storage.googleapis.com/calcofi-db/{gcs_prefix}")

  # try to load manifest for table discovery and partitioning info
  manifest_path <- file.path(parquet_dir, "manifest.json")
  if (file.exists(manifest_path)) {
    manifest <- jsonlite::read_json(manifest_path)
  } else {
    # try downloading manifest from GCS
    manifest_url <- glue::glue("{gcs_base}/manifest.json")
    manifest <- tryCatch(
      jsonlite::read_json(manifest_url),
      error = function(e) NULL)
  }

  if (is.null(manifest)) {
    stop(glue::glue(
      "Cannot discover tables: no local dir and no manifest at GCS.\n",
      "parquet_dir: {parquet_dir}\n",
      "gcs_prefix: {gcs_prefix}"))
  }

  all_tables   <- unlist(manifest$tables)
  partitioned  <- unlist(manifest$partitioned %||% list())

  if (!is.null(tables)) {
    all_tables <- intersect(all_tables, tables)
  }

  # load httpfs extension
  load_duckdb_extension(con, "httpfs")

  or_replace <- if (overwrite) "OR REPLACE " else ""

  purrr::map_dfr(all_tables, function(tbl_name) {
    is_part <- tbl_name %in% partitioned

    if (is_part) {
      read_expr <- glue::glue(
        "read_parquet('{gcs_base}/{tbl_name}/**/*.parquet', ",
        "hive_partitioning = true)")
    } else {
      read_expr <- glue::glue(
        "read_parquet('{gcs_base}/{tbl_name}.parquet')")
    }

    tryCatch({
      DBI::dbExecute(con, glue::glue(
        "CREATE {or_replace}TABLE {tbl_name} AS
         SELECT * FROM {read_expr}"))

      has_geom <- .convert_wkb_to_geometry(con, tbl_name, geom_tables)

      n <- DBI::dbGetQuery(con, glue::glue(
        "SELECT COUNT(*) AS n FROM {tbl_name}"))$n
      suffix <- if (is_part) " (partitioned)" else ""
      suffix <- paste0(suffix, if (has_geom) " (WKB->GEOMETRY)" else "")
      message(glue::glue("Loaded {tbl_name} from GCS: {n} rows{suffix}"))

      tibble::tibble(table = tbl_name, rows = n, has_geom = has_geom)
    }, error = function(e) {
      warning(glue::glue("Failed to load {tbl_name} from GCS: {e$message}"))
      tibble::tibble(table = tbl_name, rows = NA_integer_, has_geom = FALSE)
    })
  })
}

#' Add provenance columns to a data frame
#'
#' Helper function to add provenance tracking columns to a data frame
#' before ingestion. Called internally by \code{ingest_to_working()}.
#'
#' @param data Data frame to modify
#' @param source_file Path to original CSV file in archive
#'   (e.g., "archive/2026-02-02_121557/swfsc/ichthyo/larva.csv")
#' @param source_row_start Starting row number (default: 1, typically 2 to skip header)
#' @param source_uuid_col Column name containing original UUIDs (optional).
#'   If provided, values are copied to `_source_uuid` column.
#'
#' @return Data frame with added provenance columns:
#'   \itemize{
#'     \item \code{_source_file} (character): Path to original CSV
#'     \item \code{_source_row} (integer): Row number in source file
#'     \item \code{_source_uuid} (character): Original record UUID if available
#'     \item \code{_ingested_at} (POSIXct): When row was ingested (UTC)
#'   }
#'
#' @export
#' @concept ducklake
#'
#' @examples
#' \dontrun{
#' data <- tibble::tibble(x = 1:3, y = letters[1:3])
#' data_prov <- add_provenance_columns(
#'   data        = data,
#'   source_file = "archive/2026-02-02_121557/swfsc/ichthyo/test.csv",
#'   source_row_start = 2)  # skip header
#'
#' # with source uuid column
#' data <- tibble::tibble(x = 1:3, uuid = c("a1", "b2", "c3"))
#' data_prov <- add_provenance_columns(
#'   data            = data,
#'   source_file     = "test.csv",
#'   source_uuid_col = "uuid")
#' }
#' @importFrom dplyr mutate row_number
add_provenance_columns <- function(
    data,
    source_file,
    source_row_start = 1,
    source_uuid_col  = NULL) {

  stopifnot(is.data.frame(data))
  stopifnot(is.character(source_file) && length(source_file) == 1)

  # add provenance columns

  data <- data |>
    dplyr::mutate(
      `_source_file`  = source_file,
      `_source_row`   = as.integer(dplyr::row_number() + source_row_start - 1),
      `_ingested_at`  = Sys.time())

  # add source uuid column

if (!is.null(source_uuid_col)) {
    if (!source_uuid_col %in% names(data)) {
      stop(glue::glue("Column '{source_uuid_col}' not found in data"))
    }
    data <- data |>
      dplyr::mutate(`_source_uuid` = as.character(.data[[source_uuid_col]]))
  } else {
    data <- data |>
      dplyr::mutate(`_source_uuid` = NA_character_)
  }

  data
}

#' Get Working DuckLake connection
#'
#' Connects to the internal Working DuckLake database used by ingestion workflows.
#' This database includes full provenance tracking columns (\code{_source_file},
#' \code{_source_row}, \code{_source_uuid}, \code{_ingested_at}).
#'
#' @param local_path Path to local DuckDB file for caching. If NULL, uses
#'   a temp directory path derived from the GCS location.
#' @param read_only Open in read-only mode (default: FALSE)
#' @param refresh Force re-download from GCS even if local cache exists (default: FALSE)
#' @param gcs_path GCS path to Working DuckLake (default: "gs://calcofi-db/ducklake/working/calcofi.duckdb")
#'
#' @return DuckDB connection object
#' @export
#' @concept ducklake
#'
#' @details
#' The Working DuckLake is stored at \code{gs://calcofi-db/ducklake/working/}.
#' It includes provenance columns and supports time travel queries via DuckLake.
#'
#' For read-only access to stable data, use \code{cc_get_db()} from the
#' \code{calcofi4r} package to access frozen releases instead.
#'
#' @examples
#' \dontrun{
#' con <- get_working_ducklake()
#' DBI::dbListTables(con)
#'
#' # read-only access
#' con <- get_working_ducklake(read_only = TRUE)
#' }
#' @importFrom glue glue
get_working_ducklake <- function(
    local_path = NULL,
    read_only  = FALSE,
    refresh    = FALSE,
    gcs_path   = "gs://calcofi-db/ducklake/working/calcofi.duckdb") {

  # set default local path in temp directory
  if (is.null(local_path)) {
    local_path <- file.path(tempdir(), "calcofi_working.duckdb")
  }

  # check if local file exists
  local_exists <- file.exists(local_path)

  # download from GCS if needed
  if (!local_exists || refresh) {
    tryCatch({
      message(glue::glue("Downloading Working DuckLake from {gcs_path}..."))
      get_gcs_file(gcs_path, local_path = local_path, overwrite = TRUE)
      message("Download complete.")
    }, error = function(e) {
      if (!local_exists) {
        message(glue::glue(
          "No existing Working DuckLake found in GCS. Creating new database at {local_path}"))
      } else {
        warning(glue::glue(
          "Failed to refresh from GCS: {e$message}. Using existing local file."))
      }
    })
  } else {
    message(glue::glue("Using cached Working DuckLake at {local_path}"))
  }

  # create connection
  con <- get_duckdb_con(
    path      = local_path,
    read_only = read_only)

  # ensure provenance columns schema exists for new databases
  if (!read_only) {
    # create metadata schema if it doesn't exist
    tryCatch({
      DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS _meta")
    }, error = function(e) NULL)
  }

  return(con)
}

#' Ingest data to Working DuckLake
#'
#' Ingests a data frame into the Working DuckLake with automatic provenance tracking.
#' Adds \code{_source_file}, \code{_source_row}, \code{_source_uuid}, and
#' \code{_ingested_at} columns to track data lineage.
#'
#' @param con DuckDB connection from \code{get_working_ducklake()}
#' @param data Data frame to ingest
#' @param table Target table name
#' @param source_file Path to original CSV file in Archive (for provenance).
#'   Should be the full path including archive timestamp
#'   (e.g., "archive/2026-02-02_121557/swfsc/ichthyo/larva.csv")
#' @param source_uuid_col Column name containing original UUIDs (optional).
#'   If provided, values are stored in \code{_source_uuid} for tracing back
#'   to source records.
#' @param source_row_start Starting row number in source file (default: 1)
#' @param mode Insert mode: "append", "replace", or "upsert" (default: "append")
#'   \itemize{
#'     \item \code{append}: Add rows to existing table
#'     \item \code{replace}: Drop and recreate table
#'     \item \code{upsert}: Update existing rows, insert new rows (requires \code{upsert_keys})
#'   }
#' @param upsert_keys Character vector of column names to use as keys for upsert mode
#'
#' @return Tibble with ingestion statistics:
#'   \itemize{
#'     \item \code{table}: Table name
#'     \item \code{mode}: Insert mode used
#'     \item \code{rows_input}: Number of rows in input data
#'     \item \code{rows_after}: Number of rows in table after ingestion
#'     \item \code{ingested_at}: Timestamp of ingestion
#'   }
#'
#' @export
#' @concept ducklake
#'
#' @examples
#' \dontrun{
#' con <- get_working_ducklake()
#'
#' larvae_data <- readr::read_csv("larvae.csv")
#' stats <- ingest_to_working(
#'   con         = con,
#'   data        = larvae_data,
#'   table       = "larva",
#'   source_file = "archive/2026-02-02_121557/swfsc/ichthyo/larva.csv",
#'   source_uuid_col = "larva_uuid",
#'   mode        = "replace")
#'
#' # append new data
#' new_data <- readr::read_csv("larvae_update.csv")
#' stats <- ingest_to_working(
#'   con         = con,
#'   data        = new_data,
#'   table       = "larva",
#'   source_file = "archive/2026-03-01_121557/swfsc/ichthyo/larva.csv",
#'   mode        = "append")
#' }
#' @importFrom DBI dbWriteTable dbGetQuery dbExecute
#' @importFrom glue glue
#' @importFrom tibble tibble
ingest_to_working <- function(
    con,
    data,
    table,
    source_file,
    source_uuid_col  = NULL,
    source_row_start = 1,
    mode             = "append",
    upsert_keys      = NULL) {

  stopifnot(is.data.frame(data))
  stopifnot(mode %in% c("append", "replace", "upsert"))
  if (mode == "upsert" && is.null(upsert_keys)) {
    stop("upsert_keys required for mode = 'upsert'")
  }

  # add provenance columns
  data_prov <- add_provenance_columns(
    data             = data,
    source_file      = source_file,
    source_row_start = source_row_start,
    source_uuid_col  = source_uuid_col)

  rows_input  <- nrow(data_prov)
  ingested_at <- Sys.time()

  # handle different modes
  if (mode == "replace") {
    # drop and recreate table
    tryCatch(
      DBI::dbExecute(con, glue::glue('DROP TABLE IF EXISTS "{table}"')),
      error = function(e) NULL)

    DBI::dbWriteTable(con, table, data_prov, overwrite = FALSE, append = FALSE)
    message(glue::glue("Created table '{table}' with {rows_input} rows"))

  } else if (mode == "append") {
    # check if table exists
    tables <- DBI::dbListTables(con)
    if (!table %in% tables) {
      DBI::dbWriteTable(con, table, data_prov, overwrite = FALSE, append = FALSE)
      message(glue::glue("Created table '{table}' with {rows_input} rows"))
    } else {
      DBI::dbWriteTable(con, table, data_prov, overwrite = FALSE, append = TRUE)
      message(glue::glue("Appended {rows_input} rows to table '{table}'"))
    }

  } else if (mode == "upsert") {
    # create temp table and merge
    temp_table <- paste0("_temp_", table, "_", format(Sys.time(), "%Y%m%d%H%M%S"))
    DBI::dbWriteTable(con, temp_table, data_prov, overwrite = TRUE)

    # build upsert query
    key_cols    <- paste(upsert_keys, collapse = ", ")
    all_cols    <- names(data_prov)
    update_cols <- setdiff(all_cols, upsert_keys)
    update_set  <- paste(
      sapply(update_cols, function(col) glue::glue("{col} = excluded.{col}")),
      collapse = ", ")

    # check if table exists
    tables <- DBI::dbListTables(con)
    if (!table %in% tables) {
      # just rename temp table
      DBI::dbExecute(con, glue::glue('ALTER TABLE "{temp_table}" RENAME TO "{table}"'))
      message(glue::glue("Created table '{table}' with {rows_input} rows"))
    } else {
      # perform upsert
      insert_cols <- paste(all_cols, collapse = ", ")
      DBI::dbExecute(con, glue::glue('
        INSERT INTO "{table}" ({insert_cols})
        SELECT {insert_cols} FROM "{temp_table}"
        ON CONFLICT ({key_cols}) DO UPDATE SET {update_set}'))

      # drop temp table
      DBI::dbExecute(con, glue::glue('DROP TABLE "{temp_table}"'))
      message(glue::glue("Upserted {rows_input} rows to table '{table}'"))
    }
  }

  # get final row count
  rows_after <- DBI::dbGetQuery(
    con,
    glue::glue('SELECT COUNT(*) as n FROM "{table}"'))$n

  # return statistics
  tibble::tibble(
    table       = table,
    mode        = mode,
    rows_input  = rows_input,
    rows_after  = rows_after,
    ingested_at = ingested_at)
}

#' Query Working DuckLake at a point in time
#'
#' Execute a query against the Working DuckLake filtering by ingestion timestamp.
#' This provides "time travel" capability to see data as it existed at a past time.
#'
#' @param con DuckDB connection from \code{get_working_ducklake()}
#' @param query SQL query string. The query can reference the \code{_ingested_at}
#'   column, but this function adds a filter automatically.
#' @param timestamp Timestamp for time travel (character "YYYY-MM-DD HH:MM:SS" or POSIXct).
#'   Filters to rows where \code{_ingested_at <= timestamp}.
#' @param table Optional table name. If provided and query is NULL, queries all columns.
#'
#' @return Query result as a tibble
#' @export
#' @concept ducklake
#'
#' @details
#' This function provides a simple time travel mechanism by filtering on the
#' \code{_ingested_at} provenance column. For full DuckLake time travel with
#' transactional consistency, use native DuckLake catalog features.
#'
#' @examples
#' \dontrun{
#' con <- get_working_ducklake(read_only = TRUE)
#'
#' # query larva table as of january 15th
#' old_data <- query_at_time(
#'   con       = con,
#'   table     = "larva",
#'   timestamp = "2026-01-15 00:00:00")
#'
#' # custom query with time filter
#' results <- query_at_time(
#'   con       = con,
#'   query     = "SELECT species_id, COUNT(*) as n FROM larva GROUP BY species_id",
#'   timestamp = "2026-01-15")
#' }
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue
#' @importFrom tibble as_tibble
query_at_time <- function(
    con,
    query     = NULL,
    timestamp,
    table     = NULL) {

  # convert timestamp to character
  if (inherits(timestamp, "POSIXt")) {
    timestamp <- format(timestamp, "%Y-%m-%d %H:%M:%S")
  }

  # build query
  if (is.null(query) && !is.null(table)) {
    query <- glue::glue("SELECT * FROM {table} WHERE _ingested_at <= '{timestamp}'")
  } else if (!is.null(query)) {
    # wrap user query with time filter using CTE
    # detect table name from query for simple cases
    query <- glue::glue("
      WITH time_filtered AS (
        {query}
      )
      SELECT * FROM time_filtered
      WHERE _ingested_at <= '{timestamp}'")
  } else {
    stop("Must provide either 'query' or 'table' parameter")
  }

  result <- DBI::dbGetQuery(con, query)
  tibble::as_tibble(result)
}

#' Save Working DuckLake to GCS
#'
#' Uploads the Working DuckLake database file to Google Cloud Storage.
#' This should be called after ingestion workflows complete to persist changes.
#'
#' @param con DuckDB connection from \code{get_working_ducklake()}
#' @param gcs_path GCS destination path (default: "gs://calcofi-db/ducklake/working/calcofi.duckdb")
#' @param checkpoint Run checkpoint before upload (default: TRUE)
#'
#' @return GCS URI of uploaded file
#' @export
#' @concept ducklake
#'
#' @examples
#' \dontrun{
#' con <- get_working_ducklake()
#' # ... perform ingestion ...
#' save_working_ducklake(con)
#' close_duckdb(con)
#' }
#' @importFrom DBI dbExecute dbGetInfo
#' @importFrom glue glue
save_working_ducklake <- function(
    con,
    gcs_path   = "gs://calcofi-db/ducklake/working/calcofi.duckdb",
    checkpoint = TRUE) {

  # get local database path
  db_info    <- DBI::dbGetInfo(con)
  local_path <- db_info$dbname

  if (local_path == ":memory:") {
    stop("Cannot save in-memory database. Use a file-based database.")
  }

  # checkpoint to flush all data
  if (checkpoint) {
    DBI::dbExecute(con, "CHECKPOINT")
  }

  # upload to GCS
  message(glue::glue("Uploading Working DuckLake to {gcs_path}..."))
  put_gcs_file(local_path, gcs_path)

  message("Upload complete")
  return(gcs_path)
}

#' Ingest Dataset into Working DuckLake
#'
#' High-level function to ingest all tables from a dataset into the Working
#' DuckLake. This wraps `transform_data()` and `ingest_to_working()` into a
#' single operation with proper provenance tracking.
#'
#' @param con DuckDB connection from `get_working_ducklake()`
#' @param d Data object from `read_csv_files()`
#' @param mode Insert mode: "replace" (default) or "append"
#' @param verbose Print progress messages (default: TRUE)
#'
#' @return Tibble with ingestion statistics for each table:
#'   \itemize{
#'     \item \code{tbl}: Original table name
#'     \item \code{tbl_new}: New table name after redefinition
#'     \item \code{gcs_path}: Source file path for provenance
#'     \item \code{rows_input}: Number of rows ingested
#'     \item \code{rows_after}: Total rows in table after ingestion
#'     \item \code{ingested_at}: Timestamp of ingestion
#'   }
#'
#' @export
#' @concept ducklake
#'
#' @examples
#' \dontrun{
#' # read and ingest dataset
#' d <- read_csv_files(
#'   provider     = "swfsc",
#'   dataset      = "ichthyo",
#'   dir_data     = "~/My Drive/projects/calcofi/data-public",
#'   metadata_dir = "metadata")
#'
#' con <- get_working_ducklake()
#' stats <- ingest_dataset(con, d, mode = "replace")
#' save_working_ducklake(con)
#' close_duckdb(con)
#' }
#' @importFrom dplyr mutate select left_join bind_rows
#' @importFrom purrr map2
#' @importFrom stringr str_detect
#' @importFrom tibble tibble
ingest_dataset <- function(
    con,
    d,
    mode    = "replace",
    verbose = TRUE) {


  # transform data using redefinitions
  if (verbose) message("Transforming data...")
  d_t <- transform_data(d, verbose = verbose)

  # collect stats for each table

  stats_list <- list()

  for (i in seq_len(nrow(d_t))) {
    tbl_old  <- d_t$tbl[i]
    tbl_new  <- d_t$tbl_new[i]
    data_new <- d_t$data_new[[i]]
    gcs_path <- d_t$gcs_path[i]

    if (is.null(data_new) || nrow(data_new) == 0) {
      if (verbose) message(glue::glue("  Skipping empty table: {tbl_new}"))
      next
    }

    # find uuid column if present
    uuid_cols <- names(data_new)[stringr::str_detect(names(data_new), "_uuid$")]
    uuid_col  <- if (length(uuid_cols) > 0) uuid_cols[1] else NULL

    if (verbose) message(glue::glue("  Ingesting: {tbl_new} ({nrow(data_new)} rows)"))

    # ingest to working ducklake
    stats <- ingest_to_working(
      con              = con,
      data             = data_new,
      table            = tbl_new,
      source_file      = gcs_path,
      source_uuid_col  = uuid_col,
      source_row_start = 2,  # skip header row
      mode             = mode)

    # add table mapping info
    stats$tbl     <- tbl_old
    stats$tbl_new <- tbl_new
    stats$gcs_path <- gcs_path

    stats_list[[i]] <- stats
  }

  # combine all stats
  result <- dplyr::bind_rows(stats_list) |>
    dplyr::select(tbl, tbl_new, gcs_path, rows_input, rows_after, ingested_at)

  if (verbose) {
    message(glue::glue(
      "Ingested {nrow(result)} tables, {sum(result$rows_input)} total rows"))
  }

  result
}

#' Strip provenance columns from data
#'
#' Removes the provenance tracking columns (\code{_source_file}, \code{_source_row},
#' \code{_source_uuid}, \code{_ingested_at}) from a data frame. Used when
#' preparing data for frozen releases.
#'
#' @param data Data frame with provenance columns
#'
#' @return Data frame without provenance columns
#' @export
#' @concept ducklake
#'
#' @examples
#' \dontrun{
#' # strip provenance for public release
#' clean_data <- strip_provenance_columns(data_with_prov)
#' }
#' @importFrom dplyr select
strip_provenance_columns <- function(data) {
  provenance_cols <- c("_source_file", "_source_row", "_source_uuid", "_ingested_at")
  cols_to_remove  <- intersect(provenance_cols, names(data))

  if (length(cols_to_remove) > 0) {
    data <- data |>
      dplyr::select(-dplyr::all_of(cols_to_remove))
  }

  data
}

#' List tables with provenance in Working DuckLake
#'
#' Lists all tables in the Working DuckLake along with provenance statistics
#' (ingestion times, source files, row counts).
#'
#' @param con DuckDB connection from \code{get_working_ducklake()}
#'
#' @return Tibble with columns:
#'   \itemize{
#'     \item \code{table}: Table name
#'     \item \code{rows}: Total row count
#'     \item \code{first_ingested}: Earliest ingestion timestamp
#'     \item \code{last_ingested}: Most recent ingestion timestamp
#'     \item \code{source_files}: Number of distinct source files
#'   }
#'
#' @export
#' @concept ducklake
#'
#' @examples
#' \dontrun{
#' con <- get_working_ducklake(read_only = TRUE)
#' tables <- list_working_tables(con)
#' tables
#' }
#' @importFrom DBI dbListTables dbGetQuery
#' @importFrom tibble tibble
#' @importFrom purrr map_dfr
list_working_tables <- function(con) {
  tables <- DBI::dbListTables(con)

  # filter out system tables
  tables <- tables[!grepl("^_", tables)]

  if (length(tables) == 0) {
    return(tibble::tibble(
      table          = character(),
      rows           = integer(),
      first_ingested = as.POSIXct(character()),
      last_ingested  = as.POSIXct(character()),
      source_files   = integer()))
  }

  purrr::map_dfr(tables, function(tbl) {
    # check if table has provenance columns
    cols <- DBI::dbGetQuery(
      con,
      glue::glue("SELECT column_name FROM information_schema.columns
                  WHERE table_name = '{tbl}'"))$column_name

    has_provenance <- "_ingested_at" %in% cols

    if (has_provenance) {
      stats <- DBI::dbGetQuery(con, glue::glue("
        SELECT
          COUNT(*) as rows,
          MIN(_ingested_at) as first_ingested,
          MAX(_ingested_at) as last_ingested,
          COUNT(DISTINCT _source_file) as source_files
        FROM {tbl}"))

      tibble::tibble(
        table          = tbl,
        rows           = stats$rows,
        first_ingested = stats$first_ingested,
        last_ingested  = stats$last_ingested,
        source_files   = stats$source_files)
    } else {
      row_count <- DBI::dbGetQuery(
        con,
        glue::glue("SELECT COUNT(*) as n FROM {tbl}"))$n

      tibble::tibble(
        table          = tbl,
        rows           = row_count,
        first_ingested = as.POSIXct(NA),
        last_ingested  = as.POSIXct(NA),
        source_files   = NA_integer_)
    }
  })
}
