# workflow output functions for calcofi data ingestion
# writes parquet files + json manifests to GCS

#' Write Ingest Workflow Outputs to GCS
#'
#' Transforms data and writes parquet files + manifest to GCS ingest folder.
#' Each ingest workflow produces parquet files for its tables and a manifest
#' tracking provenance back to the source archive.
#'
#' @param data_info Output from `read_csv_files()`
#' @param provider Data provider (e.g., "swfsc.noaa.gov")
#' @param dataset Dataset name (e.g., "calcofi-db")
#' @param gcs_bucket GCS bucket for ingest outputs (default: "calcofi-db")
#' @param compression Parquet compression method (default: "snappy")
#'
#' @return List with:
#'   \itemize{
#'     \item \code{gcs_base}: Base GCS path for this ingest
#'     \item \code{parquet_paths}: Named list of GCS paths to parquet files
#'     \item \code{manifest_path}: GCS path to manifest.json
#'     \item \code{manifest}: The manifest data as a list
#'   }
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' d <- read_csv_files(
#'   provider     = "swfsc.noaa.gov",
#'   dataset      = "calcofi-db",
#'   metadata_dir = "metadata")
#'
#' result <- write_ingest_outputs(
#'   data_info  = d,
#'   provider   = "swfsc.noaa.gov",
#'   dataset    = "calcofi-db")
#'
#' # check manifest
#' result$manifest$tables
#' }
#' @importFrom glue glue
#' @importFrom arrow write_parquet
#' @importFrom jsonlite write_json
write_ingest_outputs <- function(
    data_info,
    provider,
    dataset,
    gcs_bucket  = "calcofi-db",
    compression = "snappy") {

  # transform data
  transformed <- transform_data(data_info)

  # base GCS path for this ingest
  gcs_base <- glue::glue("gs://{gcs_bucket}/ingest/{provider}/{dataset}")

  parquet_paths <- list()
  table_stats   <- list()

  message(glue::glue("Writing ingest outputs to {gcs_base}"))

  # write each table to GCS as parquet
  for (i in seq_len(nrow(transformed))) {
    tbl_name <- transformed$tbl_new[i]
    data     <- transformed$data_new[[i]]

    if (is.null(data) || nrow(data) == 0) {
      message(glue::glue("  Skipping empty table: {tbl_name}"))
      next
    }

    # write to temp file, then upload to GCS
    temp_path <- tempfile(fileext = ".parquet")
    arrow::write_parquet(data, temp_path, compression = compression)

    gcs_path <- glue::glue("{gcs_base}/parquet/{tbl_name}.parquet")
    put_gcs_file(temp_path, gcs_path)
    unlink(temp_path)

    parquet_paths[[tbl_name]] <- gcs_path
    table_stats[[tbl_name]]   <- list(
      rows = nrow(data),
      cols = ncol(data))

    message(glue::glue("  Uploaded: {tbl_name}.parquet ({nrow(data)} rows)"))
  }

  # create manifest with source file provenance
  manifest <- list(
    created_at    = format(Sys.time(), "%Y-%m-%dT%H:%M:%S"),
    provider      = provider,
    dataset       = dataset,
    source_files  = data_info$source_files,
    tables        = table_stats,
    parquet_files = parquet_paths)

  # write manifest to GCS
  manifest_path <- glue::glue("{gcs_base}/manifest.json")
  temp_manifest <- tempfile(fileext = ".json")
  jsonlite::write_json(manifest, temp_manifest, pretty = TRUE, auto_unbox = TRUE)
  put_gcs_file(temp_manifest, manifest_path)
  unlink(temp_manifest)

  message(glue::glue("  Uploaded: manifest.json"))

  list(
    gcs_base      = gcs_base,
    parquet_paths = parquet_paths,
    manifest_path = manifest_path,
    manifest      = manifest)
}

#' Read Ingest Manifest from GCS
#'
#' Retrieves the manifest.json for a specific ingest workflow output.
#'
#' @param provider Data provider (e.g., "swfsc.noaa.gov")
#' @param dataset Dataset name (e.g., "calcofi-db")
#' @param gcs_bucket GCS bucket (default: "calcofi-db")
#'
#' @return List containing manifest data
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' manifest <- read_ingest_manifest(
#'   provider = "swfsc.noaa.gov",
#'   dataset  = "calcofi-db")
#'
#' manifest$source_files
#' manifest$tables
#' }
#' @importFrom glue glue
#' @importFrom jsonlite read_json
read_ingest_manifest <- function(
    provider,
    dataset,
    gcs_bucket = "calcofi-db") {

  gcs_path   <- glue::glue("gs://{gcs_bucket}/ingest/{provider}/{dataset}/manifest.json")
  local_path <- get_gcs_file(gcs_path)
  jsonlite::read_json(local_path)
}

#' Read Ingest Parquet Table from GCS
#'
#' Downloads and reads a parquet table from an ingest output.
#'
#' @param provider Data provider (e.g., "swfsc.noaa.gov")
#' @param dataset Dataset name (e.g., "calcofi-db")
#' @param table Table name (e.g., "cruise")
#' @param gcs_bucket GCS bucket (default: "calcofi-db")
#'
#' @return Data frame (tibble) of the table
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' cruise <- read_ingest_parquet(
#'   provider = "swfsc.noaa.gov",
#'   dataset  = "calcofi-db",
#'   table    = "cruise")
#' }
#' @importFrom glue glue
#' @importFrom arrow read_parquet
read_ingest_parquet <- function(
    provider,
    dataset,
    table,
    gcs_bucket = "calcofi-db") {

  gcs_path   <- glue::glue("gs://{gcs_bucket}/ingest/{provider}/{dataset}/parquet/{table}.parquet")
  local_path <- get_gcs_file(gcs_path)
  arrow::read_parquet(local_path)
}

#' List Available Ingest Outputs
#'
#' Lists all providers and datasets that have ingest outputs on GCS.
#'
#' @param gcs_bucket GCS bucket (default: "calcofi-db")
#'
#' @return Tibble with provider, dataset, and manifest_path columns
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' ingests <- list_ingest_outputs()
#' }
#' @importFrom tibble tibble
#' @importFrom stringr str_extract
list_ingest_outputs <- function(gcs_bucket = "calcofi-db") {

  files <- list_gcs_files(gcs_bucket, prefix = "ingest/")

  if (nrow(files) == 0) {
    return(tibble::tibble(
      provider      = character(),
      dataset       = character(),
      manifest_path = character()))
  }

  # find manifest.json files
  manifests <- files |>
    dplyr::filter(grepl("manifest\\.json$", name)) |>
    dplyr::mutate(
      provider = stringr::str_extract(name, "ingest/([^/]+)/", group = 1),
      dataset  = stringr::str_extract(name, "ingest/[^/]+/([^/]+)/", group = 1),
      manifest_path = glue::glue("gs://{gcs_bucket}/{name}")) |>
    dplyr::select(provider, dataset, manifest_path)

  manifests
}

#' Integrate Ingest Outputs into Working DuckLake
#'
#' Reads parquet files from multiple ingest outputs and integrates them into
#' the Working DuckLake. Tables from different ingests with the same name are
#' combined (e.g., cruise tables from different sources). Provenance columns
#' (`_ingest_provider`, `_ingest_dataset`) are added to track origin.
#'
#' @param ingests List of ingest result lists (from `write_ingest_outputs()`)
#'   or a tibble from `list_ingest_outputs()`
#' @param gcs_bucket GCS bucket (default: "calcofi-db")
#' @param ducklake_path Path to Working DuckLake within bucket
#'   (default: "ducklake/working")
#'
#' @return List with DuckLake path and table info
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' # from targets pipeline
#' result <- integrate_to_working_ducklake(
#'   ingests = list(ingest_swfsc, ingest_bottle))
#'
#' # from existing GCS ingests
#' existing <- list_ingest_outputs()
#' result <- integrate_to_working_ducklake(existing)
#' }
#' @importFrom glue glue
#' @importFrom duckdb duckdb dbConnect dbWriteTable dbDisconnect
integrate_to_working_ducklake <- function(
    ingests,
    gcs_bucket    = "calcofi-db",
    ducklake_path = "ducklake/working") {

  # if ingests is a tibble from list_ingest_outputs(), read manifests
  if (is.data.frame(ingests)) {
    manifest_list <- lapply(seq_len(nrow(ingests)), function(i) {
      read_ingest_manifest(
        provider   = ingests$provider[i],
        dataset    = ingests$dataset[i],
        gcs_bucket = gcs_bucket)
    })
    ingests <- lapply(manifest_list, function(m) {
      list(manifest = m)
    })
  }

  # collect all parquet paths by table name
  all_tables <- list()
  for (ingest in ingests) {
    manifest <- ingest$manifest
    for (tbl_name in names(manifest$parquet_files)) {
      if (!tbl_name %in% names(all_tables)) {
        all_tables[[tbl_name]] <- list()
      }
      all_tables[[tbl_name]] <- c(
        all_tables[[tbl_name]],
        list(list(
          path     = manifest$parquet_files[[tbl_name]],
          provider = manifest$provider,
          dataset  = manifest$dataset)))
    }
  }

  # create local duckdb file
  local_db <- tempfile(fileext = ".duckdb")
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = local_db)
  on.exit(duckdb::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  table_info <- list()

  for (tbl_name in names(all_tables)) {
    sources <- all_tables[[tbl_name]]

    # read all sources for this table
    dfs <- lapply(sources, function(src) {
      local_pq <- get_gcs_file(src$path)
      df <- arrow::read_parquet(local_pq)
      # add source tracking
      df$`_ingest_provider` <- src$provider
      df$`_ingest_dataset`  <- src$dataset
      df
    })

    # combine (bind rows)
    combined <- dplyr::bind_rows(dfs)

    # write to duckdb
    duckdb::dbWriteTable(con, tbl_name, combined, overwrite = TRUE)

    table_info[[tbl_name]] <- list(
      rows    = nrow(combined),
      cols    = ncol(combined),
      sources = length(sources))

    message(glue::glue("  Created table: {tbl_name} ({nrow(combined)} rows from {length(sources)} source(s))"))
  }

  # close connection before upload
  duckdb::dbDisconnect(con, shutdown = TRUE)
  on.exit()  # clear the on.exit handler

  # upload duckdb to GCS (to ducklake/working/)
  # note: for full DuckLake integration, use get_working_ducklake() and ingest_to_working()
  gcs_db_path <- glue::glue("gs://{gcs_bucket}/{ducklake_path}/calcofi.duckdb")
  put_gcs_file(local_db, gcs_db_path)
  unlink(local_db)

  message(glue::glue("Uploaded database to {gcs_db_path}"))

  list(
    ducklake_path = glue::glue("gs://{gcs_bucket}/{ducklake_path}"),
    duckdb_path   = gcs_db_path,
    tables        = table_info)
}
