#' Read CSV Files and Extract Metadata
#'
#' Reads all CSV files in a directory, extracts metadata about tables and fields,
#' and writes this metadata to files for further processing.
#'
#' @param dir_csv Directory containing CSV files
#' @param dir_ingest Directory to store metadata files
#' @param create_dirs Whether to create directories if they don't exist
#'
#' @return A list with two data frames: tables and fields metadata
#' @export
#' @concept read
#' @importFrom purrr list_c map map_chr map_dbl map_int map2
#'
#' @examples
#' \dontrun{
#' csv_metadata <- read_csv_metadata(
#'   dir_csv = "/path/to/data/provider/dataset",
#'   dir_ingest = "/path/to/ingest/provider/dataset")
#' }
read_csv_metadata <- function(dir_csv, dir_ingest, create_dirs = TRUE) {
  # check if directories exist
  if (!dir.exists(dir_csv)) {
    stop(glue::glue("CSV directory does not exist: {dir_csv}"))
  }

  if (create_dirs && !dir.exists(dir_ingest)) {
    dir.create(dir_ingest, recursive = TRUE)
  }

  # Create paths for metadata files
  tbls_in_csv <- file.path(dir_ingest, "tbls_raw.csv")
  flds_in_csv <- file.path(dir_ingest, "flds_raw.csv")

  # Read data, extract field headers and file metadata
  d <- tibble::tibble(
    csv = list.files(dir_csv, pattern = "\\.csv$", full.names = TRUE)) |>
    dplyr::mutate(
      tbl           = tools::file_path_sans_ext(basename(csv)),
      file_info     = purrr::map(csv, file.info),
      file_size     = purrr::map_dbl(file_info, ~ .x$size),
      last_modified = purrr::map(file_info, ~ .x$mtime) |> purrr::list_c(),
      data          = purrr::map(csv, readr::read_csv),
      nrow          = purrr::map_int(data, nrow),
      ncol          = purrr::map_int(data, ncol),
      flds          = purrr::map2(tbl, data, \(tbl, data) {
        tibble::tibble(
          fld  = names(data),
          type = purrr::map_chr(fld, \(fld) class(data[[fld]])[1]))
      })) |>
    dplyr::select(-file_info) |>
    dplyr::relocate(tbl)

  # Extract and write tables metadata
  d_tbls_in <- d |>
    dplyr::select(tbl, nrow, ncol)
  readr::write_csv(d_tbls_in, tbls_in_csv)

  # Extract and write fields metadata
  d_flds_in <- d |>
    dplyr::select(tbl, flds) |>
    tidyr::unnest(flds)
  readr::write_csv(d_flds_in, flds_in_csv)

  # Return both metadata tables
  list(
    tables = d_tbls_in,
    fields = d_flds_in,
    data = d
  )
}

#' Read CSV Files and Their Metadata
#'
#' Reads CSV files from a directory or GCS archive and prepares them for
#' ingestion into a database. This function is the primary entry point for
#' the CalCOFI data ingestion workflow. It performs the following steps:
#'
#' 1. Reads CSV files from local directory or downloads from GCS archive
#' 2. If using local files, syncs to GCS archive for immutable provenance
#' 3. Extracts metadata about tables and fields from the CSV files
#' 4. Creates or reads redefinition files for table and field transformations
#'
#' The function returns a comprehensive data structure containing:
#' - Raw CSV data and metadata (d_csv)
#' - Source files with provenance tracking (source_files)
#' - Table redefinitions (d_tbls_rd) for renaming/describing tables
#' - Field redefinitions (d_flds_rd) for renaming/typing/transforming fields
#' - File paths used in the workflow
#'
#' @param provider Data provider (e.g., "swfsc.noaa.gov")
#' @param dataset Dataset name (e.g., "calcofi-db")
#' @param subdir Optional subdirectory (i.e.,
#'   {dir_data}/{provider}/{dataset}/{subdir}) for CSV files. Use for datasets
#'   organized with `raw/` or `derived/` subdirectories.
#' @param dir_data Directory path of CalCOFI base data folder available locally,
#'   with CSVs under {provider}/{dataset} directory. If NULL and gcs_archive
#'   is also NULL, will error. Set to NULL to use gcs_archive instead.
#' @param metadata_dir Directory containing redefinition metadata files
#'   (tbls_redefine.csv, flds_redefine.csv). The directory should be structured
#'   as {metadata_dir}/{provider}/{dataset}/. If NULL, falls back to the
#'   legacy location in calcofi4db/inst/ingest/ (deprecated).
#' @param gcs_archive GCS archive path to read from (for reproducibility).
#'   Can be either a timestamp (e.g., "2026-02-02_121557") or full path
#'   (e.g., "gs://calcofi-files-public/archive/2026-02-02_121557").
#'   If provided, downloads from archive instead of using local files.
#' @param gcs_bucket GCS bucket for archives (default: "calcofi-files-public")
#' @param archive_prefix Prefix for archive folder (default: "archive")
#' @param sync_archive Whether to sync local files to GCS archive (default: TRUE).
#'   Only applies when using dir_data (local files).
#' @param verbose Print detailed messages. Default: FALSE
#'
#' @return A list containing:
#'   \describe{
#'     \item{d_csv}{List with CSV data including:
#'       - data: tibble with columns (tbl, csv, file_size, last_modified,
#'         data, nrow, ncol, flds, gcs_path)
#'       - tables: summary of tables (tbl, nrow, ncol)
#'       - fields: summary of fields (tbl, fld, type)}
#'     \item{source_files}{Data frame for provenance tracking with columns:
#'       table, local_path, gcs_path, file_size, last_modified, nrow, ncol}
#'     \item{d_tbls_rd}{Table redefinition data frame with columns:
#'       tbl_old, tbl_new, tbl_description}
#'     \item{d_flds_rd}{Field redefinition data frame with columns:
#'       tbl_old, tbl_new, fld_old, fld_new, order_old, order_new,
#'       type_old, type_new, fld_description, notes, mutation}
#'     \item{paths}{List of file paths used in the workflow}
#'   }
#' @export
#' @concept read
#'
#' @examples
#' \dontrun{
#' # Read from local Google Drive mount (syncs to GCS archive)
#' d <- read_csv_files(
#'   provider     = "swfsc.noaa.gov",
#'   dataset      = "calcofi-db",
#'   dir_data     = "~/My Drive/projects/calcofi/data-public",
#'   metadata_dir = "metadata")
#'
#' # Read from specific GCS archive (for reproducibility)
#' d <- read_csv_files(
#'   provider     = "swfsc.noaa.gov",
#'   dataset      = "calcofi-db",
#'   gcs_archive  = "2026-02-02_121557",
#'   metadata_dir = "metadata")
#'
#' # Access the raw CSV data
#' d$d_csv$data
#'
#' # Check source file provenance
#' d$source_files
#' }
#' @importFrom stringr str_extract
#' @importFrom glue glue
read_csv_files <- function(
    provider,
    dataset,
    subdir         = NULL,
    dir_data       = NULL,
    metadata_dir   = NULL,
    gcs_archive    = NULL,
    gcs_bucket     = "calcofi-files-public",
    archive_prefix = "archive",
    sync_archive   = TRUE,
    verbose        = FALSE) {

  # determine source: GCS archive or local
  archive_info <- NULL

  if (!is.null(gcs_archive)) {
    # extract timestamp from full path if provided
    if (grepl("^gs://", gcs_archive)) {
      # extract timestamp from path like gs://bucket/archive/2026-02-02_121557/...
      archive_timestamp <- stringr::str_extract(
        gcs_archive,
        glue::glue("{archive_prefix}/([^/]+)"),
        group = 1)
    } else {
      archive_timestamp <- gcs_archive
    }

    # download from GCS archive
    message(glue::glue("Reading from GCS archive: {archive_timestamp}"))
    dir_csv <- download_archive(
      archive_timestamp = archive_timestamp,
      provider          = provider,
      dataset           = dataset,
      gcs_bucket        = gcs_bucket,
      archive_prefix    = archive_prefix)

    archive_info <- list(
      archive_timestamp = archive_timestamp,
      archive_path      = glue::glue("gs://{gcs_bucket}/{archive_prefix}/{archive_timestamp}/{provider}/{dataset}"),
      created_new       = FALSE,
      source            = "gcs")

  } else if (!is.null(dir_data)) {
    # use local files
    dir_csv <- ifelse(
      is.null(subdir),
      file.path(dir_data, provider, dataset),
      file.path(dir_data, provider, dataset, subdir))

    stopifnot(
      "Local CSV directory does not exist. Check dir_data path." = dir.exists(dir_csv))

    # sync to GCS archive if requested
    if (sync_archive) {
      sync_result <- sync_to_gcs_archive(
        dir_csv        = dir_csv,
        provider       = provider,
        dataset        = dataset,
        gcs_bucket     = gcs_bucket,
        archive_prefix = archive_prefix)

      archive_info <- list(
        archive_timestamp = sync_result$archive_timestamp,
        archive_path      = sync_result$archive_path,
        created_new       = sync_result$created_new,
        source            = "local")
    } else {
      archive_info <- list(
        archive_timestamp = NA_character_,
        archive_path      = NA_character_,
        created_new       = FALSE,
        source            = "local")
    }

  } else {
    stop("Either dir_data (local path) or gcs_archive (archive timestamp/path) must be provided")
  }

  # determine dir_ingest: use metadata_dir if provided, else fall back to legacy path
  if (!is.null(metadata_dir)) {
    # new preferred location: workflows/metadata/{provider}/{dataset}
    dir_ingest <- file.path(metadata_dir, provider, dataset)
    if (!dir.exists(dir_ingest)) {
      dir.create(dir_ingest, recursive = TRUE)
      message(glue::glue("Created metadata directory: {dir_ingest}"))
    }
  } else {
    # legacy location in calcofi4db package (deprecated)
    warning(
      "metadata_dir not specified. Using deprecated inst/ingest path. ",
      "Please migrate metadata to workflows/metadata/{provider}/{dataset}/")
    if (basename(here::here()) == "calcofi4db") {
      dir_ingest <- file.path(here::here(), "inst", "ingest", provider, dataset)
    } else {
      dir_ingest <- file.path(here::here(), "calcofi4db", "inst", "ingest", provider, dataset)
    }
  }

  stopifnot(dir.exists(dir_csv))

  # create ingest directory if it doesn't exist
  if (!dir.exists(dir_ingest)) {
    dir.create(dir_ingest, recursive = TRUE)
    message(glue::glue("Created ingest directory: {dir_ingest}"))
  }

  if (verbose) {
    message(glue::glue("Reading CSV data from: {dir_csv}"))
    message(glue::glue("Reading redefinition metadata from: {dir_ingest}"))
  }

  # metadata file paths
  tbls_in_csv <- file.path(dir_ingest, "tbls_raw.csv")
  flds_in_csv <- file.path(dir_ingest, "flds_raw.csv")
  tbls_rd_csv <- file.path(dir_ingest, "tbls_redefine.csv")
  flds_rd_csv <- file.path(dir_ingest, "flds_redefine.csv")

  # read CSV metadata and data
  d_csv <- read_csv_metadata(dir_csv, dir_ingest)

  # add GCS paths to data tibble for provenance tracking
  if (!is.na(archive_info$archive_path)) {
    d_csv$data <- d_csv$data |>
      dplyr::mutate(
        gcs_path = glue::glue("{archive_info$archive_path}/{basename(csv)}"))
  } else {
    d_csv$data <- d_csv$data |>
      dplyr::mutate(gcs_path = NA_character_)
  }

  # determine if redefinition files need to be created
  if (!file.exists(flds_rd_csv)) {
    # Create redefinition files
    create_redefinition_files(
      d_tbls_in   = d_csv$tables,
      d_flds_in   = d_csv$fields,
      d           = d_csv$data,
      tbls_rd_csv = tbls_rd_csv,
      flds_rd_csv = flds_rd_csv)
  }

  # Read redefinition files
  d_tbls_rd <- readr::read_csv(tbls_rd_csv)
  d_flds_rd <- readr::read_csv(flds_rd_csv)

  # create source_files dataframe for provenance tracking
  source_files <- d_csv$data |>
    dplyr::select(
      table         = tbl,
      local_path    = csv,
      gcs_path,
      file_size,
      last_modified,
      nrow,
      ncol)

  # return all data and metadata
  list(
    d_csv        = d_csv,
    source_files = source_files,
    d_tbls_rd    = d_tbls_rd,
    d_flds_rd    = d_flds_rd,
    paths = list(
      dir_csv     = dir_csv,
      dir_ingest  = dir_ingest,
      tbls_in_csv = tbls_in_csv,
      flds_in_csv = flds_in_csv,
      tbls_rd_csv = tbls_rd_csv,
      flds_rd_csv = flds_rd_csv))
}

#' Create Redefinition Files for Tables and Fields
#'
#' Creates redefinition files for tables and fields if they don't exist.
#'
#' @param d_tbls_in Data frame with table metadata
#' @param d_flds_in Data frame with field metadata
#' @param d Data frame with raw data
#' @param tbls_rd_csv Path to table redefinition CSV file
#' @param flds_rd_csv Path to field redefinition CSV file
#'
#' @return Invisible NULL (files are written to disk)
#' @export
#' @concept read
#'
#' @examples
#' \dontrun{
#' create_redefinition_files(
#'   d_tbls_in = tables_metadata,
#'   d_flds_in = fields_metadata,
#'   d = raw_data,
#'   tbls_rd_csv = "path/to/tbls_redefine.csv",
#'   flds_rd_csv = "path/to/flds_redefine.csv"
#' )
#' }
create_redefinition_files <- function(d_tbls_in, d_flds_in, d,
                                     tbls_rd_csv, flds_rd_csv) {
  # Create table redefinition file
  d_tbls_in |>
    dplyr::select(
      tbl_old = tbl,
      tbl_new = tbl) |>
    dplyr::mutate(
      tbl_description = "") |>
    readr::write_csv(tbls_rd_csv)

  # Create field redefinition file
  d_flds_in |>
    dplyr::group_by(tbl) |>
    dplyr::mutate(
      fld_new = janitor::make_clean_names(fld),
      order = 1:dplyr::n(),
      fld_description = "",
      units = "",
      notes = "",
      mutation = "",
      type_new = determine_field_types(d, tbl, fld)) |>
    dplyr::select(
      tbl_old = tbl, tbl_new = tbl,
      fld_old = fld, fld_new,
      order_old = order, order_new = order,
      type_old = type, type_new,
      fld_description, units, notes, mutation) |>
    readr::write_csv(flds_rd_csv)

  message(glue::glue(
    "Please update the table and field redefine tables before proceeding:
      {tbls_rd_csv}
      {flds_rd_csv}"))

  invisible(NULL)
}

#' Determine Field Types for Database
#'
#' Determines appropriate PostgreSQL data types for fields based on data values.
#'
#' @param d Data frame with raw data
#' @param tbl Table name
#' @param fld Field name
#'
#' @return Character vector of PostgreSQL data types
#' @export
#' @concept read
#'
#' @examples
#' \dontrun{
#' determine_field_types(raw_data, "species", "species_name")
#' }
determine_field_types <- function(d, tbl, fld) {
  purrr::map2_chr(tbl, fld, function(x, y) {
    v <- d |>
      dplyr::filter(tbl == x) |>
      dplyr::pull(data) |>
      purrr::pluck(1) |>
      dplyr::pull(y)

    determine_field_type(v)
  })
}
