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
#'
#' @examples
#' \dontrun{
#' csv_metadata <- read_csv_metadata(
#'   dir_csv = "/path/to/data/provider/dataset", 
#'   dir_ingest = "/path/to/ingest/provider/dataset")
#' }
read_csv_metadata <- function(dir_csv, dir_ingest, create_dirs = TRUE) {
  # Check if directories exist
  if (!dir.exists(dir_csv)) {
    stop(glue::glue("CSV directory does not exist: {dir_csv}"))
  }
  
  if (create_dirs && !dir.exists(dirname(dir_ingest))) {
    dir.create(dirname(dir_ingest), recursive = TRUE)
  }
  
  # Create paths for metadata files
  tbls_in_csv <- file.path(dir_ingest, "tbls_raw.csv")
  flds_in_csv <- file.path(dir_ingest, "flds_raw.csv")
  
  # Read data, extract field headers
  d <- tibble::tibble(
    csv = list.files(dir_csv, pattern = "\\.csv$", full.names = TRUE)) |>
    dplyr::mutate(
      tbl = tools::file_path_sans_ext(basename(csv)),
      data = purrr::map(csv, readr::read_csv),
      nrow = purrr::map_int(data, nrow),
      ncol = purrr::map_int(data, ncol),
      flds = purrr::map2(tbl, data, \(tbl, data) {
        tibble::tibble(
          fld = names(data),
          type = purrr::map_chr(fld, \(fld) class(data[[fld]])[1])
        )
      })) |>
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

#' Load CSV Files and Their Metadata
#'
#' Reads CSV files from a directory and prepares them for ingestion into a database.
#'
#' @param provider Data provider (e.g., "swfsc.noaa.gov")
#' @param dataset Dataset name (e.g., "calcofi-db")
#' @param dir_data Base directory for data
#' @param dir_googledata Optional Google Drive data directory
#' @param use_gdrive Whether to query Google Drive for metadata
#' @param email Google Drive authentication email (if use_gdrive=TRUE)
#'
#' @return A list containing table data and metadata
#' @export
#'
#' @examples
#' \dontrun{
#' dataset_info <- load_csv_files(
#'   provider = "swfsc.noaa.gov",
#'   dataset = "calcofi-db",
#'   dir_data = "/path/to/data",
#'   use_gdrive = TRUE,
#'   email = "user@example.com"
#' )
#' }
load_csv_files <- function(provider, dataset, dir_data, 
                          dir_googledata = NULL, use_gdrive = FALSE, 
                          email = NULL) {
  
  # Define paths
  dir_csv <- file.path(dir_data, provider, dataset)
  dir_ingest <- file.path(here::here(), "workflows/ingest", provider, dataset)
  
  # Get workflow info
  workflow_info <- get_workflow_info(provider, dataset)
  
  # Input/output tables (version controlled)
  tbls_in_csv <- file.path(dir_ingest, "tbls_raw.csv")
  flds_in_csv <- file.path(dir_ingest, "flds_raw.csv")
  
  # Rename tables (version controlled)
  tbls_rn_csv <- file.path(dir_ingest, "tbls_redefine.csv")
  flds_rn_csv <- file.path(dir_ingest, "flds_redefine.csv")
  
  # Google Drive metadata
  d_gdir_data <- NULL
  
  # Query Google Drive if requested
  if (use_gdrive) {
    if (is.null(dir_googledata)) {
      stop("dir_googledata must be provided when use_gdrive=TRUE")
    }
    
    if (is.null(email)) {
      stop("email must be provided when use_gdrive=TRUE")
    }
    
    # Authenticate with Google Drive
    googledrive::drive_auth(
      email = email,
      scopes = "drive")
    
    # Get file metadata from Google Drive
    d_gdir_data <- googledrive::drive_ls(dir_googledata) |>
      dplyr::mutate(
        web_view_link = purrr::map_chr(drive_resource, \(x) x$webViewLink),
        created_time = purrr::map_chr(drive_resource, \(x) x$createdTime) |>
          lubridate::as_datetime()) |>
      dplyr::filter(
        stringr::str_detect(name, "\\.csv$"))
  }
  
  # Read CSV metadata files
  csv_metadata <- read_csv_metadata(dir_csv, dir_ingest)
  
  # Determine if redefinition files need to be created
  if (!file.exists(flds_rn_csv)) {
    # Create redefinition files
    create_redefinition_files(
      d_tbls_in = csv_metadata$tables,
      d_flds_in = csv_metadata$fields,
      d = csv_metadata$data,
      tbls_rn_csv = tbls_rn_csv,
      flds_rn_csv = flds_rn_csv
    )
  }
  
  # Read redefinition files
  d_tbls_rn <- readr::read_csv(tbls_rn_csv)
  d_flds_rn <- readr::read_csv(flds_rn_csv)
  
  # Return all data and metadata
  list(
    csv_metadata = csv_metadata,
    d_tbls_rn = d_tbls_rn,
    d_flds_rn = d_flds_rn,
    d_gdir_data = d_gdir_data,
    workflow_info = workflow_info,
    paths = list(
      dir_csv = dir_csv,
      dir_ingest = dir_ingest,
      tbls_in_csv = tbls_in_csv,
      flds_in_csv = flds_in_csv,
      tbls_rn_csv = tbls_rn_csv,
      flds_rn_csv = flds_rn_csv
    )
  )
}

#' Create Redefinition Files for Tables and Fields
#'
#' Creates redefinition files for tables and fields if they don't exist.
#'
#' @param d_tbls_in Data frame with table metadata
#' @param d_flds_in Data frame with field metadata
#' @param d Data frame with raw data
#' @param tbls_rn_csv Path to table redefinition CSV file
#' @param flds_rn_csv Path to field redefinition CSV file
#'
#' @return Invisible NULL (files are written to disk)
#' @export
#'
#' @examples
#' \dontrun{
#' create_redefinition_files(
#'   d_tbls_in = tables_metadata,
#'   d_flds_in = fields_metadata,
#'   d = raw_data,
#'   tbls_rn_csv = "path/to/tbls_redefine.csv",
#'   flds_rn_csv = "path/to/flds_redefine.csv"
#' )
#' }
create_redefinition_files <- function(d_tbls_in, d_flds_in, d, 
                                     tbls_rn_csv, flds_rn_csv) {
  # Create table redefinition file
  d_tbls_in |>
    dplyr::select(
      tbl_old = tbl,
      tbl_new = tbl) |>
    dplyr::mutate(
      tbl_description = "") |>
    readr::write_csv(tbls_rn_csv)
  
  # Create field redefinition file
  d_flds_in |>
    dplyr::group_by(tbl) |>
    dplyr::mutate(
      fld_new = janitor::make_clean_names(fld),
      order = 1:dplyr::n(),
      fld_description = "",
      notes = "",
      mutation = "",
      type_new = determine_field_types(d, tbl, fld)) |>
    dplyr::select(
      tbl_old = tbl, tbl_new = tbl,
      fld_old = fld, fld_new,
      order_old = order, order_new = order,
      type_old = type, type_new,
      fld_description, notes, mutation) |>
    readr::write_csv(flds_rn_csv)
  
  message(glue::glue(
    "Please update the table and field rename tables before proceeding:
      {tbls_rn_csv}
      {flds_rn_csv}"))
  
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
#'
#' @examples
#' \dontrun{
#' determine_field_types(raw_data, "species", "species_name")
#' }
determine_field_types <- function(d, tbl, fld) {
  purrr::map2_chr(tbl, fld, function(x, y) {
    v <- d |>
      dplyr::filter(tbl == x) |>
      purrr::pull(data) |>
      purrr::pluck(1) |>
      purrr::pull(y)
    
    determine_field_type(v)
  })
}