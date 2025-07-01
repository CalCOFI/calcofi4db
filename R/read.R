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

#' Read CSV Files and Their Metadata
#'
#' Reads CSV files from a directory and prepares them for ingestion into a
#' database. This function is the primary entry point for the CalCOFI data
#' ingestion workflow. It performs the following steps:
#' 
#' 1. Reads all CSV files from the specified provider/dataset directory
#' 2. Extracts metadata about tables and fields from the CSV files
#' 3. Creates or reads redefinition files for table and field transformations
#' 4. Optionally queries Google Drive for file metadata (creation dates, etc.)
#' 
#' The function returns a comprehensive data structure containing:
#' - Raw CSV data and metadata (d_csv)
#' - Table redefinitions (d_tbls_rd) for renaming/describing tables
#' - Field redefinitions (d_flds_rd) for renaming/typing/transforming fields
#' - Google Drive metadata if requested (d_gdata)
#' - Workflow information and file paths
#'
#' @param provider Data provider (e.g., "swfsc.noaa.gov")
#' @param dataset Dataset name (e.g., "calcofi-db")
#' @param dir_data directory path of CalCOFI base data folder available locally,
#'   with CSVs under {provider}/{dataset} directory. Default: "~/My
#'   Drive/projects/calcofi/data"
#' @param url_gdata URL of CalCOFI base data folder in Google Drive (with CSVs
#'   under {provider}/{dataset} directory) with metadata information on CSVs.
#'   Default: [data - Google
#'   Drive](https://drive.google.com/drive/u/0/folders/1xxdWa4mWkmfkJUQsHxERTp9eBBXBMbV7)
#' @param use_gdrive Whether to query Google Drive for metadata. Default: TRUE
#' @param email Google Drive authentication email (if use_gdrive=TRUE). Default:
#'   "ben@ecoquants.com"
#'
#' @return A list containing:
#'   \describe{
#'     \item{d_csv}{List with CSV data including:
#'       - data: tibble with columns (tbl, csv, data, nrow, ncol, flds)
#'       - tables: summary of tables (tbl, nrow, ncol)
#'       - fields: summary of fields (tbl, fld, type)}
#'     \item{d_gdata}{Google Drive metadata (if use_gdrive=TRUE) including
#'       file names, IDs, modification times, and web links}
#'     \item{d_tbls_rd}{Table redefinition data frame with columns:
#'       tbl_old, tbl_new, tbl_description}
#'     \item{d_flds_rd}{Field redefinition data frame with columns:
#'       tbl_old, tbl_new, fld_old, fld_new, order_old, order_new,
#'       type_old, type_new, fld_description, notes, mutation}
#'     \item{workflow_info}{Information about the workflow including
#'       workflow name, QMD file path, and URL}
#'     \item{paths}{List of file paths used in the workflow}
#'   }
#' @export
#' @concept read
#'
#' @examples
#' \dontrun{
#' # Basic usage
#' d <- read_csv_files(
#'   provider = "swfsc.noaa.gov",
#'   dataset  = "calcofi-db")
#' 
#' # Access the raw CSV data
#' d$d_csv$data
#' 
#' # Check table redefinitions
#' d$d_tbls_rd
#' 
#' # Check field redefinitions
#' d$d_flds_rd
#' 
#' # Without Google Drive metadata
#' d <- read_csv_files(
#'   provider = "swfsc.noaa.gov",
#'   dataset  = "calcofi-db",
#'   use_gdrive = FALSE)
#' }
read_csv_files <- function(
    provider,
    dataset,
    dir_data    = "~/My Drive/projects/calcofi/data",
    url_gdata   = "https://drive.google.com/drive/u/0/folders/1xxdWa4mWkmfkJUQsHxERTp9eBBXBMbV7",
    use_gdrive  = TRUE,
    email       = "ben@ecoquants.com") {

  # Define paths
  dir_csv    <- file.path(dir_data, provider, dataset)
  dir_ingest <- file.path(here::here(), "ingest", provider, dataset)
  stopifnot(dir.exists(dir_csv))

  # Get workflow info
  workflow_info <- get_workflow_info(provider, dataset)
  stopifnot(file.exists(file.path(here::here(), workflow_info$workflow_qmd)))
  path_workflow_qmd <- file.path(here::here(), workflow_info$workflow_qmd)
  if (!file.exists(path_workflow_qmd))
    stop(glue::glue(
      "Workflow file does not exist: {path_workflow_qmd}.
       This function should be called from inside the workflow."))

  # Input/output tables (version controlled)
  tbls_in_csv <- file.path(dir_ingest, "tbls_raw.csv")
  flds_in_csv <- file.path(dir_ingest, "flds_raw.csv")

  # redefine tables (version controlled)
  tbls_rd_csv <- file.path(dir_ingest, "tbls_redefine.csv")
  flds_rd_csv <- file.path(dir_ingest, "flds_redefine.csv")

  # Google Drive metadata
  d_gdir_data <- NULL

  # Query Google Drive if requested
  if (use_gdrive) {

    if (is.null(url_gdata)) {
      stop("url_gdata must be provided when use_gdrive=TRUE")
    }

    if (is.null(email)) {
      stop("email must be provided when use_gdrive=TRUE")
    }

    # Authenticate with Google Drive
    googledrive::drive_auth(
      email  = email,
      scopes = "drive")

    # check/get Google Drive calcofi/data/provider/dataset folder
    g_provider <- drive_ls(url_gdata, q = glue::glue("name = '{provider}'"))
    if (!nrow(g_provider) != 0)
      stop(glue::glue("No unique folder found in Google Drive url_gdata for provider: {provider}"))
    g_dataset <- drive_ls(g_provider$id, q = glue::glue("name = '{dataset}'"))
    if (!nrow(g_dataset) != 0)
      stop(glue::glue("No unique folder found in Google Drive url_gdata for provider/dataset: {provider}/{dataset}"))

    # get metadata for CSV files in Google Drive calcofi/data/provider/dataset folder
    d_gdata <- googledrive::drive_ls(g_dataset, q = "name contains '.csv$'") |>
      dplyr::mutate(
        web_view_link = purrr::map_chr(drive_resource, \(x) x$webViewLink),
        created_time  = purrr::map_chr(drive_resource, \(x) x$createdTime) |>
          lubridate::as_datetime())
  }

  # Read CSV metadata and data
  d_csv <- read_csv_metadata(dir_csv, dir_ingest)

  # Determine if redefinition files need to be created
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

  # Return all data and metadata
  list(
    d_csv         = d_csv,
    d_gdata       = d_gdata,
    d_tbls_rd     = d_tbls_rd,
    d_flds_rd     = d_flds_rd,
    workflow_info = workflow_info,
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
      notes = "",
      mutation = "",
      type_new = determine_field_types(d, tbl, fld)) |>
    dplyr::select(
      tbl_old = tbl, tbl_new = tbl,
      fld_old = fld, fld_new,
      order_old = order, order_new = order,
      type_old = type, type_new,
      fld_description, notes, mutation) |>
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
      purrr::pull(data) |>
      purrr::pluck(1) |>
      purrr::pull(y)

    determine_field_type(v)
  })
}
