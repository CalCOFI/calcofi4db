#' Record Schema Version
#'
#' Records a new schema version in the schema_version table with metadata about
#' the ingestion. This function should be called after successful completion of
#' all dataset ingestions and relationship creation.
#'
#' The function:
#' 1. Creates the schema_version table if it doesn't exist
#' 2. Reads any existing versions from inst/schema_version.csv
#' 3. Inserts the new version record into the database
#' 4. Appends the new version to inst/schema_version.csv
#'
#' @param con Database connection object
#' @param schema Schema name (default: "dev")
#' @param version Semantic version number (e.g., "1.0.0", "1.1.0")
#' @param description Description of changes in this version
#' @param script_permalink GitHub permalink to the versioned ingestion script
#'
#' @return Invisible NULL (version is recorded in database and CSV file)
#' @export
#' @concept version
#'
#' @examples
#' \dontrun{
#' record_schema_version(
#'   con = con,
#'   schema = "dev",
#'   version = "1.0.0",
#'   description = "Initial ingestion of NOAA CalCOFI Database",
#'   script_permalink = "https://github.com/CalCOFI/calcofi4db/blob/abc123/inst/ingest.qmd"
#' )
#' }
#' @importFrom DBI dbExecute dbWriteTable dbExistsTable
#' @importFrom readr read_csv write_csv
#' @importFrom dplyr bind_rows
#' @importFrom tibble tibble
#' @importFrom glue glue
#' @importFrom lubridate now
#' @importFrom here here
record_schema_version <- function(
    con,
    schema = "dev",
    version,
    description,
    script_permalink) {

  # validate inputs
  stopifnot(
    !missing(con),
    !missing(version),
    !missing(description),
    !missing(script_permalink))

  # path to schema version CSV file
  csv_path <- here::here("inst", "schema_version.csv")

  # create schema_version table if it doesn't exist
  table_name <- glue::glue("{schema}.schema_version")

  if (!DBI::dbExistsTable(con, DBI::Id(schema = schema, table = "schema_version"))) {
    DBI::dbExecute(con, glue::glue("
      CREATE TABLE {table_name} (
        version VARCHAR(50) NOT NULL UNIQUE,
        description TEXT,
        date_created TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        script_permalink TEXT
      )
    "))
    message(glue::glue("Created schema_version table in {schema} schema"))
  }

  # read existing versions from CSV if it exists
  if (file.exists(csv_path)) {
    d_existing <- readr::read_csv(csv_path, show_col_types = FALSE)
  } else {
    d_existing <- tibble::tibble(
      version          = character(),
      description      = character(),
      date_created     = lubridate::POSIXct(),
      script_permalink = character()
    )
  }

  # create new version record
  d_new <- tibble::tibble(
    version = version,
    description = description,
    date_created = lubridate::now(),
    script_permalink = script_permalink
  )

  # check if version already exists
  if (version %in% d_existing$version) {
    stop(glue::glue("Version {version} already exists. Please use a new version number."))
  }

  # insert into database
  DBI::dbWriteTable(
    con,
    DBI::Id(schema = schema, table = "schema_version"),
    d_new,
    append = TRUE,
    row.names = FALSE
  )

  # append to CSV file
  d_all <- dplyr::bind_rows(d_existing, d_new)
  readr::write_csv(d_all, csv_path)

  message(glue::glue(
    "Recorded schema version {version} in {schema}.schema_version table and {csv_path}"
  ))

  invisible(NULL)
}

#' Get Schema Version History
#'
#' Retrieves the schema version history from either the database or CSV file.
#'
#' @param con Database connection object (optional if from_csv = TRUE)
#' @param schema Schema name (default: "dev")
#' @param from_csv Logical, read from CSV file instead of database (default: FALSE)
#'
#' @return A tibble with version history
#' @export
#' @concept version
#'
#' @examples
#' \dontrun{
#' # Get from database
#' get_schema_versions(con, schema = "dev")
#'
#' # Get from CSV file
#' get_schema_versions(from_csv = TRUE)
#' }
#' @importFrom DBI dbReadTable Id
#' @importFrom readr read_csv
#' @importFrom dplyr arrange desc
#' @importFrom here here
get_schema_versions <- function(con = NULL, schema = "dev", from_csv = FALSE) {

  if (from_csv) {
    csv_path <- here::here("calcofi4db", "inst", "schema_version.csv")
    if (!file.exists(csv_path)) {
      stop(glue::glue("Schema version CSV file not found: {csv_path}"))
    }
    d <- readr::read_csv(csv_path, show_col_types = FALSE)
  } else {
    if (is.null(con)) {
      stop("Database connection 'con' is required when from_csv = FALSE")
    }
    d <- DBI::dbReadTable(con, DBI::Id(schema = schema, table = "schema_version"))
  }

  d |>
    dplyr::arrange(dplyr::desc(date_created))
}

#' Initialize Schema Version CSV
#'
#' Creates an empty schema_version.csv file in inst/ directory if it doesn't exist.
#' This is useful for starting a new versioning system.
#'
#' @return Invisible NULL (CSV file is created)
#' @export
#' @concept version
#'
#' @examples
#' \dontrun{
#' init_schema_version_csv()
#' }
#' @importFrom tibble tibble
#' @importFrom readr write_csv
#' @importFrom here here
#' @importFrom lubridate POSIXct
init_schema_version_csv <- function() {
  csv_path <- here::here("calcofi4db", "inst", "schema_version.csv")

  if (file.exists(csv_path)) {
    message(glue::glue("Schema version CSV already exists: {csv_path}"))
    return(invisible(NULL))
  }

  # create directory if it doesn't exist
  dir.create(dirname(csv_path), recursive = TRUE, showWarnings = FALSE)

  # create empty tibble with proper structure
  d <- tibble::tibble(
    version = character(),
    description = character(),
    date_created = lubridate::POSIXct(),
    script_permalink = character()
  )

  readr::write_csv(d, csv_path)
  message(glue::glue("Created empty schema version CSV: {csv_path}"))

  invisible(NULL)
}
