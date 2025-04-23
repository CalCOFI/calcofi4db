# Utility functions for database operations

#' Get a database connection to the CalCOFI PostgreSQL database
#'
#' @param schemas Character vector of schema search paths (default: "public")
#' @param host Database host (default: automatically detected based on system)
#' @param port Database port (default: 5432)
#' @param dbname Database name (default: "gis")
#' @param user Database user (default: "admin")
#' @param password_file Path to file containing database password (default: system-dependent location)
#'
#' @return A database connection object
#' @export
#' @concept utils
get_db_con <- function(
    schemas = "public",
    host = NULL,
    port = 5432,
    dbname = "gis",
    user = "admin",
    password_file = NULL) {

  # Determine if running on server or local system
  is_server <- Sys.info()[["sysname"]] == "Linux"

  # Set default host if not provided
  if (is.null(host)) {
    host <- ifelse(
      is_server,
      "postgis",   # from rstudio to postgis docker container on server
      "localhost") # from laptop to locally tunneled connection to db container on server
  }

  # Set default password file if not provided
  if (is.null(password_file)) {
    password_file <- ifelse(
      is_server,
      "/share/.calcofi_db_pass.txt",
      "~/.calcofi_db_pass.txt")
  }

  # Check if password file exists
  stopifnot(file.exists(password_file))

  # Create and return connection
  DBI::dbConnect(
    RPostgres::Postgres(),
    dbname   = dbname,
    host     = host,
    port     = port,
    user     = user,
    password = readLines(password_file),
    options  = glue::glue("-c search_path={paste(schemas, collapse = ',')}"))
}

#' Get primary key constraints for a table
#'
#' @param con Database connection
#' @param tbl Table name
#'
#' @return Character vector of column names that are primary keys
#'
#' @noRd
tbl_pkeys <- function(con, tbl) {
  dbGetQuery(con, glue::glue("
    SELECT a.attname as column_name
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid
    AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = '{tbl}'::regclass
    AND i.indisprimary;
  "))$column_name
}

#' Add a foreign key constraint to a table
#'
#' @param con Database connection
#' @param tbl_m Table with the foreign key
#' @param fld_m Field in tbl_m that is the foreign key
#' @param tbl_1 Referenced table
#' @param fld_1 Field in tbl_1 that is referenced, defaults to fld_m
#' @param schema Database schema
#'
#' @return Result of dbExecute
#'
#' @noRd
add_fkey <- function(con, tbl_m, fld_m, tbl_1, fld_1 = fld_m, schema = "public") {
  q <- glue::glue(
    "ALTER TABLE {schema}.{tbl_m} ADD FOREIGN KEY ({fld_m})
    REFERENCES {schema}.{tbl_1} ({fld_1})")
  DBI::dbExecute(con, q)
}

#' Create a table index
#'
#' @param con Database connection
#' @param tbl Table name
#' @param fld Field to index
#' @param type Type of index (default: btree, or 'gist' for spatial)
#'
#' @return Result of dbExecute
#'
#' @noRd
create_index <- function(con, tbl, fld, type = "btree") {
  q <- glue::glue("CREATE INDEX IF NOT EXISTS idx_{tbl}_{fld} ON {tbl} USING {type} ({fld})")
  DBI::dbExecute(con, q)
}

#' Generate a workflow path from provider and dataset
#'
#' @param provider Data provider name
#' @param dataset Dataset name
#'
#' @return A list with workflow name, qmd path, and URL
#'
#' @noRd
get_workflow_info <- function(provider, dataset) {
  workflow <- glue::glue("ingest_{provider}_{dataset}")
  workflow_qmd <- glue::glue("{workflow}.qmd")
  workflow_url <- glue::glue("https://calcofi.io/workflows/{workflow}.html")

  list(
    workflow = workflow,
    workflow_qmd = workflow_qmd,
    workflow_url = workflow_url
  )
}

#' Determine field type based on sample data
#'
#' @param value Sample data value
#' @param cl R class of value
#'
#' @return PostgreSQL data type as character string
#'
#' @noRd
determine_field_type <- function(value, cl = class(value)[1]) {
  if (cl == "character") {
    if (all(!is.na(as.UUID(value)))) {
      return("uuid")
    }
    return("varchar")
  }

  # Integer types
  if (cl == "numeric" && all(value %% 1 == 0, na.rm = TRUE)) {
    # https://www.postgresql.org/docs/current/datatype-numeric.html
    if (min(value, na.rm = TRUE) >= -32768 &&
        max(value, na.rm = TRUE) <= 32767) {
      return("smallint")
    }
    if (min(value, na.rm = TRUE) >= -2147483648 &&
        max(value, na.rm = TRUE) <= 2147483647) {
      return("integer")
    }
    if (min(value, na.rm = TRUE) >= -9223372036854775808 &&
        max(value, na.rm = TRUE) <= 9223372036854775807) {
      return("bigint")
    }
  }

  # Date/time types
  if (cl == "POSIXct") {
    return("timestamp")
  }
  if (cl == "Date") {
    return("date")
  }

  # Default: use R class
  return(cl)
}
