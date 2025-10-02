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

#' Copy Database Schema
#'
#' Copies a database schema with all tables, data, sequences, and constraints
#' to a new schema. Useful for promoting dev to prod.
#'
#' @param con Database connection
#' @param from_schema Source schema name (default: "dev")
#' @param to_schema Destination schema name (default: "prod")
#' @param drop_existing Logical, drop existing destination schema (default: TRUE)
#' @param copy_fk Logical, copy foreign key constraints (default: TRUE)
#' @param verbose Logical, print progress messages (default: TRUE)
#'
#' @return List with copy summary statistics
#' @export
#' @concept utils
#'
#' @examples
#' \dontrun{
#' # Copy dev to prod
#' result <- copy_schema(con, from_schema = "dev", to_schema = "prod")
#'
#' # Check results
#' result$summary
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
#' @importFrom tibble tibble
copy_schema <- function(
    con,
    from_schema = "dev",
    to_schema = "prod",
    drop_existing = TRUE,
    copy_fk = TRUE,
    verbose = TRUE) {

  if (verbose) {
    message(glue::glue("
      ═══════════════════════════════════════════════════════════════
      Copying {from_schema} schema to {to_schema}
      ═══════════════════════════════════════════════════════════════
    "))
  }

  # drop existing schema if requested
  if (drop_existing) {
    DBI::dbExecute(con, glue::glue("DROP SCHEMA IF EXISTS {to_schema} CASCADE"))
    if (verbose) message(glue::glue("Dropped existing {to_schema} schema"))
  }

  # create new schema
  DBI::dbExecute(con, glue::glue("CREATE SCHEMA {to_schema}"))
  if (verbose) message(glue::glue("Created {to_schema} schema"))

  # get all tables in source schema
  from_tables <- DBI::dbGetQuery(con, glue::glue("
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = '{from_schema}'
    ORDER BY tablename
  "))$tablename

  if (verbose) message(glue::glue("Found {length(from_tables)} tables to copy"))

  # copy each table with data and constraints
  for (tbl in from_tables) {
    # copy table structure and data
    DBI::dbExecute(con, glue::glue(
      "CREATE TABLE {to_schema}.{tbl} (LIKE {from_schema}.{tbl} INCLUDING ALL)"
    ))

    DBI::dbExecute(con, glue::glue(
      "INSERT INTO {to_schema}.{tbl} SELECT * FROM {from_schema}.{tbl}"
    ))

    if (verbose) message(glue::glue("  ✓ Copied table: {tbl}"))
  }

  # copy sequences
  from_sequences <- DBI::dbGetQuery(con, glue::glue("
    SELECT sequence_name
    FROM information_schema.sequences
    WHERE sequence_schema = '{from_schema}'
  "))

  n_sequences <- 0
  if (nrow(from_sequences) > 0) {
    for (seq in from_sequences$sequence_name) {
      # get current sequence value
      curr_val <- DBI::dbGetQuery(con, glue::glue(
        "SELECT last_value FROM {from_schema}.{seq}"
      ))$last_value

      # create sequence in destination
      DBI::dbExecute(con, glue::glue(
        "CREATE SEQUENCE {to_schema}.{seq} START WITH {curr_val}"
      ))

      n_sequences <- n_sequences + 1
      if (verbose) message(glue::glue("  ✓ Copied sequence: {seq}"))
    }
  }

  # copy foreign key constraints
  n_fk_success <- 0
  n_fk_failed <- 0

  if (copy_fk) {
    fk_constraints <- DBI::dbGetQuery(con, glue::glue("
      SELECT
        tc.table_name,
        tc.constraint_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
      FROM information_schema.table_constraints AS tc
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = '{from_schema}'
    "))

    if (nrow(fk_constraints) > 0) {
      for (i in seq_len(nrow(fk_constraints))) {
        fk <- fk_constraints[i, ]

        tryCatch({
          DBI::dbExecute(con, glue::glue(
            "ALTER TABLE {to_schema}.{fk$table_name}
             ADD CONSTRAINT {fk$constraint_name}
             FOREIGN KEY ({fk$column_name})
             REFERENCES {to_schema}.{fk$foreign_table_name} ({fk$foreign_column_name})"
          ))
          n_fk_success <- n_fk_success + 1
          if (verbose) {
            message(glue::glue(
              "  ✓ Added FK: {fk$table_name}.{fk$column_name} -> {fk$foreign_table_name}.{fk$foreign_column_name}"
            ))
          }
        }, error = function(e) {
          n_fk_failed <- n_fk_failed + 1
          warning(glue::glue("  ✗ Failed to add FK {fk$constraint_name}: {e$message}"))
        })
      }
    }
  }

  # verify copy
  to_tables <- DBI::dbGetQuery(con, glue::glue("
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = '{to_schema}'
  "))$tablename

  # create summary
  summary <- tibble::tibble(
    from_schema = from_schema,
    to_schema = to_schema,
    tables_copied = length(to_tables),
    sequences_copied = n_sequences,
    fk_success = n_fk_success,
    fk_failed = n_fk_failed,
    copy_successful = length(from_tables) == length(to_tables)
  )

  if (verbose) {
    message(glue::glue("
      ═══════════════════════════════════════════════════════════════
      Schema Copy Summary
      ═══════════════════════════════════════════════════════════════
      {from_schema} tables: {length(from_tables)}
      {to_schema} tables: {length(to_tables)}
      Sequences copied: {n_sequences}
      Foreign keys: {n_fk_success} success, {n_fk_failed} failed
      Copy successful: {summary$copy_successful[1]}
      ═══════════════════════════════════════════════════════════════
    "))
  }

  invisible(list(
    summary = summary,
    from_tables = from_tables,
    to_tables = to_tables,
    success = summary$copy_successful[1]
  ))
}
