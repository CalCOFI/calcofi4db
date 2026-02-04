# validation functions for calcofi data quality checks

#' Validate Foreign Key References
#'
#' Checks that all values in a foreign key column exist in the referenced table's
#' primary key column. Returns a tibble of orphan rows that fail the check.
#'
#' @param con DuckDB connection
#' @param data_tbl Table name or lazy tbl containing foreign key column
#' @param fk_col Name of the foreign key column to validate
#' @param ref_tbl Name of the referenced table containing the primary key
#' @param ref_col Name of the primary key column in the referenced table
#' @param label Optional label for error messages (default: "{data_tbl}.{fk_col}")
#'
#' @return Tibble with orphan rows (rows where fk_col value not in ref_tbl.ref_col).
#'   Contains all columns from data_tbl where the reference fails.
#' @export
#' @concept validate
#'
#' @examples
#' \dontrun{
#' con <- get_duckdb_con()
#' orphans <- validate_fk_references(
#'   con     = con,
#'   data_tbl = "ichthyo",
#'   fk_col   = "species_id",
#'   ref_tbl  = "species",
#'   ref_col  = "species_id")
#'
#' if (nrow(orphans) > 0) {
#'   warning(glue("Found {nrow(orphans)} orphan species_id values"))
#' }
#' }
#' @importFrom DBI dbGetQuery
#' @importFrom dplyr tbl anti_join collect
#' @importFrom glue glue
#' @importFrom tibble as_tibble
validate_fk_references <- function(
    con,
    data_tbl,
    fk_col,
    ref_tbl,
    ref_col,
    label = NULL) {

  if (is.null(label)) {
    label <- glue::glue("{data_tbl}.{fk_col}")
  }

  # get orphan rows using anti_join
  orphans <- dplyr::tbl(con, data_tbl) |>
    dplyr::anti_join(
      dplyr::tbl(con, ref_tbl),
      by = stats::setNames(ref_col, fk_col)) |>
    dplyr::collect()

  if (nrow(orphans) > 0) {
    message(glue::glue(
      "Validation: {label} has {nrow(orphans)} orphan rows ",
      "(values not in {ref_tbl}.{ref_col})"))
  }

  tibble::as_tibble(orphans)
}

#' Validate Lookup Values Exist
#'
#' Checks that all values in a column exist in the lookup table for a given
#' lookup type. Returns a tibble of rows with invalid lookup values.
#'
#' @param con DuckDB connection
#' @param data_tbl Table name containing values to validate
#' @param value_col Name of the column containing values to check
#' @param lookup_type Type of lookup to validate against (e.g., "egg_stage", "larva_stage")
#' @param lookup_tbl Name of the lookup table (default: "lookup")
#' @param lookup_type_col Column in lookup table containing the type (default: "lookup_type")
#' @param lookup_num_col Column in lookup table containing the numeric key values (default: "lookup_num")
#'
#' @return Tibble with rows containing invalid lookup values
#' @export
#' @concept validate
#'
#' @examples
#' \dontrun{
#' invalid_stages <- validate_lookup_values(
#'   con         = con,
#'   data_tbl    = "ichthyo",
#'   value_col   = "measurement_value",
#'   lookup_type = "egg_stage")
#' }
#' @importFrom DBI dbGetQuery
#' @importFrom dplyr tbl filter anti_join collect distinct pull
#' @importFrom glue glue
validate_lookup_values <- function(
    con,
    data_tbl,
    value_col,
    lookup_type,
    lookup_tbl      = "lookup",
    lookup_type_col = "lookup_type",
    lookup_num_col  = "lookup_num") {

  # get valid lookup values for this type
  valid_values <- dplyr::tbl(con, lookup_tbl) |>
    dplyr::filter(.data[[lookup_type_col]] == lookup_type) |>
    dplyr::select(dplyr::all_of(lookup_num_col)) |>
    dplyr::collect() |>
    dplyr::pull(lookup_num_col)

  # build query to find invalid values
  invalid_rows <- dplyr::tbl(con, data_tbl) |>
    dplyr::filter(!is.na(.data[[value_col]])) |>
    dplyr::filter(!(.data[[value_col]] %in% valid_values)) |>
    dplyr::collect()

  if (nrow(invalid_rows) > 0) {
    invalid_vals <- invalid_rows |>
      dplyr::distinct(.data[[value_col]]) |>
      dplyr::pull()
    message(glue::glue(
      "Validation: {data_tbl}.{value_col} has {nrow(invalid_rows)} rows ",
      "with invalid {lookup_type} values: {paste(invalid_vals, collapse = ', ')}"))
  }

  tibble::as_tibble(invalid_rows)
}

#' Flag and Export Invalid Rows
#'
#' Writes invalid rows to a CSV file for manual review. Creates the output
#' directory if it doesn't exist. Returns the path to the created file.
#'
#' @param invalid_rows Tibble of invalid rows to export
#' @param output_path Path for output CSV file
#' @param description Description of the validation failure (for logging)
#' @param append If TRUE, append to existing file; if FALSE (default), overwrite
#'
#' @return Path to the created/updated CSV file, or NULL if no rows to flag
#' @export
#' @concept validate
#'
#' @examples
#' \dontrun{
#' orphan_species <- validate_fk_references(con, "ichthyo", "species_id", "species", "species_id")
#' if (nrow(orphan_species) > 0) {
#'   flag_invalid_rows(
#'     invalid_rows = orphan_species,
#'     output_path  = "data/flagged/orphan_species.csv",
#'     description  = "Species IDs not found in species table")
#' }
#' }
#' @importFrom readr write_csv
#' @importFrom glue glue
flag_invalid_rows <- function(
    invalid_rows,
    output_path,
    description,
    append = FALSE) {

  if (nrow(invalid_rows) == 0) {
    message(glue::glue("No invalid rows to flag for: {description}"))
    return(invisible(NULL))
  }

  # ensure output directory exists
  output_dir <- dirname(output_path)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    message(glue::glue("Created flagged output directory: {output_dir}"))
  }

  # write CSV
  readr::write_csv(
    invalid_rows,
    output_path,
    append = append)

  message(glue::glue(
    "Flagged {nrow(invalid_rows)} rows to {output_path}: {description}"))

  return(output_path)
}

#' Run All Validations for a Dataset
#'
#' Runs a set of validation checks on the database and exports flagged rows.
#' Returns a summary of all validation results.
#'
#' @param con DuckDB connection
#' @param validations List of validation definitions, each containing:
#'   \itemize{
#'     \item \code{type}: "fk" (foreign key) or "lookup" (lookup value)
#'     \item \code{data_tbl}: Table to validate
#'     \item \code{col}: Column to check
#'     \item \code{ref_tbl}: Reference table (for fk) or lookup_type (for lookup)
#'     \item \code{ref_col}: Reference column (for fk only)
#'     \item \code{output_file}: Filename for flagged rows (placed in output_dir)
#'     \item \code{description}: Human-readable description
#'   }
#' @param output_dir Directory for flagged CSV files (default: "data/flagged")
#'
#' @return List containing:
#'   \itemize{
#'     \item \code{checks}: Tibble summarizing each validation check
#'     \item \code{total_flagged}: Total number of flagged rows
#'     \item \code{flagged_files}: Vector of created flagged file paths
#'     \item \code{invalid_data}: List of invalid data tibbles by check name
#'   }
#' @export
#' @concept validate
#'
#' @examples
#' \dontrun{
#' validations <- list(
#'   list(
#'     type        = "fk",
#'     data_tbl    = "ichthyo",
#'     col         = "species_id",
#'     ref_tbl     = "species",
#'     ref_col     = "species_id",
#'     output_file = "orphan_species.csv",
#'     description = "Species IDs not found in species table"),
#'   list(
#'     type        = "fk",
#'     data_tbl    = "ichthyo",
#'     col         = "net_id",
#'     ref_tbl     = "net",
#'     ref_col     = "net_id",
#'     output_file = "orphan_nets.csv",
#'     description = "Net IDs not found in net table"),
#'   list(
#'     type        = "lookup",
#'     data_tbl    = "ichthyo",
#'     col         = "measurement_value",
#'     ref_tbl     = "egg_stage",
#'     output_file = "invalid_egg_stages.csv",
#'     description = "Egg stage values not in vocabulary"))
#'
#' results <- validate_dataset(con, validations, output_dir = "data/flagged")
#' results$checks |> print()
#' }
#' @importFrom purrr map_dfr map
#' @importFrom tibble tibble
#' @importFrom glue glue
validate_dataset <- function(
    con,
    validations,
    output_dir = "data/flagged") {

  # ensure output directory exists
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  # run each validation
  results <- list()
  checks  <- list()
  flagged_files <- character()
  invalid_data  <- list()

  for (i in seq_along(validations)) {
    v <- validations[[i]]
    check_name <- paste0(v$data_tbl, "_", v$col, "_", v$type)

    # run validation based on type
    if (v$type == "fk") {
      invalid_rows <- validate_fk_references(
        con      = con,
        data_tbl = v$data_tbl,
        fk_col   = v$col,
        ref_tbl  = v$ref_tbl,
        ref_col  = v$ref_col,
        label    = v$description)
    } else if (v$type == "lookup") {
      invalid_rows <- validate_lookup_values(
        con         = con,
        data_tbl    = v$data_tbl,
        value_col   = v$col,
        lookup_type = v$ref_tbl)
    } else {
      warning(glue::glue("Unknown validation type: {v$type}"))
      next
    }

    # determine status
    n_flagged <- nrow(invalid_rows)
    status <- if (n_flagged == 0) "pass" else "fail"

    # flag invalid rows if any
    output_path <- NULL
    if (n_flagged > 0 && !is.null(v$output_file)) {
      output_path <- file.path(output_dir, v$output_file)
      flag_invalid_rows(
        invalid_rows = invalid_rows,
        output_path  = output_path,
        description  = v$description)
      flagged_files <- c(flagged_files, output_path)
    }

    # store results
    checks[[i]] <- tibble::tibble(
      check       = check_name,
      type        = v$type,
      data_tbl    = v$data_tbl,
      col         = v$col,
      ref_tbl     = v$ref_tbl,
      status      = status,
      n_flagged   = n_flagged,
      output_file = output_path %||% NA_character_,
      description = v$description)

    if (n_flagged > 0) {
      invalid_data[[check_name]] <- invalid_rows
    }
  }

  # combine check results
  checks_df <- dplyr::bind_rows(checks)
  total_flagged <- sum(checks_df$n_flagged)

  # summary message
  n_pass <- sum(checks_df$status == "pass")
  n_fail <- sum(checks_df$status == "fail")
  message(glue::glue(
    "Validation complete: {n_pass} passed, {n_fail} failed, ",
    "{total_flagged} total rows flagged"))

  list(
    checks        = checks_df,
    total_flagged = total_flagged,
    flagged_files = flagged_files,
    invalid_data  = invalid_data)
}

#' Delete Flagged Rows from Database
#'
#' Removes rows that were flagged during validation from the database tables.
#' Should be called after reviewing flagged files to clean up invalid data.
#'
#' @param con DuckDB connection
#' @param validation_results Results from \code{validate_dataset()}
#' @param tables_to_clean Optional character vector of table names to clean.
#'   If NULL (default), cleans all tables with flagged rows.
#' @param dry_run If TRUE (default), only reports what would be deleted without

#'   actually deleting. Set to FALSE to perform deletion.
#'
#' @return Tibble with deletion statistics per table
#' @export
#' @concept validate
#'
#' @examples
#' \dontrun{
#' # first, run validation
#' results <- validate_dataset(con, validations)
#'
#' # dry run to see what would be deleted
#' delete_flagged_rows(con, results, dry_run = TRUE)
#'
#' # actually delete
#' delete_flagged_rows(con, results, dry_run = FALSE)
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom dplyr filter distinct pull
#' @importFrom glue glue
#' @importFrom tibble tibble
delete_flagged_rows <- function(
    con,
    validation_results,
    tables_to_clean = NULL,
    dry_run = TRUE) {

  checks <- validation_results$checks
  invalid_data <- validation_results$invalid_data

  # filter to tables with flagged rows
  failed_checks <- checks |>
    dplyr::filter(status == "fail")

  if (nrow(failed_checks) == 0) {
    message("No flagged rows to delete")
    return(tibble::tibble(
      table   = character(),
      deleted = integer()))
  }

  # optionally filter to specific tables
  if (!is.null(tables_to_clean)) {
    failed_checks <- failed_checks |>
      dplyr::filter(data_tbl %in% tables_to_clean)
  }

  # collect deletion stats
  stats <- list()

  for (i in seq_len(nrow(failed_checks))) {
    check      <- failed_checks[i, ]
    check_name <- check$check
    tbl_name   <- check$data_tbl
    col_name   <- check$col

    if (!check_name %in% names(invalid_data)) {
      next
    }

    invalid_rows <- invalid_data[[check_name]]
    invalid_vals <- invalid_rows |>
      dplyr::distinct(.data[[col_name]]) |>
      dplyr::pull()

    # format values for SQL IN clause (quote strings, not numbers)
    if (is.numeric(invalid_vals)) {
      sql_vals <- paste(invalid_vals, collapse = ", ")
    } else {
      # escape single quotes and wrap in single quotes for SQL strings
      sql_vals <- paste(paste0("'", gsub("'", "''", invalid_vals), "'"), collapse = ", ")
    }

    # count rows to delete
    count_query <- glue::glue(
      "SELECT COUNT(*) as n FROM {tbl_name} WHERE {col_name} IN ({sql_vals})")
    n_to_delete <- DBI::dbGetQuery(con, count_query)$n

    if (dry_run) {
      message(glue::glue(
        "[DRY RUN] Would delete {n_to_delete} rows from {tbl_name} ",
        "where {col_name} in ({length(invalid_vals)} values)"))
    } else {
      delete_query <- glue::glue(
        "DELETE FROM {tbl_name} WHERE {col_name} IN ({sql_vals})")
      DBI::dbExecute(con, delete_query)
      message(glue::glue(
        "Deleted {n_to_delete} rows from {tbl_name} ",
        "where {col_name} in ({length(invalid_vals)} values)"))
    }

    stats[[i]] <- tibble::tibble(
      table   = tbl_name,
      col     = col_name,
      deleted = if (dry_run) 0L else as.integer(n_to_delete),
      would_delete = as.integer(n_to_delete))
  }

  dplyr::bind_rows(stats)
}

#' Validate Egg Stage Values
#'
#' Specific validation for egg stages - checks that values are in valid range
#' (1-11 per Moser & Ahlstrom 1985). Values 12-15 are invalid.
#'
#' @param con DuckDB connection
#' @param table_name Table containing egg stage data
#' @param stage_col Column containing stage values (default: "stage")
#'
#' @return Tibble of rows with invalid egg stage values (>11)
#' @export
#' @concept validate
#'
#' @examples
#' \dontrun{
#' invalid_stages <- validate_egg_stages(con, "eggstage", "stage")
#' }
#' @importFrom DBI dbGetQuery
#' @importFrom dplyr tbl filter collect
#' @importFrom glue glue
validate_egg_stages <- function(
    con,
    table_name,
    stage_col = "stage") {

  invalid_rows <- dplyr::tbl(con, table_name) |>
    dplyr::filter(.data[[stage_col]] > 11) |>
    dplyr::collect()

  if (nrow(invalid_rows) > 0) {
    invalid_stages <- unique(invalid_rows[[stage_col]])
    message(glue::glue(
      "Validation: {table_name}.{stage_col} has {nrow(invalid_rows)} rows ",
      "with invalid stage values (>11): {paste(invalid_stages, collapse = ', ')}"))
  }

  tibble::as_tibble(invalid_rows)
}
