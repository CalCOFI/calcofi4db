#' Transform Data for Database Ingestion
#'
#' Applies transformations to raw data based on redefinition files.
#'
#' @param data_info List containing data and metadata from `read_csv_files()`
#'
#' @return List with transformed data ready for database ingestion
#' @export
#' @concept transform
#'
#' @examples
#' \dontrun{
#' transformed_data <- transform_data(data_info)
#' }
transform_data <- function(data_info) {
  d <- data_info$csv$data
  d_tbls_rd <- data_info$d_tbls_rd
  d_flds_rd <- data_info$d_flds_rd

  # Function to mutate a table according to redefinition files
  mutate_table <- function(tbl, data) {
    d_f <- d_flds_rd |>
      dplyr::filter(tbl_old == tbl)

    # redefine fields
    f_rd <- d_f |>
      dplyr::select(fld_old, fld_new) |>
      tibble::deframe()

    y <- dplyr::redefine_with(data, ~ f_rd[.x])

    # Mutate fields
    d_m <- d_f |>
      dplyr::select(fld_new, mutation) |>
      dplyr::filter(!is.na(mutation))

    for (i in seq_len(nrow(d_m))) {
      fld <- d_m$fld_new[i]
      mx <- d_m$mutation[i]

      fld_sym <- rlang::sym(fld)
      mx_expr <- rlang::parse_expr(mx)

      y <- y |>
        dplyr::mutate(!!fld_sym := eval(mx_expr, envir = y))
    }

    # Order fields
    flds_ordered <- d_f |>
      dplyr::arrange(order_new) |>
      dplyr::pull(fld_new)

    y <- y |>
      dplyr::relocate(dplyr::all_of(flds_ordered))

    return(y)
  }

  # Apply transformations to all tables
  d_transformed <- d |>
    dplyr::left_join(
      d_tbls_rd,
      by = c("tbl" = "tbl_old")) |>
    dplyr::mutate(
      data_new = purrr::map2(tbl, data, mutate_table))

  # Return transformed data
  return(d_transformed)
}

#' Detect Changes in CSV Files
#'
#' Compares current CSV files with existing database tables to detect changes
#' in tables, fields, and data types.
#'
#' @param con Database connection
#' @param schema Database schema
#' @param transformed_data Transformed data from transform_data()
#' @param d_flds_rd Field redefinition data frame
#'
#' @return A list of detected changes in tables, fields, and types
#' @export
#' @concept transform
#'
#' @examples
#' \dontrun{
#' changes <- detect_csv_changes(
#'   con = db_connection,
#'   schema = "public",
#'   transformed_data = transformed_data,
#'   d_flds_rd = data_info$d_flds_rd
#' )
#' }
detect_csv_changes <- function(con, schema, transformed_data, d_flds_rd) {
  # Get list of tables in the database
  existing_tables <- DBI::dbListTables(con)

  # Get list of tables in the transformed data
  new_tables <- transformed_data |>
    dplyr::pull(tbl_new) |>
    unique()

  # Identify added, removed, and common tables
  added_tables <- setdiff(new_tables, existing_tables)
  common_tables <- intersect(new_tables, existing_tables)

  # Initialize changes list
  changes <- list(
    new_tables = added_tables,
    existing_tables = common_tables,
    field_changes = list(),
    type_changes = list(),
    data_changes = list()
  )

  # Check for field and type changes in common tables
  for (tbl in common_tables) {
    # Get existing table schema
    existing_fields <- DBI::dbListFields(con, DBI::Id(schema = schema, table = tbl))

    # Get new table schema
    new_data <- transformed_data |>
      dplyr::filter(tbl_new == tbl) |>
      dplyr::pull(data_new) |>
      purrr::pluck(1)

    new_fields <- names(new_data)

    # Identify field changes
    added_fields <- setdiff(new_fields, existing_fields)
    removed_fields <- setdiff(existing_fields, new_fields)

    if (length(added_fields) > 0 || length(removed_fields) > 0) {
      changes$field_changes[[tbl]] <- list(
        added = added_fields,
        removed = removed_fields
      )
    }

    # Check field type changes (for existing fields)
    common_fields <- intersect(existing_fields, new_fields)

    type_changes <- list()

    # Only check if there are common fields
    if (length(common_fields) > 0) {
      # Get existing field types
      existing_types_query <- glue::glue("
        SELECT
          column_name,
          data_type
        FROM
          information_schema.columns
        WHERE
          table_schema = '{schema}' AND
          table_name = '{tbl}'
      ")

      existing_types_df <- DBI::dbGetQuery(con, existing_types_query)

      # Get new field types
      new_types_df <- d_flds_rd |>
        dplyr::filter(tbl_new == tbl) |>
        dplyr::select(column_name = fld_new, data_type = type_new)

      # Compare types
      for (field in common_fields) {
        existing_type <- existing_types_df |>
          dplyr::filter(column_name == field) |>
          dplyr::pull(data_type)

        new_type <- new_types_df |>
          dplyr::filter(column_name == field) |>
          dplyr::pull(data_type)

        if (length(existing_type) > 0 && length(new_type) > 0 &&
            existing_type != new_type) {
          type_changes[[field]] <- list(
            from = existing_type,
            to = new_type
          )
        }
      }
    }

    if (length(type_changes) > 0) {
      changes$type_changes[[tbl]] <- type_changes
    }

    # TODO: Implement data comparison to detect changes in values
    # This would require comparing sample data or checksums
  }

  return(changes)
}
