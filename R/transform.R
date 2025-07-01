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
#' Compares latest CSV files with redefinition metadata to detect changes
#' in tables, fields, and data types. This function compares the raw CSV data
#' (d$d_csv) with the table and field redefinitions (d$d_tbls_rd and d$d_flds_rd)
#' to identify mismatches before database ingestion.
#'
#' @param d List output from read_csv_files() containing:
#'   - d_csv: CSV metadata with tables, fields, and data
#'   - d_tbls_rd: Table redefinition data frame
#'   - d_flds_rd: Field redefinition data frame
#'
#' @return A list containing detected changes:
#'   - tables_added: Tables in CSV but not in redefinitions
#'   - tables_removed: Tables in redefinitions but not in CSV
#'   - fields_added: Fields in CSV but not in redefinitions (by table)
#'   - fields_removed: Fields in redefinitions but not in CSV (by table)
#'   - type_mismatches: Field type differences between CSV and redefinitions
#'   - summary: Data frame summarizing all changes for display
#' @export
#' @concept transform
#'
#' @examples
#' \dontrun{
#' # Read CSV files and metadata
#' d <- read_csv_files("swfsc.noaa.gov", "calcofi-db")
#' 
#' # Detect changes between CSV files and redefinitions
#' changes <- detect_csv_changes(d)
#' 
#' # Display summary of changes
#' print(changes$summary)
#' }
detect_csv_changes <- function(d) {
  # Extract components
  d_csv <- d$d_csv$data
  d_tbls_rd <- d$d_tbls_rd
  d_flds_rd <- d$d_flds_rd
  
  # Get tables from CSV and redefinitions
  csv_tables <- unique(d_csv$tbl)
  rd_tables <- unique(d_tbls_rd$tbl_old)
  
  # Identify table-level changes
  tables_added <- setdiff(csv_tables, rd_tables)
  tables_removed <- setdiff(rd_tables, csv_tables)
  tables_common <- intersect(csv_tables, rd_tables)
  
  # Initialize results
  fields_added <- list()
  fields_removed <- list()
  type_mismatches <- list()
  summary_rows <- list()
  
  # Check field-level changes for each common table
  for (tbl in tables_common) {
    # Get fields from CSV
    csv_fields_data <- d_csv |>
      dplyr::filter(tbl == !!tbl) |>
      dplyr::pull(flds) |>
      purrr::pluck(1)
    
    csv_fields <- csv_fields_data$fld
    csv_types <- csv_fields_data$type
    
    # Get fields from redefinitions
    rd_fields_data <- d_flds_rd |>
      dplyr::filter(tbl_old == !!tbl)
    
    rd_fields <- rd_fields_data$fld_old
    rd_types <- rd_fields_data$type_old
    
    # Find field differences
    flds_added <- setdiff(csv_fields, rd_fields)
    flds_removed <- setdiff(rd_fields, csv_fields)
    flds_common <- intersect(csv_fields, rd_fields)
    
    if (length(flds_added) > 0) {
      fields_added[[tbl]] <- flds_added
      for (fld in flds_added) {
        fld_type <- csv_types[csv_fields == fld]
        summary_rows <- append(summary_rows, list(
          tibble::tibble(
            table = tbl,
            field = fld,
            change_type = "added",
            old_value = NA_character_,
            new_value = fld_type,
            description = "Field added in CSV"
          )
        ))
      }
    }
    
    if (length(flds_removed) > 0) {
      fields_removed[[tbl]] <- flds_removed
      for (fld in flds_removed) {
        fld_type <- rd_types[rd_fields == fld]
        summary_rows <- append(summary_rows, list(
          tibble::tibble(
            table = tbl,
            field = fld,
            change_type = "removed",
            old_value = fld_type,
            new_value = NA_character_,
            description = "Field removed from CSV"
          )
        ))
      }
    }
    
    # Check type mismatches for common fields
    type_changes <- list()
    for (fld in flds_common) {
      csv_type <- csv_types[csv_fields == fld]
      rd_type <- rd_types[rd_fields == fld]
      
      if (!is.na(csv_type) && !is.na(rd_type) && csv_type != rd_type) {
        type_changes[[fld]] <- list(
          csv_type = csv_type,
          rd_type = rd_type
        )
        summary_rows <- append(summary_rows, list(
          tibble::tibble(
            table = tbl,
            field = fld,
            change_type = "type_mismatch",
            old_value = rd_type,
            new_value = csv_type,
            description = "Field type changed"
          )
        ))
      }
    }
    
    if (length(type_changes) > 0) {
      type_mismatches[[tbl]] <- type_changes
    }
  }
  
  # Add table-level changes to summary
  for (tbl in tables_added) {
    summary_rows <- append(summary_rows, list(
      tibble::tibble(
        table = tbl,
        field = NA_character_,
        change_type = "table_added",
        old_value = NA_character_,
        new_value = "new table",
        description = "Table added in CSV"
      )
    ))
  }
  
  for (tbl in tables_removed) {
    summary_rows <- append(summary_rows, list(
      tibble::tibble(
        table = tbl,
        field = NA_character_,
        change_type = "table_removed",
        old_value = "existing table",
        new_value = NA_character_,
        description = "Table removed from CSV"
      )
    ))
  }
  
  # Combine summary rows
  summary <- if (length(summary_rows) > 0) {
    dplyr::bind_rows(summary_rows) |>
      dplyr::arrange(table, field)
  } else {
    tibble::tibble(
      table = character(),
      field = character(),
      change_type = character(),
      old_value = character(),
      new_value = character(),
      description = character()
    )
  }
  
  # Print warnings if changes detected
  if (nrow(summary) > 0) {
    warning("Mismatches detected between CSV files and redefinitions:")
    
    if (length(tables_added) > 0) {
      warning(sprintf("  Tables added: %s", paste(tables_added, collapse = ", ")))
    }
    
    if (length(tables_removed) > 0) {
      warning(sprintf("  Tables removed: %s", paste(tables_removed, collapse = ", ")))
    }
    
    if (length(fields_added) > 0) {
      warning(sprintf("  Fields added in %d table(s)", length(fields_added)))
    }
    
    if (length(fields_removed) > 0) {
      warning(sprintf("  Fields removed in %d table(s)", length(fields_removed)))
    }
    
    if (length(type_mismatches) > 0) {
      warning(sprintf("  Type mismatches in %d table(s)", length(type_mismatches)))
    }
  }
  
  # Return results
  list(
    tables_added = tables_added,
    tables_removed = tables_removed,
    fields_added = fields_added,
    fields_removed = fields_removed,
    type_mismatches = type_mismatches,
    summary = summary
  )
}
