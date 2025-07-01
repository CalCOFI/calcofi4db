#' Display CSV Changes in a Formatted Table
#'
#' Creates a formatted table showing changes detected between CSV files and
#' redefinitions. Changes are color-coded: additions in green, removals in red,
#' and type mismatches in orange.
#'
#' @param changes List output from detect_csv_changes() containing change information
#' @param format Output format: "DT" for interactive DataTable (default),
#'   "kable" for static knitr table, or "tibble" for raw data frame
#' @param title Optional title for the table
#'
#' @return Formatted table object (DT, kable, or tibble) showing changes
#' @export
#' @concept transform
#'
#' @examples
#' \dontrun{
#' # Read CSV files and detect changes
#' d <- read_csv_files("swfsc.noaa.gov", "calcofi-db")
#' changes <- detect_csv_changes(d)
#' 
#' # Display interactive table
#' display_csv_changes(changes)
#' 
#' # Display static table for reports
#' display_csv_changes(changes, format = "kable")
#' }
display_csv_changes <- function(changes, format = "DT", title = NULL) {
  # Extract summary data
  summary_df <- changes$summary
  
  # If no changes, return message
  if (nrow(summary_df) == 0) {
    message("No changes detected between CSV files and redefinitions.")
    return(invisible(NULL))
  }
  
  # Add color coding based on change type
  summary_df <- summary_df |>
    dplyr::mutate(
      color = dplyr::case_when(
        change_type %in% c("added", "table_added") ~ "#28a745",      # green
        change_type %in% c("removed", "table_removed") ~ "#dc3545",  # red
        change_type == "type_mismatch" ~ "#fd7e14",                  # orange
        TRUE ~ "#000000"                                              # black
      ),
      icon = dplyr::case_when(
        change_type %in% c("added", "table_added") ~ "+",
        change_type %in% c("removed", "table_removed") ~ "-",
        change_type == "type_mismatch" ~ "⚠",
        TRUE ~ ""
      )
    )
  
  # Format based on requested output
  if (format == "DT") {
    # Create interactive DataTable
    if (!requireNamespace("DT", quietly = TRUE)) {
      stop("Package 'DT' required for interactive tables. Please install it.")
    }
    
    # Prepare display columns
    display_df <- summary_df |>
      dplyr::select(
        Icon = icon,
        Table = table,
        Field = field,
        `Change Type` = change_type,
        `Old Value` = old_value,
        `New Value` = new_value,
        Description = description
      )
    
    # Create DataTable with color coding
    dt <- DT::datatable(
      display_df,
      caption = title %||% "CSV Changes Summary",
      options = list(
        pageLength = 25,
        autoWidth = TRUE,
        columnDefs = list(
          list(className = 'dt-center', targets = 0),
          list(width = '5%', targets = 0),
          list(width = '15%', targets = 1),
          list(width = '15%', targets = 2),
          list(width = '15%', targets = 3),
          list(width = '15%', targets = 4),
          list(width = '15%', targets = 5),
          list(width = '20%', targets = 6)
        )
      ),
      rownames = FALSE
    )
    
    # Apply row colors based on change type
    for (i in seq_len(nrow(summary_df))) {
      row_color <- summary_df$color[i]
      dt <- dt |>
        DT::formatStyle(
          columns = 1:7,
          valueColumns = NULL,
          target = "row",
          backgroundColor = DT::styleRow(i, paste0(row_color, "20"))
        )
    }
    
    # Style the icon column
    dt <- dt |>
      DT::formatStyle(
        "Icon",
        color = DT::styleEqual(
          c("+", "-", "⚠"),
          c("#28a745", "#dc3545", "#fd7e14")
        ),
        fontWeight = "bold",
        fontSize = "120%"
      )
    
    return(dt)
    
  } else if (format == "kable") {
    # Create static kable table
    if (!requireNamespace("knitr", quietly = TRUE)) {
      stop("Package 'knitr' required for kable tables. Please install it.")
    }
    
    display_df <- summary_df |>
      dplyr::mutate(
        Change = paste(icon, change_type),
        field = tidyr::replace_na(field, ""),
        old_value = tidyr::replace_na(old_value, ""),
        new_value = tidyr::replace_na(new_value, "")
      ) |>
      dplyr::select(
        Table = table,
        Field = field,
        Change = Change,
        `Old Value` = old_value,
        `New Value` = new_value,
        Description = description
      )
    
    knitr::kable(
      display_df,
      caption = title %||% "CSV Changes Summary",
      format = "html"
    )
    
  } else if (format == "tibble") {
    # Return raw tibble
    return(summary_df)
    
  } else {
    stop("Invalid format. Choose 'DT', 'kable', or 'tibble'.")
  }
}

#' Print CSV Change Statistics
#'
#' Prints a summary of change statistics from detect_csv_changes() output.
#'
#' @param changes List output from detect_csv_changes()
#' @param verbose Logical, whether to print detailed information
#'
#' @return Invisible NULL (prints to console)
#' @export
#' @concept transform
#'
#' @examples
#' \dontrun{
#' d <- read_csv_files("swfsc.noaa.gov", "calcofi-db")
#' changes <- detect_csv_changes(d)
#' print_csv_change_stats(changes)
#' }
print_csv_change_stats <- function(changes, verbose = TRUE) {
  # Count changes by type
  n_tables_added <- length(changes$tables_added)
  n_tables_removed <- length(changes$tables_removed)
  n_tables_with_field_changes <- length(changes$fields_added) + length(changes$fields_removed)
  n_tables_with_type_changes <- length(changes$type_mismatches)
  
  # Count total field changes
  n_fields_added <- sum(sapply(changes$fields_added, length))
  n_fields_removed <- sum(sapply(changes$fields_removed, length))
  n_type_mismatches <- sum(sapply(changes$type_mismatches, length))
  
  # Print summary
  cat("CSV Change Summary:\n")
  cat("==================\n")
  cat(sprintf("Tables added:    %d\n", n_tables_added))
  cat(sprintf("Tables removed:  %d\n", n_tables_removed))
  cat(sprintf("Fields added:    %d (across %d tables)\n", n_fields_added, length(changes$fields_added)))
  cat(sprintf("Fields removed:  %d (across %d tables)\n", n_fields_removed, length(changes$fields_removed)))
  cat(sprintf("Type mismatches: %d (across %d tables)\n", n_type_mismatches, length(changes$type_mismatches)))
  
  if (verbose) {
    # Print detailed information
    if (n_tables_added > 0) {
      cat("\nTables Added:\n")
      for (tbl in changes$tables_added) {
        cat(sprintf("  + %s\n", tbl))
      }
    }
    
    if (n_tables_removed > 0) {
      cat("\nTables Removed:\n")
      for (tbl in changes$tables_removed) {
        cat(sprintf("  - %s\n", tbl))
      }
    }
    
    if (length(changes$fields_added) > 0) {
      cat("\nFields Added:\n")
      for (tbl in names(changes$fields_added)) {
        cat(sprintf("  %s:\n", tbl))
        for (fld in changes$fields_added[[tbl]]) {
          cat(sprintf("    + %s\n", fld))
        }
      }
    }
    
    if (length(changes$fields_removed) > 0) {
      cat("\nFields Removed:\n")
      for (tbl in names(changes$fields_removed)) {
        cat(sprintf("  %s:\n", tbl))
        for (fld in changes$fields_removed[[tbl]]) {
          cat(sprintf("    - %s\n", fld))
        }
      }
    }
    
    if (length(changes$type_mismatches) > 0) {
      cat("\nType Mismatches:\n")
      for (tbl in names(changes$type_mismatches)) {
        cat(sprintf("  %s:\n", tbl))
        for (fld in names(changes$type_mismatches[[tbl]])) {
          type_info <- changes$type_mismatches[[tbl]][[fld]]
          cat(sprintf("    ⚠ %s: %s -> %s\n", fld, type_info$rd_type, type_info$csv_type))
        }
      }
    }
  }
  
  invisible(NULL)
}