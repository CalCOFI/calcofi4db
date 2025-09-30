#' Check Data Integrity for Ingestion
#'
#' Validates that CSV files match their redefinition metadata before database
#' ingestion. This function is designed to be called from Quarto notebooks and
#' will stop notebook execution if mismatches are detected.
#'
#' The function:
#' 1. Detects changes between CSV files and redefinitions using detect_csv_changes()
#' 2. Prints summary statistics of detected changes
#' 3. Displays interactive table of changes if any exist
#' 4. Returns appropriate status for notebook control flow
#'
#' When called from a Quarto notebook in an output: asis chunk, this function
#' will render markdown messages and can control chunk evaluation via knitr options.
#'
#' @param d List output from read_csv_files() containing CSV and redefinition data
#' @param dataset_name Name of dataset for display purposes (e.g., "NOAA CalCOFI Database")
#' @param halt_on_fail Logical, whether to set knitr eval=FALSE on failure (default: TRUE)
#' @param display_format Format for displaying changes: "DT" (DataTable), "kable", or "print" (default: "DT")
#' @param verbose Logical, print detailed messages (default: TRUE)
#'
#' @return List with:
#'   - passed: Logical indicating if integrity check passed
#'   - changes: Full changes object from detect_csv_changes()
#'   - n_changes: Number of changes detected
#'   - message: Character string with markdown-formatted message
#'
#' @export
#' @concept check
#'
#' @examples
#' \dontrun{
#' # In a Quarto notebook chunk with output: asis
#' d_noaa <- read_csv_files("swfsc.noaa.gov", "calcofi-db")
#' integrity_check <- check_data_integrity(
#'   d = d_noaa,
#'   dataset_name = "NOAA CalCOFI Database"
#' )
#'
#' # Continue only if check passed
#' if (!integrity_check$passed) {
#'   stop("Data integrity check failed")
#' }
#' }
#' @importFrom knitr opts_chunk
#' @importFrom glue glue
check_data_integrity <- function(
    d,
    dataset_name = "Dataset",
    halt_on_fail = TRUE,
    display_format = "DT",
    verbose = TRUE) {

  # detect changes between csv files and redefinitions
  changes <- detect_csv_changes(d)

  # print summary statistics
  if (verbose) {
    print_csv_change_stats(changes, verbose = TRUE)
  }

  # count total changes
  n_changes <- nrow(changes$summary)

  # determine if check passed
  passed <- n_changes == 0

  # prepare markdown message
  if (passed) {
    msg <- glue::glue("
## ✅ Data Integrity Check Passed: {dataset_name}

### All Systems Go

No mismatches were found between the CSV files and redefinition metadata.
The data structures are properly aligned and ready for database ingestion.

---
")
  } else {
    msg <- glue::glue("
## ⚠️ Data Integrity Check Failed: {dataset_name}

### Workflow Halted

Mismatches have been detected between the CSV files and redefinition metadata.
These must be resolved before proceeding with database ingestion to ensure data integrity.

### Detected Issues

- **Total changes**: {n_changes}
- **Tables added**: {length(changes$tables_added)}
- **Tables removed**: {length(changes$tables_removed)}
- **Fields added**: {length(changes$fields_added)} table(s)
- **Fields removed**: {length(changes$fields_removed)} table(s)
- **Type mismatches**: {length(changes$type_mismatches)} table(s)

### Required Actions

Please review the changes detected above and update the following redefinition files:

- **Tables redefinition**: `{d$paths$tbls_rd_csv}`
- **Fields redefinition**: `{d$paths$flds_rd_csv}`

### Common Resolutions

1. **New tables/fields in CSV**: Add them to the appropriate redefinition file
2. **Removed tables/fields from CSV**: Remove obsolete entries from redefinition files
3. **Type mismatches**: Update field types in redefinition files to match CSV data types
4. **Field name changes**: Update `fld_old` entries to match current CSV field names

### Next Steps

After updating the redefinition files, re-run this workflow. The remaining code chunks
have been disabled and will not execute until all mismatches are resolved.

---

*Note: The remainder of this document contains code that will not be executed due to
data integrity issues.*

")
  }

  # display changes if any exist
  if (n_changes > 0) {
    display_csv_changes(
      changes,
      format = display_format,
      title = glue::glue("{dataset_name}: CSV vs Redefinition Mismatches")
    )
  }

  # control notebook execution via knitr options
  if (halt_on_fail && !passed) {
    # disable evaluation of remaining chunks
    knitr::opts_chunk$set(eval = FALSE)
  } else if (passed) {
    # ensure evaluation is enabled
    knitr::opts_chunk$set(eval = TRUE)
  }

  # return results
  invisible(list(
    passed = passed,
    changes = changes,
    n_changes = n_changes,
    message = msg
  ))
}

#' Render Data Integrity Check Message
#'
#' Renders the markdown message from check_data_integrity() output.
#' Use this in output: asis chunks to display formatted messages.
#'
#' @param integrity_check List output from check_data_integrity()
#'
#' @return Invisible NULL (message is rendered via cat())
#' @export
#' @concept check
#'
#' @examples
#' \dontrun{
#' # In a Quarto chunk with output: asis
#' integrity_check <- check_data_integrity(d, "NOAA CalCOFI Database")
#' render_integrity_message(integrity_check)
#' }
render_integrity_message <- function(integrity_check) {
  cat(integrity_check$message)
  invisible(NULL)
}

#' Check Multiple Datasets for Integrity
#'
#' Convenience function to check integrity of multiple datasets and halt
#' if any fail. Useful for master ingestion scripts with multiple datasets.
#'
#' @param datasets Named list where names are dataset labels and values are
#'   outputs from read_csv_files()
#' @param halt_on_first_fail Logical, stop checking after first failure (default: FALSE)
#' @param display_format Format for displaying changes (default: "DT")
#'
#' @return List with:
#'   - all_passed: Logical indicating if all checks passed
#'   - results: Named list of individual check results
#'   - n_failed: Number of datasets that failed
#'   - failed_datasets: Character vector of failed dataset names
#'
#' @export
#' @concept check
#'
#' @examples
#' \dontrun{
#' datasets <- list(
#'   "NOAA CalCOFI DB" = d_noaa,
#'   "Bottle Database" = d_bottle
#' )
#' check_results <- check_multiple_datasets(datasets)
#'
#' if (!check_results$all_passed) {
#'   stop("One or more datasets failed integrity checks")
#' }
#' }
check_multiple_datasets <- function(
    datasets,
    halt_on_first_fail = FALSE,
    display_format = "DT") {

  results <- list()
  failed_datasets <- character()

  for (dataset_name in names(datasets)) {
    d <- datasets[[dataset_name]]

    # check integrity
    result <- check_data_integrity(
      d = d,
      dataset_name = dataset_name,
      halt_on_fail = FALSE,  # handle halting at the end
      display_format = display_format,
      verbose = TRUE
    )

    # render message
    render_integrity_message(result)

    # store result
    results[[dataset_name]] <- result

    # track failures
    if (!result$passed) {
      failed_datasets <- c(failed_datasets, dataset_name)

      if (halt_on_first_fail) {
        break
      }
    }
  }

  all_passed <- length(failed_datasets) == 0

  # halt execution if any failed
  if (!all_passed) {
    knitr::opts_chunk$set(eval = FALSE)
  }

  list(
    all_passed = all_passed,
    results = results,
    n_failed = length(failed_datasets),
    failed_datasets = failed_datasets
  )
}