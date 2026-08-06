#' Check Data Integrity for Ingestion
#'
#' Validates that CSV files match their redefinition metadata before database
#' ingestion. This function is designed to be called from Quarto notebooks and
#' will stop notebook execution if mismatches are detected.
#'
#' The function:
#' 1. Detects changes between CSV files and redefinitions using detect_csv_changes()
#' 2. Optionally filters out known acceptable type mismatches via `type_exceptions`
#' 3. Prints summary statistics of detected changes
#' 4. Displays interactive table of changes if any exist
#' 5. Returns appropriate status for notebook control flow
#'
#' When called from a Quarto notebook in an output: asis chunk, this function
#' will render markdown messages and can control chunk evaluation via knitr options.
#'
#' @param d List output from read_csv_files() containing CSV and redefinition data
#' @param dataset_name Name of dataset for display purposes (e.g., "NOAA CalCOFI Database")
#' @param halt_on_fail Logical, whether to set knitr eval=FALSE on failure (default: TRUE)
#' @param type_exceptions Character vector of known acceptable type mismatches.
#'   Use `"all"` to accept all type mismatches, or specific `"table.field"`
#'   patterns (e.g., `c("casts.time", "bottle.t_qual")`). Default: NULL (no exceptions).
#' @param display_format Format for displaying changes: "DT" (DataTable), "kable", or "print" (default: "DT")
#' @param verbose Logical, print detailed messages (default: TRUE)
#' @param header_level Integer, markdown header level for output messages (default: 3).
#'   Controls the top-level header depth; sub-headers use header_level + 1.
#'   Set to match the parent section level in your Quarto document to keep
#'   the Table of Contents hierarchy correct.
#'
#' @return List with:
#'   - passed: Logical indicating if integrity check passed
#'   - changes: Full changes object from detect_csv_changes()
#'   - n_changes: Number of changes detected (after filtering exceptions)
#'   - n_exceptions: Number of type mismatches accepted as exceptions
#'   - message: Character string with markdown-formatted message
#'
#' @export
#' @concept check
#'
#' @examples
#' \dontrun{
#' # strict check — halt on any mismatch
#' integrity <- check_data_integrity(d, "NOAA CalCOFI Database")
#'
#' # accept all type mismatches (e.g., readr infers types differently)
#' integrity <- check_data_integrity(
#'   d               = d,
#'   dataset_name    = "CalCOFI Bottle Database",
#'   halt_on_fail    = FALSE,
#'   type_exceptions = "all")
#'
#' # use header_level = 2 for top-level sections
#' integrity <- check_data_integrity(
#'   d            = d,
#'   dataset_name = "NOAA CalCOFI Database",
#'   header_level = 2)
#' }
#' @importFrom dplyr filter
#' @importFrom knitr opts_chunk
#' @importFrom glue glue
check_data_integrity <- function(
    d,
    dataset_name    = "Dataset",
    halt_on_fail    = TRUE,
    type_exceptions = NULL,
    display_format  = "DT",
    verbose         = TRUE,
    header_level    = 3) {

  # detect changes between csv files and redefinitions
  changes <- detect_csv_changes(d)

  # print summary statistics
  if (verbose) {
    print_csv_change_stats(changes, verbose = TRUE)
  }

  # -- apply type_exceptions ----
  n_exceptions     <- 0
  exception_detail <- character(0)

  if (!is.null(type_exceptions) && length(changes$type_mismatches) > 0) {
    # count original type mismatches
    n_type_orig <- sum(sapply(changes$type_mismatches, length))

    if (identical(type_exceptions, "all")) {
      # accept all type mismatches
      exception_detail <- unlist(lapply(
        names(changes$type_mismatches), function(tbl) {
          paste0(tbl, ".", names(changes$type_mismatches[[tbl]]))
        }))
      changes$type_mismatches <- list()
    } else {
      # filter specific table.field patterns
      for (tbl in names(changes$type_mismatches)) {
        flds <- names(changes$type_mismatches[[tbl]])
        matched <- paste0(tbl, ".", flds) %in% type_exceptions
        if (any(matched)) {
          exception_detail <- c(
            exception_detail, paste0(tbl, ".", flds[matched]))
          changes$type_mismatches[[tbl]][flds[matched]] <- NULL
        }
        if (length(changes$type_mismatches[[tbl]]) == 0) {
          changes$type_mismatches[[tbl]] <- NULL
        }
      }
    }

    n_exceptions <- length(exception_detail)

    # rebuild summary removing excepted type mismatches
    if (n_exceptions > 0) {
      changes$summary <- changes$summary |>
        dplyr::filter(
          !(change_type == "type_mismatch" &
              paste0(table, ".", field) %in% exception_detail))

      if (verbose && n_exceptions > 0) {
        message(glue::glue(
          "\nType exceptions accepted: {n_exceptions} mismatch(es) ",
          "treated as known/acceptable"))
      }
    }
  }

  # count changes after filtering exceptions
  n_changes <- nrow(changes$summary)

  # determine if check passed
  passed <- n_changes == 0

  # -- build detail bullets for detected issues ----
  detail_bullets <- character(0)

  if (length(changes$tables_added) > 0)
    detail_bullets <- c(detail_bullets, glue::glue(
      "- **Tables added**: {paste(changes$tables_added, collapse = ', ')}"))

  if (length(changes$tables_removed) > 0)
    detail_bullets <- c(detail_bullets, glue::glue(
      "- **Tables removed**: {paste(changes$tables_removed, collapse = ', ')}"))

  if (length(changes$fields_added) > 0) {
    fld_detail <- paste(sapply(names(changes$fields_added), function(tbl) {
      glue::glue("{tbl} ({length(changes$fields_added[[tbl]])} fields)")
    }), collapse = ", ")
    detail_bullets <- c(detail_bullets, glue::glue(
      "- **Fields added**: {fld_detail}"))
  }

  if (length(changes$fields_removed) > 0) {
    fld_detail <- paste(sapply(names(changes$fields_removed), function(tbl) {
      glue::glue("{tbl} ({length(changes$fields_removed[[tbl]])} fields)")
    }), collapse = ", ")
    detail_bullets <- c(detail_bullets, glue::glue(
      "- **Fields removed**: {fld_detail}"))
  }

  if (length(changes$type_mismatches) > 0) {
    n_remaining <- sum(sapply(changes$type_mismatches, length))
    type_detail <- paste(sapply(names(changes$type_mismatches), function(tbl) {
      glue::glue("{tbl} ({length(changes$type_mismatches[[tbl]])} fields)")
    }), collapse = ", ")
    detail_bullets <- c(detail_bullets, glue::glue(
      "- **Type mismatches**: {n_remaining} in {type_detail}"))
  }

  detail_section <- paste(detail_bullets, collapse = "\n")

  # -- prepare markdown message ----
  h1 <- strrep("#", header_level)
  h2 <- strrep("#", header_level + 1)

  if (passed && n_exceptions == 0) {
    msg <- glue::glue("
{h1} \u2705 Data Integrity Check Passed: {dataset_name}

{h2} All Systems Go

No mismatches were found between the CSV files and redefinition metadata.
The data structures are properly aligned and ready for database ingestion.

---
")
  } else if (passed && n_exceptions > 0) {
    msg <- glue::glue("
{h1} \u2705 Data Integrity Check Passed: {dataset_name}

{h2} Passed with Accepted Exceptions

{n_exceptions} type mismatch(es) were found but accepted as known exceptions
(e.g., readr infers types differently from redefinition metadata; resolved
during ingestion via `flds_redefine.csv` `type_new` column).

---
")
  } else if (!halt_on_fail) {
    msg <- glue::glue("
{h1} \u26a0\ufe0f Data Integrity Check: {dataset_name}

{h2} Issues Detected (Continuing)

Mismatches have been detected between the CSV files and redefinition metadata.
The workflow is continuing because `halt_on_fail = FALSE`.

{h2} Detected Issues ({n_changes} remaining)

{detail_section}

{if (n_exceptions > 0) glue::glue('*{n_exceptions} type mismatch(es) accepted as known exceptions.*\n') else ''}
---
")
  } else {
    msg <- glue::glue("
{h1} \u26a0\ufe0f Data Integrity Check Failed: {dataset_name}

{h2} Workflow Halted

Mismatches have been detected between the CSV files and redefinition metadata.
These must be resolved before proceeding with database ingestion.

{h2} Detected Issues ({n_changes} total)

{detail_section}

{h2} Required Actions

Please review the changes detected above and update the following redefinition files:

- **Tables redefinition**: `{d$paths$tbls_rd_csv}`
- **Fields redefinition**: `{d$paths$flds_rd_csv}`

{h2} Common Resolutions

1. **New tables/fields in CSV**: Add them to the appropriate redefinition file
2. **Removed tables/fields from CSV**: Remove obsolete entries from redefinition files
3. **Type mismatches**: Update field types in redefinition files to match CSV data types
4. **Field name changes**: Update `fld_old` entries to match current CSV field names

{h2} Next Steps

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
      title = glue::glue("{dataset_name}: CSV vs Redefinition Mismatches"))
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
    passed       = passed,
    changes      = changes,
    n_changes    = n_changes,
    n_exceptions = n_exceptions,
    message      = msg
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
# check_taxon_ids --------------------------------------------------------------

#' The taxa that no authority resolved, per dataset — reported, and gated
#'
#' A taxon that reaches the release without an authority id is invisible to any
#' consumer that filters or joins on one, and nothing used to say so. That is how
#' all 128 Farallon taxa and 64,956 observations became unreachable through
#' `db-viz-hex::get_sp()`'s `worms_id` join while every check in the pipeline
#' passed.
#'
#' Two conditions, deliberately graded differently:
#'
#' - **A dataset-local `taxon_key`** (no `worms:` / `itis:` prefix) means *no
#'   authority resolved this taxon at all*. This **fails** unless the key is in
#'   `allow` — the allowlist is where a genuinely non-taxonomic class such as
#'   `cce-lter_zooscan:16` (naupliar stage) is declared, in the open, one key at
#'   a time. A new unresolved taxon can then never hide among the known ones.
#' - **An authority key with no `worms_id`** is reported but does not fail:
#'   WoRMS legitimately lacks some taxa (trinomial subspecies, mostly), and an
#'   `itis:`-keyed bird is correctly keyed either way.
#'
#' @param con a DBI connection holding `taxon` and `dataset_taxon` (and `obs`, if
#'   present, for the observation-level counts)
#' @param allow character vector of dataset-local `taxon_key`s that are known to
#'   be unresolvable and are accepted as such
#' @param halt logical; `stop()` on an unallowlisted local key (default `TRUE`)
#' @param verbose logical; message the summary
#' @return a data.frame, one row per `dataset_key`, with the taxon- and
#'   observation-level counts (invisibly when `verbose = FALSE`)
#' @export
#' @concept check
check_taxon_ids <- function(con, allow = character(), halt = TRUE,
                            verbose = TRUE) {
  present <- DBI::dbListTables(con)
  if (!all(c("taxon", "dataset_taxon") %in% present))
    stop("check_taxon_ids(): needs `taxon` and `dataset_taxon` in `con`.")
  has_obs <- "obs" %in% present

  # "no authority resolved this" — the key carries neither prefix
  is_local <- function(col) glue::glue(
    "{col} NOT LIKE 'worms:%' AND {col} NOT LIKE 'itis:%'")

  rpt <- DBI::dbGetQuery(con, glue::glue("
    SELECT dt.dataset_key,
           COUNT(DISTINCT dt.taxon_key)                                       AS n_taxa,
           COUNT(DISTINCT CASE WHEN t.worms_id IS NULL THEN dt.taxon_key END) AS n_no_worms,
           COUNT(DISTINCT CASE WHEN t.itis_id  IS NULL THEN dt.taxon_key END) AS n_no_itis,
           COUNT(DISTINCT CASE WHEN t.rank     IS NULL THEN dt.taxon_key END) AS n_no_rank,
           -- rank_order was NULL for 100% of ITIS-keyed taxa and 252 WoRMS ones
           -- for as long as the lookup lived in a single ingest's connection,
           -- and nothing here said so. A column whose only job is ordering a
           -- hierarchy is useless half-populated, so count it.
           COUNT(DISTINCT CASE WHEN t.rank IS NOT NULL AND t.rank_order IS NULL
                               THEN dt.taxon_key END)                         AS n_no_rank_order,
           COUNT(DISTINCT CASE WHEN {is_local('dt.taxon_key')}
                               THEN dt.taxon_key END)                         AS n_local_key
    FROM dataset_taxon dt LEFT JOIN taxon t ON t.taxon_key = dt.taxon_key
    GROUP BY 1 ORDER BY 1"))

  if (has_obs) {
    o <- DBI::dbGetQuery(con, glue::glue("
      SELECT o.dataset_key,
             COUNT(*)                                                AS n_obs,
             COUNT(*) FILTER (WHERE t.worms_id IS NULL)              AS n_obs_no_worms,
             COUNT(*) FILTER (WHERE {is_local('o.taxon_key')})       AS n_obs_local_key
      FROM obs o LEFT JOIN taxon t ON t.taxon_key = o.taxon_key
      WHERE o.taxon_key IS NOT NULL GROUP BY 1"))
    rpt <- dplyr::left_join(rpt, o, by = "dataset_key")
  }

  # the gate: dataset-local keys that are not declared
  local_keys <- DBI::dbGetQuery(con, glue::glue("
    SELECT DISTINCT dt.dataset_key, dt.taxon_key, dt.ds_scientific_name, dt.ds_common_name
    FROM dataset_taxon dt WHERE {is_local('dt.taxon_key')}
    ORDER BY 1, 2"))
  undeclared <- local_keys[!local_keys$taxon_key %in% allow, , drop = FALSE]

  if (verbose) {
    message(glue::glue(
      "taxon ids: {sum(rpt$n_taxa)} taxa across {nrow(rpt)} dataset(s); ",
      "{sum(rpt$n_no_worms)} without worms_id, ",
      "{sum(rpt$n_local_key)} with a dataset-local key ",
      "({nrow(local_keys) - nrow(undeclared)} allowlisted)"))
  }

  if (nrow(undeclared)) {
    detail <- paste(sprintf("  %s  %s", undeclared$taxon_key,
                            ifelse(is.na(undeclared$ds_scientific_name),
                                   undeclared$ds_common_name,
                                   undeclared$ds_scientific_name)),
                    collapse = "\n")
    msg <- paste0(
      nrow(undeclared), " taxon(s) resolved to no authority id and are not in ",
      "`allow`:\n", detail,
      "\n  A dataset-local taxon_key is invisible to any consumer joining on ",
      "worms_id/itis_id.\n  Either resolve it (metadata/taxon_override.csv, or ",
      "clean the source name so\n  the WoRMS lookup can match it), or declare it ",
      "in the allowlist as a known\n  non-taxonomic class.")
    if (halt) stop(msg, call. = FALSE) else warning(msg, call. = FALSE)
  }

  if (verbose) rpt else invisible(rpt)
}
