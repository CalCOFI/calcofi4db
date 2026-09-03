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

# check_dataset_taxon ----------------------------------------------------------

#' The ingest asserts its own taxon crosswalk (taxon plan D6)
#'
#' Call it after [resolve_dataset_taxon()] and before `append_obs()`. Three
#' findings, each a row of the returned report:
#'
#' - **`missing_code`** — a code the observations reference (`codes`) that is not
#'   in this dataset's `dataset_taxon`. Farallon's `MEGU` (the pre-split Mew Gull
#'   code, present in the observations and absent from the species list) is the
#'   case that motivated it: an `obs` projection joining on the code would drop
#'   or NULL those rows with no error anywhere.
#' - **`unresolved`** — a `dataset_taxon` row with no authority `taxon_key`
#'   (`worms:` / `itis:`), unless its dataset-local key is in `allow` — the
#'   ingest's own declaration of a genuinely non-taxonomic class (zooscan
#'   "nauplii", phyto "undefined code"), one key at a time, with a comment.
#' - **`aves_not_itis`** — a taxon whose class is Aves that did not key `itis:`
#'   (no accepted TSN resolved; see [taxon_key_of()]). Checked here because this
#'   is where it is cheap to fix — a TSN in `taxon_override.csv`. An ingest that
#'   accepts the `worms:` key lists that key in `allow`. Needs `taxon` in `con`
#'   ([build_taxon_reference()]) for the class; skipped without it.
#'
#' `release_database.qmd`'s [check_taxon_ids()] stays as the backstop.
#'
#' @param con a DBI connection holding `dataset_taxon` (and `taxon`)
#' @param dataset_key the dataset whose crosswalk is checked
#' @param allow character vector of `taxon_key`s accepted as-is: dataset-local
#'   keys of non-taxonomic classes, or a `worms:` key for an Aves taxon with no
#'   TSN
#' @param halt logical; `stop()` on any finding (default `TRUE`)
#' @param codes optional character vector of the `ds_taxa_code`s the
#'   observations reference (e.g. `DISTINCT species_code` of the source
#'   observation table); every one must be in the vocabulary
#' @param verbose logical; message the summary
#' @return a data.frame with one row per finding (`check`, `ds_taxon_key`,
#'   `ds_taxa_code`, `taxon_key`, `detail`); zero rows when clean. Invisible
#'   when `verbose = FALSE`.
#' @export
#' @concept check
check_dataset_taxon <- function(con, dataset_key, allow = character(), halt = TRUE,
                                codes = NULL, verbose = TRUE) {
  stopifnot(is.character(dataset_key), length(dataset_key) == 1L)
  present <- DBI::dbListTables(con)
  dt <- if ("dataset_taxon" %in% present) DBI::dbGetQuery(con, "
    SELECT ds_taxon_key, ds_taxa_code, ds_scientific_name, taxon_key
    FROM dataset_taxon WHERE dataset_key = ? ORDER BY ds_taxon_key",
    params = list(dataset_key)) else NULL
  if (is.null(dt) || !nrow(dt)) stop(glue::glue(
    "check_dataset_taxon(): no dataset_taxon rows for '{dataset_key}' — call ",
    "append_dataset_taxon() and resolve_dataset_taxon() first."), call. = FALSE)

  empty <- data.frame(check = character(), ds_taxon_key = character(),
                      ds_taxa_code = character(), taxon_key = character(),
                      detail = character(), stringsAsFactors = FALSE)
  find <- list()

  # 1. every code the observations use exists in the vocabulary
  if (!is.null(codes)) {
    codes <- unique(as.character(codes)); codes <- codes[!is.na(codes)]
    miss  <- setdiff(codes, dt$ds_taxa_code)
    if (length(miss)) find$missing <- data.frame(
      check = "missing_code", ds_taxon_key = NA_character_, ds_taxa_code = miss,
      taxon_key = NA_character_,
      detail = "referenced by the observations, absent from dataset_taxon",
      stringsAsFactors = FALSE)
  }

  # 2. every row has an authority key, or is declared
  local <- is.na(dt$taxon_key) | !grepl("^(worms|itis):", dt$taxon_key)
  un <- dt[local & !(dt$taxon_key %in% allow), , drop = FALSE]
  if (nrow(un)) find$unresolved <- data.frame(
    check = "unresolved", ds_taxon_key = un$ds_taxon_key, ds_taxa_code = un$ds_taxa_code,
    taxon_key = un$taxon_key,
    detail = paste0("no authority id resolved",
                    ifelse(is.na(un$ds_scientific_name), "",
                           paste0(" for \"", un$ds_scientific_name, "\""))),
    stringsAsFactors = FALSE)

  # 3. every Aves taxon keys itis:
  if ("taxon" %in% present) {
    cls <- DBI::dbGetQuery(con, "SELECT taxon_key FROM taxon WHERE \"class\" = 'Aves'")$taxon_key
    av <- dt[dt$taxon_key %in% cls & !grepl("^itis:", dt$taxon_key) &
             !(dt$taxon_key %in% allow), , drop = FALSE]
    if (nrow(av)) find$aves <- data.frame(
      check = "aves_not_itis", ds_taxon_key = av$ds_taxon_key, ds_taxa_code = av$ds_taxa_code,
      taxon_key = av$taxon_key,
      detail = "class Aves but keyed worms: (no accepted TSN resolved)",
      stringsAsFactors = FALSE)
  }

  rpt <- if (length(find)) dplyr::bind_rows(find) else empty
  rpt <- as.data.frame(rpt, stringsAsFactors = FALSE)

  if (verbose) message(glue::glue(
    "check_dataset_taxon('{dataset_key}'): {nrow(dt)} taxa, ",
    "{sum(!local)} keyed by an authority, {sum(local)} local ",
    "({sum(local & dt$taxon_key %in% allow)} allowed); {nrow(rpt)} finding(s)"))

  if (nrow(rpt)) {
    detail <- paste(sprintf("  [%s] %s  %s", rpt$check,
                            ifelse(is.na(rpt$ds_taxon_key), rpt$ds_taxa_code, rpt$ds_taxon_key),
                            rpt$detail), collapse = "\n")
    msg <- paste0(
      "check_dataset_taxon('", dataset_key, "'): ", nrow(rpt), " finding(s):\n", detail,
      "\n  missing_code: add the code to the vocabulary (append_dataset_taxon());",
      "\n  unresolved: resolve it (metadata/taxon_override.csv, or clean the source name)",
      "\n    or declare it in `allow` as a known non-taxonomic class;",
      "\n  aves_not_itis: supply the TSN via metadata/taxon_override.csv, or `allow` the worms: key.")
    if (halt) stop(msg, call. = FALSE) else warning(msg, call. = FALSE)
  }
  if (verbose) rpt else invisible(rpt)
}

# check_taxon_registries -------------------------------------------------------

#' Every dataset a taxon registry names must be one some dataset supplies
#'
#' `metadata/taxon_override.csv` and `metadata/taxon_group.csv` are read whole
#' by every ingest while each loads only its own vocabulary, so a row for
#' another dataset is normal there and cannot be validated. This is the check
#' for the place where every dataset IS present — the release, after
#' `assemble_core()` — replacing the hard-coded list of dataset names the
#' package used to validate against (taxon plan D5). The allowed set is the
#' `dataset_key`s present in `dataset_taxon` ∪ `measurement_taxon`; a row naming
#' anything else (a typo, a retired dataset) errors, because a registry row that
#' matches nothing is how a missing id hides.
#'
#' @param con a DBI connection holding the assembled `dataset_taxon`
#' @param overrides the override registry (`metadata/taxon_override.csv`), or NULL
#' @param group_rules the group registry ([read_taxon_group_rules()]), or NULL
#' @param measurement_taxon the composite crosswalk (`metadata/measurement_taxon.csv`),
#'   whose `dataset_key`s count as supplied, or NULL
#' @param halt logical; `stop()` on an orphan (default `TRUE`)
#' @return (invisibly) a named list of the orphan `dataset_key`s per registry
#' @export
#' @concept check
check_taxon_registries <- function(con, overrides = NULL, group_rules = NULL,
                                   measurement_taxon = NULL, halt = TRUE) {
  if (!"dataset_taxon" %in% DBI::dbListTables(con))
    stop("check_taxon_registries(): needs `dataset_taxon` in `con`.")
  known <- DBI::dbGetQuery(con, "SELECT DISTINCT dataset_key FROM dataset_taxon")$dataset_key
  if (!is.null(measurement_taxon) && nrow(measurement_taxon))
    known <- union(known, as.character(measurement_taxon$dataset_key))
  known <- stats::na.omit(known)

  orphans <- function(reg) {
    if (is.null(reg) || !nrow(reg) || is.null(reg$dataset_key)) return(character())
    setdiff(stats::na.omit(unique(as.character(reg$dataset_key))), known)
  }
  out <- list(overrides = orphans(overrides), group_rules = orphans(group_rules))
  bad <- Filter(length, out)
  if (length(bad)) {
    msg <- paste0(
      "taxon registries name dataset_key(s) nothing supplies:\n",
      paste(sprintf("  %s: %s", names(bad),
                    vapply(bad, function(x) paste(sprintf("`%s`", x), collapse = ", "), "")),
            collapse = "\n"),
      "\n  Known: ", paste(sort(known), collapse = ", "),
      "\n  A registry row that matches no dataset is how a typo becomes a missing id.")
    if (halt) stop(msg, call. = FALSE) else warning(msg, call. = FALSE)
  }
  invisible(out)
}

# check_cruise_coverage --------------------------------------------------------

#' Cruises that carry samples but no observations — the silent-loss guard
#'
#' A cruise can leave `obs` without leaving `sample`, and nothing about that
#' violates a foreign key: FK validation runs child -> parent, so every surviving
#' `obs` row still has a parent, and a parent with **no children** breaks no
#' constraint. Release `v2026.08.08` shipped in exactly that state — 10
#' `calcofi_ctd-cast` cruises kept all 1,186 of their casts and lost all 874,000
#' of their observations, because a Google Drive placeholder read as zero rows and
#' the direction letter the thinning step needs came from the filename of a
#' conflict copy. No check anywhere looked at the parent side.
#'
#' The grain is the **cruise**, deliberately, and it is not the sample. A CTD
#' `sample` row is one physical cast *per direction* while `obs` keeps a single
#' direction, so about half of `calcofi_ctd-cast`'s cast rows legitimately carry
#' no observations and a per-sample assertion would be wrong on arrival. A whole
#' cruise with none is never legitimate.
#'
#' A dataset that emits **no** observations at all is exempt rather than failing
#' 587 times: `sio_pic-zooplankton` is a net-tow registry whose biovolumes are
#' still pending from the provider, so contributing `sample` alone is its designed
#' state. The rule is therefore relative — *if a dataset contributes observations,
#' every one of its cruises must* — which needs no allowlist to say so.
#'
#' A third case sits between those two: a dataset that emits observations, but
#' whose `sample` table also carries rows that are an **inventory rather than an
#' analyzed event**. `cdfw_dungeness-crab` is the worked example — its 310
#' `subsample` rows are lab-examined aliquots and every one of them yields `obs`
#' (310/310), while its 2,011 `tow` rows are a 60-year sorting log recording which
#' archived jars *exist*. Only 216 of those were ever examined. The remaining
#' 1,795 have no observation to lose, and 14 cruises consist of nothing else.
#' Raising `max_orphan_cruises` would paper over that, and would go on hiding a
#' real loss in the same dataset up to the allowance — so the exemption is
#' declared at the grain where the distinction actually lives, the sample type.
#'
#' @param con a DBI connection holding `sample` and `obs`
#' @param obs_tbl name of the observation table (default `"obs"`)
#' @param effort_only_types optional named character vector keyed by
#'   `dataset_key`, naming `sample_type`s that record effort or inventory rather
#'   than an analyzed event (e.g. `c("cdfw_dungeness-crab" = "tow")`). Those rows
#'   are excluded from the orphan calculation entirely, so a cruise made only of
#'   them is not a finding, while the same dataset's observing sample types stay
#'   held to the full standard. Repeat the name to exempt several types.
#' @param max_orphan_cruises integer allowance, or a named integer vector keyed by
#'   `dataset_key` for a per-dataset ratchet. Use `0` where the correct answer is
#'   known to be zero (an ingest asserting its own output); use the current counts
#'   as a ratchet at release time so a *new* orphan fails while a documented
#'   backlog does not. May only ever be lowered.
#' @param halt logical; `stop()` when the allowance is exceeded (default `TRUE`)
#' @param verbose logical; message the summary
#' @return a data.frame, one row per `dataset_key`, with `cruises`,
#'   `cruises_no_obs`, `orphan_samples` and `emits_obs` (invisibly when
#'   `verbose = FALSE`)
#' @export
#' @concept check
check_cruise_coverage <- function(con, obs_tbl = "obs",
                                  max_orphan_cruises = 0L,
                                  effort_only_types = NULL,
                                  halt = TRUE, verbose = TRUE) {
  present <- DBI::dbListTables(con)
  if (!all(c("sample", obs_tbl) %in% present))
    stop(glue::glue(
      "check_cruise_coverage(): needs `sample` and `{obs_tbl}` in `con`."))

  # inventory/effort sample types drop out before anything is counted, so a
  # cruise made only of them never becomes a finding. Built as an explicit
  # (dataset_key, sample_type) pair list rather than two IN clauses, which would
  # exempt the type across every dataset that happens to use the same word —
  # `tow` is an observing type for the net-tow ingests.
  ex_sql <- ""
  if (length(effort_only_types)) {
    if (is.null(names(effort_only_types)))
      stop("check_cruise_coverage(): `effort_only_types` must be named by dataset_key.")
    q <- function(x) DBI::dbQuoteString(con, as.character(x))
    pairs <- paste0("(", q(names(effort_only_types)), ", ",
                    q(unname(effort_only_types)), ")", collapse = ", ")
    ex_sql <- glue::glue(
      " AND (dataset_key, sample_type) NOT IN ({pairs})")
  }

  # join through sample_key, never through obs.cruise_key: the denormalized
  # cruise_key on obs is NULL for 59,274 swfsc_cufes rows and 14,170 euphausiid
  # ones, which would invent orphans that do not exist.
  rpt <- DBI::dbGetQuery(con, glue::glue("
    WITH s AS (
      SELECT dataset_key, cruise_key, sample_key
      FROM sample WHERE cruise_key IS NOT NULL{ex_sql}),
    o AS (SELECT DISTINCT sample_key FROM {obs_tbl}),
    j AS (
      SELECT s.dataset_key, s.cruise_key, COUNT(*) AS samples,
             COUNT(*) FILTER (WHERE o.sample_key IS NOT NULL) AS samples_with_obs
      FROM s LEFT JOIN o USING (sample_key) GROUP BY 1, 2)
    SELECT dataset_key,
           COUNT(*)                                             AS cruises,
           COUNT(*) FILTER (WHERE samples_with_obs = 0)         AS cruises_no_obs,
           COALESCE(SUM(samples) FILTER (WHERE samples_with_obs = 0), 0)
                                                                AS orphan_samples
    FROM j GROUP BY 1 ORDER BY 1"))
  rpt$emits_obs <- rpt$cruises_no_obs < rpt$cruises
  # a registry-only dataset has no observations to lose
  rpt$cruises_no_obs[!rpt$emits_obs]  <- 0L
  rpt$orphan_samples[!rpt$emits_obs]  <- 0L

  allow <- if (is.null(names(max_orphan_cruises))) {
    stats::setNames(rep(as.integer(max_orphan_cruises)[1], nrow(rpt)),
                    rpt$dataset_key)
  } else {
    a <- stats::setNames(rep(0L, nrow(rpt)), rpt$dataset_key)
    a[names(max_orphan_cruises)] <- as.integer(max_orphan_cruises)
    a
  }
  over <- rpt[rpt$cruises_no_obs > allow[rpt$dataset_key], , drop = FALSE]

  if (verbose)
    message(glue::glue(
      "cruise coverage: {sum(rpt$cruises_no_obs)} cruise(s) with samples and no ",
      "{obs_tbl} across {sum(rpt$emits_obs)} observing dataset(s) ",
      "({sum(!rpt$emits_obs)} registry-only, exempt)"))

  if (nrow(over)) {
    detail <- paste(sprintf(
      "  %s: %d of %d cruise(s), %d orphan sample(s) — allowance %d",
      over$dataset_key, over$cruises_no_obs, over$cruises,
      over$orphan_samples, allow[over$dataset_key]), collapse = "\n")
    msg <- paste0(
      "cruise(s) carry samples but no ", obs_tbl, ":\n", detail,
      "\n  The casts survive and every FK still resolves, so nothing else will\n",
      "  report this. Find where the observations were dropped before releasing;\n",
      "  raising the allowance to make the check pass republishes the loss.")
    if (halt) stop(msg, call. = FALSE) else warning(msg, call. = FALSE)
  }

  if (verbose) rpt else invisible(rpt)
}
# check_ungridded_obs -------------------------------------------------------

#' Observations that resolve no CalCOFI grid cell — reported, never dropped
#'
#' Until v2026.08.11 every ingest's core projection filtered
#' `WHERE grid_key IS NOT NULL`, so an observation whose event did not land on a
#' station grid cell never reached `obs` at all — while the `sample` arm kept the
#' event. That asymmetry is what let four `calcofi_mets` cruises reach a release
#' as 11,762 underway samples with zero observations, their 1.7M measurements
#' reachable only through the supplemental table.
#'
#' Excluding them was also inconsistent with the pipeline's own reasoning:
#' `obs_mets_full` had already been deliberately gated on *a position* rather
#' than on `grid_key`, because "a ship on transit is legitimately outside the
#' CalCOFI station grid" — and `calcofi_phytoplankton` is region-pooled and has
#' emitted ungridded `obs` from the start. The headline table now agrees with
#' both: no grid cell is not a reason to delete an observation.
#'
#' It IS a reason to ask. An ungridded observation is one of three things and the
#' pipeline cannot tell them apart: a genuinely off-grid position (transit, an
#' historical station outside the modern pattern), a coarser spatial notion (a
#' region-pooled sample with no point at all), or **a coordinate error** — the
#' sign-flipped `Longitude_W` that put five CalCOFI cruises in the Taiwan Strait
#' was invisible precisely because being off-grid silently removed the rows. So
#' this reports every dataset's share and is meant to drive a `questions.csv`
#' entry per dataset, not to be quietly tolerated.
#'
#' @param con a DBI connection holding `obs`
#' @param obs_tbl name of the observation table (default `"obs"`)
#' @param verbose logical; message the headline
#' @return a data.frame, one row per `dataset_key`: `n_obs`, `n_ungridded`,
#'   `pct_ungridded`, `n_no_position` (ungridded AND no lat/lon at all), and
#'   `finding`, a sentence ready to paste into a `questions.csv` `context` cell
#' @export
#' @concept check
check_ungridded_obs <- function(con, obs_tbl = "obs", verbose = TRUE) {
  if (!obs_tbl %in% DBI::dbListTables(con))
    stop(glue::glue("check_ungridded_obs(): `{obs_tbl}` not in `con`."))

  rpt <- DBI::dbGetQuery(con, glue::glue("
    SELECT dataset_key,
           COUNT(*)                                              AS n_obs,
           COUNT(*) FILTER (WHERE grid_key IS NULL)               AS n_ungridded,
           COUNT(*) FILTER (WHERE grid_key IS NULL
                              AND (latitude IS NULL OR longitude IS NULL))
                                                                  AS n_no_position
    FROM {obs_tbl} GROUP BY 1 ORDER BY 1"))
  rpt$pct_ungridded <- round(100 * rpt$n_ungridded / pmax(rpt$n_obs, 1), 2)

  rpt$finding <- ifelse(
    rpt$n_ungridded == 0, NA_character_,
    sprintf(paste0(
      "%s of %s obs rows (%.2f%%) resolve no CalCOFI grid cell; %s of those ",
      "carry no latitude/longitude at all. These are now RELEASED rather than ",
      "dropped. Are they off-grid positions (transit, historical stations), a ",
      "coarser spatial notion (region-pooled), or a coordinate error?"),
      format(rpt$n_ungridded, big.mark = ","), format(rpt$n_obs, big.mark = ","),
      rpt$pct_ungridded, format(rpt$n_no_position, big.mark = ",")))

  if (verbose)
    message(glue::glue(
      "ungridded obs: {format(sum(rpt$n_ungridded), big.mark = ',')} of ",
      "{format(sum(rpt$n_obs), big.mark = ',')} rows ",
      "({round(100*sum(rpt$n_ungridded)/max(sum(rpt$n_obs),1), 2)}%) across ",
      "{sum(rpt$n_ungridded > 0)} dataset(s) — reported, not dropped"))

  if (verbose) rpt else invisible(rpt)
}
