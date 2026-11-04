# the consent gate -------------------------------------------------------------
#
# A dataset enters the pipeline held (`in_release: false`), is shown to its
# provider in a private preview, and the provider's decision is folded into the
# dataset's standing consent question: the row of
# `metadata/{provider}/{dataset}/questions.csv` whose `related_field` is
# `consent` (`scripts/pull_preview_consent.R` in workflows). Until 2026-11-04
# nothing checked that the consent was given before the dataset was released:
# flipping `in_release` was enough. This gate closes that gap.
#
# Every public dataset carries a consent row too (added 2026-10-22, agreed with
# Betty Huang and Erin Satterthwaite): the openly licensed ones are `answered`
# with "No ask needed: openly licensed at its public source", the others hold an
# ask that is still out with the PI. Those datasets entered the public database
# before the gate existed, so a pending ask on one of them is reported, not a
# stop; a provider's `no` is a stop for every dataset (the cetacean sightings
# were withdrawn at the lab's request, 2026-10-08).

#' The consent gate: no dataset reaches the public database before its provider accepts
#'
#' Reads each released dataset's standing consent question — the row of
#' `metadata/{provider}/{dataset}/questions.csv` whose `related_field` is
#' `consent` — and classifies it:
#' \describe{
#'   \item{`none`}{no `questions.csv`, or no consent row (ok: the dataset predates
#'     the consent process)}
#'   \item{`pending`}{the row is `open` or `proposed`: the ask is out}
#'   \item{`declined`}{the row is `wontfix`, or `answered` with an answer that is
#'     not a yes}
#'   \item{`given`}{the row is `answered` and the answer begins with `yes`, or with
#'     `no ask needed` (the 2026-10-22 convention for a dataset openly licensed at
#'     its public source, `who = "Ben Best (licence at source)"`); it is `ok` only
#'     when `who` names who gave it}
#' }
#'
#' A dataset is `ok` when its consent is `none`, or `given` with a `who`. A
#' `pending` ask is ok for a dataset in `exempt` (a private preview run passes
#' `CALCOFI_RELEASE_INCLUDE`: the preview is where the decision is made) and for
#' a dataset in `legacy` (one that entered the public database before the gate
#' existed; by default a notebook with no `in_release` key at all — un-holding a
#' dataset leaves `in_release: true` in its notebook, so a dataset that came in
#' through the preview pathway is never legacy). `declined` is ok only for an
#' exempt dataset: a provider's no withdraws the data. A public run has an empty
#' `exempt`, so every dataset un-held through the preview must carry a given
#' consent before `release_database.qmd` proceeds.
#'
#' @param workflow_dir the workflows checkout: holds the `ingest_*.qmd`
#'   notebooks and `metadata/{provider}/{dataset}/questions.csv`
#' @param datasets dataset keys (`{provider}_{dataset}`) to check; default: the
#'   datasets the release will include (`in_release` not `false`) plus `exempt`
#' @param exempt dataset keys reported but never failed (a preview run's
#'   `CALCOFI_RELEASE_INCLUDE`)
#' @param legacy dataset keys released before the gate existed, for which a
#'   pending ask is reported but does not fail; default: every notebook with no
#'   `in_release` key. Pass `character()` to require a given consent everywhere.
#' @param halt stop when any dataset is not ok (default TRUE), naming each
#'   failing dataset and its state
#' @return A [tibble][tibble::tibble], one row per dataset: `dataset_key`,
#'   `consent` (`none` | `pending` | `declined` | `given`), `label`, `status`,
#'   `answer`, `who`, `answered_date`, `ok`, plus `exempt`, `legacy` and `why`
#'   (the basis of `ok`).
#' @export
#' @concept release
#' @importFrom tibble tibble
#' @importFrom rlang %||%
#' @seealso [read_questions()], [release_excluded_datasets()]
#' @examples
#' \dontrun{
#' # release_database.qmd, after ds_excluded is settled
#' check_dataset_consent(here::here(), exempt = release_include, halt = TRUE)
#' }
check_dataset_consent <- function(workflow_dir, datasets = NULL, exempt = character(),
                                  legacy = NULL, halt = TRUE) {
  stopifnot(dir.exists(workflow_dir))
  yaml_all <- read_ingest_yaml(workflow_dir)
  exempt   <- as.character(exempt)
  if (is.null(datasets))
    datasets <- union(names(read_ingest_yaml(workflow_dir, in_release_only = TRUE)), exempt)
  datasets <- as.character(datasets)
  unknown  <- setdiff(datasets, names(yaml_all))
  if (length(unknown))
    stop("check_dataset_consent(): no ingest notebook declares dataset(s): ",
         paste(unknown, collapse = ", "), call. = FALSE)
  if (is.null(legacy))
    legacy <- names(yaml_all)[vapply(yaml_all, function(cc) is.null(cc$in_release), logical(1))]
  legacy <- as.character(legacy)

  rows <- lapply(datasets, function(key) {
    cc <- yaml_all[[key]]
    .consent_row(key, cc$provider, cc$dataset, workflow_dir,
                 exempt = key %in% exempt, legacy = key %in% legacy)
  })
  out <- do.call(rbind, c(rows, list(.consent_row_empty())))

  if (isTRUE(halt) && any(!out$ok)) {
    bad <- out[!out$ok, , drop = FALSE]
    stop("check_dataset_consent(): ", nrow(bad), " dataset(s) lack the provider's consent:\n",
         paste0("  - ", bad$dataset_key, ": ", bad$consent,
                ifelse(is.na(bad$label), "", paste0(" (", bad$label, ", status ", bad$status, ")")),
                " — ", bad$why, collapse = "\n"),
         "\n  A dataset reaches the public database only after its consent row is",
         " `answered` with a `yes` and a `who`; a preview run exempts the datasets",
         " it includes (CALCOFI_RELEASE_INCLUDE).", call. = FALSE)
  }
  out
}

.consent_row_empty <- function() {
  tibble::tibble(
    dataset_key = character(), consent = character(), label = character(),
    status = character(), answer = character(), who = character(),
    answered_date = character(), ok = logical(), exempt = logical(),
    legacy = logical(), why = character())
}

# classify one dataset's consent row
.consent_row <- function(key, provider, dataset, workflow_dir, exempt, legacy) {
  path <- file.path(workflow_dir, "metadata", provider, dataset, "questions.csv")
  q <- NULL
  if (file.exists(path)) {
    q <- read_questions(path)
    q <- q[!is.na(q$related_field) & q$related_field == "consent", , drop = FALSE]
    if (nrow(q) > 1)
      stop("check_dataset_consent(): ", path, " holds ", nrow(q),
           " consent rows (related_field = consent); exactly one is the standing consent question",
           call. = FALSE)
  }
  row <- tibble::tibble(
    dataset_key = key, consent = "none", label = NA_character_, status = NA_character_,
    answer = NA_character_, who = NA_character_, answered_date = NA_character_,
    ok = TRUE, exempt = exempt, legacy = legacy, why = "no consent row")
  if (is.null(q) || nrow(q) == 0) return(row)

  s   <- function(x) { x <- trimws(x %||% NA_character_); if (is.na(x)) "" else x }
  status <- s(q$status); answer <- s(q$answer); who <- s(q$who)
  row$label         <- q$label
  row$status        <- status
  row$answer        <- if (nzchar(answer)) answer else NA_character_
  row$who           <- if (nzchar(who)) who else NA_character_
  row$answered_date <- if (nzchar(s(q$answered_date))) s(q$answered_date) else NA_character_

  yes <- grepl("^(yes\\b|no ask needed)", tolower(answer))
  row$consent <- if (status %in% c("open", "proposed")) "pending" else
    if (status == "answered" && yes) "given" else "declined"

  if (row$consent == "given") {
    row$ok  <- nzchar(who)
    row$why <- if (nzchar(who)) "consent given" else
      "consent given but `who` is empty: record who gave it"
  } else if (exempt) {
    row$ok  <- TRUE
    row$why <- paste0(row$consent, "; exempt (included by the preview run, where the decision is made)")
  } else if (row$consent == "pending") {
    row$ok  <- legacy
    row$why <- if (legacy) "ask pending; released before the consent gate (no in_release key)" else
      "ask pending: the provider has not yet said yes"
  } else {
    row$ok  <- FALSE
    row$why <- if (status == "wontfix") "closed without consent (wontfix)" else
      if (nzchar(answer)) "answered, but not a yes" else "answered with no answer text"
  }
  row
}
