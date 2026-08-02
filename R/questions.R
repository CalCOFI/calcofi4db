# the provider-question registry -----------------------------------------------
#
# Every dataset carries `metadata/{provider}/{dataset}/questions.csv`: what we
# could not settle from the data, with the evidence that raised it. 17 files and
# 136 questions accumulated four spellings of "done" (`open` / `answered` /
# `resolved` / `wontfix`) and two of "normal" (`normal` / `medium`), because each
# ingest notebook read the CSV with a bare `read_csv()` and sorted by its own
# hand-written factor level vector — a status nobody's vector listed simply sorted
# to the bottom and was never seen again.
#
# So the vocabulary lives here, in one validated read that every notebook calls.

#' The controlled vocabulary of the question registry
#'
#' `status`:
#' \describe{
#'   \item{`open`}{asked, no answer and no proposal}
#'   \item{`proposed`}{**we have an answer to approve, not a problem to hand over**
#'     — `proposed_answer` holds what we did or suggest, and the provider is
#'     confirming it}
#'   \item{`answered`}{settled; `answer` holds the resolution}
#'   \item{`wontfix`}{closed without an answer, deliberately}
#' }
#'
#' `priority`: `blocker` (the ingest cannot be released as-is), `high`, `normal`,
#' `low`.
#'
#' @return Character vector of the allowed values.
#' @export
#' @concept registry
#' @examples
#' question_statuses()
#' question_priorities()
question_statuses <- function() c("open", "proposed", "answered", "wontfix")

#' @rdname question_statuses
#' @export
question_priorities <- function() c("blocker", "high", "normal", "low")

QUESTION_COLS <- c(
  "label", "id", "question", "context", "status", "priority",
  "proposed_answer", "answer", "asked_date", "answered_date", "who",
  "related_table", "related_field")

#' Read a dataset's `questions.csv`, validated and ranked
#'
#' The single reader for the provider-question registry. Reads strictly
#' (`na = ""`, everything character, so a date or an id like `01` is never
#' silently retyped), checks the controlled vocabulary, and returns the questions
#' ranked `blocker` → `low` then by `label`.
#'
#' Two identifiers, deliberately:
#' * **`id`** — `{provider}_{dataset}_{nn}`, globally unique and durable. This is
#'   what a cross-dataset reference or an issue tracker cites.
#' * **`label`** — the short form (`Q15`), unique *within* the dataset. This is
#'   what prose in a notebook says, and what the rendered table shows first, so
#'   "see Q15" resolves for a reader.
#'
#' @param path path to a `questions.csv`
#' @param validate error on an unknown `status`/`priority`, a duplicate `label`,
#'   or a `label` that disagrees with `id` (default TRUE)
#'
#' @return A [tibble][tibble::tibble], all columns character, ranked.
#' @export
#' @concept registry
#' @importFrom readr read_csv cols col_character
#' @examples
#' \dontrun{
#' read_questions("metadata/calcofi/ctd-cast/questions.csv")
#' }
read_questions <- function(path, validate = TRUE) {
  stopifnot("questions.csv not found" = file.exists(path))
  # na = "" and all-character: an `asked_date` of "" must stay empty rather than
  # becoming the string "NA" on the next write (see R/registry.R)
  d <- readr::read_csv(path, na = "", show_col_types = FALSE,
                       col_types = readr::cols(.default = readr::col_character()))

  miss <- setdiff(QUESTION_COLS, names(d))
  if (length(miss))
    stop("questions registry ", path, " is missing column(s): ",
         paste(miss, collapse = ", "),
         "\n  Expected: ", paste(QUESTION_COLS, collapse = ", "), call. = FALSE)

  if (isTRUE(validate)) {
    check_registry_na_strings(d, path)

    bad <- setdiff(stats::na.omit(unique(d$status)), question_statuses())
    if (length(bad))
      stop("unknown question status in ", path, ": ", paste(bad, collapse = ", "),
           "\n  Allowed: ", paste(question_statuses(), collapse = " | "),
           call. = FALSE)

    bad <- setdiff(stats::na.omit(unique(d$priority)), question_priorities())
    if (length(bad))
      stop("unknown question priority in ", path, ": ", paste(bad, collapse = ", "),
           "\n  Allowed: ", paste(question_priorities(), collapse = " | "),
           call. = FALSE)

    dup <- unique(d$label[duplicated(d$label)])
    if (length(dup))
      stop("duplicate question label(s) in ", path, ": ",
           paste(dup, collapse = ", "),
           "\n  `label` must be unique within a dataset — it is what prose cites.",
           call. = FALSE)

    # `label` is AUTHORED, not derived from `id`. For 16 of the 17 registries it
    # is mechanically `Q` + the id's numeric suffix, but `calcofi/hydro-master`
    # carries two id namespaces (`hydro_master_*` and `recon_*`) that would
    # collide on that rule, and its ids are cited by name in the protocol and the
    # CTD ingest — so the id stays and the label disambiguates (`Q01` / `QR01`).
    off <- which(is.na(d$label) | !grepl("^Q[0-9A-Za-z]+$", d$label))
    if (length(off))
      stop("malformed question label(s) in ", path, ": ",
           paste(sprintf("%s (id %s)", d$label[off], d$id[off]), collapse = "; "),
           "\n  Expected the short display form, e.g. Q15.", call. = FALSE)
  }

  d[order(match(d$priority, question_priorities()), d$label), , drop = FALSE]
}

#' Render a question registry as the standard notebook table
#'
#' The `## Questions for Data Providers` section every ingest notebook ends with.
#' One call so the 16 notebooks cannot show different columns in different orders
#' — which they did, each with its own hand-written priority factor.
#'
#' Columns that are empty for every question are dropped, so a dataset with no
#' answers yet does not render two blank columns.
#'
#' @param x a path to a `questions.csv`, or a data.frame from [read_questions()]
#' @param caption table caption
#' @param page_length rows per page
#'
#' @return A [DT::datatable()] htmlwidget.
#' @export
#' @concept registry
#' @importFrom DT datatable
#' @examples
#' \dontrun{
#' questions_datatable(here::here(cc$questions_file))
#' }
questions_datatable <- function(x, caption = "Questions for data providers (ranked)",
                                page_length = 25) {
  d <- if (is.character(x)) read_questions(x) else x
  keep <- c("label", "priority", "status", "question", "context",
            "proposed_answer", "answer", "related_table", "related_field")
  keep <- intersect(keep, names(d))
  d <- d[, keep, drop = FALSE]
  # an all-empty column is noise, not information
  d <- d[, vapply(d, function(v) any(!is.na(v) & nzchar(v)), logical(1)), drop = FALSE]

  DT::datatable(
    d, caption = caption, rownames = FALSE,
    options = list(pageLength = page_length, scrollX = TRUE, dom = "tip",
                   columnDefs = list(list(width = "60px", targets = 0))))
}
