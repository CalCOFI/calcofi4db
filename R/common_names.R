# Vernacular (common) names -----------------------------------------------------
#
# `common_name` reaches the release from a DATASET'S OWN vocabulary — the ichthyo
# species list, the bird/mammal list — and nowhere else. Every taxon resolved
# through `measurement_taxon.csv` / `taxon_override.csv` instead therefore lands
# with a scientific name and no common name: 1,208 of the release's 2,125 taxa
# (57%) at v2026.08.14, including `worms:440388` Metacarcinus magister, whose
# absent "Dungeness crab" is what surfaced this.
#
# WoRMS has the names, but WILL NOT CHOOSE BETWEEN THEM. Its vernacular endpoint
# returns an unordered bag with no `isPreferredName` flag through `worrms`, so
# AphiaID 440388 comes back as four equally-weighted English strings:
#
#     Californian crab | Dungeness crab | Dungeness rock crab | Pacific crab
#
# Any automatic rule picks wrong here. Alphabetical-first gives "Californian
# crab"; longest gives "Dungeness rock crab"; shortest gives "Pacific crab". The
# name a reader expects is the second one, and nothing in the payload says so.
#
# So the rule is: FETCH ALWAYS, CHOOSE ONLY WHEN THERE IS NOTHING TO CHOOSE.
# One English name is not a choice, so it is taken automatically. Two or more is
# a judgement, so `common_name` is left EMPTY with every candidate recorded in
# `candidates_en`, and a human fills the cell. An unfilled cell publishes no
# common name, which is the honest state — it never guesses and calls it data.
#
# `metadata/taxon_common.csv` is therefore BOTH the generated cache and the place
# the selection is made. A re-run never overwrites a non-empty `common_name`, so
# an edit survives; delete the file to refetch from scratch. Written with
# `na = ""` (see the round-trip trap in the workflows CLAUDE.md).
#
# APPLIED CENTRALLY, not per ingest. `release_database.qmd` merges the per-dataset
# taxon shards rather than rebuilding them, so filling `common_name` here — once,
# on the merged table — is both the cheaper path (no ingest re-run) and the one
# that cannot drift between 10 shards. Same reasoning as `dataset` and the
# observed coverage columns, which are also derived centrally.

.common_cache_cols <- c("taxon_key", "scientific_name", "common_name",
                        "candidates_en", "n_candidates_en", "source",
                        "checked_date", "notes")

.empty_common <- function()
  data.frame(taxon_key = character(), scientific_name = character(),
             common_name = character(), candidates_en = character(),
             n_candidates_en = integer(), source = character(),
             checked_date = character(), notes = character(),
             stringsAsFactors = FALSE)

# English vernaculars for one AphiaID, de-duplicated and case-folded.
# character(0) when WoRMS has none — a 204, which worrms raises as an error
# rather than returning an empty frame.
.worms_vernaculars_en <- function(worms_id, sleep = 0.3) {
  if (is.na(worms_id)) return(character())
  r <- try(worrms::wm_common_id(as.integer(worms_id)), silent = TRUE)
  if (sleep > 0) Sys.sleep(sleep)
  if (inherits(r, "try-error")) return(character())
  r <- as.data.frame(r)
  if (!nrow(r) || !"language_code" %in% names(r)) return(character())
  v <- r$vernacular[r$language_code %in% "eng"]
  v <- unique(trimws(v[!is.na(v) & nzchar(v)]))
  # "Dungeness crab" and "dungeness crab" are one name, not two candidates
  v[!duplicated(tolower(v))]
}

#' Read the vernacular-name registry
#'
#' Strict read of `metadata/taxon_common.csv` — every column character, so an
#' empty cell stays empty rather than round-tripping as the string `"NA"`.
#'
#' @param path path to the registry; a missing file yields an empty frame, so a
#'   first run and a deleted cache behave alike.
#' @return a data frame with the registry columns.
#' @export
#' @concept taxa
read_taxon_common <- function(path) {
  if (is.null(path) || !file.exists(path)) return(.empty_common())
  d <- utils::read.csv(path, colClasses = "character", na.strings = c("NA", ""))
  for (cl in setdiff(.common_cache_cols, names(d))) d[[cl]] <- NA_character_
  d$n_candidates_en <- suppressWarnings(as.integer(d$n_candidates_en))
  d$n_candidates_en[is.na(d$n_candidates_en)] <- 0L
  d[.common_cache_cols]
}

#' Fetch and cache vernacular (common) names from WoRMS
#'
#' **This is where a multi-vernacular choice is made, and a human makes it.**
#' WoRMS returns English vernaculars as an unordered bag with no preferred-name
#' flag, so:
#'
#' * **exactly one** English name — taken automatically, since there is no choice;
#' * **two or more** — `common_name` is left empty, every candidate is written to
#'   `candidates_en`, and someone picks by editing the cell. Nothing is guessed,
#'   and an unresolved taxon simply publishes no common name;
#' * **none** — recorded with `n_candidates_en = 0` so it is not re-queried.
#'
#' A re-run never overwrites a non-empty `common_name`, so a hand-picked value is
#' permanent even under `refresh = TRUE`.
#'
#' @param taxa data frame with `taxon_key`, `worms_id` and `scientific_name`
#'   (extra columns ignored). Rows with no `worms_id` are skipped — there is
#'   nothing to ask WoRMS about.
#' @param cache_csv path to the registry. Required: this is pointless without a
#'   place to record the choice.
#' @param refresh re-query taxa already cached (hand-picked names still survive).
#' @param sleep seconds between WoRMS calls.
#' @param verbose report progress and how many await a choice.
#' @return the registry, invisibly.
#' @export
#' @concept taxa
#' @examples
#' \dontrun{
#' ensure_taxon_common(taxa, cache_csv = here("metadata/taxon_common.csv"))
#' }
ensure_taxon_common <- function(taxa, cache_csv = NULL, refresh = FALSE,
                                sleep = 0.3, verbose = TRUE) {
  if (is.null(cache_csv))
    stop("ensure_taxon_common(): `cache_csv` is required — it is where the ",
         "choice between multiple vernaculars is recorded.", call. = FALSE)
  if (!requireNamespace("worrms", quietly = TRUE))
    stop("ensure_taxon_common() needs the 'worrms' package", call. = FALSE)
  stopifnot(is.data.frame(taxa),
            all(c("taxon_key", "worms_id") %in% names(taxa)))
  if (!"scientific_name" %in% names(taxa)) taxa$scientific_name <- NA_character_

  cache <- read_taxon_common(cache_csv)

  need <- taxa[!is.na(taxa$worms_id), c("taxon_key", "worms_id", "scientific_name")]
  need <- need[!duplicated(need$taxon_key), , drop = FALSE]
  if (!refresh) need <- need[!need$taxon_key %in% cache$taxon_key, , drop = FALSE]

  if (verbose && nrow(need))
    message(glue::glue("ensure_taxon_common(): querying WoRMS for {nrow(need)} taxon/taxa"))

  today <- format(Sys.Date())
  for (i in seq_len(nrow(need))) {
    tk <- need$taxon_key[i]
    v  <- .worms_vernaculars_en(need$worms_id[i], sleep = sleep)
    prior <- cache$common_name[match(tk, cache$taxon_key)]
    # a hand-picked name is permanent, even under refresh = TRUE
    keep <- if (length(prior) && !is.na(prior) && nzchar(prior)) prior
            else if (length(v) == 1L) v
            else NA_character_
    cache <- rbind(
      cache[cache$taxon_key != tk, , drop = FALSE],
      data.frame(
        taxon_key       = tk,
        scientific_name = need$scientific_name[i],
        common_name     = keep,
        candidates_en   = paste(v, collapse = " | "),
        n_candidates_en = length(v),
        source          = if (length(v)) "worms" else NA_character_,
        checked_date    = today,
        notes           = if (length(v) > 1L && (is.na(keep) || !nzchar(keep)))
                            "multiple English vernaculars - pick one" else NA_character_,
        stringsAsFactors = FALSE))
    if (verbose && i %% 100 == 0)
      message(glue::glue("  {i}/{nrow(need)} …"))
  }

  cache <- cache[order(cache$taxon_key), , drop = FALSE]
  # na = "" : never let an empty cell round-trip as the string "NA"
  utils::write.csv(cache[.common_cache_cols], cache_csv, row.names = FALSE, na = "")

  if (verbose) {
    named   <- sum(!is.na(cache$common_name) & nzchar(cache$common_name))
    pending <- sum(cache$n_candidates_en > 1 &
                     (is.na(cache$common_name) | !nzchar(cache$common_name)))
    message(glue::glue(
      "ensure_taxon_common(): {nrow(cache)} cached; {named} named; ",
      "{pending} awaiting a choice (edit `common_name` in {basename(cache_csv)})"))
  }
  invisible(cache)
}

#' Apply the vernacular-name registry to a taxon table
#'
#' Fills `common_name` on `tbl` for taxa that have none of their own. **A
#' dataset's own vocabulary always wins** — it is the name the provider
#' publishes, and overwriting it from WoRMS would rename their data under them.
#' A taxon still awaiting a choice keeps NULL, which publishes no common name
#' rather than a guessed one.
#'
#' Called by `release_database.qmd` on the merged `taxon` table, so the registry
#' is applied once rather than in each of the 10 taxa-emitting ingests.
#'
#' @param con DBI connection holding `tbl`.
#' @param cache_csv path to the registry (see [ensure_taxon_common()]).
#' @param tbl taxon table name (default `"taxon"`).
#' @param verbose report how many names were filled.
#' @return number of rows filled, invisibly.
#' @export
#' @concept taxa
apply_taxon_common <- function(con, cache_csv, tbl = "taxon", verbose = TRUE) {
  cache <- read_taxon_common(cache_csv)
  cache <- cache[!is.na(cache$common_name) & nzchar(cache$common_name),
                 c("taxon_key", "common_name"), drop = FALSE]
  if (!nrow(cache)) {
    if (verbose) message("apply_taxon_common(): registry has no chosen names")
    return(invisible(0L))
  }
  DBI::dbWriteTable(con, "_taxon_common", cache, overwrite = TRUE)
  before <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) n FROM {tbl} WHERE common_name IS NULL OR common_name = ''"))$n
  # CTAS, not UPDATE: `taxon` may carry a CRS-tagged GEOMETRY elsewhere in the
  # release and DuckDB fails UPDATE on such a table (see workflows CLAUDE.md)
  DBI::dbExecute(con, glue::glue("
    CREATE OR REPLACE TABLE {tbl} AS
    SELECT t.* REPLACE (
      COALESCE(NULLIF(t.common_name, ''), c.common_name) AS common_name)
    FROM {tbl} t LEFT JOIN _taxon_common c USING (taxon_key)"))
  after <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) n FROM {tbl} WHERE common_name IS NULL OR common_name = ''"))$n
  DBI::dbExecute(con, "DROP TABLE IF EXISTS _taxon_common")
  filled <- before - after
  if (verbose) message(glue::glue(
    "apply_taxon_common(): filled {filled} common_name(s); {after} still unnamed"))
  invisible(filled)
}
