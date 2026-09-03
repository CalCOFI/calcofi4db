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
#
# ONE WRITTEN PRECEDENCE (taxon plan D5, decided 2026-09-02). Until 3.29.0 the
# per-dataset arms' priority picked which dataset's string won and this registry
# filled what was still empty; nothing stated the order. It is now one COALESCE:
#   1. a human choice in taxon_common.csv (`source = "manual"`)
#   2. the curated species list's own name (`swfsc_ichthyo`)
#   3. WoRMS, when it offers exactly one English vernacular (`source = "worms"`)
#   4. any other dataset's ds_common_name, in dataset_key order
#   5. empty. Never a guess.
# Two codes of one dataset resolving to one taxon (a species-level code carrying
# a genus AphiaID) are broken by the code whose ds_scientific_name IS the taxon's
# accepted name, then by ds_taxon_key — see apply_taxon_common().

.common_cache_cols <- c("taxon_key", "scientific_name", "common_name",
                        "candidates_en", "n_candidates_en", "source",
                        "checked_date", "notes")

# the `source` tag of a filled row: "worms" only when the value IS the single
# English vernacular WoRMS offered; anything else a human typed — a pick among
# several, a name where WoRMS had none, or an override of the single one.
# Unfilled rows keep "worms" when something was fetched and their prior tag
# otherwise.
.common_source_of <- function(common_name, candidates_en, n_candidates_en,
                              prior = NA_character_) {
  cn <- as.character(common_name); cand <- as.character(candidates_en)
  n  <- suppressWarnings(as.integer(n_candidates_en)); n[is.na(n)] <- 0L
  filled <- !is.na(cn) & nzchar(cn)
  single <- filled & n == 1L & !is.na(cand) & cn == cand
  out <- ifelse(filled, ifelse(single, "worms", "manual"),
                ifelse(n > 0L, "worms", NA_character_))
  prior <- rep(as.character(prior), length.out = length(out))
  keep <- !filled & is.na(out) & !is.na(prior)
  out[keep] <- prior[keep]
  out
}

#' Write the vernacular-name registry
#'
#' The one writer for `metadata/taxon_common.csv`: fixed column order, sorted by
#' `taxon_key`, `na = ""` so an empty cell never round-trips as the string
#' `"NA"`. Use it instead of a bare `write.csv()` after editing a `common_name`.
#'
#' @param cache the registry (as read by [read_taxon_common()])
#' @param path where to write it
#' @return `path`, invisibly
#' @export
#' @concept taxa
write_taxon_common <- function(cache, path) {
  for (cl in setdiff(.common_cache_cols, names(cache))) cache[[cl]] <- NA_character_
  cache <- cache[order(cache$taxon_key), .common_cache_cols, drop = FALSE]
  utils::write.csv(cache, path, row.names = FALSE, na = "")
  invisible(path)
}

#' Tag the hand-picked rows of the registry as `source = "manual"`
#'
#' The registry never carried a literal `"manual"`: every fetched row was stamped
#' `source = "worms"` whether the value was auto-filled or hand-picked. The two
#' are told apart by construction — [ensure_taxon_common()] only ever auto-fills
#' the one candidate WoRMS offered — so a filled row whose value is not that
#' single candidate was necessarily a human edit. This writes that
#' reconstruction into the registry once (it is idempotent), and
#' [ensure_taxon_common()] keeps the tag from then on. [apply_taxon_common()]
#' reads it as rank 1 of the precedence.
#'
#' @param cache_csv path to the registry
#' @param verbose report how many rows were tagged
#' @return the number of rows newly tagged `manual`, invisibly
#' @export
#' @concept taxa
mark_taxon_common_manual <- function(cache_csv, verbose = TRUE) {
  cache <- read_taxon_common(cache_csv)
  if (!nrow(cache)) return(invisible(0L))
  src <- .common_source_of(cache$common_name, cache$candidates_en,
                           cache$n_candidates_en, cache$source)
  n_new <- sum(src %in% "manual" & !(cache$source %in% "manual"))
  if (!identical(src, cache$source)) {
    cache$source <- src
    write_taxon_common(cache, cache_csv)
  }
  if (verbose) message(glue::glue(
    "mark_taxon_common_manual(): {sum(src %in% 'manual')} manual, ",
    "{sum(src %in% 'worms' & !is.na(cache$common_name) & nzchar(cache$common_name))} ",
    "WoRMS-single ({n_new} newly tagged)"))
  invisible(n_new)
}

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
        source          = .common_source_of(keep, paste(v, collapse = " | "), length(v)),
        checked_date    = today,
        notes           = if (length(v) > 1L && (is.na(keep) || !nzchar(keep)))
                            "multiple English vernaculars - pick one" else NA_character_,
        stringsAsFactors = FALSE))
    if (verbose && i %% 100 == 0)
      message(glue::glue("  {i}/{nrow(need)} …"))
  }

  write_taxon_common(cache, cache_csv)
  cache <- read_taxon_common(cache_csv)

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

#' Apply the common-name precedence to the merged `taxon` table
#'
#' Sets `common_name` on `tbl` for every taxon as one `COALESCE`, in this order
#' (taxon plan D5):
#'
#' 1. a **human choice** in the registry (`source = "manual"`) — the override;
#' 2. the **curated species list's** own name — `dataset_taxon.ds_common_name`
#'    where `dataset_key = curated` (`swfsc_ichthyo`, CalCOFI's own names);
#' 3. **WoRMS**, when it offers exactly one English vernacular
#'    (`source = "worms"`, `n_candidates_en = 1`);
#' 4. any **other dataset's** `ds_common_name`, in `dataset_key` order (this is
#'    where the seabird and marine-mammal names come from — WoRMS holds almost no
#'    bird vernaculars);
#' 5. empty. Never a guess.
#'
#' The merged table's existing `common_name` is **not** a rank: it is whichever
#' shard won the merge, which is the undocumented order this replaces.
#'
#' When two codes of one dataset resolve to the same taxon (ichthyo 683
#' *Sebastes* "Rockfishes" and 3023 *Sebastes crocotulus* "Sunset rockfish" both
#' carry the genus AphiaID), the code whose `ds_scientific_name` equals
#' `taxon.scientific_name` — the code that *is* the taxon rather than one finer
#' or coarser than it — wins; failing that, `ds_taxon_key` ascending.
#'
#' Called by `release_database.qmd` on the merged `taxon` table, so the registry
#' is applied once rather than in each of the 10 taxa-emitting ingests.
#'
#' @param con DBI connection holding `tbl` (and `dataset_taxon`, for ranks 2
#'   and 4; without it those ranks are simply empty).
#' @param cache_csv path to the registry (see [ensure_taxon_common()]).
#' @param tbl taxon table name (default `"taxon"`).
#' @param dataset_taxon crosswalk table name (default `"dataset_taxon"`).
#' @param curated the dataset whose vocabulary is rank 2 (default
#'   `"swfsc_ichthyo"`, the CalCOFI species list).
#' @param verbose report how many names each rank supplied.
#' @return a data.frame of per-rank counts (`rank`, `source`, `n`), invisibly.
#' @export
#' @concept taxa
apply_taxon_common <- function(con, cache_csv, tbl = "taxon",
                               dataset_taxon = "dataset_taxon",
                               curated = "swfsc_ichthyo", verbose = TRUE) {
  cache  <- read_taxon_common(cache_csv)
  filled <- !is.na(cache$common_name) & nzchar(cache$common_name)
  manual <- cache[filled & cache$source %in% "manual", c("taxon_key", "common_name"), drop = FALSE]
  single <- cache[filled & cache$source %in% "worms" & cache$n_candidates_en == 1L,
                  c("taxon_key", "common_name"), drop = FALSE]

  tx <- DBI::dbGetQuery(con, glue::glue("SELECT taxon_key, scientific_name FROM {tbl}"))
  dt <- if (dataset_taxon %in% DBI::dbListTables(con)) DBI::dbGetQuery(con, glue::glue("
    SELECT dataset_key, ds_taxon_key, taxon_key, ds_scientific_name, ds_common_name
    FROM {dataset_taxon}
    WHERE taxon_key IS NOT NULL AND ds_common_name IS NOT NULL AND ds_common_name <> ''"))
  else data.frame(dataset_key = character(), ds_taxon_key = character(),
                  taxon_key = character(), ds_scientific_name = character(),
                  ds_common_name = character(), stringsAsFactors = FALSE)
  # the intra-dataset tie-break: the code named as the taxon first, then ds_taxon_key
  self <- dt$ds_scientific_name == tx$scientific_name[match(dt$taxon_key, tx$taxon_key)]
  self[is.na(self)] <- FALSE
  dt <- dt[order(dt$taxon_key, dt$dataset_key, !self, dt$ds_taxon_key), , drop = FALSE]
  ich <- dt[dt$dataset_key %in% curated, , drop = FALSE]
  ich <- ich[!duplicated(ich$taxon_key), , drop = FALSE]
  oth <- dt[!dt$dataset_key %in% curated, , drop = FALSE]
  oth <- oth[!duplicated(oth$taxon_key), , drop = FALSE]

  r1 <- manual$common_name[match(tx$taxon_key, manual$taxon_key)]
  r2 <- ich$ds_common_name[match(tx$taxon_key, ich$taxon_key)]
  r3 <- single$common_name[match(tx$taxon_key, single$taxon_key)]
  r4 <- oth$ds_common_name[match(tx$taxon_key, oth$taxon_key)]
  src <- dplyr::case_when(!is.na(r1) ~ "manual", !is.na(r2) ~ curated,
                          !is.na(r3) ~ "worms_single", !is.na(r4) ~ "other",
                          TRUE ~ "empty")
  name <- dplyr::coalesce(r1, r2, r3, r4)

  counts <- data.frame(
    rank   = 1:5,
    source = c("manual", curated, "worms_single", "other", "empty"),
    stringsAsFactors = FALSE)
  counts$n <- vapply(counts$source, function(x) sum(src == x), integer(1))

  DBI::dbWriteTable(con, "_taxon_common", data.frame(
    taxon_key = tx$taxon_key, common_name = as.character(name),
    stringsAsFactors = FALSE), overwrite = TRUE)
  # CTAS, not UPDATE: `taxon` may carry a CRS-tagged GEOMETRY elsewhere in the
  # release and DuckDB fails UPDATE on such a table (see workflows CLAUDE.md)
  DBI::dbExecute(con, glue::glue("
    CREATE OR REPLACE TABLE {tbl} AS
    SELECT t.* REPLACE (c.common_name AS common_name)
    FROM {tbl} t LEFT JOIN _taxon_common c USING (taxon_key)"))
  DBI::dbExecute(con, "DROP TABLE IF EXISTS _taxon_common")

  if (verbose) message(glue::glue(
    "apply_taxon_common(): {nrow(tx)} taxa — ",
    "{paste(sprintf('%s %d', counts$source, counts$n), collapse = ', ')}"))
  invisible(counts)
}
