# taxon authority cross-reference ----------------------------------------------
# Fill the hole that made every seabird and marine mammal unreachable.
#
# `taxon_key_of()` keys class Aves on `itis:<TSN>` because WoRMS bird taxonomy
# lags (it still calls these Oceanodroma / Puffinus / Phalacrocorax). That rule
# is right, but nothing ever populated the `worms_id` COLUMN for those taxa, so a
# consumer joining on `worms_id` — which is what `db-viz-hex::get_sp()` does —
# matched zero rows for all 128 Farallon taxa and 64,956 observations. The key
# authority and the cross-reference columns are different questions, and the code
# only ever answered the first.
#
# This module answers the second, in both directions:
#   ITIS TSN -> WoRMS AphiaID   `wm_record_by_external(tsn, type = "tsn")`
#   WoRMS AphiaID -> ITIS TSN   `wm_external(aphia, type = "tsn")`
#   name -> WoRMS AphiaID       `wm_records_names(clean_taxon_name(name))`
# The first is an EXACT id crosswalk, not a name match: 91 of the 92 Farallon
# bird TSNs resolve through it, with no fuzzy-matching risk. The name route is
# the fallback for taxa carrying neither id (the source column header reads
# `Bathophilus sp.`, which WoRMS has never heard of but whose genus it holds).
#
# Two invariants, both learned the hard way:
#
#   1. A KEY must be an ACCEPTED id. `itis:174553` (Puffinus griseus) is
#      deprecated in ITIS — which is also why 28 Farallon taxa reached the
#      release with no rank, no parent and no classification at all. A `tsn`
#      query therefore resolves through `itis_acceptname()` and the taxon is
#      re-keyed to `itis:1255050` (Ardenna grisea), with the event recorded.
#   2. A CROSS-REFERENCE is whatever the authority links, stored verbatim. The
#      TSN that `wm_external()` returns for an AphiaID is WoRMS's assertion; it
#      is not a key here, so it is not second-guessed through ITIS.
#
# `taxonomic_status` used to be the hardcoded string "accepted", stamped on all
# 2,090 taxa including the ones demonstrably not accepted. It is fetched here,
# alongside `status_checked` — a status with no date is not a fact.
#
# The cache is a reviewable registry like `taxon_lineage.csv`: written with
# `na = ""` (see the round-trip trap in the workflows CLAUDE.md), keyed on
# (query_type, query_value), so a re-run costs no API calls and is fully offline.

.xref_cache_cols <- c("query_type", "query_value", "worms_id", "itis_id",
                      "matched_name", "accepted_name", "rank", "status",
                      "checked_date", "notes")

.empty_xref <- function()
  data.frame(query_type = character(), query_value = character(),
             worms_id = integer(), itis_id = integer(),
             matched_name = character(), accepted_name = character(),
             rank = character(), status = character(),
             checked_date = character(), notes = character(),
             stringsAsFactors = FALSE)

# first non-empty scalar from a worrms record field (wm_record_by_external
# returns a LIST, wm_records_names a data.frame; both may carry NULL/"")
.x1 <- function(x) {
  if (is.null(x) || !length(x)) return(NA)
  x <- x[[1]]
  if (is.null(x) || (is.character(x) && !nzchar(x))) NA else x
}
.x1c <- function(x) { v <- .x1(x); if (is.na(v)) NA_character_ else as.character(v) }
.x1i <- function(x) { v <- .x1(x); suppressWarnings(as.integer(v)) }

# the status of the taxon we actually STORE, which is the accepted one.
#
# A WoRMS record matched by a synonym reports the SYNONYM's status ("unaccepted",
# "superseded combination"), but we follow valid_AphiaID and store the accepted
# id and name — so reporting "unaccepted" would describe a name the release does
# not carry. When the match was a synonym the stored taxon is accepted; the
# synonym itself is recorded in `notes`. Terminal statuses where WoRMS offers no
# other name (nomen dubium, taxon inquirendum, temporary name) have
# valid == self and are kept as-is, because they DO describe the stored taxon.
.status_of <- function(rec) {
  st <- .x1c(rec$status)
  v  <- .x1i(rec$valid_AphiaID)
  a  <- .x1i(rec$AphiaID)
  if (!is.na(v) && !is.na(a) && v != a) "accepted" else st
}

# append a datestamped line to a notes cell, but only if it is not already
# there. The whole point of `notes` is that it accumulates across runs rather
# than being rewritten, so a warm-cache re-run must add nothing.
.append_note <- function(existing, line) {
  if (is.na(line) || !nzchar(line)) return(existing)
  if (is.na(existing) || !nzchar(existing)) return(line)
  have <- strsplit(existing, "\n", fixed = TRUE)[[1]]
  if (line %in% have) existing else paste(existing, line, sep = "\n")
}

# clean_taxon_name -------------------------------------------------------------

#' Normalize a source taxon name for an authority lookup
#'
#' Strips the open-nomenclature and qualifier noise that source spreadsheets
#' carry in their column headers and species lists, so the name reaches WoRMS in
#' a form it can match. `"Bathophilus sp."` becomes `"Bathophilus"` (WoRMS holds
#' the genus, never the `sp.` form), `"Phaeocystis cf pouchetti"` becomes
#' `"Phaeocystis pouchetti"`, `"indistinguished Pterosperma spp."` becomes
#' `"Pterosperma"`.
#'
#' **Use the result as the lookup query only — never as `ds_taxa_code`.** For
#' `sio_mesopelagic-fish` the local code *is* the verbatim column header and is
#' the join key from `obs`; rewriting it would silently orphan every observation.
#'
#' This generalizes the hand-maintained `name_query` column that
#' `metadata/calcofi/phytoplankton/taxon_worms.csv` already carries for one
#' dataset.
#'
#' @param x character vector of source names
#' @return character vector of cleaned names (NA in, NA out)
#' @export
#' @concept taxonomy
#' @examples
#' clean_taxon_name(c("Bathophilus sp.", "Phaeocystis cf pouchetti",
#'                    "indistinguished Pterosperma spp.", "Uria aalge"))
clean_taxon_name <- function(x) {
  x <- as.character(x)
  ok <- !is.na(x)
  v <- x[ok]

  # parenthetical authorship: "Uria aalge (Pontoppidan, 1763)"
  v <- gsub("\\s*\\([^)]*\\)", " ", v)
  # leading qualifiers, possibly stacked ("unidentified larval ...")
  qual <- paste0("^\\s*(indistinguished|unidentified|unident\\.?|undetermined|",
                 "undet\\.?|unknown|larval|juvenile|adult|misc\\.?|other)\\s+")
  for (i in 1:3) v <- sub(qual, "", v, ignore.case = TRUE)
  # open-nomenclature tokens anywhere in the string. "cf" sits BETWEEN genus and
  # epithet ("Phaeocystis cf pouchetti") so dropping it repairs the binomial;
  # "sp."/"spp." sit at the end so dropping them leaves the genus.
  onom <- "(?i)(^|\\s)(sp|spp|ssp|cf|aff|nr|indet|incertae)\\.?(?=\\s|$)"
  v <- gsub(onom, " ", v, perl = TRUE)
  # trailing single-letter variant tag ("Pterosperma sp. a" -> "Pterosperma a")
  v <- sub("\\s+[A-Za-z]\\s*$", "", v)
  # collapse
  v <- gsub("\\s+", " ", v)
  v <- trimws(v)
  v[!nzchar(v)] <- NA_character_

  x[ok] <- v
  x
}

# per-query fetchers -----------------------------------------------------------
# Each returns one cache row, or NULL. NULL on failure is deliberate and matches
# .fetch_worms_chain(): one taxon the authority cannot resolve must not abort the
# other 300.

# ITIS TSN -> accepted TSN + WoRMS AphiaID. The TSN is a KEY here, so it is
# resolved to the ITIS-accepted TSN (invariant 1 above).
.fetch_xref_tsn <- function(tsn, today, sleep = 0.3) {
  tsn <- suppressWarnings(as.integer(tsn))
  if (is.na(tsn)) return(NULL)
  note <- character()

  # a. ITIS: is this TSN still accepted?
  acc_tsn <- tsn; acc_nm <- NA_character_; itis_ok <- FALSE
  if (requireNamespace("taxize", quietly = TRUE)) {
    a <- tryCatch(taxize::itis_acceptname(tsn), error = function(e) NULL)
    if (!is.null(a) && is.data.frame(a) && nrow(a)) {
      itis_ok <- TRUE
      at <- suppressWarnings(as.integer(a$acceptedtsn[1]))
      if (!is.na(at)) acc_tsn <- at
      acc_nm <- as.character(a$acceptedname[1])
    }
  }

  # b. WoRMS, by that exact TSN — an id crosswalk, not a name match
  rec <- tryCatch(worrms::wm_record_by_external(tsn, type = "tsn"),
                  error = function(e) NULL)
  Sys.sleep(sleep)
  if (is.null(rec) && acc_tsn != tsn) {
    rec <- tryCatch(worrms::wm_record_by_external(acc_tsn, type = "tsn"),
                    error = function(e) NULL)
    Sys.sleep(sleep)
  }

  w_id <- NA_integer_; m_nm <- NA_character_; v_nm <- NA_character_
  rk <- NA_character_; st <- NA_character_
  if (!is.null(rec)) {
    w_id <- .x1i(rec$valid_AphiaID); if (is.na(w_id)) w_id <- .x1i(rec$AphiaID)
    m_nm <- .x1c(rec$scientificname); v_nm <- .x1c(rec$valid_name)
    rk   <- .x1c(rec$rank);           st   <- .status_of(rec)
    note <- c(note, sprintf("worms_id %s via WoRMS TSN crosswalk (status %s)",
                            w_id, if (is.na(st)) "unknown" else st))
    if (!is.na(v_nm) && !is.na(m_nm) && !identical(v_nm, m_nm))
      note <- c(note, sprintf("WoRMS %s unaccepted -> %s", m_nm, v_nm))
  }
  if (is.null(rec) && !itis_ok) return(NULL)

  if (!identical(acc_tsn, tsn))
    note <- c(note, sprintf("itis:%s deprecated in ITIS -> itis:%s%s",
                            tsn, acc_tsn,
                            if (is.na(acc_nm)) "" else sprintf(" (%s)", acc_nm)))

  data.frame(
    query_type = "tsn", query_value = as.character(tsn),
    worms_id = w_id, itis_id = acc_tsn,
    matched_name = m_nm,
    accepted_name = if (!is.na(acc_nm)) acc_nm else v_nm,
    rank = rk, status = st, checked_date = today,
    notes = if (length(note)) paste0(today, ": ", paste(note, collapse = "; ")) else NA_character_,
    stringsAsFactors = FALSE)
}

# WoRMS AphiaID -> linked ITIS TSN + the real status. The TSN is a
# CROSS-REFERENCE here, stored as WoRMS asserts it (invariant 2 above).
.fetch_xref_aphia <- function(aphia, today, sleep = 0.3) {
  aphia <- suppressWarnings(as.integer(aphia))
  if (is.na(aphia)) return(NULL)

  rec <- tryCatch(worrms::wm_record(aphia), error = function(e) NULL)
  Sys.sleep(sleep)
  tsn <- tryCatch(worrms::wm_external(aphia, type = "tsn"), error = function(e) NULL)
  Sys.sleep(sleep)
  if (is.null(rec) && is.null(tsn)) return(NULL)

  w_id <- if (is.null(rec)) aphia else {
    v <- .x1i(rec$valid_AphiaID); if (is.na(v)) aphia else v }
  m_nm <- if (is.null(rec)) NA_character_ else .x1c(rec$scientificname)
  v_nm <- if (is.null(rec)) NA_character_ else .x1c(rec$valid_name)
  st   <- if (is.null(rec)) NA_character_ else .status_of(rec)
  rk   <- if (is.null(rec)) NA_character_ else .x1c(rec$rank)
  t_id <- .x1i(tsn)

  note <- character()
  if (!is.na(t_id)) note <- c(note, sprintf("itis_id %s via WoRMS external link", t_id))
  if (!is.na(v_nm) && !is.na(m_nm) && !identical(v_nm, m_nm))
    note <- c(note, sprintf("WoRMS %s unaccepted -> %s (worms:%s)", m_nm, v_nm, w_id))

  data.frame(
    query_type = "aphia", query_value = as.character(aphia),
    worms_id = w_id, itis_id = t_id, matched_name = m_nm, accepted_name = v_nm,
    rank = rk, status = st, checked_date = today,
    notes = if (length(note)) paste0(today, ": ", paste(note, collapse = "; ")) else NA_character_,
    stringsAsFactors = FALSE)
}

# the batched form of .fetch_xref_aphia(). Both `wm_record()` and `wm_external_()`
# accept a vector, which turns ~2,000 sequential request pairs (35+ minutes) into
# ~40 calls. Falls back to the per-id path for the whole chunk if the batch
# errors — one bad AphiaID must not cost the other 49.
.fetch_xref_aphia_batch <- function(aphias, today, sleep = 0.3, chunk = 50L) {
  aphias <- unique(stats::na.omit(suppressWarnings(as.integer(aphias))))
  if (!length(aphias)) return(list())
  out <- list()
  for (grp in split(aphias, ceiling(seq_along(aphias) / chunk))) {
    rec <- tryCatch(worrms::wm_record(grp), error = function(e) NULL)
    Sys.sleep(sleep)
    ext <- tryCatch(worrms::wm_external_(grp, type = "tsn"), error = function(e) NULL)
    Sys.sleep(sleep)
    if (is.null(rec) || !NROW(rec)) {           # batch failed -> one at a time
      for (a in grp) out[[length(out) + 1L]] <- .fetch_xref_aphia(a, today, sleep)
      next
    }
    rec <- as.data.frame(rec, stringsAsFactors = FALSE)
    for (a in grp) {
      i <- which(as.integer(rec$AphiaID) == a)
      t_id <- suppressWarnings(as.integer(.x1(ext[[as.character(a)]])))
      if (!length(i)) {
        # WoRMS knows the external link but not the record (or vice versa)
        if (is.na(t_id)) next
        out[[length(out) + 1L]] <- data.frame(
          query_type = "aphia", query_value = as.character(a), worms_id = a,
          itis_id = t_id, matched_name = NA_character_, accepted_name = NA_character_,
          rank = NA_character_, status = NA_character_, checked_date = today,
          notes = paste0(today, ": itis_id ", t_id, " via WoRMS external link"),
          stringsAsFactors = FALSE)
        next
      }
      r    <- rec[i[1], , drop = FALSE]
      w_id <- suppressWarnings(as.integer(r$valid_AphiaID)); if (is.na(w_id)) w_id <- a
      m_nm <- .x1c(r$scientificname); v_nm <- .x1c(r$valid_name)
      note <- character()
      if (!is.na(t_id)) note <- c(note, sprintf("itis_id %s via WoRMS external link", t_id))
      if (!is.na(v_nm) && !is.na(m_nm) && !identical(v_nm, m_nm))
        note <- c(note, sprintf("WoRMS %s unaccepted -> %s (worms:%s)", m_nm, v_nm, w_id))
      out[[length(out) + 1L]] <- data.frame(
        query_type = "aphia", query_value = as.character(a), worms_id = w_id,
        itis_id = t_id, matched_name = m_nm, accepted_name = v_nm,
        rank = .x1c(r$rank), status = .status_of(r), checked_date = today,
        notes = if (length(note)) paste0(today, ": ", paste(note, collapse = "; ")) else NA_character_,
        stringsAsFactors = FALSE)
    }
  }
  out
}

# cleaned name -> WoRMS AphiaID. The fallback for taxa carrying neither id.
.fetch_xref_name <- function(name, today, sleep = 0.3) {
  if (is.na(name) || !nzchar(name)) return(NULL)
  res <- tryCatch(worrms::wm_records_name(name, fuzzy = FALSE, marine_only = FALSE),
                  error = function(e) NULL)
  Sys.sleep(sleep)
  if (is.null(res) || !NROW(res)) return(NULL)
  res <- res[1, , drop = FALSE]

  w_id <- .x1i(res$valid_AphiaID); if (is.na(w_id)) w_id <- .x1i(res$AphiaID)
  m_nm <- .x1c(res$scientificname); v_nm <- .x1c(res$valid_name)
  note <- sprintf("worms_id %s matched by name \"%s\"", w_id, name)
  if (!is.na(v_nm) && !is.na(m_nm) && !identical(v_nm, m_nm))
    note <- paste0(note, sprintf("; WoRMS %s unaccepted -> %s", m_nm, v_nm))

  data.frame(
    query_type = "name", query_value = as.character(name),
    worms_id = w_id, itis_id = NA_integer_, matched_name = m_nm,
    accepted_name = v_nm, rank = .x1c(res$rank), status = .status_of(res),
    checked_date = today, notes = paste0(today, ": ", note),
    stringsAsFactors = FALSE)
}

# fetch_taxon_xref -------------------------------------------------------------

#' Fetch (and cache) the WoRMS <-> ITIS cross-reference for a set of taxa
#'
#' Resolves each requested identifier through the authority that can answer it,
#' and returns one row per query with the **accepted** ids, the authority's real
#' `status`, and the date it was checked:
#'
#' - `itis_ids` — exact TSN -> AphiaID crosswalk via
#'   `worrms::wm_record_by_external(type = "tsn")`, plus the ITIS-accepted TSN
#'   via `taxize::itis_acceptname()`. This is where a bird gains its `worms_id`
#'   without losing its `itis:` key.
#' - `worms_ids` — the reverse direction, `worrms::wm_external(type = "tsn")`,
#'   backfilling `itis_id` on WoRMS-keyed taxa.
#' - `names` — `worrms::wm_records_name()` on [clean_taxon_name()] output, the
#'   fallback for taxa carrying neither id.
#'
#' Queries already present in `cache_csv` are not re-fetched, so a re-run is free
#' and offline. `notes` accumulates datestamped lines and is never rewritten.
#'
#' @param itis_ids integer ITIS TSNs to crosswalk (NA/duplicates dropped)
#' @param worms_ids integer WoRMS AphiaIDs to backfill an `itis_id` for
#' @param names character source names; cleaned with [clean_taxon_name()] first
#' @param cache_csv path to the cross-reference cache CSV (`metadata/taxon_xref.csv`);
#'   read if it exists, rewritten when anything new is fetched. `NULL` fetches
#'   everything and caches nothing.
#' @param refresh logical; re-fetch queries already cached (and re-date them)
#' @param sleep seconds between API calls (rate limit)
#' @param verbose logical; report what was cached vs fetched
#' @return a data.frame of cross-reference rows for the requested queries
#' @export
#' @concept taxonomy
fetch_taxon_xref <- function(itis_ids = integer(), worms_ids = integer(),
                             names = character(), cache_csv = NULL,
                             refresh = FALSE, sleep = 0.3, verbose = TRUE) {
  itis_ids  <- unique(stats::na.omit(suppressWarnings(as.integer(itis_ids))))
  worms_ids <- unique(stats::na.omit(suppressWarnings(as.integer(worms_ids))))
  names     <- unique(stats::na.omit(clean_taxon_name(names)))
  names     <- names[nzchar(names)]

  cached <- .empty_xref()
  if (!is.null(cache_csv) && file.exists(cache_csv)) {
    cached <- utils::read.csv(cache_csv, stringsAsFactors = FALSE,
                              na.strings = c("", "NA"))
    for (cl in setdiff(.xref_cache_cols, base::names(cached))) cached[[cl]] <- NA
    cached <- cached[, .xref_cache_cols, drop = FALSE]
    for (cl in c("worms_id", "itis_id"))
      cached[[cl]] <- suppressWarnings(as.integer(cached[[cl]]))
    cached$query_value <- as.character(cached$query_value)
  }

  have <- function(qt) if (isTRUE(refresh)) character() else
    cached$query_value[cached$query_type == qt]
  need_i <- setdiff(as.character(itis_ids),  have("tsn"))
  need_w <- setdiff(as.character(worms_ids), have("aphia"))
  need_n <- setdiff(names,                   have("name"))
  n_need <- length(need_i) + length(need_w) + length(need_n)

  if (verbose) message(glue::glue(
    "taxon xref: {length(itis_ids)} TSN + {length(worms_ids)} AphiaID + ",
    "{length(names)} name requested; {n_need} to fetch, ",
    "{length(itis_ids) + length(worms_ids) + length(names) - n_need} cached"))

  today <- format(Sys.Date())
  fetched <- list()
  if (n_need) {
    if (!requireNamespace("worrms", quietly = TRUE))
      stop("Package 'worrms' is required to fetch the taxon cross-reference. ",
           "Install it, or pre-populate cache_csv.")
    if (length(need_i) && !requireNamespace("taxize", quietly = TRUE))
      message("Package 'taxize' not installed; TSNs will not be checked for ",
              "deprecation (WoRMS crosswalk still runs).")
    run <- function(vals, fn, label) {
      for (i in seq_along(vals)) {
        if (verbose && (i %% 25 == 0 || i == 1))
          message(glue::glue("  {label} {i}/{length(vals)}"))
        fetched[[length(fetched) + 1L]] <<- fn(vals[i], today, sleep)
      }
    }
    run(need_i, .fetch_xref_tsn,   "TSN")
    if (length(need_w)) {
      if (verbose) message(glue::glue("  AphiaID 1/{length(need_w)} (batched)"))
      fetched <- c(fetched, .fetch_xref_aphia_batch(need_w, today, sleep))
    }
    run(need_n, .fetch_xref_name,  "name")
  }
  fetched <- Filter(function(x) !is.null(x) && nrow(x), fetched)

  out <- cached
  if (length(fetched)) {
    new <- dplyr::bind_rows(fetched)
    # merge onto the cache: a re-fetched query keeps its accumulated notes and
    # gains the new line; everything else is overwritten by the fresh answer
    key_old <- paste(out$query_type, out$query_value)
    key_new <- paste(new$query_type, new$query_value)
    m <- match(key_new, key_old)
    for (j in which(!is.na(m)))
      new$notes[j] <- .append_note(out$notes[m[j]], new$notes[j])
    out <- rbind(out[is.na(match(key_old, key_new)), , drop = FALSE], new)
  }
  out <- out[order(out$query_type, out$query_value), , drop = FALSE]
  rownames(out) <- NULL

  # the CACHE is global and grows across datasets; write all of it
  if (length(fetched) && !is.null(cache_csv)) {
    dir.create(dirname(cache_csv), showWarnings = FALSE, recursive = TRUE)
    # na = "" : never let an empty cell round-trip as the string "NA"
    utils::write.csv(out, cache_csv, row.names = FALSE, na = "")
    if (verbose) message(glue::glue(
      "taxon xref cache: {nrow(out)} rows -> {cache_csv}"))
  }

  # ...but the RETURN is only what was asked for — same reason fetch_taxon_lineage()
  # scopes its return: an unscoped one puts every dataset's taxa in every shard.
  keep <- (out$query_type == "tsn"   & out$query_value %in% as.character(itis_ids)) |
          (out$query_type == "aphia" & out$query_value %in% as.character(worms_ids)) |
          (out$query_type == "name"  & out$query_value %in% names)
  out <- out[keep, , drop = FALSE]

  n_unres <- n_need - length(fetched)
  if (verbose && n_unres > 0) message(glue::glue(
    "taxon xref: {n_unres} quer(ies) did not resolve"))
  out
}

# ensure_taxon_xref ------------------------------------------------------------

#' Materialize the authority cross-reference `.taxon_norm_sources()` reads
#'
#' Works out which identifiers this dataset's vocabulary reaches, resolves them
#' (cached), and stages the result in `con` as `_taxon_xref`. Every taxon builder
#' then picks it up automatically: `worms_id` is filled on ITIS-keyed taxa,
#' `itis_id` on WoRMS-keyed ones, deprecated ids are replaced by their accepted
#' form, and `taxonomic_status` / `status_checked` / `notes` are carried through.
#'
#' Call it **before** [ensure_taxon_lineage()] and the three builders — the
#' lineage fetch should ask about the accepted id, not the deprecated one.
#'
#' @param con a DuckDB connection holding this dataset's taxon vocabulary tables
#' @param measurement_taxon the composite crosswalk
#'   (`metadata/measurement_taxon.csv`), already filtered to this dataset
#' @param overrides the manual id registry (`metadata/taxon_override.csv`)
#' @param cache_csv path to the shared cross-reference cache
#'   (`metadata/taxon_xref.csv`)
#' @param tbl staging table to write (default `"_taxon_xref"`)
#' @inheritParams fetch_taxon_xref
#' @return (invisibly) a list with `n_queries`, `n_resolved`, `n_rekeyed`
#' @export
#' @concept taxonomy
ensure_taxon_xref <- function(con, measurement_taxon = NULL, overrides = NULL,
                              cache_csv = NULL, tbl = "_taxon_xref",
                              refresh = FALSE, sleep = 0.3, verbose = TRUE) {
  # read the vocabulary BEFORE staging, so this call is not self-referential
  rows <- .taxon_norm_sources(con, measurement_taxon, overrides, xref = NULL)

  # whichever id the SOURCE supplied decides what to ask for (taxon plan D2 —
  # no source flag, no key yet):
  #  - a TSN and no AphiaID -> crosswalk the TSN, gain worms_id (the seabirds)
  #  - an AphiaID           -> backfill the itis_id cross-reference
  #  - neither              -> last resort, match the name
  has_w <- !is.na(rows$worms_id)
  has_i <- !is.na(rows$itis_id)
  i_ids <- rows$itis_id[has_i & !has_w]
  w_ids <- rows$worms_id[has_w]
  nms <- rows$scientific_name[!has_w & !has_i]
  nms <- nms[!is.na(nms)]

  xref <- fetch_taxon_xref(itis_ids = i_ids, worms_ids = w_ids, names = nms,
                           cache_csv = cache_csv, refresh = refresh,
                           sleep = sleep, verbose = verbose)

  # second pass: a TSN the crosswalk could not place still has a NAME. WoRMS
  # links TSNs unevenly at genus level — it holds `Ardenna` but does not link
  # ITIS 1255018 to it — and without this those taxa keep a NULL worms_id, which
  # is the exact hole this module exists to close.
  unplaced <- xref$query_type == "tsn" & is.na(xref$worms_id)
  if (any(unplaced)) {
    by_tsn <- match(as.character(rows$itis_id), xref$query_value[unplaced])
    more   <- unique(stats::na.omit(rows$scientific_name[!is.na(by_tsn)]))
    if (length(more)) {
      if (verbose) message(glue::glue(
        "taxon xref: {sum(unplaced)} TSN(s) unplaced in WoRMS; retrying by name"))
      nx <- fetch_taxon_xref(names = more, cache_csv = cache_csv,
                             refresh = refresh, sleep = sleep, verbose = verbose)
      xref <- dplyr::bind_rows(xref, nx)
    }
  }
  .replace_table(con, tbl, as.data.frame(xref))

  n_rekey <- sum(xref$query_type == "tsn" &
                 !is.na(xref$itis_id) &
                 xref$query_value != as.character(xref$itis_id))
  if (verbose) message(glue::glue(
    "{tbl}: {nrow(xref)} cross-reference rows ",
    "({sum(!is.na(xref$worms_id))} with worms_id, ",
    "{sum(!is.na(xref$itis_id))} with itis_id, {n_rekey} re-keyed)"))
  invisible(list(n_queries = nrow(xref),
                 n_resolved = sum(!is.na(xref$worms_id) | !is.na(xref$itis_id)),
                 n_rekeyed = n_rekey))
}

# apply the staged cross-reference to a normalized taxon frame, in place, BEFORE
# taxon_key_of() mints the key. Deprecated ids are replaced by their accepted
# form (so the key is an accepted id), the opposite authority's id is filled in
# as a cross-reference, and the real status + check date + provenance note ride
# along. Returns `rows` unchanged when nothing is staged.
.apply_xref <- function(rows, xref, rekey = TRUE) {
  if (is.null(xref) || !nrow(xref)) return(rows)
  # coerce explicitly: an all-empty column read back from CSV comes in as logical,
  # and ifelse() would then write logical NAs into a character column
  for (cl in c("worms_id", "itis_id"))
    xref[[cl]] <- suppressWarnings(as.integer(xref[[cl]]))
  for (cl in c("query_value", "status", "checked_date", "notes"))
    xref[[cl]] <- as.character(xref[[cl]])

  pick <- function(qt, vals) {
    x <- xref[xref$query_type == qt, , drop = FALSE]
    x[match(as.character(vals), x$query_value), , drop = FALSE]
  }
  take <- function(dst, src) ifelse(is.na(dst), src, dst)
  # `rekey = FALSE` fills gaps but never REPLACES an id the row already carries.
  # Lineage ancestors need this: their key comes from the classification chain
  # they were fetched in, and swapping the id under them would break the parent
  # links that chain just established.
  set <- if (rekey) function(dst, src) ifelse(is.na(src), dst, src) else take

  # the branches are chosen from the ids the row ARRIVES with, before (a) fills
  # anything: a bird that gains its worms_id in (a) must not then be handed to
  # (b), where the AphiaID's linked TSN could re-key it off its own accepted TSN
  has_w0 <- !is.na(rows$worms_id)
  has_i0 <- !is.na(rows$itis_id)

  # a. taxa carrying a TSN and no AphiaID (the seabirds, and every ITIS lineage
  # row): gain worms_id, re-key onto the ITIS-accepted TSN
  ki <- which(has_i0 & !has_w0)
  if (length(ki)) {
    x <- pick("tsn", rows$itis_id[ki])
    hit <- !is.na(x$query_value)
    if (any(hit)) {
      k <- ki[hit]; xh <- x[hit, , drop = FALSE]
      rows$worms_id[k]         <- take(rows$worms_id[k], xh$worms_id)
      rows$itis_id[k]          <- set(rows$itis_id[k], xh$itis_id)
      rows$taxonomic_status[k] <- take(rows$taxonomic_status[k], xh$status)
      rows$status_checked[k]   <- take(rows$status_checked[k], xh$checked_date)
      rows$notes[k] <- as.character(mapply(
        .append_note, rows$notes[k], xh$notes, USE.NAMES = FALSE))
    }
  }

  # b. taxa carrying an AphiaID: backfill the itis_id cross-reference
  kw <- which(has_w0)
  if (length(kw)) {
    x <- pick("aphia", rows$worms_id[kw])
    hit <- !is.na(x$query_value)
    if (any(hit)) {
      k <- kw[hit]; xh <- x[hit, , drop = FALSE]
      rows$itis_id[k]          <- take(rows$itis_id[k], xh$itis_id)
      rows$worms_id[k]         <- set(rows$worms_id[k], xh$worms_id)
      rows$taxonomic_status[k] <- take(rows$taxonomic_status[k], xh$status)
      rows$status_checked[k]   <- take(rows$status_checked[k], xh$checked_date)
      rows$notes[k] <- as.character(mapply(
        .append_note, rows$notes[k], xh$notes, USE.NAMES = FALSE))
    }
  }

  # c. anything STILL without a worms_id after (a) and (b): last-resort name
  # match. Not just the taxa with neither id — a TSN WoRMS does not link (genus
  # `Ardenna`, ITIS 1255018) leaves an itis_id and no worms_id, which is the very
  # hole this closes. Only `worms_id` is filled here; the key authority is untouched.
  kn <- which(is.na(rows$worms_id) & !is.na(rows$scientific_name))
  if (length(kn)) {
    x <- pick("name", clean_taxon_name(rows$scientific_name[kn]))
    hit <- !is.na(x$query_value)
    if (any(hit)) {
      k <- kn[hit]; xh <- x[hit, , drop = FALSE]
      rows$worms_id[k]         <- take(rows$worms_id[k], xh$worms_id)
      rows$taxonomic_status[k] <- take(rows$taxonomic_status[k], xh$status)
      rows$status_checked[k]   <- take(rows$status_checked[k], xh$checked_date)
      rows$notes[k] <- as.character(mapply(
        .append_note, rows$notes[k], xh$notes, USE.NAMES = FALSE))
    }
  }
  rows
}
