# taxon lineage ----------------------------------------------------------------
# Fill the hole that made hierarchy rollups silently return nothing.
#
# `build_taxon_reference()` takes rank / parent_taxon_key / classification from a
# WoRMS **lineage hierarchy** table named `taxon` in the connection — DwC-shaped
# (taxonID, parentNameUsageID, scientificName, taxonRank). Exactly one ingest
# built one: `swfsc_ichthyo`, via `build_taxon_hierarchy()` over its species list.
# Every other dataset's taxa therefore reached the release with a `taxon_key` and
# a `scientific_name` and NOTHING ELSE — no rank, no parent, no classification —
# so "all Decapoda" did not find the Metacarcinus magister records, and no error
# was raised anywhere along the way.
#
# `ensure_taxon_lineage()` closes it at the same seam: it fetches each taxon's
# classification from WoRMS (or ITIS, for the Aves-keyed seabirds), caches it in a
# reviewable CSV so a re-run costs no API calls, and materializes it as that same
# `taxon` hierarchy table. `build_taxon_reference()` then needs no change and no
# new argument — it already reads that table as its authority.
#
# The cache is one row per (requested taxon, ancestor-or-self), which is enough to
# derive both halves of what was missing: the ancestor ROWS (so parent_taxon_key
# chains resolve for descendant expansion) and the flattened kingdom / phylum /
# class / order / family columns.

.lineage_cache_cols <- c("requested_id", "authority", "taxonID",
                         "parentNameUsageID", "scientificName", "taxonRank")

.empty_lineage <- function()
  data.frame(requested_id = integer(), authority = character(),
             taxonID = integer(), parentNameUsageID = integer(),
             scientificName = character(), taxonRank = character(),
             stringsAsFactors = FALSE)

# WoRMS capitalizes ranks ("Species", "Subphylum"), ITIS does not ("species").
# `taxa_rank.rank_order` is keyed on the capitalized form, so an un-normalized
# ITIS rank joins to nothing and every seabird loses its rank ordering. Normalize
# on read, which also repairs a cache written before this existed.
.title_rank <- function(x) {
  x <- as.character(x)
  ok <- !is.na(x) & nzchar(x)
  x[ok] <- paste0(toupper(substr(x[ok], 1, 1)), substr(x[ok], 2, nchar(x[ok])))
  x
}

# one taxon's WoRMS classification chain, self last. NULL on any failure — a
# taxon WoRMS cannot resolve must not abort the other 300.
.fetch_worms_chain <- function(aid, sleep = 0.3) {
  out <- tryCatch({
    cl <- worrms::wm_classification(aid)
    if (is.null(cl) || !nrow(cl)) NULL else data.frame(
      requested_id      = as.integer(aid),
      authority         = "WoRMS",
      taxonID           = as.integer(cl$AphiaID),
      # the chain is ordered root -> self, so each row's parent is its predecessor
      parentNameUsageID = as.integer(c(NA, cl$AphiaID[-nrow(cl)])),
      scientificName    = as.character(cl$scientificname),
      taxonRank         = as.character(cl$rank),
      stringsAsFactors  = FALSE)
  }, error = function(e) NULL)
  Sys.sleep(sleep)
  out
}

# the ITIS equivalent, for taxa keyed itis: (seabirds — see taxon_key_of())
.fetch_itis_chain <- function(tsn, sleep = 0.3) {
  if (!requireNamespace("taxize", quietly = TRUE)) return(NULL)
  out <- tryCatch({
    cl <- taxize::classification(tsn, db = "itis", messages = FALSE)[[1]]
    if (is.null(cl) || !is.data.frame(cl) || !nrow(cl)) NULL else data.frame(
      requested_id      = as.integer(tsn),
      authority         = "ITIS",
      taxonID           = as.integer(cl$id),
      parentNameUsageID = as.integer(c(NA, cl$id[-nrow(cl)])),
      scientificName    = as.character(cl$name),
      taxonRank         = as.character(cl$rank),
      stringsAsFactors  = FALSE)
  }, error = function(e) NULL)
  Sys.sleep(sleep)
  out
}

#' Fetch (and cache) the WoRMS/ITIS lineage for a set of taxon ids
#'
#' One row per (requested taxon, ancestor-or-self), which is what both halves of
#' a usable hierarchy need: the ancestor rows so `parent_taxon_key` chains
#' resolve, and the ranks so `kingdom`/`phylum`/`class`/`order_taxon`/`family` can
#' be flattened onto each taxon.
#'
#' Ids already present in `cache_csv` are not re-fetched, so a re-run is free and
#' offline. The cache is a reviewable registry like the others under `metadata/`;
#' it is written with `na = ""` (see the round-trip trap in the workflows
#' `CLAUDE.md` — `readr`'s default turns an empty cell into the two-character
#' string `"NA"`, which DuckDB then reads as data).
#'
#' @param worms_ids integer vector of AphiaIDs to resolve (NA/duplicates dropped)
#' @param itis_ids integer vector of ITIS TSNs to resolve (the Aves-keyed taxa)
#' @param cache_csv path to the lineage cache CSV; read if it exists, rewritten
#'   when anything new is fetched. `NULL` fetches everything and caches nothing.
#' @param refresh logical; re-fetch ids already cached
#' @param sleep seconds between API calls (rate limit)
#' @param verbose logical; report what was cached vs fetched
#' @return a data.frame of lineage rows for the requested ids
#' @export
#' @concept taxonomy
fetch_taxon_lineage <- function(worms_ids = integer(), itis_ids = integer(),
                                cache_csv = NULL, refresh = FALSE,
                                sleep = 0.3, verbose = TRUE) {
  worms_ids <- unique(stats::na.omit(suppressWarnings(as.integer(worms_ids))))
  itis_ids  <- unique(stats::na.omit(suppressWarnings(as.integer(itis_ids))))

  cached <- .empty_lineage()
  if (!is.null(cache_csv) && file.exists(cache_csv)) {
    cached <- utils::read.csv(cache_csv, stringsAsFactors = FALSE,
                              na.strings = c("", "NA"))
    for (cl in setdiff(.lineage_cache_cols, names(cached))) cached[[cl]] <- NA
    cached <- cached[, .lineage_cache_cols, drop = FALSE]
    for (cl in c("requested_id", "taxonID", "parentNameUsageID"))
      cached[[cl]] <- suppressWarnings(as.integer(cached[[cl]]))
  }

  have <- function(auth) if (isTRUE(refresh)) integer() else
    unique(cached$requested_id[cached$authority == auth])
  need_w <- setdiff(worms_ids, have("WoRMS"))
  need_i <- setdiff(itis_ids,  have("ITIS"))

  if (verbose) message(glue::glue(
    "taxon lineage: {length(worms_ids)} WoRMS + {length(itis_ids)} ITIS requested; ",
    "{length(need_w) + length(need_i)} to fetch, ",
    "{length(worms_ids) + length(itis_ids) - length(need_w) - length(need_i)} cached"))

  fetched <- list()
  if (length(need_w)) {
    if (!requireNamespace("worrms", quietly = TRUE))
      stop("Package 'worrms' is required to fetch WoRMS lineage. ",
           "Install it, or pre-populate cache_csv.")
    for (i in seq_along(need_w)) {
      if (verbose && (i %% 25 == 0 || i == 1))
        message(glue::glue("  WoRMS {i}/{length(need_w)}"))
      fetched[[length(fetched) + 1L]] <- .fetch_worms_chain(need_w[i], sleep)
    }
  }
  if (length(need_i)) {
    if (!requireNamespace("taxize", quietly = TRUE)) {
      message("Package 'taxize' not installed; skipping ", length(need_i),
              " ITIS lineage(s).")
    } else {
      for (i in seq_along(need_i)) {
        if (verbose && (i %% 25 == 0 || i == 1))
          message(glue::glue("  ITIS {i}/{length(need_i)}"))
        fetched[[length(fetched) + 1L]] <- .fetch_itis_chain(need_i[i], sleep)
      }
    }
  }
  fetched <- Filter(function(x) !is.null(x) && nrow(x), fetched)

  out <- if (length(fetched))
    dplyr::bind_rows(cached, dplyr::bind_rows(fetched)) else cached
  out <- out[!duplicated(out[, c("requested_id", "authority", "taxonID")]), ,
             drop = FALSE]
  out$taxonRank <- .title_rank(out$taxonRank)
  out <- out[order(out$authority, out$requested_id, out$taxonID), , drop = FALSE]

  # the CACHE is global and grows across datasets; write all of it
  if (length(fetched) && !is.null(cache_csv)) {
    dir.create(dirname(cache_csv), showWarnings = FALSE, recursive = TRUE)
    # na = "" : never let an empty cell round-trip as the string "NA"
    utils::write.csv(out, cache_csv, row.names = FALSE, na = "")
    if (verbose) message(glue::glue(
      "taxon lineage cache: {nrow(out)} rows -> {cache_csv}"))
  }

  # ...but the RETURN is only what was asked for. Returning the whole cache
  # instead put every dataset's lineage into every shard: `calcofi_phyllosoma`
  # went from 1 taxon to 2,101. It looked fine on `swfsc_ichthyo` only because
  # that notebook calls prune_taxon_shard() afterwards, which trimmed it back.
  keep <- (out$authority == "WoRMS" & out$requested_id %in% worms_ids) |
          (out$authority == "ITIS"  & out$requested_id %in% itis_ids)
  out <- out[keep, , drop = FALSE]

  n_unres <- length(setdiff(c(need_w, need_i), out$requested_id))
  if (verbose && n_unres > 0) message(glue::glue(
    "taxon lineage: {n_unres} id(s) did not resolve and stay bare"))
  out
}

# flatten a lineage frame to one row per requested taxon with the five headline
# ranks. `rank` and the parent come from the taxon's OWN row in its chain.
.lineage_flat <- function(lin) {
  if (!nrow(lin)) return(data.frame(
    requested_id = integer(), authority = character(), rank = character(),
    parent_id = integer(), scientific_name = character(),
    kingdom = character(), phylum = character(), class = character(),
    order_taxon = character(), family = character(), stringsAsFactors = FALSE))
  pick <- function(g, want) {
    v <- g$scientificName[tolower(g$taxonRank) == want]
    if (length(v)) v[1] else NA_character_
  }
  spl <- split(lin, list(lin$authority, lin$requested_id), drop = TRUE)
  do.call(rbind, lapply(spl, function(g) {
    self <- g[g$taxonID == g$requested_id[1], , drop = FALSE]
    data.frame(
      requested_id    = g$requested_id[1],
      authority       = g$authority[1],
      rank            = if (nrow(self)) self$taxonRank[1] else NA_character_,
      parent_id       = if (nrow(self)) self$parentNameUsageID[1] else NA_integer_,
      scientific_name = if (nrow(self)) self$scientificName[1] else NA_character_,
      kingdom         = pick(g, "kingdom"),
      phylum          = pick(g, "phylum"),
      class           = pick(g, "class"),
      order_taxon     = pick(g, "order"),
      family          = pick(g, "family"),
      stringsAsFactors = FALSE)
  }))
}

#' Materialize the WoRMS/ITIS lineage `build_taxon_reference()` reads
#'
#' Resolves every authority id this dataset's vocabulary reaches — from its own
#' taxon tables *and* from `measurement_taxon.csv`, which is where the taxa that
#' had no lineage at all came from — fetches their classification (cached), and
#' writes it into `con` as the DwC-shaped `taxon` hierarchy table.
#'
#' Call it **before** [build_taxon_reference()], which reads that table as its
#' rank / parent / classification authority. An existing hierarchy is merged, not
#' replaced, so `swfsc_ichthyo` (which builds its own via
#' [build_taxon_hierarchy()]) keeps what it has and gains only what is missing.
#'
#' @param con a DuckDB connection holding this dataset's taxon vocabulary tables
#' @param measurement_taxon the composite crosswalk (`metadata/measurement_taxon.csv`),
#'   already filtered to this dataset
#' @param overrides the manual id registry (`metadata/taxon_override.csv`)
#' @param cache_csv path to the shared lineage cache
#'   (`metadata/taxon_lineage.csv`)
#' @param tbl hierarchy table to write (default `"taxon"` — the name
#'   [build_taxon_reference()] reads)
#' @inheritParams fetch_taxon_lineage
#' @return (invisibly) a list with `n_ids`, `n_rows` and `n_unresolved`
#' @export
#' @concept taxonomy
ensure_taxon_lineage <- function(con, measurement_taxon = NULL, overrides = NULL,
                                 cache_csv = NULL, tbl = "taxon",
                                 refresh = FALSE, sleep = 0.3, verbose = TRUE) {
  rows <- .taxon_norm_sources(con, measurement_taxon, overrides)
  # which authority each taxon is KEYED on decides which one to ask: worms: for
  # everything except the Aves-keyed seabirds (see taxon_key_of()).
  keyed_itis <- rows$is_bird & !is.na(rows$itis_id)
  w_ids <- rows$worms_id[!keyed_itis]
  i_ids <- rows$itis_id[keyed_itis]

  lin <- fetch_taxon_lineage(w_ids, i_ids, cache_csv = cache_csv,
                             refresh = refresh, sleep = sleep, verbose = verbose)

  # DwC hierarchy rows: every ancestor-or-self, deduped across requesters. This
  # is the shape build_taxon_reference() consumes.
  hier <- unique(lin[, c("authority", "taxonID", "parentNameUsageID",
                         "scientificName", "taxonRank")])
  hier$taxonomicStatus          <- "accepted"
  hier$scientificNameAuthorship <- NA_character_

  existing <- .read_cols(con, tbl, c(
    "authority", "taxonID", "parentNameUsageID", "scientificName", "taxonRank",
    "taxonomicStatus", "scientificNameAuthorship"))
  if (!is.null(existing) && nrow(existing)) {
    if (all(is.na(existing$authority))) existing$authority <- "WoRMS"
    # existing rows win: an ingest that built its own hierarchy keeps it
    hier <- dplyr::bind_rows(existing, hier)
  }
  hier <- hier[!is.na(hier$taxonID), , drop = FALSE]
  hier <- hier[!duplicated(hier[, c("authority", "taxonID")]), , drop = FALSE]
  .replace_table(con, tbl, as.data.frame(hier))

  # the flattened classification, for build_taxon_reference() to coalesce onto
  # each taxon. Staged as a table rather than returned, so the notebook does not
  # have to thread it through.
  .replace_table(con, "_taxon_lineage_flat", .lineage_flat(lin))

  n_ids   <- length(unique(stats::na.omit(c(w_ids, i_ids))))
  n_unres <- n_ids - length(unique(lin$requested_id))
  if (verbose) message(glue::glue(
    "{tbl}: {nrow(hier)} lineage rows for {n_ids} taxa ",
    "({max(n_unres, 0)} unresolved)"))
  invisible(list(n_ids = n_ids, n_rows = nrow(hier),
                 n_unresolved = max(n_unres, 0)))
}
