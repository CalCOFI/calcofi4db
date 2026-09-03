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

# the xref cache lives beside the lineage cache in every ingest
# (metadata/taxon_lineage.csv + metadata/taxon_xref.csv), so derive one from the
# other rather than making all 10 notebooks pass a second path
.xref_csv_beside <- function(cache_csv) {
  if (is.null(cache_csv) || !nzchar(cache_csv)) return(NULL)
  file.path(dirname(cache_csv), "taxon_xref.csv")
}

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
#
# ITIS returns NO classification for a TSN it has deprecated, and the empty
# result is indistinguishable from "no such taxon". That is how 28 Farallon
# birds reached the release with no rank, no parent and no classification at
# all: their source TSNs (Puffinus griseus 174553, Sterna caspia 176924,
# Phalacrocorax penicillatus 174724, …) are all invalid, each with an
# acceptedTSN ITIS was never asked for. Resolve to the accepted TSN and retry
# before giving up — and record the chain under the id that was REQUESTED, so
# the cache and `_taxon_lineage_flat` still join on what the caller asked about.
.fetch_itis_chain <- function(tsn, sleep = 0.3) {
  if (!requireNamespace("taxize", quietly = TRUE)) return(NULL)
  chain <- function(id) tryCatch({
    cl <- taxize::classification(id, db = "itis", messages = FALSE)[[1]]
    if (is.null(cl) || !is.data.frame(cl) || !nrow(cl)) NULL else cl
  }, error = function(e) NULL)

  cl <- chain(tsn)
  Sys.sleep(sleep)
  if (is.null(cl)) {
    acc <- tryCatch(taxize::itis_acceptname(tsn), error = function(e) NULL)
    at  <- if (!is.null(acc) && is.data.frame(acc) && nrow(acc))
      suppressWarnings(as.integer(acc$acceptedtsn[1])) else NA_integer_
    if (!is.na(at) && !identical(at, as.integer(tsn))) {
      cl <- chain(at)
      Sys.sleep(sleep)
    }
  }
  if (is.null(cl)) return(NULL)

  data.frame(
    requested_id      = as.integer(tsn),
    authority         = "ITIS",
    taxonID           = as.integer(cl$id),
    parentNameUsageID = as.integer(c(NA, cl$id[-nrow(cl)])),
    scientificName    = as.character(cl$name),
    taxonRank         = as.character(cl$rank),
    stringsAsFactors  = FALSE)
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

# Flatten a lineage frame to one row per DISTINCT TAXON — every node appearing
# anywhere in any chain, not just the ids that were requested — with the five
# headline ranks, its own rank, parent and name.
#
# It used to emit only the requested ids, which left every lineage ANCESTOR with
# a key, a name, a rank and nothing else: in release v2026.08.06, 430 of
# swfsc_ichthyo's 1,553 taxa at or below family rank had no `family` and no
# `kingdom`, and the same held for both authorities (44% of ITIS ancestors, 34%
# of WoRMS). An ancestor is a real taxon a consumer can select and roll up on;
# it should not be a second-class row because of how it happened to be fetched.
#
# No API call is needed to fix it. A node's classification is the set of its
# ancestors-or-self, and every chain that passes through the node already
# contains them. So: build ONE parent map across all chains, then walk each node
# up it, recording the headline ranks as they go.
#
# The walk is by parent POINTER, not by row order. `fetch_taxon_lineage()` sorts
# its output by (authority, requested_id, taxonID), which destroys the root->self
# ordering the fetchers produced — so anything that assumed positional order
# (e.g. "the last row is the taxon itself") would be reading an arbitrary row.
.lineage_flat <- function(lin) {
  empty <- data.frame(
    requested_id = integer(), authority = character(), rank = character(),
    parent_id = integer(), scientific_name = character(),
    kingdom = character(), phylum = character(), class = character(),
    order_taxon = character(), family = character(), stringsAsFactors = FALSE)
  if (!nrow(lin)) return(empty)

  # one row per (authority, taxonID) across every chain
  nd <- lin[!duplicated(lin[, c("authority", "taxonID")]),
            c("authority", "taxonID", "parentNameUsageID", "scientificName",
              "taxonRank"), drop = FALSE]
  nd <- nd[!is.na(nd$taxonID), , drop = FALSE]
  if (!nrow(nd)) return(empty)

  self_key <- paste(nd$authority, nd$taxonID)
  par_key  <- paste(nd$authority, nd$parentNameUsageID)
  up       <- match(par_key, self_key)          # row index of the parent, NA at a root

  want  <- c(kingdom = "kingdom", phylum = "phylum", class = "class",
             order_taxon = "order", family = "family")
  rk    <- tolower(nd$taxonRank)
  out   <- lapply(want, function(w) rep(NA_character_, nrow(nd)))
  names(out) <- names(want)

  # climb one level at a time for the whole vector at once: depth is bounded
  # (~15 ranks), so this is a handful of passes rather than a per-node loop
  cur <- seq_len(nrow(nd))
  for (step in seq_len(64L)) {
    live <- !is.na(cur)
    if (!any(live)) break
    for (nm in names(want)) {
      hit <- live & !is.na(rk[cur]) & rk[cur] == want[[nm]] & is.na(out[[nm]])
      out[[nm]][hit] <- nd$scientificName[cur[hit]]
    }
    cur[live] <- up[cur[live]]
  }

  res <- data.frame(
    requested_id    = nd$taxonID,
    authority       = nd$authority,
    rank            = nd$taxonRank,
    parent_id       = nd$parentNameUsageID,
    scientific_name = nd$scientificName,
    kingdom         = out$kingdom, phylum = out$phylum, class = out$class,
    order_taxon     = out$order_taxon, family = out$family,
    stringsAsFactors = FALSE)

  # A requested id the authority has DEPRECATED has no node of its own: the chain
  # came back under the accepted id instead (ITIS 174553 Puffinus griseus is
  # fetched as 1255050 Ardenna grisea). ensure_taxon_xref() normally re-keys the
  # taxon onto that accepted id before we get here, but it cannot when `taxize`
  # is unavailable — and without an alias row such a taxon would silently lose
  # its whole classification. Point it at the deepest node of its own chain.
  req  <- unique(lin[, c("authority", "requested_id"), drop = FALSE])
  req  <- req[!is.na(req$requested_id), , drop = FALSE]
  miss <- which(!paste(req$authority, req$requested_id) %in% self_key)
  if (length(miss)) {
    alias <- do.call(rbind, lapply(miss, function(i) {
      g <- lin[lin$authority == req$authority[i] &
               lin$requested_id == req$requested_id[i], , drop = FALSE]
      # the chain's leaf is the node that is nobody else's parent
      leaf <- g$taxonID[!g$taxonID %in% g$parentNameUsageID]
      j <- match(paste(g$authority[1], leaf[1]), self_key)
      if (is.na(j)) return(NULL)
      r <- res[j, , drop = FALSE]
      r$requested_id <- req$requested_id[i]
      r
    }))
    if (!is.null(alias) && nrow(alias)) res <- rbind(res, alias)
  }
  res[!duplicated(res[, c("authority", "requested_id")]), , drop = FALSE]
}

#' Materialize the WoRMS/ITIS lineage `build_taxon_reference()` reads
#'
#' Resolves every authority id this dataset's vocabulary reaches — from the
#' staged `dataset_taxon` rows, its own taxon tables *and* from
#' `measurement_taxon.csv`, which is where the taxa that had no lineage at all
#' came from — fetches their classification (cached), and writes it into `con`
#' as the DwC-shaped `taxon` hierarchy table.
#'
#' **Two cached passes** (taxon plan D2), because the class decides the key:
#'
#' 1. the classification by the resolved AphiaID where present, else by TSN —
#'    this yields each taxon's `class`;
#' 2. for rows whose class is Aves and whose TSN resolved, the **ITIS chain**, so
#'    `parent_taxon_key` ancestry is `itis:` all the way up.
#'
#' What is staged is the chain of the authority each taxon is **keyed** on: the
#' ITIS chain for an Aves taxon with a TSN, the WoRMS chain for everything else.
#' A bird's WoRMS chain is fetched (and cached) only to learn its class; it
#' never becomes `worms:` ancestor rows beside the `itis:` ones. A bird with no
#' TSN keys `worms:` and its WoRMS chain is staged, with a note on the taxon.
#'
#' Call it **after** [ensure_taxon_xref()] (so the fetch asks about the accepted
#' id) and **before** [build_taxon_reference()] / [resolve_dataset_taxon()],
#' which read the staged class. An existing hierarchy is merged, not replaced, so
#' `swfsc_ichthyo` (which builds its own via [build_taxon_hierarchy()]) keeps
#' what it has and gains only what is missing.
#'
#' @param con a DuckDB connection holding this dataset's taxon vocabulary tables
#' @param measurement_taxon the composite crosswalk (`metadata/measurement_taxon.csv`),
#'   already filtered to this dataset
#' @param overrides the manual id registry (`metadata/taxon_override.csv`)
#' @param cache_csv path to the shared lineage cache
#'   (`metadata/taxon_lineage.csv`)
#' @param tbl hierarchy table to write (default `"taxon"` — the name
#'   [build_taxon_reference()] reads)
#' @param xref_cache_csv path to the cross-reference cache
#'   ([fetch_taxon_xref()]), used to top up `_taxon_xref` for the lineage
#'   ANCESTORS discovered here — [ensure_taxon_xref()] runs first and can only
#'   see the dataset's own vocabulary. Defaults to `taxon_xref.csv` sitting
#'   beside `cache_csv`, which is the layout every ingest uses; `NULL` skips it.
#' @inheritParams fetch_taxon_lineage
#' @return (invisibly) a list with `n_ids`, `n_rows` and `n_unresolved`
#' @export
#' @concept taxonomy
ensure_taxon_lineage <- function(con, measurement_taxon = NULL, overrides = NULL,
                                 cache_csv = NULL, tbl = "taxon",
                                 refresh = FALSE, sleep = 0.3, verbose = TRUE,
                                 xref_cache_csv = .xref_csv_beside(cache_csv)) {
  rows <- .taxon_norm_sources(con, measurement_taxon, overrides)
  has_w <- !is.na(rows$worms_id)
  has_i <- !is.na(rows$itis_id)

  # pass (a): the classification — by AphiaID where present, else by TSN. This
  # is where the class comes from; no source flag is consulted.
  lin_a <- fetch_taxon_lineage(rows$worms_id[has_w], rows$itis_id[has_i & !has_w],
                               cache_csv = cache_csv, refresh = refresh,
                               sleep = sleep, verbose = verbose)
  flat_a <- .lineage_flat(lin_a)
  fk  <- paste(flat_a$authority, flat_a$requested_id)
  cls <- dplyr::coalesce(
    as.character(flat_a$class[match(paste("WoRMS", rows$worms_id), fk)]),
    as.character(flat_a$class[match(paste("ITIS",  rows$itis_id),  fk)]),
    as.character(rows$class))
  keyed_itis <- !is.na(cls) & cls == "Aves" & has_i

  # pass (b): the ITIS chain for the Aves taxa that carry a TSN (the ones
  # taxon_key_of() will key itis:), so their ancestry is itis: all the way up
  lin_b <- if (any(keyed_itis))
    fetch_taxon_lineage(integer(), rows$itis_id[keyed_itis], cache_csv = cache_csv,
                        refresh = refresh, sleep = sleep, verbose = verbose)
  else .empty_lineage()

  # stage the chain of the authority each taxon is KEYED on, and only that one:
  # a bird's WoRMS chain taught us its class and stops there
  w_ids <- unique(rows$worms_id[has_w & !keyed_itis])
  i_ids <- unique(rows$itis_id[keyed_itis])
  lin <- rbind(
    lin_a[lin_a$authority == "WoRMS" & lin_a$requested_id %in% w_ids, , drop = FALSE],
    lin_b[lin_b$authority == "ITIS"  & lin_b$requested_id %in% i_ids, , drop = FALSE])
  if (verbose) message(glue::glue(
    "taxon lineage: {sum(keyed_itis)} Aves taxa keyed on ITIS, ",
    "{length(w_ids)} taxa on WoRMS"))

  # DwC hierarchy rows: every ancestor-or-self, deduped across requesters. This
  # is the shape build_taxon_reference() consumes.
  hier <- unique(lin[, c("authority", "taxonID", "parentNameUsageID",
                         "scientificName", "taxonRank")])
  # taxonomicStatus is NOT stamped here. It used to be the literal "accepted",
  # which is how all 2,090 released taxa claimed that status — including the 28
  # whose ITIS TSN is demonstrably deprecated, and the override rows whose own
  # note reads "WoRMS status: unaccepted". A classification chain says nothing
  # about whether the taxon is accepted; ensure_taxon_xref() fetches the real
  # status (with the date it was checked) and build_taxon_reference() coalesces
  # it in. NA here lets that value through instead of masking it.
  hier$taxonomicStatus          <- NA_character_
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

  # top up the cross-reference for the ANCESTORS we just learned about.
  #
  # ensure_taxon_xref() runs before this (it has to — the lineage fetch should
  # ask about the accepted id, not the deprecated one), so it only ever sees the
  # dataset's own vocabulary. The ancestors are discovered here, and without this
  # they stay second-class: in release v2026.08.06, 657 of 732 ancestor rows had
  # no itis_id and 198 no taxonomic_status, while every one of their answers was
  # already sitting in the xref cache. Cached ids cost no API call, so a warm
  # cache makes this free.
  if (!is.null(xref_cache_csv) && nrow(lin)) {
    anc_w <- lin$taxonID[lin$authority == "WoRMS"]
    anc_i <- lin$taxonID[lin$authority == "ITIS"]
    ax <- tryCatch(
      fetch_taxon_xref(itis_ids = anc_i, worms_ids = anc_w,
                       cache_csv = xref_cache_csv, refresh = refresh,
                       sleep = sleep, verbose = verbose),
      error = function(e) { message("ancestor xref skipped: ", conditionMessage(e)); NULL })
    if (!is.null(ax) && nrow(ax)) {
      staged <- .read_cols(con, "_taxon_xref", .xref_cache_cols)
      both   <- if (is.null(staged)) ax else dplyr::bind_rows(staged, ax)
      both   <- both[!duplicated(paste(both$query_type, both$query_value)), , drop = FALSE]
      .replace_table(con, "_taxon_xref", as.data.frame(both))
    }
  }

  n_ids   <- length(unique(stats::na.omit(c(w_ids, i_ids))))
  n_unres <- n_ids - length(unique(lin$requested_id))
  if (verbose) message(glue::glue(
    "{tbl}: {nrow(hier)} lineage rows for {n_ids} taxa ",
    "({max(n_unres, 0)} unresolved)"))
  invisible(list(n_ids = n_ids, n_rows = nrow(hier),
                 n_unresolved = max(n_unres, 0)))
}
