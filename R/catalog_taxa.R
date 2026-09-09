# the species catalog record ------------------------------------------------------
#
# One generated record per release — `taxa.json`, written beside `datasets.json`
# at release (plan 2026-09-09 "Landing follow-ups — … Species not Taxa (a species
# catalog) …", § D6 and D7). It is the dataset catalog's pattern with the `taxon`
# table in the place of `dataset`: one entry per key, every number read from the
# release, nothing authored on a page.
#
# What it holds, and why that shape:
#
# * `taxa[]` is one entry per row of the release's `taxon` table **with an
#   observation at or below it** — the observed taxa plus their ancestors (D6 (b)),
#   so *Sebastes* rolls up its species and the tree has no dead ends. The rows
#   with no observation anywhere (a dataset's vocabulary the release never saw)
#   get no entry; they are listed under the dataset that declares them, in
#   `datasets[].vocabulary_only[]`, which is where a provider looks for them.
# * `direct{}` counts observations keyed to the taxon itself; `rollup{}` counts it
#   and every descendant. `sum(rollup$n_obs)` over the roots is `obs_bio`'s keyed
#   row count — the record's own arithmetic check.
# * `datasets[]` inside a taxon is one entry per dataset with a direct
#   observation, each carrying `sources[]`: the `dataset_taxon` rows that resolve
#   to this taxon — "what the source called it" — flagged where the authority says
#   otherwise ([taxa_source_flags()]). One page per `taxon_key` and nothing else
#   (Ben, 2026-09-09: "the whole point of the integrated database is to integrate
#   across datasets"), with the datasets' own names under it.
#
# Deterministic: no wall clock, no network. The taxon order is the `taxon`
# table's; `datasets[]` follows the record's catalog order.

#' @keywords internal
CC_TAXA_SCHEMA_VERSION <- "1.0"

# the id authorities a taxon_key can carry; anything else is a dataset-local class
#' @keywords internal
CC_TAXA_AUTHORITIES <- c("worms", "itis")

# the deepest parent chain walked before the lineage is called broken (measured
# max depth on v2026.09.06 is 17)
#' @keywords internal
CC_TAXA_MAX_DEPTH <- 64L

# helpers ----------------------------------------------------------------------------

# a scalar for the record: NULL / zero-length / NA / "" all become NA, which
# jsonlite writes as `null` under `na = "null"`
.taxa_val <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA)
  x <- x[[1]]
  if (is.character(x) && !is.na(x) && !nzchar(trimws(x))) return(NA)
  if (is.na(x)) NA else x
}

.taxa_chr <- function(x) {
  v <- .taxa_val(x)
  if (is.na(v)) NA_character_ else trimws(as.character(v))
}

# an id as a JSON integer. `as.integer()` on an id above the machine maximum
# answers NA with a warning — a silently lost identifier — so anything larger
# stays a whole double, which is still an `integer` to a JSON schema validator.
.taxa_int <- function(x) {
  v <- .taxa_val(x)
  if (is.na(v)) return(NA_integer_)
  v <- suppressWarnings(as.numeric(v))
  if (is.na(v)) NA_integer_ else if (abs(v) <= .Machine$integer.max) as.integer(v) else v
}

# min/max over the values that are values: min(numeric(0)) is Inf with a warning
.taxa_min <- function(x) { x <- x[!is.na(x)]; if (length(x)) as.integer(min(x)) else NA_integer_ }
.taxa_max <- function(x) { x <- x[!is.na(x)]; if (length(x)) as.integer(max(x)) else NA_integer_ }

.taxa_norm <- function(x) tolower(trimws(x))

# `years{}` must be a JSON object even when the dataset carries no year: a
# zero-length *named* list is written `{}`, a bare `list()` is written `[]`
.taxa_years <- function(year, n) {
  o <- stats::setNames(as.list(as.integer(n)), as.character(year))
  if (!length(o)) o <- stats::setNames(list(), character())
  o
}

# the ids the source itself supplied, from `dataset_taxon.ds_source_json`
.taxa_source_ids <- function(js) {
  empty <- list(worms_id = NA_integer_, itis_id = NA_integer_, gbif_id = NA_integer_)
  js <- .taxa_chr(js)
  if (is.na(js)) return(empty)
  j <- tryCatch(jsonlite::fromJSON(js, simplifyVector = TRUE), error = function(e) list())
  if (!is.list(j)) return(empty)
  list(worms_id = .taxa_int(j[["worms_id"]]), itis_id = .taxa_int(j[["itis_id"]]),
       gbif_id = .taxa_int(j[["gbif_id"]]))
}

# A "… sp." / "… spp." / "… sp A" name: an identification that stopped at the
# genus. It is only `sp_to_genus` when the key it resolved to is a Genus —
# "Delphinus sp." keyed to *Delphinus delphis* is a synonym, not a genus rollup.
.taxa_is_sp_name <- function(name) grepl("\\bspp?\\.?( ?[A-Z])?$", name)

# the flags of one `dataset_taxon` row against the taxon it resolved to
.taxa_flags_of <- function(ds_name, ds_json, rank, scientific_name, worms_id, itis_id) {
  f <- character()
  ids  <- .taxa_source_ids(ds_json)
  name <- .taxa_chr(ds_name)
  sp   <- !is.na(name) && .taxa_is_sp_name(name) && identical(.taxa_chr(rank), "Genus")
  if (sp) f <- c(f, "sp_to_genus")
  if (is.na(name)) {
    f <- c(f, "no_name")
  } else {
    accepted <- .taxa_chr(scientific_name)
    if (!sp && !is.na(accepted) && .taxa_norm(name) != .taxa_norm(accepted)) f <- c(f, "synonym")
  }
  t_worms <- .taxa_int(worms_id); t_itis <- .taxa_int(itis_id)
  rekeyed <- (!is.na(ids$itis_id)  && !is.na(t_itis)  && ids$itis_id  != t_itis) ||
             (!is.na(ids$worms_id) && !is.na(t_worms) && ids$worms_id != t_worms)
  if (rekeyed) f <- c(f, "rekeyed")
  f
}

# name / colour / category for the dots: the record is the authority (it is what
# the pages already read), the release `dataset` table the fallback
.taxa_dataset_meta <- function(record, dataset_tbl = NULL) {
  out <- list()
  for (d in .rows(record[["datasets"]])) {
    k <- .s(d[["dataset_key"]])
    if (!nzchar(k)) next
    ct <- d[["category"]]
    out[[k]] <- list(
      dataset_name_short = .taxa_chr(d[["dataset_name_short"]]),
      color              = .taxa_chr(d[["color"]]),
      # the record's category block carries a description and a `registered` flag
      # the taxa schema does not admit; take the five keys the dots need
      category = list(key = .taxa_chr(ct[["key"]]), name = .taxa_chr(ct[["name"]]),
                      icon = .taxa_chr(ct[["icon"]]), realm = .taxa_chr(ct[["realm"]]),
                      order = .taxa_int(ct[["order"]])))
  }
  if (!is.null(dataset_tbl) && nrow(dataset_tbl)) {
    for (i in seq_len(nrow(dataset_tbl))) {
      k <- .taxa_chr(dataset_tbl[["dataset_key"]][i])
      if (is.na(k)) next
      have <- out[[k]]
      if (is.null(have)) have <- list(dataset_name_short = NA_character_, color = NA_character_,
                                      category = list(key = NA_character_, name = NA_character_,
                                                      icon = NA_character_, realm = NA_character_,
                                                      order = NA_integer_))
      if (is.na(have$dataset_name_short) && "dataset_name_short" %in% names(dataset_tbl))
        have$dataset_name_short <- .taxa_chr(dataset_tbl[["dataset_name_short"]][i])
      if (is.na(have$color) && "color" %in% names(dataset_tbl))
        have$color <- .taxa_chr(dataset_tbl[["color"]][i])
      if (is.na(have$category$name) && "category" %in% names(dataset_tbl))
        have$category$name <- .taxa_chr(dataset_tbl[["category"]][i])
      out[[k]] <- have
    }
  }
  out
}

# vocabulary ---------------------------------------------------------------------------

#' The `flags[]` a `taxa.json` source row can carry
#'
#' A `sources[]` entry is one `dataset_taxon` row — what a dataset calls a taxon.
#' A flag says how that name or id differs from the accepted one, so a species
#' page can show a quiet pill beside it rather than two names with no explanation:
#'
#' * `synonym` — the dataset's `ds_scientific_name` is not the taxon's accepted
#'   `scientific_name` (*Antennarius avalonis* → *Fowlerichthys avalonis*).
#' * `sp_to_genus` — a `… sp.` / `… spp.` / `… sp A` name that resolved to a
#'   Genus-rank taxon (mesopelagic "Cyclothone sp." → *Cyclothone*). It replaces
#'   `synonym`: the name is not wrong, it is less precise.
#' * `rekeyed` — an id the source supplied in `ds_source_json` that the authority
#'   has since deprecated; the row is keyed to the successor (54 Farallon rows on
#'   v2026.09.06, `taxon.notes` naming the move).
#' * `no_name` — the dataset carries only a code (`ds_scientific_name` is NULL).
#'
#' @return A character vector, the enum of `taxa.schema.json`.
#' @export
#' @concept taxonomy
#' @examples
#' taxa_source_flags()
taxa_source_flags <- function() c("synonym", "sp_to_genus", "rekeyed", "no_name")

# build ---------------------------------------------------------------------------------

#' Build the species catalog record (`taxa.json`)
#'
#' One entry per taxon of the release's `taxon` table with an observation at or
#' below it — the observed taxa plus their ancestors — with its lineage, the ids,
#' the groups it belongs to, its direct and rolled-up observation counts, and one
#' block per dataset that observed it carrying that dataset's own name for it.
#' The 204 vocabulary-only rows of v2026.09.06 get no entry: they are listed
#' under their dataset in `datasets[].vocabulary_only[]`.
#'
#' Everything is read from the release: `taxon`, `dataset_taxon`, `taxon_group`
#' and `obs_bio` on `con`, the dataset names, colours and categories from
#' `record` (the `datasets.json` the release has just written, so the dots on a
#' species page and on a dataset page cannot disagree). Nothing is authored and
#' nothing is fetched.
#'
#' Six grouped queries do the counting and every per-taxon lookup is a split
#' index, so the builder is linear in the number of `obs_bio` groups, not
#' quadratic in the number of taxa.
#'
#' @param con a DBI connection holding the release tables `taxon`,
#'   `dataset_taxon`, `taxon_group` and `obs_bio` (and, optionally, `dataset`)
#' @param record the `datasets.json` record — a path or the list from
#'   [build_dataset_catalog()] — read for `datasets[].dataset_name_short`,
#'   `color` and `category`, and for the catalog order of `datasets[]`
#' @param release_version the release version (default: the record's)
#' @param release_date the release date, `YYYY-MM-DD` (default: the record's)
#' @return A list ready for [write_taxa_catalog()] /
#'   `jsonlite::write_json(auto_unbox = TRUE)`, validating against
#'   `inst/schema/taxa.schema.json`.
#' @export
#' @concept taxonomy
#' @seealso [write_taxa_catalog()], [validate_taxa_catalog()], [check_taxa_catalog()]
build_taxa_catalog <- function(con, record, release_version = NULL, release_date = NULL) {
  stopifnot("build_taxa_catalog(): `con` must be an open DBI connection to the release tables" =
              inherits(con, "DBIConnection"))
  record <- .read_json(record)
  release_version <- .s(release_version %||% record[["release"]][["version"]])
  release_date    <- .s(release_date    %||% record[["release"]][["release_date"]])
  stopifnot(
    "build_taxa_catalog(): no release version (pass `release_version`, or a record with release$version)" =
      nzchar(release_version),
    "build_taxa_catalog(): no release date (pass `release_date`, or a record with release$release_date)" =
      nzchar(release_date))

  # what the release measures ----------------------------------------------------
  taxon <- DBI::dbGetQuery(con, "SELECT * FROM taxon ORDER BY taxon_key")
  dtx   <- DBI::dbGetQuery(con, "SELECT * FROM dataset_taxon ORDER BY ds_taxon_key")
  tgrp  <- DBI::dbGetQuery(con, "SELECT taxon_group_key, taxon_key FROM taxon_group
                                 ORDER BY taxon_key, taxon_group_key")
  # one grouped scan of obs_bio per grain; CAST so the driver answers integers
  txd <- DBI::dbGetQuery(con, "
    SELECT taxon_key, dataset_key,
           CAST(count(*) AS INTEGER)                    AS n_obs,
           CAST(count(DISTINCT sample_key) AS INTEGER)  AS n_samples,
           CAST(min(year) AS INTEGER)                   AS year_min,
           CAST(max(year) AS INTEGER)                   AS year_max
    FROM obs_bio WHERE taxon_key IS NOT NULL
    GROUP BY 1, 2 ORDER BY 1, 2")
  txdy <- DBI::dbGetQuery(con, "
    SELECT taxon_key, dataset_key, CAST(year AS INTEGER) AS year, CAST(count(*) AS INTEGER) AS n
    FROM obs_bio WHERE taxon_key IS NOT NULL AND year IS NOT NULL
    GROUP BY 1, 2, 3 ORDER BY 1, 2, 3")
  txds <- DBI::dbGetQuery(con, "
    SELECT taxon_key, dataset_key, life_stage, CAST(count(*) AS INTEGER) AS n
    FROM obs_bio WHERE taxon_key IS NOT NULL AND life_stage IS NOT NULL
    GROUP BY 1, 2, 3 ORDER BY n DESC, 1, 2, 3")
  n_obs_bio <- DBI::dbGetQuery(con,
    "SELECT CAST(count(*) AS INTEGER) AS n FROM obs_bio WHERE taxon_key IS NOT NULL")$n
  dataset_tbl <- tryCatch(DBI::dbGetQuery(con, "SELECT * FROM dataset"), error = function(e) NULL)

  stopifnot("build_taxa_catalog(): the `taxon` table is empty" = nrow(taxon) > 0)

  # the tree: which taxa get an entry, and how deep each sits --------------------
  tk     <- taxon[["taxon_key"]]
  t_i    <- stats::setNames(seq_along(tk), tk)
  parent <- taxon[["parent_taxon_key"]]
  parent[!parent %in% tk] <- NA_character_   # an edge out of the table is a root
  names(parent) <- tk

  observed  <- sort(unique(txd[["taxon_key"]]))
  seeds     <- intersect(observed, tk)       # an observed key absent from `taxon` cannot be paged
  keep      <- stats::setNames(logical(length(tk)), tk)
  keep[seeds] <- TRUE
  p_i <- stats::setNames(unname(t_i[parent]), tk)   # parent's row, NA at a root
  for (k in seeds) {
    i <- p_i[[k]]
    while (!is.na(i) && !keep[[i]]) { keep[[i]] <- TRUE; i <- p_i[[i]] }
  }

  pages   <- tk[keep]                        # the taxon table's own order
  n_pages <- length(pages)
  pg_i    <- stats::setNames(seq_len(n_pages), pages)
  par_pg  <- unname(parent[pages])
  par_i   <- unname(pg_i[par_pg])            # parent's page, NA at a root

  # depth by relaxation, so a broken chain is reported instead of looping forever
  depth <- rep(NA_integer_, n_pages)
  depth[is.na(par_i)] <- 0L
  for (lvl in seq_len(CC_TAXA_MAX_DEPTH)) {
    todo <- is.na(depth) & !is.na(par_i) & !is.na(depth[par_i])
    if (!any(todo)) break
    depth[todo] <- depth[par_i[todo]] + 1L
  }
  if (anyNA(depth))
    stop("build_taxa_catalog(): taxon.parent_taxon_key does not reach a root within ",
         CC_TAXA_MAX_DEPTH, " steps for ", sum(is.na(depth)), " taxa (a cycle?): ",
         paste(utils::head(pages[is.na(depth)], 5), collapse = ", "), call. = FALSE)

  # every per-taxon lookup is a split index, taken once ---------------------------
  pair <- function(taxon_key, dataset_key) paste(taxon_key, dataset_key, sep = "\r")
  txd_by_taxon   <- split(seq_len(nrow(txd)),  txd[["taxon_key"]])
  txdy_by_pair   <- split(seq_len(nrow(txdy)), pair(txdy[["taxon_key"]], txdy[["dataset_key"]]))
  txds_by_pair   <- split(seq_len(nrow(txds)), pair(txds[["taxon_key"]], txds[["dataset_key"]]))
  txds_by_taxon  <- split(txds[["life_stage"]], txds[["taxon_key"]])
  dtx_by_pair    <- split(seq_len(nrow(dtx)),  pair(dtx[["taxon_key"]], dtx[["dataset_key"]]))
  dtx_by_taxon   <- split(seq_len(nrow(dtx)),  dtx[["taxon_key"]])
  grp_by_taxon   <- split(tgrp[["taxon_group_key"]], tgrp[["taxon_key"]])

  # the datasets, in the record's catalog order -----------------------------------
  rec_order <- vapply(.rows(record[["datasets"]]), function(d) .s(d[["dataset_key"]]), "")
  dtx_dk    <- dtx[["dataset_key"]]
  ds_seen   <- unique(c(txd[["dataset_key"]], dtx_dk[!is.na(dtx_dk)]))
  ds_keys   <- c(intersect(rec_order, ds_seen), sort(setdiff(ds_seen, rec_order)))
  ds_meta   <- .taxa_dataset_meta(record, dataset_tbl)

  # direct counts, then the rollup ------------------------------------------------
  d_obs <- d_smp <- d_nds <- integer(n_pages)
  d_ymin <- d_ymax <- rep(NA_integer_, n_pages)
  ds_mat <- matrix(FALSE, n_pages, length(ds_keys), dimnames = list(NULL, ds_keys))
  for (i in seq_len(n_pages)) {
    rr <- txd_by_taxon[[pages[i]]]
    if (is.null(rr)) next
    d_obs[i]  <- sum(txd[["n_obs"]][rr])
    d_smp[i]  <- sum(txd[["n_samples"]][rr])
    d_nds[i]  <- length(unique(txd[["dataset_key"]][rr]))
    d_ymin[i] <- .taxa_min(txd[["year_min"]][rr])
    d_ymax[i] <- .taxa_max(txd[["year_max"]][rr])
    ds_mat[i, txd[["dataset_key"]][rr]] <- TRUE
  }
  is_species <- stats::setNames(!is.na(taxon[["rank"]]) & taxon[["rank"]] == "Species", tk)
  obs_pg <- pages %in% observed
  r_obs  <- d_obs
  r_taxa <- as.integer(obs_pg)
  r_sp   <- as.integer(obs_pg & unname(is_species[pages]))
  r_ymin <- d_ymin; r_ymax <- d_ymax
  r_mat  <- ds_mat
  # postorder: a child is always deeper than its parent, so one deepest-first pass
  # accumulates every subtree exactly once
  for (i in order(depth, decreasing = TRUE)) {
    p <- par_i[i]
    if (is.na(p)) next
    r_obs[p]  <- r_obs[p]  + r_obs[i]
    r_taxa[p] <- r_taxa[p] + r_taxa[i]
    r_sp[p]   <- r_sp[p]   + r_sp[i]
    r_ymin[p] <- .taxa_min(c(r_ymin[p], r_ymin[i]))
    r_ymax[p] <- .taxa_max(c(r_ymax[p], r_ymax[i]))
    r_mat[p, ] <- r_mat[p, ] | r_mat[i, ]
  }
  r_nds <- as.integer(rowSums(r_mat))

  local_re <- paste0("^(", paste(CC_TAXA_AUTHORITIES, collapse = "|"), "):")

  # taxa[] ------------------------------------------------------------------------
  taxa <- lapply(seq_len(n_pages), function(i) {
    k <- pages[i]; ti <- t_i[[k]]
    dsr <- txd_by_taxon[[k]]
    if (!is.null(dsr))
      dsr <- dsr[order(-txd[["n_obs"]][dsr], txd[["dataset_key"]][dsr])]
    datasets <- lapply(dsr, function(j) {
      dk <- txd[["dataset_key"]][j]; pk <- pair(k, dk)
      yr <- txdy_by_pair[[pk]]
      st <- txds[["life_stage"]][txds_by_pair[[pk]]]
      sources <- lapply(dtx_by_pair[[pk]], function(s) {
        ids <- .taxa_source_ids(dtx[["ds_source_json"]][s])
        list(name        = .taxa_chr(dtx[["ds_scientific_name"]][s]),
             common_name = .taxa_chr(dtx[["ds_common_name"]][s]),
             code        = .taxa_chr(dtx[["ds_taxa_code"]][s]),
             ids         = ids,
             flags       = .arr(.taxa_flags_of(
               dtx[["ds_scientific_name"]][s], dtx[["ds_source_json"]][s],
               taxon[["rank"]][ti], taxon[["scientific_name"]][ti],
               taxon[["worms_id"]][ti], taxon[["itis_id"]][ti])))
      })
      list(dataset_key = dk,
           n_obs       = as.integer(txd[["n_obs"]][j]),
           n_samples   = as.integer(txd[["n_samples"]][j]),
           year_min    = .taxa_int(txd[["year_min"]][j]),
           year_max    = .taxa_int(txd[["year_max"]][j]),
           life_stages = .arr(unique(st)),
           years       = .taxa_years(txdy[["year"]][yr], txdy[["n"]][yr]),
           sources     = sources)
    })
    out <- list(
      taxon_key        = k,
      slug             = sub(":", "-", k, fixed = TRUE),
      scientific_name  = .taxa_chr(taxon[["scientific_name"]][ti]),
      common_name      = .taxa_chr(taxon[["common_name"]][ti]),
      rank             = .taxa_chr(taxon[["rank"]][ti]),
      rank_order       = .taxa_int(taxon[["rank_order"]][ti]),
      taxonomic_status = .taxa_chr(taxon[["taxonomic_status"]][ti]),
      status_checked   = .taxa_chr(taxon[["status_checked"]][ti]),
      parent_taxon_key = .taxa_chr(par_pg[i]),
      lineage = list(kingdom = .taxa_chr(taxon[["kingdom"]][ti]),
                     phylum  = .taxa_chr(taxon[["phylum"]][ti]),
                     class   = .taxa_chr(taxon[["class"]][ti]),
                     order   = .taxa_chr(taxon[["order_taxon"]][ti]),
                     family  = .taxa_chr(taxon[["family"]][ti])),
      ids = list(worms_id = .taxa_int(taxon[["worms_id"]][ti]),
                 itis_id  = .taxa_int(taxon[["itis_id"]][ti]),
                 gbif_id  = .taxa_int(taxon[["gbif_id"]][ti]),
                 ncbi_id  = .taxa_int(taxon[["ncbi_id"]][ti]),
                 inat_id  = .taxa_int(taxon[["inat_id"]][ti])),
      groups = .arr(unique(grp_by_taxon[[k]])),
      notes  = .taxa_chr(taxon[["notes"]][ti]),
      direct = list(n_obs = as.integer(d_obs[i]), n_samples = as.integer(d_smp[i]),
                    n_datasets = as.integer(d_nds[i]),
                    year_min = d_ymin[i], year_max = d_ymax[i],
                    life_stages = .arr(unique(txds_by_taxon[[k]]))),
      rollup = list(n_obs = as.integer(r_obs[i]), n_taxa = as.integer(r_taxa[i]),
                    n_species = as.integer(r_sp[i]), n_datasets = r_nds[i],
                    year_min = r_ymin[i], year_max = r_ymax[i]),
      datasets = datasets)
    # a dataset-local class (`calcofi_phytoplankton:218`) is a page like any other,
    # headed by the dataset's own name for it
    if (!grepl(local_re, k)) {
      lr <- dtx_by_taxon[[k]]
      j  <- if (length(lr)) lr[[1]] else NA_integer_
      out[["local"]] <- list(
        dataset_key = sub(":.*$", "", k),
        name        = if (is.na(j)) NA_character_ else .taxa_chr(dtx[["ds_scientific_name"]][j]),
        common_name = if (is.na(j)) NA_character_ else .taxa_chr(dtx[["ds_common_name"]][j]),
        code        = if (is.na(j)) NA_character_ else .taxa_chr(dtx[["ds_taxa_code"]][j]))
    }
    out
  })

  # datasets[] --------------------------------------------------------------------
  datasets <- lapply(ds_keys, function(dk) {
    m  <- ds_meta[[dk]]
    rr <- which(txd[["dataset_key"]] == dk)
    # a vocabulary row is one whose taxon has no observation anywhere, so no page.
    # A row that resolved to nothing at all has no key to point at and is left to
    # check_dataset_taxon(), the gate that owns it.
    vo <- which(dk == dtx[["dataset_key"]] & !is.na(dtx[["taxon_key"]]) &
                  !dtx[["taxon_key"]] %in% observed)
    list(dataset_key        = dk,
         dataset_name_short = if (is.null(m)) NA_character_ else m$dataset_name_short,
         color              = if (is.null(m)) NA_character_ else m$color,
         category           = if (is.null(m))
           list(key = NA_character_, name = NA_character_, icon = NA_character_,
                realm = NA_character_, order = NA_integer_) else m$category,
         n_obs  = as.integer(sum(txd[["n_obs"]][rr])),
         n_taxa = length(unique(txd[["taxon_key"]][rr])),
         vocabulary_only = lapply(vo, function(j) list(
           taxon_key = .taxa_chr(dtx[["taxon_key"]][j]),
           name      = .taxa_chr(dtx[["ds_scientific_name"]][j]),
           code      = .taxa_chr(dtx[["ds_taxa_code"]][j]))))
  })

  list(schema_version = CC_TAXA_SCHEMA_VERSION,
       release = list(version = release_version, release_date = release_date),
       counts = list(
         taxa_observed    = length(observed),
         species_observed = sum(is_species[seeds]),
         taxon_rows       = nrow(taxon),
         datasets         = length(ds_keys),
         pages            = n_pages,
         obs_bio_rows     = as.integer(n_obs_bio)),
       datasets = datasets,
       taxa = taxa)
}

# write -----------------------------------------------------------------------------

#' Write `taxa.json`
#'
#' Minified, not pretty-printed: the record is ~2.4 MB on v2026.09.06 and is read
#' by a build, never by a person. `na = "null"` is what turns the record's `NA`
#' scalars into the JSON `null` the schema declares.
#'
#' @param record from [build_taxa_catalog()]
#' @param dir the release directory (created if missing)
#' @return The path written, invisibly.
#' @export
#' @concept taxonomy
#' @seealso [build_taxa_catalog()]
write_taxa_catalog <- function(record, dir) {
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  path <- file.path(dir, "taxa.json")
  jsonlite::write_json(record, path, auto_unbox = TRUE, digits = NA, null = "null", na = "null")
  invisible(path)
}

# validate --------------------------------------------------------------------------

#' Validate a `taxa.json` against the package's JSON schema
#'
#' The schema is `inst/schema/taxa.schema.json` (draft-07). Uses \pkg{jsonvalidate}
#' when installed; otherwise a structural check of the required top-level and
#' per-taxon keys, which is what the tests can always run.
#'
#' @param x a `taxa.json` path, its text, or the record list
#' @param schema path to the schema file
#' @param verbose return the validator's error table on failure
#' @return `TRUE`, or stops with the first errors.
#' @export
#' @concept taxonomy
#' @seealso [build_taxa_catalog()], [check_taxa_catalog()]
validate_taxa_catalog <- function(x, schema = system.file("schema", "taxa.schema.json", package = "calcofi4db"),
                                  verbose = TRUE) {
  stopifnot("validate_taxa_catalog(): the schema file was not found" = file.exists(schema))
  txt <- if (is.character(x) && length(x) == 1 && file.exists(x))
    paste(readLines(x, warn = FALSE, encoding = "UTF-8"), collapse = "\n") else
      if (is.character(x)) paste(x, collapse = "\n") else
        jsonlite::toJSON(x, auto_unbox = TRUE, digits = NA, null = "null", na = "null")
  if (requireNamespace("jsonvalidate", quietly = TRUE)) {
    v  <- jsonvalidate::json_validator(schema, engine = "ajv")
    ok <- v(txt, verbose = verbose)
    if (!isTRUE(ok)) {
      err <- attr(ok, "errors")
      msg <- if (is.data.frame(err) && nrow(err))
        paste(utils::head(paste(err$instancePath, err$message), 10), collapse = "\n  ") else "schema violation"
      stop("taxa.json does not validate against ", basename(schema), ":\n  ", msg, call. = FALSE)
    }
    return(TRUE)
  }
  j <- jsonlite::fromJSON(txt, simplifyVector = FALSE)
  miss <- setdiff(c("schema_version", "release", "counts", "datasets", "taxa"), names(j))
  if (length(miss)) stop("taxa.json is missing: ", paste(miss, collapse = ", "), call. = FALSE)
  need <- c("taxon_key", "slug", "scientific_name", "common_name", "rank", "rank_order",
            "taxonomic_status", "status_checked", "parent_taxon_key", "lineage", "ids",
            "groups", "direct", "rollup", "datasets")
  for (t in j[["taxa"]]) {
    miss <- setdiff(need, names(t))
    if (length(miss))
      stop("taxon ", .s(t[["taxon_key"]]), " is missing: ", paste(miss, collapse = ", "), call. = FALSE)
  }
  TRUE
}

# check -----------------------------------------------------------------------------

#' The checks [check_taxa_catalog()] runs, with their level
#'
#' Every `error` finding stops the release: the record is generated, so a failure
#' is a bug in the generator or a break in the release's own tables, never
#' something a registry row can excuse.
#'
#' @return A named character vector, check -> level.
#' @export
#' @concept taxonomy
taxa_catalog_checks <- function() c(
  taxa_observed     = "error",   # counts$taxa_observed == distinct obs_bio.taxon_key
  species_observed  = "error",   # counts$species_observed == those at rank Species
  obs_bio_rows      = "error",   # counts$obs_bio_rows == obs_bio rows with a taxon_key
  pages             = "error",   # counts$pages == length(taxa)
  observed_paged    = "error",   # every observed taxon has an entry
  parents_resolve   = "error",   # every parent_taxon_key is a key in taxa[]
  rollup_total      = "error",   # sum of rollup$n_obs over the roots == counts$obs_bio_rows
  slugs_unique      = "error",   # one page directory per taxon
  datasets_known    = "error",   # every datasets[].dataset_key is in the record
  dataset_meta      = "error",   # every dataset has a short name and a #rrggbb colour
  flags_known       = "error",   # every source flag is in taxa_source_flags()
  local_blocks      = "error")   # a dataset-local key carries local{}, an authority key does not

#' Check a `taxa.json` against the release it was built from
#'
#' One row per check with an `ok` flag, so a release chunk can print the table
#' whether or not it passes. The counts are re-measured against `obs_bio` when
#' `con` is given (the release's own connection, or a promoted release read back);
#' without it the arithmetic that lives inside the record is still checked.
#'
#' @param record from [build_taxa_catalog()] (or a `taxa.json` path or URL)
#' @param con a DBI connection holding `taxon` and `obs_bio`; NULL to check only
#'   what the record can prove about itself
#' @param dataset_record the `datasets.json` record (or its path) whose
#'   `datasets[]` every `dataset_key` must appear in; NULL to skip that check
#' @return A [tibble][tibble::tibble]: `check`, `level`, `ok`, `expected`,
#'   `observed`, `detail`.
#' @export
#' @concept taxonomy
#' @seealso [assert_taxa_catalog()], [taxa_catalog_checks()]
check_taxa_catalog <- function(record, con = NULL, dataset_record = NULL) {
  record <- .read_json(record)
  levels <- taxa_catalog_checks()
  rows <- list()
  add <- function(check, ok, expected = NA_character_, observed = NA_character_, detail = "") {
    rows[[length(rows) + 1]] <<- tibble::tibble(
      check = check, level = unname(levels[check]), ok = isTRUE(ok),
      expected = as.character(expected), observed = as.character(observed), detail = detail)
  }
  num <- function(x) { v <- .taxa_val(x); if (is.na(v)) 0 else as.numeric(v) }
  taxa   <- .rows(record[["taxa"]])
  counts <- record[["counts"]]
  keys   <- vapply(taxa, function(t) .s(t[["taxon_key"]]), "")
  slugs  <- vapply(taxa, function(t) .s(t[["slug"]]), "")

  # counts against obs_bio ------------------------------------------------------
  if (!is.null(con)) {
    obs <- DBI::dbGetQuery(con,
      "SELECT CAST(count(DISTINCT taxon_key) AS INTEGER) AS n_taxa,
              CAST(count(*) AS INTEGER)                  AS n_obs
       FROM obs_bio WHERE taxon_key IS NOT NULL")
    n_sp <- DBI::dbGetQuery(con,
      "SELECT CAST(count(*) AS INTEGER) AS n FROM taxon t
       WHERE t.rank = 'Species'
         AND t.taxon_key IN (SELECT DISTINCT taxon_key FROM obs_bio WHERE taxon_key IS NOT NULL)")$n
    add("taxa_observed", identical(as.integer(counts[["taxa_observed"]]), obs$n_taxa),
        obs$n_taxa, counts[["taxa_observed"]], "distinct obs_bio.taxon_key")
    add("species_observed", identical(as.integer(counts[["species_observed"]]), n_sp),
        n_sp, counts[["species_observed"]], "observed taxa at rank Species")
    add("obs_bio_rows", identical(as.integer(counts[["obs_bio_rows"]]), obs$n_obs),
        obs$n_obs, counts[["obs_bio_rows"]], "obs_bio rows with a taxon_key")
    obs_keys <- DBI::dbGetQuery(con,
      "SELECT DISTINCT taxon_key FROM obs_bio WHERE taxon_key IS NOT NULL")$taxon_key
    miss <- setdiff(obs_keys, keys)
    add("observed_paged", length(miss) == 0, length(obs_keys), length(obs_keys) - length(miss),
        if (length(miss)) paste("no entry for", paste(utils::head(miss, 5), collapse = ", ")) else
          "every observed taxon has an entry")
  } else {
    add("taxa_observed", TRUE, counts[["taxa_observed"]], counts[["taxa_observed"]],
        "not re-measured (no connection)")
    add("species_observed", TRUE, counts[["species_observed"]], counts[["species_observed"]],
        "not re-measured (no connection)")
    obs_from_rec <- sum(vapply(taxa, function(t) num(t[["direct"]][["n_obs"]]), 0))
    add("obs_bio_rows", isTRUE(as.numeric(counts[["obs_bio_rows"]]) == obs_from_rec),
        obs_from_rec, counts[["obs_bio_rows"]], "sum of direct n_obs over taxa[] (no connection)")
    add("observed_paged", TRUE, counts[["taxa_observed"]], counts[["taxa_observed"]],
        "not re-measured (no connection)")
  }

  add("pages", identical(as.integer(counts[["pages"]]), length(taxa)),
      length(taxa), counts[["pages"]], "counts$pages is the length of taxa[]")

  # the tree closes on itself ----------------------------------------------------
  par <- vapply(taxa, function(t) .s(t[["parent_taxon_key"]]), "")
  bad <- setdiff(par[nzchar(par)], keys)
  add("parents_resolve", length(bad) == 0, 0, length(bad),
      if (length(bad)) paste("parent(s) with no entry:", paste(utils::head(bad, 5), collapse = ", ")) else
        "every parent_taxon_key is a key in taxa[]")

  # the rollup arithmetic: the roots hold every observation ----------------------
  roots <- which(!nzchar(par))
  root_obs <- sum(vapply(taxa[roots], function(t) num(t[["rollup"]][["n_obs"]]), 0))
  add("rollup_total", isTRUE(root_obs == as.numeric(counts[["obs_bio_rows"]])),
      counts[["obs_bio_rows"]], root_obs,
      sprintf("sum of rollup$n_obs over %d root(s)", length(roots)))

  add("slugs_unique", anyDuplicated(slugs) == 0L, length(slugs), length(unique(slugs)),
      if (anyDuplicated(slugs) > 0L)
        paste("duplicate slug(s):", paste(utils::head(unique(slugs[duplicated(slugs)]), 5), collapse = ", ")) else
          "one page directory per taxon")

  # the datasets --------------------------------------------------------------
  ds  <- .rows(record[["datasets"]])
  dsk <- vapply(ds, function(d) .s(d[["dataset_key"]]), "")
  in_taxa <- unique(unlist(lapply(taxa, function(t)
    vapply(.rows(t[["datasets"]]), function(d) .s(d[["dataset_key"]]), ""))))
  if (!is.null(dataset_record)) {
    dataset_record <- .read_json(dataset_record)
    known <- vapply(.rows(dataset_record[["datasets"]]), function(d) .s(d[["dataset_key"]]), "")
    unk <- setdiff(unique(c(dsk, in_taxa)), known)
    add("datasets_known", length(unk) == 0, length(unique(c(dsk, in_taxa))), length(unk),
        if (length(unk)) paste("not in datasets.json:", paste(unk, collapse = ", ")) else
          "every dataset_key is in the release record")
  } else {
    unk <- setdiff(in_taxa, dsk)
    add("datasets_known", length(unk) == 0, length(in_taxa), length(unk),
        if (length(unk)) paste("observed but not in datasets[]:", paste(unk, collapse = ", ")) else
          "every dataset a taxon names is in datasets[] (datasets.json not supplied)")
  }
  no_meta <- dsk[!vapply(ds, function(d)
    nzchar(.s(d[["dataset_name_short"]])) && grepl("^#[0-9a-fA-F]{6}$", .s(d[["color"]])), logical(1))]
  add("dataset_meta", length(no_meta) == 0, length(dsk), length(dsk) - length(no_meta),
      if (length(no_meta)) paste("no short name or #rrggbb colour:", paste(no_meta, collapse = ", ")) else
        "every dataset carries a short name and a colour")

  # the flags ------------------------------------------------------------------
  flags <- unlist(lapply(taxa, function(t) unlist(lapply(.rows(t[["datasets"]]), function(d)
    unlist(lapply(.rows(d[["sources"]]), function(s) unlist(s[["flags"]])))))))
  unk <- setdiff(unique(flags), taxa_source_flags())
  add("flags_known", length(unk) == 0, length(flags), length(unk),
      if (length(unk)) paste("unknown flag(s):", paste(unk, collapse = ", ")) else
        sprintf("%d source flag(s), all in taxa_source_flags()", length(flags)))

  # local{} is exactly the dataset-local keys ----------------------------------
  local_re <- paste0("^(", paste(CC_TAXA_AUTHORITIES, collapse = "|"), "):")
  has_local <- vapply(taxa, function(t) !is.null(t[["local"]]), logical(1))
  want_local <- !grepl(local_re, keys)
  add("local_blocks", identical(has_local, want_local), sum(want_local), sum(has_local),
      if (identical(has_local, want_local)) "local{} on every dataset-local key and no other" else
        paste("local{} mismatch on:", paste(utils::head(keys[has_local != want_local], 5), collapse = ", ")))

  do.call(rbind, rows)
}

#' Stop on any failing check from [check_taxa_catalog()]
#'
#' @param d the table from [check_taxa_catalog()]
#' @param quiet suppress the passing summary
#' @return `d`, invisibly, when nothing blocks.
#' @export
#' @concept taxonomy
#' @seealso [check_taxa_catalog()]
assert_taxa_catalog <- function(d, quiet = FALSE) {
  bad <- d[!d[["ok"]] & d[["level"]] == "error", , drop = FALSE]
  if (nrow(bad))
    stop("taxa catalog check: ", nrow(bad), " blocking finding(s):\n",
         paste0("  ", bad[["check"]], ": ", bad[["detail"]],
                "  (expected ", bad[["expected"]], ", got ", bad[["observed"]], ")", collapse = "\n"),
         "\n  The record is generated: fix build_taxa_catalog() or the release's taxon / obs_bio tables.",
         call. = FALSE)
  if (!quiet) message("taxa catalog check: ", nrow(d), " check(s) pass")
  invisible(d)
}
