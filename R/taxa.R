# unified taxon model ----------------------------------------------------------
# Collapses the per-dataset taxon vocabularies into three shared references that
# every consumer reads:
#   - taxon         : one authoritative row per taxon, keyed by a lowercase
#                     authority-prefixed `taxon_key` ("worms:<id>" for all taxa,
#                     "itis:<id>" for class Aves; dataset-local "<dataset>:<code>"
#                     only where no authority id resolves)
#   - dataset_taxon : crosswalk from each dataset's local vocabulary to `taxon`
#                     (ds_taxon_key + the dataset's own name/code + taxon_key +
#                     ds_source_json, the ids the source itself supplied)
#   - taxon_group   : named groupings (many taxon_key per taxon_group_key), from
#                     metadata/taxon_group.csv
#
# Since 3.29.0 (taxon plan, `.claude/plans/2026-09-02 Taxon crosswalk …`) the
# ingest STAGES its vocabulary with append_dataset_taxon() — `taxon_key` empty —
# and the package fills `taxon_key` from the authorities with
# resolve_dataset_taxon(). The key rule reads the CLASS from the cached lineage,
# never a source flag. The seven per-dataset arms in .taxon_norm_sources() still
# serve datasets that have not staged (coexistence, Phase 1); a staged dataset
# wins over its arm. Phase 3 deletes the arms.
#
# Coarse / composite taxa (cufes "sardine_eggs", phyllosoma stages, euphausiid
# family, phyto functional groups, seabird/mammal species) are resolved to real
# WoRMS/ITIS ids via two reviewable registries passed in from the workflows repo:
#   - measurement_taxon : decompose composite `measurement_type` names into
#                         (taxon, canonical type, life_stage, target grain)
#   - overrides         : manual id resolution for source taxa lacking a clean id
# See design_env-bio-consolidation.md (CalCOFI/workflows).

# taxon_key_of -----------------------------------------------------------------

#' Encode an authority-prefixed `taxon_key`
#'
#' The single rule for minting the global taxon key, stated once:
#'
#' > A taxon keys **`itis:<tsn>` exactly when its class is Aves and an accepted
#' > TSN resolves**; otherwise **`worms:<aphia>`**; otherwise `NA`, which callers
#' > turn into the dataset-local fallback `<dataset_key>:<code>` that
#' > [check_taxon_ids()] refuses unless allow-listed.
#'
#' The class is a fact from the authority's classification (staged by
#' [ensure_taxon_lineage()]), not a flag a source declares: only one dataset
#' ever carried an `is_bird` column, so Aves reaching the release through any
#' other dataset would have keyed `worms:` and one species could have carried two
#' keys. Birds key on ITIS because WoRMS bird taxonomy lags (it still says
#' *Oceanodroma*, *Puffinus*, *Phalacrocorax*). A bird with no accepted TSN keys
#' `worms:` and gets a note in `taxon.notes` — visible, not silent. All prefixes
#' are lowercase. Vectorized over the ids; `class` recycles.
#'
#' @param worms_id integer WoRMS AphiaID(s) (NA where unknown)
#' @param itis_id integer ITIS TSN(s) (NA where unknown)
#' @param class character; the taxon's class from the lineage (`"Aves"` selects
#'   the `itis:` authority). NA means "not known to be Aves".
#' @return character vector of `taxon_key`s (NA where no authority id resolves)
#' @export
#' @concept taxonomy
#' @examples
#' taxon_key_of(217452L, 161729L)                       # "worms:217452"  (Pacific sardine)
#' taxon_key_of(137179L, 174715L, class = "Aves")       # "itis:174715"   (Great Cormorant)
#' taxon_key_of(137179L, NA_integer_, class = "Aves")   # "worms:137179"  (a bird with no TSN)
taxon_key_of <- function(worms_id, itis_id = NA_integer_, class = NA_character_) {
  n <- max(length(worms_id), length(itis_id))
  if (length(class) > 1L && length(class) != n)
    stop("taxon_key_of(): `class` must be length 1 or match the ids.")
  worms_id <- rep(worms_id, length.out = n)
  itis_id  <- rep(itis_id,  length.out = n)
  class    <- rep(as.character(class), length.out = n)
  wi <- suppressWarnings(as.integer(worms_id))
  ii <- suppressWarnings(as.integer(itis_id))
  out <- rep(NA_character_, n)
  # class Aves with an accepted TSN -> itis:
  aves <- !is.na(class) & class == "Aves" & !is.na(ii)
  out[aves] <- paste0("itis:", ii[aves])
  # everything else -> worms: when an AphiaID is present
  w <- is.na(out) & !is.na(wi)
  out[w] <- paste0("worms:", wi[w])
  out
}

# the key of a LINEAGE row is the authority of the chain it was fetched in — an
# ancestor above class rank has no class to read, and an ITIS ancestor of a bird
# must stay itis: for the parent links to resolve
.key_of_authority <- function(authority, id) {
  id <- suppressWarnings(as.integer(id))
  ifelse(is.na(id), NA_character_,
         paste0(ifelse(!is.na(authority) & authority == "ITIS", "itis:", "worms:"), id))
}

# TRUE only for scalar/logical TRUE, treating NA as FALSE (vectorized)
isTRUE_vec <- function(x) !is.na(x) & as.logical(x)

# taxa_rank_reference ----------------------------------------------------------

#' The canonical taxonomic rank ordering (`taxa_rank`)
#'
#' One row per rank, ordered kingdom-down, so `taxon.rank_order` sorts a
#' hierarchy without a consumer hard-coding rank names.
#'
#' This used to be a vector inside [build_taxon_hierarchy()], which exactly one
#' ingest calls — so the `taxa_rank` lookup existed in the `swfsc_ichthyo`
#' connection and nowhere else, and `build_taxon_reference()`'s left join to it
#' produced `rank_order = NA` for every other dataset's taxa. In release
#' v2026.08.06 that was **100% of ITIS-keyed taxa** (all 169, i.e. every seabird
#' and marine mammal) plus 252 WoRMS-keyed ones — 172 species, 83 genera and 49
#' families with no sortable rank.
#'
#' The vocabulary spans BOTH authorities. WoRMS and ITIS do not use the same rank
#' set, and eight ranks the release actually carries were absent from the old
#' vector — `Gigaclass`, `Infrakingdom`, `Megaclass`, `Parvphylum`,
#' `Phylum (Division)`, `Subphylum (Subdivision)`, `Subterclass`, `Superdomain` —
#' so those taxa had no `rank_order` even where the lookup was present.
#'
#' Ordering is by nesting depth, not by a strict Linnaean canon: what a consumer
#' needs is "does this rank sit above or below that one", and ties are harmless.
#'
#' @return a data.frame of `taxonRank` + `rank_order`
#' @export
#' @concept taxonomy
#' @examples
#' head(taxa_rank_reference())
taxa_rank_reference <- function() {
  ranks <- c(
    "Superdomain", "Domain", "Empire",
    "Kingdom", "Subkingdom", "Infrakingdom", "Superphylum",
    "Phylum", "Phylum (Division)", "Subphylum", "Subphylum (Subdivision)",
    "Infraphylum", "Parvphylum",
    "Gigaclass", "Megaclass", "Superclass", "Class", "Subclass",
    "Infraclass", "Subterclass",
    "Megacohort", "Supercohort", "Cohort", "Subcohort", "Infracohort",
    "Superorder", "Order", "Suborder", "Infraorder", "Parvorder",
    # WoRMS puts Section/Subsection BELOW Infraorder for decapods — Brachyura
    # (Infraorder) > Eubrachyura (Section) > Heterotremata (Subsection) >
    # Cancroidea (Superfamily) — not between order and family as in botany
    "Section", "Subsection",
    "Superfamily", "Family", "Subfamily",
    "Supertribe", "Tribe", "Subtribe",
    "Genus", "Subgenus",
    "Series", "Subseries",
    "Species", "Subspecies",
    "Natio", "Mutatio",
    "Form", "Forma", "Subform", "Subforma",
    "Variety", "Subvariety",
    "Coll. sp.", "Aggr.")
  data.frame(taxonRank = ranks, rank_order = seq_along(ranks),
             stringsAsFactors = FALSE)
}

# internal helpers -------------------------------------------------------------

.tbl_has <- function(con, tbl) tbl %in% DBI::dbListTables(con)

# read `cols` (that exist) from `tbl`; return NULL if the table is absent
.read_cols <- function(con, tbl, cols) {
  if (!.tbl_has(con, tbl)) return(NULL)
  have <- DBI::dbListFields(con, tbl)
  sel  <- intersect(cols, have)
  if (!length(sel)) return(NULL)
  df <- DBI::dbGetQuery(con, glue::glue(
    "SELECT {paste(sel, collapse = ', ')} FROM {tbl}"))
  # add any requested-but-missing columns as NA so downstream binds align
  for (c in setdiff(cols, sel)) df[[c]] <- NA
  df
}

# drop an object whatever its type (DuckDB errors on DROP VIEW of a TABLE and
# vice-versa, even IF EXISTS), then write a data.frame as a base table
.replace_table <- function(con, name, df) {
  t <- DBI::dbGetQuery(con, glue::glue(
    "SELECT table_type FROM information_schema.tables WHERE table_name = '{name}'"))
  if (nrow(t)) {
    kind <- if (grepl("VIEW", t$table_type[1], ignore.case = TRUE)) "VIEW" else "TABLE"
    DBI::dbExecute(con, glue::glue('DROP {kind} IF EXISTS "{name}"'))
  }
  DBI::dbWriteTable(con, name, df, overwrite = TRUE)
  invisible(nrow(df))
}

# the released dataset_taxon columns, in order. `ds_source_json` is the one
# additive column of 3.29.0: what the SOURCE claimed, beside taxon.worms_id /
# itis_id (what the authority says), so the two can be audited against each other.
.dataset_taxon_cols <- c("ds_taxon_key", "dataset_key", "taxon_key",
                         "ds_scientific_name", "ds_common_name", "ds_taxa_code",
                         "ds_source_json")

# one JSON object per row of the ids / rank the source supplied, NA when it
# supplied nothing. Key order is fixed so the value is byte-stable across runs.
.source_json <- function(worms_id, itis_id, gbif_id, rank) {
  n <- max(length(worms_id), length(itis_id), length(gbif_id), length(rank))
  if (!n) return(character())
  w <- rep(suppressWarnings(as.integer(worms_id)), length.out = n)
  i <- rep(suppressWarnings(as.integer(itis_id)),  length.out = n)
  g <- rep(suppressWarnings(as.integer(gbif_id)),  length.out = n)
  r <- rep(as.character(rank), length.out = n)
  r[!is.na(r) & !nzchar(trimws(r))] <- NA_character_
  vapply(seq_len(n), function(k) {
    x <- list()
    if (!is.na(w[k])) x$worms_id <- w[k]
    if (!is.na(i[k])) x$itis_id  <- i[k]
    if (!is.na(g[k])) x$gbif_id  <- g[k]
    if (!is.na(r[k])) x$rank     <- r[k]
    if (!length(x)) return(NA_character_)
    as.character(jsonlite::toJSON(x, auto_unbox = TRUE))
  }, character(1))
}

# the inverse: a data.frame of worms_id / itis_id / gbif_id / rank from the
# staged JSON (NA where absent)
.parse_source_json <- function(x) {
  out <- data.frame(worms_id = rep(NA_integer_, length(x)), itis_id = NA_integer_,
                    gbif_id = NA_integer_, rank = NA_character_,
                    stringsAsFactors = FALSE)
  for (k in which(!is.na(x) & nzchar(x))) {
    j <- jsonlite::fromJSON(x[k])
    if (!is.null(j$worms_id)) out$worms_id[k] <- as.integer(j$worms_id)
    if (!is.null(j$itis_id))  out$itis_id[k]  <- as.integer(j$itis_id)
    if (!is.null(j$gbif_id))  out$gbif_id[k]  <- as.integer(j$gbif_id)
    if (!is.null(j$rank))     out$rank[k]     <- as.character(j$rank)
  }
  out
}

# the common normalized shape one row per (dataset, local taxon)
.taxon_row_template <- function(n = 0L) {
  tibble::tibble(
    dataset_key      = character(n), ds_prefix = character(n),
    ds_taxa_code     = character(n),
    ds_scientific_name = character(n), ds_common_name = character(n),
    ds_source_json   = character(n),
    worms_id = integer(n), itis_id = integer(n), gbif_id = integer(n),
    scientific_name = character(n), common_name = character(n),
    rank = character(n), taxonomic_status = character(n),
    # when the authority last confirmed `taxonomic_status`, and an append-only
    # log of how this taxon's ids were resolved / re-keyed. A status with no
    # check date is not a fact — the column used to be the hardcoded string
    # "accepted" on all 2,090 taxa, including ones demonstrably not accepted.
    status_checked = character(n), notes = character(n),
    parent_worms_id = integer(n),
    # the resolved parent key. Carried rather than derived at the end, because an
    # ITIS-keyed taxon (the Aves rule) has an ITIS parent, and pasting
    # "worms:<parent_worms_id>" would mint a key that resolves to nothing.
    parent_taxon_key = character(n),
    kingdom = character(n), phylum = character(n), class = character(n),
    order_taxon = character(n), family = character(n))
}

# coerce a per-source frame to the template (missing cols -> NA of right type).
# `ds_source_json` is derived from the ids AS SUPPLIED here — before overrides
# and the cross-reference touch them — unless the caller carries one already
# (staged rows do).
.as_taxon_rows <- function(df) {
  tmpl <- .taxon_row_template(0L)
  has_json <- !is.null(df[["ds_source_json"]])
  for (c in names(tmpl)) if (is.null(df[[c]])) df[[c]] <- rep(tmpl[[c]][NA_integer_], nrow(df))
  df <- df[, names(tmpl), drop = FALSE]
  df$worms_id        <- suppressWarnings(as.integer(df$worms_id))
  df$itis_id         <- suppressWarnings(as.integer(df$itis_id))
  df$gbif_id         <- suppressWarnings(as.integer(df$gbif_id))
  df$parent_worms_id <- suppressWarnings(as.integer(df$parent_worms_id))
  df$ds_taxa_code    <- as.character(df$ds_taxa_code)
  df$ds_source_json  <- if (has_json) as.character(df$ds_source_json) else
    .source_json(df$worms_id, df$itis_id, df$gbif_id, df$rank)
  tibble::as_tibble(df)
}

# apply an overrides frame (dataset_key, match_column, match_value, worms_id,
# itis_id, scientific_name, rank) to a per-source normalized frame in place,
# filling worms_id/itis_id/scientific_name/rank. Overrides take precedence over
# the source-supplied id (they exist because the source id is missing or coarse).
#
# `match_cols` is a NAMED LIST of candidate columns this source exposes, each
# aligned with `rows` — the override row's declared `match_column` selects which
# one to match against. A staged vocabulary (append_dataset_taxon()) exposes
# exactly `ds_taxa_code`, `ds_scientific_name`, `ds_common_name`. That
# declaration used to be ignored entirely; it now errors when a PRESENT dataset's
# row names a column the source does not expose. A row for a dataset absent from
# this connection is another ingest's business and is left alone —
# check_taxon_registries() is where a dataset nobody supplies fails.
.apply_overrides <- function(rows, overrides, dataset_key, match_cols) {
  if (is.null(overrides) || !nrow(overrides)) return(rows)
  ov <- overrides[!is.na(overrides$dataset_key) &
                  overrides$dataset_key == dataset_key, , drop = FALSE]
  if (!nrow(ov)) return(rows)

  if (is.null(ov$match_column))
    stop("taxon_override.csv is missing the `match_column` column.")
  bad <- setdiff(unique(as.character(ov$match_column)), names(match_cols))
  if (length(bad)) stop(glue::glue(
    "taxon_override.csv: dataset_key '{dataset_key}' declares match_column(s) ",
    "{paste(sprintf('`%s`', bad), collapse = ', ')} that this source does not ",
    "expose. Available: {paste(sprintf('`%s`', names(match_cols)), collapse = ', ')}."))

  # one pass per declared match_column, so a dataset can key some overrides on a
  # code and others on a name
  for (mc in unique(as.character(ov$match_column))) {
    o <- ov[ov$match_column == mc, , drop = FALSE]
    m   <- match(as.character(match_cols[[mc]]), as.character(o$match_value))
    hit <- !is.na(m)
    if (!any(hit)) next
    rows$worms_id[hit] <- dplyr::coalesce(
      suppressWarnings(as.integer(o$worms_id[m[hit]])), rows$worms_id[hit])
    rows$itis_id[hit]  <- dplyr::coalesce(
      suppressWarnings(as.integer(o$itis_id[m[hit]])), rows$itis_id[hit])
    if (!is.null(o$scientific_name))
      rows$scientific_name[hit] <- dplyr::coalesce(
        o$scientific_name[m[hit]], rows$scientific_name[hit])
    if (!is.null(o$rank))
      rows$rank[hit] <- dplyr::coalesce(o$rank[m[hit]], rows$rank[hit])
  }
  rows
}

# the datasets whose vocabulary was staged by append_dataset_taxon(): a marker
# table beside `dataset_taxon`, because after resolve_dataset_taxon() every row
# in that table looks alike and the arms must not re-read their own output as a
# staged vocabulary
.staged_datasets <- function(con) {
  empty <- data.frame(dataset_key = character(), ds_prefix = character(),
                      stringsAsFactors = FALSE)
  if (!.tbl_has(con, "_dataset_taxon_staged")) return(empty)
  st <- DBI::dbGetQuery(con, "SELECT dataset_key, ds_prefix FROM _dataset_taxon_staged")
  if (nrow(st) && !.tbl_has(con, "dataset_taxon"))
    stop("_dataset_taxon_staged names ", nrow(st), " dataset(s) but `dataset_taxon` ",
         "is gone. Re-run append_dataset_taxon().")
  st
}

# strict integer coercion for a D1 id column: NA stays NA, a whole number or a
# string of digits is fine, anything else is an error naming the column
.as_int_strict <- function(x, col, dataset_key) {
  if (is.null(x)) return(NA_integer_)
  if (is.factor(x)) x <- as.character(x)
  if (is.logical(x)) {
    if (all(is.na(x))) return(rep(NA_integer_, length(x)))
    stop(glue::glue("append_dataset_taxon('{dataset_key}'): `{col}` is logical, not an id."))
  }
  if (is.character(x)) {
    x <- trimws(x); x[!is.na(x) & !nzchar(x)] <- NA_character_
    bad <- !is.na(x) & !grepl("^[0-9]+$", x)
    if (any(bad)) stop(glue::glue(
      "append_dataset_taxon('{dataset_key}'): `{col}` has value(s) that are not an ",
      "integer id: {paste(sprintf('\"%s\"', utils::head(x[bad], 5)), collapse = ', ')}."))
    return(as.integer(x))
  }
  if (is.numeric(x)) {
    bad <- !is.na(x) & (x != round(x) | x < 0)
    if (any(bad)) stop(glue::glue(
      "append_dataset_taxon('{dataset_key}'): `{col}` has value(s) that are not an ",
      "integer id: {paste(utils::head(x[bad], 5), collapse = ', ')}."))
    return(as.integer(x))
  }
  stop(glue::glue("append_dataset_taxon('{dataset_key}'): `{col}` must be integer or character."))
}

# append_dataset_taxon ---------------------------------------------------------

#' Stage a dataset's taxon vocabulary in `dataset_taxon` (taxon plan D1)
#'
#' The ingest declares its vocabulary; the package resolves it. This writes one
#' row per local taxon into `dataset_taxon` with `taxon_key` **empty** — filled
#' later, in place, by [resolve_dataset_taxon()] from the authorities — and
#' replaces any rows the table already holds for `dataset_key`.
#'
#' The column contract is explicit, and a deviation is a hard stop at ingest
#' rather than an `NA` at release (which is how dropping `itis_id` from a
#' species table would have un-keyed every seabird without an error anywhere):
#'
#' | column | required | meaning |
#' |---|---|---|
#' | `ds_taxa_code` | yes; unique; non-NA | the code `obs` stores — verbatim, never cleaned |
#' | `ds_scientific_name` | yes (NA allowed for an operational class) | the source's name; the lookup query after [clean_taxon_name()] |
#' | `ds_common_name` | no | |
#' | `worms_id`, `itis_id`, `gbif_id`, `rank` | no; ids integer | what **the source supplied** — hints to resolution, stored together as `ds_source_json` |
#'
#' Errors on a missing required column, an unknown column, a duplicate or NA
#' code, an id that does not coerce to an integer, or an empty frame.
#'
#' `ds_source_json` is one JSON object of whatever ids / rank the source
#' supplied (e.g. `{"itis_id":174715}`), `NULL` when it supplied nothing. It sits
#' beside `taxon.worms_id` / `itis_id` so "what did the source claim?" can be
#' audited against "what does the authority say?" with
#' `json_extract(ds_source_json, '$.itis_id')`. The notebook never writes JSON by
#' hand.
#'
#' @param con a DuckDB connection
#' @param dataset_key `provider_dataset` of the observing dataset (what `obs`
#'   joins on)
#' @param df the vocabulary, one row per local taxon (columns above)
#' @param ds_prefix prefix of `ds_taxon_key` (`"<ds_prefix>:<ds_taxa_code>"`);
#'   defaults to `dataset_key`. `swfsc_ichthyo` uses `"calcofi"`, the shared
#'   CalCOFI species list.
#' @return (invisibly) the number of rows staged
#' @seealso [resolve_dataset_taxon()], [check_dataset_taxon()]
#' @export
#' @concept taxonomy
#' @examples
#' \dontrun{
#' append_dataset_taxon(con, "farallon_bird-mammal", d_species |>
#'   transmute(ds_taxa_code = species, ds_scientific_name = scientific_name,
#'             ds_common_name = common_name, itis_id))
#' ensure_taxon_xref(con, mt_taxon, tx_over, cache_csv = here("metadata/taxon_xref.csv"))
#' ensure_taxon_lineage(con, mt_taxon, tx_over, cache_csv = here("metadata/taxon_lineage.csv"))
#' resolve_dataset_taxon(con, mt_taxon, tx_over)
#' }
append_dataset_taxon <- function(con, dataset_key, df, ds_prefix = dataset_key) {
  stopifnot(is.character(dataset_key), length(dataset_key) == 1L, nzchar(dataset_key),
            is.character(ds_prefix), length(ds_prefix) == 1L, nzchar(ds_prefix))
  if (!is.data.frame(df))
    stop(glue::glue("append_dataset_taxon('{dataset_key}'): `df` must be a data.frame."))
  req <- c("ds_taxa_code", "ds_scientific_name")
  opt <- c("ds_common_name", "worms_id", "itis_id", "gbif_id", "rank")

  miss <- setdiff(req, names(df))
  if (length(miss)) stop(glue::glue(
    "append_dataset_taxon('{dataset_key}'): missing required column(s) ",
    "{paste(sprintf('`%s`', miss), collapse = ', ')}."))
  unk <- setdiff(names(df), c(req, opt))
  if (length(unk)) stop(glue::glue(
    "append_dataset_taxon('{dataset_key}'): unknown column(s) ",
    "{paste(sprintf('`%s`', unk), collapse = ', ')}. The contract is ",
    "`ds_taxa_code`, `ds_scientific_name` and optionally `ds_common_name`, ",
    "`worms_id`, `itis_id`, `gbif_id`, `rank` — a column the notebook renames ",
    "or adds must fail here, not become NA at release."))
  if (!nrow(df)) stop(glue::glue(
    "append_dataset_taxon('{dataset_key}'): `df` has no rows."))

  code <- as.character(df$ds_taxa_code)
  if (anyNA(code)) stop(glue::glue(
    "append_dataset_taxon('{dataset_key}'): NA `ds_taxa_code` in row(s) ",
    "{paste(utils::head(which(is.na(code)), 5), collapse = ', ')}."))
  if (any(!nzchar(trimws(code)))) stop(glue::glue(
    "append_dataset_taxon('{dataset_key}'): empty `ds_taxa_code` in row(s) ",
    "{paste(utils::head(which(!nzchar(trimws(code))), 5), collapse = ', ')}."))
  dup <- unique(code[duplicated(code)])
  if (length(dup)) stop(glue::glue(
    "append_dataset_taxon('{dataset_key}'): duplicate `ds_taxa_code` ",
    "{paste(sprintf('\"%s\"', utils::head(dup, 5)), collapse = ', ')} — the code is ",
    "the join key from obs, so keep one row and say why."))

  w <- .as_int_strict(df[["worms_id"]], "worms_id", dataset_key)
  i <- .as_int_strict(df[["itis_id"]],  "itis_id",  dataset_key)
  g <- .as_int_strict(df[["gbif_id"]],  "gbif_id",  dataset_key)
  r <- if (is.null(df[["rank"]])) NA_character_ else as.character(df[["rank"]])

  chr <- function(x) { x <- as.character(x); x[!is.na(x) & !nzchar(trimws(x))] <- NA_character_; x }
  out <- data.frame(
    ds_taxon_key       = paste0(ds_prefix, ":", code),
    dataset_key        = dataset_key,
    taxon_key          = NA_character_,
    ds_scientific_name = chr(df$ds_scientific_name),
    ds_common_name     = if (is.null(df[["ds_common_name"]])) NA_character_ else chr(df$ds_common_name),
    ds_taxa_code       = code,
    ds_source_json     = .source_json(w, i, g, r),
    stringsAsFactors   = FALSE)
  out <- out[order(out$ds_taxon_key), , drop = FALSE]
  rownames(out) <- NULL

  .stage_dataset_taxon_rows(con, out)

  # the marker resolve_dataset_taxon() reads (see .staged_datasets())
  mark <- data.frame(dataset_key = dataset_key, ds_prefix = ds_prefix,
                     n_rows = nrow(out), stringsAsFactors = FALSE)
  if (!.tbl_has(con, "_dataset_taxon_staged")) {
    DBI::dbWriteTable(con, "_dataset_taxon_staged", mark[0, ])
  }
  DBI::dbExecute(con, "DELETE FROM _dataset_taxon_staged WHERE dataset_key = ?",
                 params = list(dataset_key))
  DBI::dbAppendTable(con, "_dataset_taxon_staged", mark)
  invisible(nrow(out))
}

# write staged rows into `dataset_taxon`, creating it with the released shape
# when absent, adding a column an older table lacks, and replacing this
# dataset's rows only
.stage_dataset_taxon_rows <- function(con, out) {
  ds <- unique(out$dataset_key)
  t <- DBI::dbGetQuery(con,
    "SELECT table_type FROM information_schema.tables WHERE table_name = 'dataset_taxon'")
  if (nrow(t) && grepl("VIEW", t$table_type[1], ignore.case = TRUE)) {
    DBI::dbExecute(con, 'DROP VIEW "dataset_taxon"')
    t <- t[0, ]
  }
  if (!nrow(t)) {
    # a 0-row data.frame of characters creates VARCHAR columns, so an all-NULL
    # taxon_key is not typed BOOLEAN by inference
    DBI::dbWriteTable(con, "dataset_taxon", out[0, .dataset_taxon_cols, drop = FALSE])
  } else {
    have <- DBI::dbListFields(con, "dataset_taxon")
    for (cl in setdiff(.dataset_taxon_cols, have))
      DBI::dbExecute(con, glue::glue('ALTER TABLE "dataset_taxon" ADD COLUMN "{cl}" VARCHAR'))
    DBI::dbExecute(con, "DELETE FROM dataset_taxon WHERE dataset_key = ?", params = list(ds))
  }
  DBI::dbAppendTable(con, "dataset_taxon", out[, .dataset_taxon_cols, drop = FALSE])
  invisible(nrow(out))
}

# the class of each row from the staged classification, WoRMS by AphiaID first,
# then ITIS by TSN, then whatever the source itself carried (zoodb/zooscan
# denormalize one). NA where nothing is staged yet — before
# ensure_taxon_lineage() has run — which only ever affects callers that read ids
# and names, never a key.
.class_from_flat <- function(con, rows) {
  flat <- .read_cols(con, "_taxon_lineage_flat", c("requested_id", "authority", "class"))
  if (is.null(flat) || !nrow(flat)) return(as.character(rows$class))
  key <- paste(flat$authority, flat$requested_id)
  cw <- as.character(flat$class[match(paste("WoRMS", rows$worms_id), key)])
  ci <- as.character(flat$class[match(paste("ITIS",  rows$itis_id),  key)])
  dplyr::coalesce(cw, ci, as.character(rows$class))
}

# gather every dataset's local taxa into one normalized frame (resolved ids +
# taxon_key + ds_taxon_key), from the staged vocabulary first, then whichever
# per-dataset source tables + registries exist for datasets that did NOT stage.
#
# Every arm consults `overrides` through .apply_overrides(), declaring which of
# its own columns are matchable.
#
# `xref` is the staged authority cross-reference (see ensure_taxon_xref()). It is
# applied AFTER the arms and BEFORE the key is minted, because it can change the
# id the key is built from: a deprecated ITIS TSN is replaced by its accepted
# form, so the key is always an accepted id. The class is then read from the
# staged lineage (ensure_taxon_lineage()) and the key minted by taxon_key_of().
.taxon_norm_sources <- function(con, measurement_taxon = NULL, overrides = NULL,
                                xref = NA) {
  arms <- list()
  staged <- .staged_datasets(con)
  staged_keys <- staged$dataset_key

  # --- the staged vocabulary (append_dataset_taxon(), D1) ---------------------
  if (nrow(staged)) {
    keys <- paste(sprintf("'%s'", gsub("'", "''", staged_keys)), collapse = ", ")
    dt <- DBI::dbGetQuery(con, glue::glue(
      "SELECT ds_taxon_key, dataset_key, ds_scientific_name, ds_common_name,
              ds_taxa_code, ds_source_json
       FROM dataset_taxon WHERE dataset_key IN ({keys}) ORDER BY ds_taxon_key"))
    src <- .parse_source_json(dt$ds_source_json)
    for (ds in staged_keys) {
      i <- dt$dataset_key == ds
      if (!any(i)) stop(glue::glue(
        "dataset '{ds}' is marked as staged but has no rows in `dataset_taxon`. ",
        "Re-run append_dataset_taxon()."))
      d <- dt[i, , drop = FALSE]; s <- src[i, , drop = FALSE]
      r <- .as_taxon_rows(data.frame(
        dataset_key = ds, ds_prefix = staged$ds_prefix[match(ds, staged$dataset_key)],
        ds_taxa_code = d$ds_taxa_code, ds_scientific_name = d$ds_scientific_name,
        ds_common_name = d$ds_common_name, ds_source_json = d$ds_source_json,
        worms_id = s$worms_id, itis_id = s$itis_id, gbif_id = s$gbif_id, rank = s$rank,
        scientific_name = d$ds_scientific_name, common_name = d$ds_common_name,
        stringsAsFactors = FALSE))
      arms[[paste0("staged:", ds)]] <- .apply_overrides(r, overrides, ds, list(
        ds_taxa_code       = d$ds_taxa_code,
        ds_scientific_name = d$ds_scientific_name,
        ds_common_name     = d$ds_common_name))
    }
  }
  # an arm whose dataset has staged is skipped: the staged rows are the
  # vocabulary, the working table is just the notebook's scratch
  arm_on <- function(ds) {
    if (ds %in% staged_keys) {
      message(glue::glue("taxon: '{ds}' is staged; its source table is not read"))
      FALSE
    } else TRUE
  }

  # --- CalCOFI species list (ichthyo + invert): worms/itis/gbif present -------
  sp <- .read_cols(con, "species",
    c("species_id", "scientific_name", "common_name", "worms_id", "itis_id", "gbif_id"))
  # dataset_key = the using dataset (swfsc_ichthyo, incl. folded invert) so obs
  # joins on (dataset_key, ds_taxa_code); ds_prefix = the known "calcofi" list.
  if (!is.null(sp) && arm_on("swfsc_ichthyo")) {
    r <- .as_taxon_rows(data.frame(
      dataset_key = "swfsc_ichthyo", ds_prefix = "calcofi",
      ds_taxa_code = sp$species_id, ds_scientific_name = sp$scientific_name,
      ds_common_name = sp$common_name, worms_id = sp$worms_id, itis_id = sp$itis_id,
      gbif_id = sp$gbif_id, scientific_name = sp$scientific_name,
      common_name = sp$common_name, stringsAsFactors = FALSE))
    arms$species <- .apply_overrides(r, overrides, "swfsc_ichthyo", list(
      species_id      = sp$species_id,
      scientific_name = sp$scientific_name,
      common_name     = sp$common_name))
  }

  # --- phytoplankton: aphia_id, coarse groups via overrides (match on `taxa`) -
  ph <- .read_cols(con, "phyto_taxon",
    c("species_code", "taxa", "aphia_id", "scientific_name_accepted", "rank", "kingdom", "phylum"))
  if (!is.null(ph) && arm_on("calcofi_phytoplankton")) {
    r <- .as_taxon_rows(data.frame(
      dataset_key = "calcofi_phytoplankton", ds_prefix = "calcofi_phytoplankton",
      ds_taxa_code = ph$species_code, ds_scientific_name = ph$scientific_name_accepted,
      ds_common_name = ph$taxa, worms_id = ph$aphia_id,
      scientific_name = ph$scientific_name_accepted, rank = ph$rank,
      kingdom = ph$kingdom, phylum = ph$phylum,
      stringsAsFactors = FALSE))
    # coarse functional groups (NULL aphia_id) resolve via overrides keyed on `taxa`
    arms$phyto <- .apply_overrides(r, overrides, "calcofi_phytoplankton", list(
      taxa            = ph$taxa,
      species_code    = ph$species_code,
      scientific_name = ph$scientific_name_accepted))
  }

  # --- zoodb / zooscan: aphia_id + denormalized lineage -----------------------
  for (nm in c("zoodb", "zooscan")) {
    tbl <- paste0(nm, "_taxon"); ds <- if (nm == "zoodb") "cce-lter_zoodb" else "cce-lter_zooscan"
    lbl <- paste0("taxon_", nm)
    z <- .read_cols(con, tbl,
      c("taxon_id", lbl, "aphia_id", "scientific_name", "rank",
        "kingdom", "class", "order_taxon", "family"))
    if (!is.null(z) && arm_on(ds)) {
      r <- .as_taxon_rows(data.frame(
        dataset_key = ds, ds_prefix = ds, ds_taxa_code = z$taxon_id,
        ds_scientific_name = z$scientific_name, ds_common_name = z[[lbl]],
        worms_id = z$aphia_id, scientific_name = z$scientific_name, rank = z$rank,
        kingdom = z$kingdom, class = z$class, order_taxon = z$order_taxon,
        family = z$family, stringsAsFactors = FALSE))
      mc <- list(taxon_id = z$taxon_id, scientific_name = z$scientific_name)
      mc[[lbl]] <- z[[lbl]]
      arms[[nm]] <- .apply_overrides(r, overrides, ds, mc)
    }
  }

  # --- euphausiids: species-resolved BTEDB export, worms_id from WoRMS --------
  # the BTEDB export names 37 species across 8 genera in its column headers; the
  # ingest resolves each to an AphiaID via standardize_species(), so euphausiids
  # crosswalks like any other per-dataset taxon vocabulary rather than through
  # measurement_taxon.csv (which only ever covered the old single-Abundance form)
  eu <- .read_cols(con, "euphausiids_taxon",
    c("taxon_id", "scientific_name", "genus", "worms_id", "rank"))
  if (!is.null(eu) && arm_on("cce-lter_euphausiids")) {
    r <- .as_taxon_rows(data.frame(
      dataset_key = "cce-lter_euphausiids", ds_prefix = "cce-lter_euphausiids",
      ds_taxa_code = eu$taxon_id, ds_scientific_name = eu$scientific_name,
      worms_id = eu$worms_id, scientific_name = eu$scientific_name,
      rank = eu$rank, stringsAsFactors = FALSE))
    arms$euphausiids <- .apply_overrides(r, overrides, "cce-lter_euphausiids", list(
      taxon_id = eu$taxon_id, scientific_name = eu$scientific_name,
      genus    = eu$genus))
  }

  # --- mesopelagic fish: taxa named by scientific name in the source columns --
  # ds_taxa_code IS the scientific name here (the source has no local code), so
  # obs joins dataset_taxon on the name it stores on the measurement row. That is
  # exactly why the " sp." suffix must NOT be cleaned out of the code: six taxa
  # arrive as the verbatim spreadsheet header `Bathophilus sp.`, and rewriting
  # the code would orphan their observations. The lookup name is cleaned instead
  # (clean_taxon_name(), via the xref name fallback), or an override supplies the id.
  mf <- .read_cols(con, "mesopelagic_fish_taxon",
    c("scientific_name", "worms_id", "rank"))
  if (!is.null(mf) && arm_on("sio_mesopelagic-fish")) {
    r <- .as_taxon_rows(data.frame(
      dataset_key = "sio_mesopelagic-fish", ds_prefix = "sio_mesopelagic-fish",
      ds_taxa_code = mf$scientific_name, ds_scientific_name = mf$scientific_name,
      worms_id = mf$worms_id, scientific_name = mf$scientific_name,
      rank = mf$rank, stringsAsFactors = FALSE))
    arms$mesopelagic <- .apply_overrides(r, overrides, "sio_mesopelagic-fish", list(
      scientific_name = mf$scientific_name))
  }

  # --- seabirds + marine mammals (the unstaged form) --------------------------
  # The source's is_bird / is_mammal / is_unidentified flags are read ONLY for
  # the coarse "unidentified" fallbacks the source itself encodes; they never
  # decide the key authority (that is the class from the lineage, D2). When the
  # dataset stages (Phase 2), those fallbacks become taxon_override.csv rows and
  # this arm goes unread.
  bm <- .read_cols(con, "bird_mammal_species",
    c("species_code", "common_name", "scientific_name", "itis_id",
      "is_bird", "is_mammal", "is_unidentified", "include_flag"))
  if (!is.null(bm) && arm_on("farallon_bird-mammal")) {
    if (!is.null(bm$include_flag)) bm <- bm[isTRUE_vec(bm$include_flag), , drop = FALSE]
    r <- .as_taxon_rows(data.frame(
      dataset_key = "farallon_bird-mammal", ds_prefix = "farallon_bird-mammal",
      ds_taxa_code = bm$species_code, ds_scientific_name = bm$scientific_name,
      ds_common_name = bm$common_name, itis_id = bm$itis_id,
      scientific_name = bm$scientific_name, common_name = bm$common_name,
      stringsAsFactors = FALSE))
    # mammals: resolve worms_id from overrides keyed by species_code. Birds go
    # through the same call — a bird override supplying an id is applied too.
    r <- .apply_overrides(r, overrides, "farallon_bird-mammal", list(
      species_code    = bm$species_code,
      scientific_name = bm$scientific_name,
      common_name     = bm$common_name))
    # coarse fallbacks for unidentified: bird -> Aves (itis 174371), mammal -> Mammalia (worms 1837)
    unid <- isTRUE_vec(bm$is_unidentified)
    r$itis_id[unid & isTRUE_vec(bm$is_bird)]    <- 174371L
    r$worms_id[unid & isTRUE_vec(bm$is_mammal)] <- 1837L
    arms$bird_mammal <- r
  }

  # --- composite measurement types (cufes / phyllosoma / crab) ---------------
  if (!is.null(measurement_taxon) && nrow(measurement_taxon)) {
    mt <- measurement_taxon
    mt <- mt[!is.na(mt$worms_id) | !is.na(mt$itis_id), , drop = FALSE]
    mt <- mt[!mt$dataset_key %in% staged_keys, , drop = FALSE]
    # one taxon per (dataset_key, resolved id)
    mt$k <- ifelse(!is.na(mt$worms_id), paste0("w", mt$worms_id), paste0("i", mt$itis_id))
    mt <- mt[!duplicated(paste(mt$dataset_key, mt$k)), , drop = FALSE]
    if (nrow(mt)) arms$measurement <- .as_taxon_rows(data.frame(
      dataset_key = mt$dataset_key, ds_prefix = mt$dataset_key,
      ds_taxa_code = tolower(gsub("[^A-Za-z0-9]+", "_", mt$taxon_scientific_name)),
      ds_scientific_name = mt$taxon_scientific_name, worms_id = mt$worms_id,
      itis_id = mt$itis_id, scientific_name = mt$taxon_scientific_name,
      stringsAsFactors = FALSE))
  }

  if (!length(arms)) stop(".taxon_norm_sources(): no taxon vocabulary found — ",
                          "append_dataset_taxon() first, or load a source table.")
  rows <- dplyr::bind_rows(arms)

  # the authority cross-reference, staged by ensure_taxon_xref(). Applied HERE,
  # before the key is minted, because it can change the id the key is built from
  # (a deprecated ITIS TSN is replaced by its accepted form). `xref = NA` means
  # "read whatever is staged"; an explicit NULL suppresses it, which is how
  # ensure_taxon_xref() reads the vocabulary without being self-referential.
  if (identical(xref, NA))
    xref <- .read_cols(con, "_taxon_xref", .xref_cache_cols)
  rows <- .apply_xref(rows, xref)

  # the class, from the staged lineage (D2) — the fact the key rule reads
  rows$class <- .class_from_flat(con, rows)

  # global taxon_key + dataset-local fallback where no authority id resolves
  rows$taxon_key <- taxon_key_of(rows$worms_id, rows$itis_id, rows$class)
  # a bird with no accepted TSN keys worms: — say so, in the append-only notes
  aves_no_tsn <- !is.na(rows$class) & rows$class == "Aves" &
                 is.na(rows$itis_id) & !is.na(rows$worms_id)
  if (any(aves_no_tsn)) {
    stamp <- dplyr::coalesce(rows$status_checked[aves_no_tsn], format(Sys.Date()))
    line  <- sprintf("%s: class Aves but no accepted TSN resolved; keyed worms:%d",
                     stamp, rows$worms_id[aves_no_tsn])
    rows$notes[aves_no_tsn] <- as.character(mapply(
      .append_note, rows$notes[aves_no_tsn], line, USE.NAMES = FALSE))
  }
  local_fb <- is.na(rows$taxon_key)
  rows$taxon_key[local_fb] <- paste0(rows$dataset_key[local_fb], ":", rows$ds_taxa_code[local_fb])
  # ds_taxon_key = "<prefix>:<local code>"
  rows$ds_taxon_key <- paste0(rows$ds_prefix, ":", rows$ds_taxa_code)
  rows
}

# resolve_dataset_taxon --------------------------------------------------------

#' Fill `taxon_key` on the `dataset_taxon` crosswalk (per-dataset vocabulary -> `taxon`)
#'
#' One row per (dataset, local taxon): the dataset's own `ds_taxon_key`
#' (`"<dataset-or-known-list>:<local id>"`, all lowercase — e.g. `calcofi:19`
#' for the shared CalCOFI species list, `cce-lter_zoodb:3` otherwise), its
#' `ds_scientific_name` / `ds_common_name` / `ds_taxa_code`, the source's own
#' claims as `ds_source_json`, and the global `taxon_key` it resolves to.
#' Deduped on `ds_taxon_key`.
#'
#' Rows staged by [append_dataset_taxon()] are **filled in place**: every column
#' but `taxon_key` comes back byte-identical, so a re-run over unchanged inputs
#' is a no-op. Datasets that have not staged are still read from their source
#' tables (the seven arms — coexistence during the migration). The key is minted
#' by [taxon_key_of()] from the resolved ids and the class the staged lineage
#' supplies, so call [ensure_taxon_xref()] then [ensure_taxon_lineage()] first.
#'
#' Renamed from `build_dataset_taxon()` in 3.29.0, which remains as a deprecated
#' alias: that name described a rebuild from the arms, which is exactly what an
#' ingest could not stage against.
#'
#' @param con a DuckDB connection with the staged vocabulary (and/or the
#'   per-dataset taxon tables) loaded
#' @param measurement_taxon optional data.frame of the composite-type crosswalk
#'   (`metadata/measurement_taxon.csv`) so cufes/phyllosoma/crab taxa,
#'   which live in `measurement_type` names not a taxon table, are included
#' @param overrides optional data.frame of manual id resolution
#'   (`metadata/taxon_override.csv`) for coarse taxa (phyto groups, mammals)
#' @param tbl target table name (default `"dataset_taxon"`)
#' @return (invisibly) the row count written
#' @export
#' @concept taxonomy
resolve_dataset_taxon <- function(con, measurement_taxon = NULL, overrides = NULL,
                                  tbl = "dataset_taxon") {
  rows <- .taxon_norm_sources(con, measurement_taxon, overrides)
  out <- rows |>
    dplyr::transmute(
      ds_taxon_key = .data$ds_taxon_key, dataset_key = .data$dataset_key,
      taxon_key = .data$taxon_key, ds_scientific_name = .data$ds_scientific_name,
      ds_common_name = .data$ds_common_name, ds_taxa_code = .data$ds_taxa_code,
      ds_source_json = .data$ds_source_json) |>
    dplyr::distinct(.data$ds_taxon_key, .keep_all = TRUE) |>
    dplyr::arrange(.data$dataset_key, .data$ds_taxon_key) |>
    as.data.frame()
  .replace_table(con, tbl, out)
}

#' @rdname resolve_dataset_taxon
#' @export
build_dataset_taxon <- function(con, measurement_taxon = NULL, overrides = NULL,
                                tbl = "dataset_taxon") {
  lifecycle::deprecate_warn("3.29.0", "build_dataset_taxon()", "resolve_dataset_taxon()")
  resolve_dataset_taxon(con, measurement_taxon, overrides, tbl)
}

# build_taxon_reference --------------------------------------------------------

#' Build the unified `taxon` reference table
#'
#' Assembles one authoritative row per distinct `taxon_key` across every dataset's
#' local taxa **plus the WoRMS/ITIS lineage ancestors** (from the pre-built `taxon`
#' hierarchy table, so `parent_taxon_key` chains resolve for descendant
#' expansion). Duplicate taxa across datasets collapse — e.g. Appendicularia
#' (AphiaID 146421) in both `zoodb_taxon` and `zooscan_taxon` becomes one
#' `worms:146421` row. Names/rank/lineage are coalesced by **source kind**, not
#' by dataset: the flattened classification (the authority) first, then the
#' hierarchy, then the vocabularies in `dataset_key` order. There is no list of
#' datasets to maintain. `rank_order` folds in the old `taxa_rank` lookup.
#'
#' `common_name` in this shard is the dataset's own; the release applies the
#' written precedence centrally with [apply_taxon_common()].
#'
#' @inheritParams resolve_dataset_taxon
#' @param tbl target table name (default `"taxon"`)
#' @return (invisibly) the row count written
#' @export
#' @concept taxonomy
build_taxon_reference <- function(con, measurement_taxon = NULL, overrides = NULL,
                                  tbl = "taxon") {
  # 1. dataset-local taxa (read BEFORE we overwrite `taxon`)
  rows <- .taxon_norm_sources(con, measurement_taxon, overrides)
  rows$.src <- 2L

  # 2. WoRMS/ITIS lineage ancestors from the `taxon` hierarchy (the authority).
  # Built either by build_taxon_hierarchy() (swfsc_ichthyo, from a local species
  # DB) or by ensure_taxon_lineage() (every other dataset, from the cached WoRMS
  # classification). Without one of those, a taxon reaches the release with a key
  # and a name and NOTHING else — no rank, no parent, no classification — and
  # "all Decapoda" silently matches nothing.
  hier <- .read_cols(con, "taxon",
    c("taxonID", "parentNameUsageID", "scientificName", "taxonRank",
      "taxonomicStatus", "authority"))
  if (!is.null(hier)) {
    # an ITIS-authority row keys itis:<tsn>, a WoRMS one worms:<aphia> — the
    # chain it was fetched in, so the lineage joins the vocabulary it came from
    is_itis <- !is.na(hier$authority) & hier$authority == "ITIS"
    hrows <- .as_taxon_rows(data.frame(
      dataset_key = NA_character_, ds_source_json = NA_character_,
      worms_id = ifelse(is_itis, NA_integer_, hier$taxonID),
      itis_id  = ifelse(is_itis, hier$taxonID, NA_integer_),
      scientific_name = hier$scientificName, rank = hier$taxonRank,
      taxonomic_status = hier$taxonomicStatus,
      parent_worms_id = ifelse(is_itis, NA_integer_, hier$parentNameUsageID),
      parent_taxon_key = .key_of_authority(hier$authority, hier$parentNameUsageID),
      stringsAsFactors = FALSE))
    hrows$taxon_key <- .key_of_authority(hier$authority, hier$taxonID)
    # cross-reference the ANCESTORS too. These rows come from the hierarchy, not
    # from .taxon_norm_sources(), so .apply_xref() never reached them — which is
    # why 657 of 732 ancestor rows released with no itis_id and 198 with no
    # taxonomic_status. Applied AFTER taxon_key is minted, deliberately: an
    # ancestor's key comes from the chain it was fetched in, and re-keying it
    # here would break the parent links that chain just established.
    hrows <- .apply_xref(hrows, .read_cols(con, "_taxon_xref", .xref_cache_cols),
                         rekey = FALSE)
    hrows$.src <- 1L
    rows <- dplyr::bind_rows(rows, hrows[!is.na(hrows$taxon_key), ])
  }

  # 2b. the flattened classification staged by ensure_taxon_lineage(): kingdom /
  # phylum / class / order / family per taxon, from its own chain. Highest
  # priority, because it IS the authority — and it is the only source that ever
  # populated `family` (no dataset did, not even ichthyo).
  flat <- .read_cols(con, "_taxon_lineage_flat", c(
    "requested_id", "authority", "rank", "parent_id", "scientific_name",
    "kingdom", "phylum", "class", "order_taxon", "family"))
  if (!is.null(flat) && nrow(flat)) {
    f_itis <- !is.na(flat$authority) & flat$authority == "ITIS"
    frows <- .as_taxon_rows(data.frame(
      dataset_key = NA_character_, ds_source_json = NA_character_,
      worms_id = ifelse(f_itis, NA_integer_, flat$requested_id),
      itis_id  = ifelse(f_itis, flat$requested_id, NA_integer_),
      scientific_name = flat$scientific_name, rank = flat$rank,
      parent_worms_id = ifelse(f_itis, NA_integer_, flat$parent_id),
      parent_taxon_key = .key_of_authority(flat$authority, flat$parent_id),
      kingdom = flat$kingdom, phylum = flat$phylum, class = flat$class,
      order_taxon = flat$order_taxon, family = flat$family,
      stringsAsFactors = FALSE))
    frows$taxon_key <- .key_of_authority(flat$authority, flat$requested_id)
    frows <- .apply_xref(frows, .read_cols(con, "_taxon_xref", .xref_cache_cols),
                         rekey = FALSE)
    frows$.src <- 0L
    rows <- dplyr::bind_rows(rows, frows[!is.na(frows$taxon_key), ])
  }

  # 3. rank ordering. The connection's own `taxa_rank` wins where it has an
  # answer; the package reference fills the rest. Previously this was ONLY the
  # connection's table, which exists in the swfsc_ichthyo connection and nowhere
  # else — so every other dataset's taxa, including all 169 ITIS-keyed ones,
  # released with rank_order NULL. See taxa_rank_reference().
  rank_ord <- .read_cols(con, "taxa_rank", c("taxonRank", "rank_order"))
  rank_ref <- taxa_rank_reference()
  rank_ord <- if (is.null(rank_ord) || !nrow(rank_ord)) rank_ref else {
    ro <- rank_ord[!is.na(rank_ord$taxonRank) & !is.na(rank_ord$rank_order), , drop = FALSE]
    # one row per rank, or the left join below fans out (a rank carrying both an
    # order and a NULL would double every taxon of that rank)
    ro <- ro[!duplicated(ro$taxonRank), , drop = FALSE]
    rbind(ro, rank_ref[!rank_ref$taxonRank %in% ro$taxonRank, , drop = FALSE])
  }

  # a deterministic order with no dataset list: source kind, then dataset_key,
  # then the local key. first_nn() below relies on order() being stable.
  rows <- rows[order(rows$.src, rows$dataset_key, rows$ds_taxon_key,
                     na.last = TRUE), , drop = FALSE]
  rows$.ord <- seq_len(nrow(rows))

  first_nn <- function(x, p) { o <- order(p); x <- x[o]; x <- x[!is.na(x)]; if (length(x)) x[1] else NA }
  # `notes` is append-only, so a taxon two datasets both reach keeps BOTH
  # provenance lines rather than the higher-priority one silently winning
  union_notes <- function(x, p) {
    x <- x[order(p)]; x <- x[!is.na(x) & nzchar(x)]
    if (!length(x)) return(NA_character_)
    paste(unique(unlist(strsplit(x, "\n", fixed = TRUE))), collapse = "\n")
  }

  taxon <- rows |>
    dplyr::filter(!is.na(.data$taxon_key)) |>
    dplyr::group_by(.data$taxon_key) |>
    dplyr::summarise(
      worms_id         = first_nn(.data$worms_id,         .data$.ord),
      itis_id          = first_nn(.data$itis_id,          .data$.ord),
      gbif_id          = first_nn(.data$gbif_id,          .data$.ord),
      scientific_name  = first_nn(.data$scientific_name,  .data$.ord),
      common_name      = first_nn(.data$common_name,      .data$.ord),
      rank             = first_nn(.data$rank,             .data$.ord),
      taxonomic_status = first_nn(.data$taxonomic_status, .data$.ord),
      status_checked   = first_nn(.data$status_checked,   .data$.ord),
      notes            = union_notes(.data$notes,         .data$.ord),
      parent_worms_id  = first_nn(.data$parent_worms_id,  .data$.ord),
      parent_taxon_key = first_nn(.data$parent_taxon_key, .data$.ord),
      kingdom          = first_nn(.data$kingdom,          .data$.ord),
      phylum           = first_nn(.data$phylum,           .data$.ord),
      class            = first_nn(.data$class,            .data$.ord),
      order_taxon      = first_nn(.data$order_taxon,      .data$.ord),
      family           = first_nn(.data$family,           .data$.ord),
      .groups = "drop") |>
    dplyr::mutate(
      # ncbi_id / inat_id: declared by the core model, populated by no source we
      # have. Kept as typed NULLs rather than dropped so the release schema does
      # not change under consumers the day a source does supply them.
      ncbi_id = NA_integer_, inat_id = NA_integer_,
      # a carried parent key wins; otherwise fall back to the WoRMS id, which is
      # what every source that supplies only `parent_worms_id` means
      parent_taxon_key = dplyr::coalesce(
        .data$parent_taxon_key,
        ifelse(is.na(.data$parent_worms_id), NA_character_,
               paste0("worms:", .data$parent_worms_id))))
  # rank_ord is never NULL now (the package reference is the floor)
  taxon <- dplyr::left_join(taxon, rank_ord, by = c("rank" = "taxonRank"))

  taxon <- taxon |>
    dplyr::select(
      "taxon_key", "worms_id", "itis_id", "gbif_id", "ncbi_id", "inat_id",
      "scientific_name", "common_name", "rank", "rank_order", "taxonomic_status",
      "status_checked", "parent_taxon_key", "kingdom", "phylum", "class",
      "order_taxon", "family", "notes") |>
    dplyr::arrange(.data$taxon_key) |>
    as.data.frame()
  .replace_table(con, tbl, taxon)
}

# build_taxon_group ------------------------------------------------------------

.group_rule_kinds <- c("class", "dataset_taxon")
.group_rule_cols  <- c("taxon_group_key", "description", "rule", "rule_value",
                       "dataset_key", "match_column", "match_value")
.group_match_cols <- c("ds_taxa_code", "ds_scientific_name", "ds_common_name")

.validate_group_rules <- function(d, where = "taxon_group rules") {
  if (!is.data.frame(d)) stop(where, ": not a data.frame.")
  miss <- setdiff(.group_rule_cols, names(d))
  if (length(miss)) stop(glue::glue(
    "{where}: missing column(s) {paste(sprintf('`%s`', miss), collapse = ', ')}."))
  d <- d[, .group_rule_cols, drop = FALSE]
  for (cl in names(d)) {
    d[[cl]] <- as.character(d[[cl]])
    d[[cl]][!is.na(d[[cl]]) & !nzchar(trimws(d[[cl]]))] <- NA_character_
  }
  if (any(d == "NA", na.rm = TRUE)) stop(glue::glue(
    "{where}: a cell holds the literal string \"NA\" — write the registry with ",
    "`na = \"\"` (see the round-trip trap in the workflows CLAUDE.md)."))
  if (anyNA(d$taxon_group_key)) stop(glue::glue("{where}: empty `taxon_group_key`."))
  bad <- setdiff(unique(d$rule), .group_rule_kinds)
  if (length(bad) || anyNA(d$rule)) stop(glue::glue(
    "{where}: unknown rule kind(s) {paste(sprintf('`%s`', c(bad, if (anyNA(d$rule)) 'NA')), collapse = ', ')}; ",
    "must be one of {paste(sprintf('`%s`', .group_rule_kinds), collapse = ', ')}."))
  cl <- d$rule == "class"
  if (any(cl & is.na(d$rule_value))) stop(glue::glue(
    "{where}: a `class` rule needs `rule_value` (the class name) — ",
    "{paste(d$taxon_group_key[cl & is.na(d$rule_value)], collapse = ', ')}."))
  dt <- d$rule == "dataset_taxon"
  for (need in c("dataset_key", "match_column", "match_value")) {
    bad <- dt & is.na(d[[need]])
    if (any(bad)) stop(glue::glue(
      "{where}: a `dataset_taxon` rule needs `{need}` — ",
      "{paste(d$taxon_group_key[bad], collapse = ', ')}."))
  }
  badc <- dt & !d$match_column %in% .group_match_cols
  if (any(badc)) stop(glue::glue(
    "{where}: `match_column` {paste(sprintf('`%s`', unique(d$match_column[badc])), collapse = ', ')} ",
    "is not a dataset_taxon column; use one of ",
    "{paste(sprintf('`%s`', .group_match_cols), collapse = ', ')}."))
  d
}

#' Read the `taxon_group` rule registry (`metadata/taxon_group.csv`)
#'
#' Strict read — every column character, an empty cell is `NA` (never the string
#' `"NA"`), and the shape is validated: `rule` is `class` (every vocabulary taxon
#' whose `class` equals `rule_value`, cross-dataset by construction) or
#' `dataset_taxon` (rows of one dataset's vocabulary matched on `match_column`
#' ∈ `ds_taxa_code` / `ds_scientific_name` / `ds_common_name` = `match_value`).
#'
#' @param path path to the registry CSV
#' @return a data.frame of rules (columns `taxon_group_key`, `description`,
#'   `rule`, `rule_value`, `dataset_key`, `match_column`, `match_value`)
#' @export
#' @concept taxonomy
read_taxon_group_rules <- function(path) {
  if (is.null(path) || !file.exists(path))
    stop("taxon_group registry not found: ", path)
  d <- utils::read.csv(path, colClasses = "character", na.strings = "",
                       check.names = FALSE)
  .validate_group_rules(d, where = basename(path))
}

#' Build the `taxon_group` grouping table (many taxa per group) from the registry
#'
#' Groups come from `metadata/taxon_group.csv` (taxon plan D4), not from code:
#'
#' - **`class`** — every vocabulary taxon whose released `class` equals
#'   `rule_value`: `calcofi:seabirds` = Aves, `calcofi:marine_mammals` =
#'   Mammalia. Cross-dataset by construction; no dataset column. Scoped to taxa
#'   some dataset actually observes (present in `dataset_taxon`), never to a bare
#'   lineage ancestor — a group selects observed taxa, and the ancestors are
#'   reachable through `parent_taxon_key` anyway.
#' - **`dataset_taxon`** — by `(dataset_key, match_column, match_value)` against
#'   `dataset_taxon`, the same matcher `taxon_override.csv` uses: the
#'   phytoplankton functional groups on `ds_common_name`.
#'
#' A rule naming a `match_column` the vocabulary lacks errors; a rule for a
#' dataset absent from this connection is skipped (it is another ingest's), and
#' [check_taxon_registries()] catches one nobody supplies at the release. Needs
#' `taxon` and `dataset_taxon` in `con`, i.e. call it after
#' [build_taxon_reference()] and [resolve_dataset_taxon()].
#'
#' @param con a DuckDB connection holding `taxon` and `dataset_taxon`
#' @param rules the registry as read by [read_taxon_group_rules()], or a path to
#'   it. `NULL` looks for `metadata/taxon_group.csv` under `here::here()`.
#' @param tbl target table name (default `"taxon_group"`)
#' @param ... the pre-3.29 `measurement_taxon` / `overrides` arguments, accepted
#'   with a deprecation warning and ignored (the groups are not derived from
#'   them any more)
#' @return (invisibly) the row count written
#' @export
#' @concept taxonomy
build_taxon_group <- function(con, rules = NULL, tbl = "taxon_group", ...) {
  dots <- list(...)
  legacy <- length(dots) > 0L || (is.data.frame(rules) && !"rule" %in% names(rules))
  if (legacy) {
    lifecycle::deprecate_warn(
      "3.29.0", "build_taxon_group(measurement_taxon = )", "build_taxon_group(rules = )",
      details = "Groups come from metadata/taxon_group.csv now (taxon plan D4).")
    rules <- NULL
  }
  if (is.null(rules)) {
    p <- tryCatch(here::here("metadata", "taxon_group.csv"), error = function(e) "")
    if (!nzchar(p) || !file.exists(p)) stop(
      "build_taxon_group(): pass `rules = read_taxon_group_rules(here('metadata/taxon_group.csv'))` ",
      "— the groups live in that registry, not in the package.", call. = FALSE)
    rules <- read_taxon_group_rules(p)
  }
  if (is.character(rules)) rules <- read_taxon_group_rules(rules)
  rules <- .validate_group_rules(rules)

  present <- DBI::dbListTables(con)
  if (!all(c("taxon", "dataset_taxon") %in% present))
    stop("build_taxon_group(): needs `taxon` and `dataset_taxon` in `con` — call ",
         "build_taxon_reference() and resolve_dataset_taxon() first.")

  grp <- list()
  for (k in seq_len(nrow(rules))) {
    r <- rules[k, ]
    keys <- if (r$rule == "class") {
      DBI::dbGetQuery(con, "
        SELECT DISTINCT t.taxon_key FROM taxon t
        WHERE t.\"class\" = ? AND t.taxon_key IN (
          SELECT taxon_key FROM dataset_taxon WHERE taxon_key IS NOT NULL)",
        params = list(r$rule_value))$taxon_key
    } else {
      DBI::dbGetQuery(con, glue::glue("
        SELECT DISTINCT taxon_key FROM dataset_taxon
        WHERE dataset_key = ? AND \"{r$match_column}\" = ? AND taxon_key IS NOT NULL"),
        params = list(r$dataset_key, r$match_value))$taxon_key
    }
    if (!length(keys)) next
    grp[[k]] <- data.frame(
      taxon_group_key = r$taxon_group_key,
      description = if (is.na(r$description)) NA_character_ else r$description,
      taxon_key = keys, stringsAsFactors = FALSE)
  }

  if (!length(grp)) { .replace_table(con, tbl, .empty_group()); return(invisible(0L)) }
  out <- dplyr::bind_rows(grp) |>
    dplyr::filter(!is.na(.data$taxon_key)) |>
    dplyr::distinct(.data$taxon_group_key, .data$taxon_key, .keep_all = TRUE) |>
    dplyr::arrange(.data$taxon_group_key, .data$taxon_key) |>
    as.data.frame()
  .replace_table(con, tbl, out)
}

.empty_group <- function()
  data.frame(taxon_group_key = character(), description = character(),
             taxon_key = character(), stringsAsFactors = FALSE)

# prune_taxon_shard ------------------------------------------------------------

#' Prune the taxa references to one dataset's shard
#'
#' `build_taxon_reference()` / `resolve_dataset_taxon()` / `build_taxon_group()`
#' read *whichever* taxon sources are present in `con`, which inside an ingest is
#' normally just that dataset's vocabulary — but not always. An ingest may have
#' loaded another dataset's tables as references, and `swfsc_ichthyo` holds a
#' WoRMS **lineage hierarchy** (`build_taxon_hierarchy()`) that is broader than
#' the taxa its own observations reach. This trims all three to this dataset's
#' shard so the release union stays small and the shards stay disjoint-ish.
#'
#' Lineage **ancestors are kept**: descendant expansion walks
#' `parent_taxon_key`, so dropping an ancestor would break the chain. The kept
#' set is therefore the transitive parent closure of `dataset_taxon.taxon_key`,
#' not just the directly-referenced taxa.
#'
#' Generic — there is nothing dataset-specific here beyond the `dataset_key`
#' argument. Call it after the three builders in an ingest that either holds a
#' hierarchy table or has other datasets' vocabulary tables in scope.
#'
#' @param con a DuckDB connection holding `taxon` / `dataset_taxon` (and
#'   optionally `taxon_group`) as built by the three builders
#' @param dataset_key provider_dataset to keep
#' @return (invisibly) a named list of the surviving row counts
#' @export
#' @concept taxonomy
prune_taxon_shard <- function(con, dataset_key) {
  present <- DBI::dbListTables(con)
  if (!all(c("taxon", "dataset_taxon") %in% present))
    stop("prune_taxon_shard(): needs `taxon` and `dataset_taxon` in `con`.")

  DBI::dbExecute(con, glue::glue(
    "DELETE FROM dataset_taxon WHERE dataset_key <> '{dataset_key}'"))
  DBI::dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE _tx_keep AS
    WITH RECURSIVE seed AS (
      SELECT taxon_key FROM dataset_taxon WHERE taxon_key IS NOT NULL
    ), chain AS (
      SELECT taxon_key FROM seed
      UNION
      SELECT t.parent_taxon_key FROM taxon t JOIN chain c ON t.taxon_key = c.taxon_key
      WHERE t.parent_taxon_key IS NOT NULL
    ) SELECT DISTINCT taxon_key FROM chain WHERE taxon_key IS NOT NULL")
  DBI::dbExecute(con,
    "DELETE FROM taxon WHERE taxon_key NOT IN (SELECT taxon_key FROM _tx_keep)")
  if ("taxon_group" %in% present)
    DBI::dbExecute(con,
      "DELETE FROM taxon_group WHERE taxon_key NOT IN (SELECT taxon_key FROM taxon)")
  DBI::dbExecute(con, "DROP TABLE IF EXISTS _tx_keep")

  out <- list()
  for (t in intersect(c("taxon", "dataset_taxon", "taxon_group"), present))
    out[[t]] <- DBI::dbGetQuery(
      con, glue::glue("SELECT COUNT(*) AS n FROM {t}"))$n
  invisible(out)
}
