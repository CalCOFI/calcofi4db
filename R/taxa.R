# unified taxon model ----------------------------------------------------------
# Collapses the ~7 per-dataset taxon tables (species, taxon-hierarchy,
# phyto_taxon, zoodb_taxon, zooscan_taxon, bird_mammal_species) into three shared
# references that every consumer reads:
#   - taxon         : one authoritative row per taxon, keyed by a lowercase
#                     authority-prefixed `taxon_key` ("worms:<id>" for all taxa,
#                     "itis:<id>" for birds; dataset-local "<dataset>:<code>" only
#                     where no authority id resolves)
#   - dataset_taxon : crosswalk from each dataset's local vocabulary to `taxon`
#                     (ds_taxon_key + the dataset's own name/code + taxon_key)
#   - taxon_group   : named groupings (many taxon_key per taxon_group_key)
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
#' The single rule for minting the global taxon key: **`worms:<worms_id>` for all
#' taxa, except birds (`is_bird = TRUE`, i.e. class Aves) which key on
#' `itis:<itis_id>`**. Falls back to `itis:` when no `worms_id` is present, and to
#' `NA` when neither id resolves (callers then apply a dataset-local key). All
#' prefixes are lowercase. Vectorized.
#'
#' @param worms_id integer WoRMS AphiaID(s) (NA where unknown)
#' @param itis_id integer ITIS TSN(s) (NA where unknown)
#' @param is_bird logical; TRUE for class Aves, forcing the `itis:` prefix
#' @return character vector of `taxon_key`s (NA where neither id resolves)
#' @export
#' @concept taxonomy
#' @examples
#' taxon_key_of(217452L, 161729L)              # "worms:217452"  (Pacific sardine)
#' taxon_key_of(137179L, 174715L, is_bird = TRUE)  # "itis:174715"  (Great Cormorant)
taxon_key_of <- function(worms_id, itis_id = NA_integer_, is_bird = FALSE) {
  n <- max(length(worms_id), length(itis_id), length(is_bird))
  worms_id <- rep(worms_id, length.out = n)
  itis_id  <- rep(itis_id,  length.out = n)
  is_bird  <- rep(is_bird,  length.out = n)
  wi <- suppressWarnings(as.integer(worms_id))
  ii <- suppressWarnings(as.integer(itis_id))
  out <- rep(NA_character_, n)
  # birds -> itis: when a TSN is present
  bird <- isTRUE_vec(is_bird) & !is.na(ii)
  out[bird] <- paste0("itis:", ii[bird])
  # everything else -> worms: when an AphiaID is present
  w <- is.na(out) & !is.na(wi)
  out[w] <- paste0("worms:", wi[w])
  # last resort -> itis:
  i <- is.na(out) & !is.na(ii)
  out[i] <- paste0("itis:", ii[i])
  out
}

# TRUE only for scalar/logical TRUE, treating NA as FALSE (vectorized)
isTRUE_vec <- function(x) !is.na(x) & as.logical(x)

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

# the common normalized shape one row per (dataset, local taxon)
.taxon_row_template <- function(n = 0L) {
  tibble::tibble(
    dataset_key      = character(n), ds_prefix = character(n),
    ds_taxa_code     = character(n),
    ds_scientific_name = character(n), ds_common_name = character(n),
    worms_id = integer(n), itis_id = integer(n), gbif_id = integer(n),
    scientific_name = character(n), common_name = character(n),
    rank = character(n), taxonomic_status = character(n),
    parent_worms_id = integer(n),
    kingdom = character(n), phylum = character(n), class = character(n),
    order_taxon = character(n), family = character(n),
    is_bird = logical(n))
}

# coerce a per-source frame to the template (missing cols -> NA of right type)
.as_taxon_rows <- function(df) {
  tmpl <- .taxon_row_template(0L)
  for (c in names(tmpl)) if (is.null(df[[c]])) df[[c]] <- rep(tmpl[[c]][NA_integer_], nrow(df))
  df <- df[, names(tmpl), drop = FALSE]
  df$worms_id        <- suppressWarnings(as.integer(df$worms_id))
  df$itis_id         <- suppressWarnings(as.integer(df$itis_id))
  df$gbif_id         <- suppressWarnings(as.integer(df$gbif_id))
  df$parent_worms_id <- suppressWarnings(as.integer(df$parent_worms_id))
  df$is_bird         <- isTRUE_vec(df$is_bird)
  df$ds_taxa_code    <- as.character(df$ds_taxa_code)
  tibble::as_tibble(df)
}

# apply an overrides frame (dataset_key, match_column, match_value, worms_id,
# itis_id, scientific_name, rank) to a per-source normalized frame in place,
# filling worms_id/itis_id/scientific_name/rank where `match_values` (a vector
# aligned with rows) hits ov$match_value. Overrides take precedence over the
# source-supplied id (they exist because the source id is missing or coarse).
.apply_overrides <- function(rows, overrides, dataset_key, match_values) {
  if (is.null(overrides) || !nrow(overrides)) return(rows)
  ov <- overrides[overrides$dataset_key == dataset_key, , drop = FALSE]
  if (!nrow(ov)) return(rows)
  m   <- match(as.character(match_values), as.character(ov$match_value))
  hit <- !is.na(m)
  rows$worms_id[hit]        <- dplyr::coalesce(suppressWarnings(as.integer(ov$worms_id[m[hit]])), rows$worms_id[hit])
  rows$itis_id[hit]         <- dplyr::coalesce(suppressWarnings(as.integer(ov$itis_id[m[hit]])), rows$itis_id[hit])
  if (!is.null(ov$scientific_name))
    rows$scientific_name[hit] <- dplyr::coalesce(ov$scientific_name[m[hit]], rows$scientific_name[hit])
  if (!is.null(ov$rank))
    rows$rank[hit] <- dplyr::coalesce(ov$rank[m[hit]], rows$rank[hit])
  rows
}

# gather every dataset's local taxa into one normalized frame (resolved ids +
# taxon_key + ds_taxon_key), from whichever source tables + registries exist.
.taxon_norm_sources <- function(con, measurement_taxon = NULL, overrides = NULL) {
  arms <- list()

  # --- CalCOFI species list (ichthyo + invert): worms/itis/gbif present -------
  sp <- .read_cols(con, "species",
    c("species_id", "scientific_name", "common_name", "worms_id", "itis_id", "gbif_id"))
  # dataset_key = the using dataset (swfsc_ichthyo, incl. folded invert) so obs
  # joins on (dataset_key, ds_taxa_code); ds_prefix = the known "calcofi" list.
  if (!is.null(sp)) arms$species <- .as_taxon_rows(data.frame(
    dataset_key = "swfsc_ichthyo", ds_prefix = "calcofi",
    ds_taxa_code = sp$species_id, ds_scientific_name = sp$scientific_name,
    ds_common_name = sp$common_name, worms_id = sp$worms_id, itis_id = sp$itis_id,
    gbif_id = sp$gbif_id, scientific_name = sp$scientific_name,
    common_name = sp$common_name, is_bird = FALSE, stringsAsFactors = FALSE))

  # --- phytoplankton: aphia_id, coarse groups via overrides (match on `taxa`) -
  ph <- .read_cols(con, "phyto_taxon",
    c("species_code", "taxa", "aphia_id", "scientific_name_accepted", "rank", "kingdom", "phylum"))
  if (!is.null(ph)) {
    r <- .as_taxon_rows(data.frame(
      dataset_key = "calcofi_phytoplankton", ds_prefix = "calcofi_phytoplankton",
      ds_taxa_code = ph$species_code, ds_scientific_name = ph$scientific_name_accepted,
      ds_common_name = ph$taxa, worms_id = ph$aphia_id,
      scientific_name = ph$scientific_name_accepted, rank = ph$rank,
      kingdom = ph$kingdom, phylum = ph$phylum, is_bird = FALSE,
      stringsAsFactors = FALSE))
    # coarse functional groups (NULL aphia_id) resolve via overrides keyed on `taxa`
    arms$phyto <- .apply_overrides(r, overrides, "calcofi_phytoplankton", ph$taxa)
  }

  # --- zoodb / zooscan: aphia_id + denormalized lineage -----------------------
  for (nm in c("zoodb", "zooscan")) {
    tbl <- paste0(nm, "_taxon"); ds <- if (nm == "zoodb") "cce-lter_zoodb" else "cce-lter_zooscan"
    lbl <- paste0("taxon_", nm)
    z <- .read_cols(con, tbl,
      c("taxon_id", lbl, "aphia_id", "scientific_name", "rank",
        "kingdom", "class", "order_taxon", "family"))
    if (!is.null(z)) arms[[nm]] <- .as_taxon_rows(data.frame(
      dataset_key = ds, ds_prefix = ds, ds_taxa_code = z$taxon_id,
      ds_scientific_name = z$scientific_name, ds_common_name = z[[lbl]],
      worms_id = z$aphia_id, scientific_name = z$scientific_name, rank = z$rank,
      kingdom = z$kingdom, class = z$class, order_taxon = z$order_taxon,
      family = z$family, is_bird = FALSE, stringsAsFactors = FALSE))
  }

  # --- seabirds + marine mammals: birds -> itis, mammals -> worms (override) --
  bm <- .read_cols(con, "bird_mammal_species",
    c("species_code", "common_name", "scientific_name", "itis_id",
      "is_bird", "is_mammal", "is_unidentified", "include_flag"))
  if (!is.null(bm)) {
    if (!is.null(bm$include_flag)) bm <- bm[isTRUE_vec(bm$include_flag), , drop = FALSE]
    r <- .as_taxon_rows(data.frame(
      dataset_key = "calcofi_bird_mammal_census", ds_prefix = "calcofi_bird_mammal_census",
      ds_taxa_code = bm$species_code, ds_scientific_name = bm$scientific_name,
      ds_common_name = bm$common_name, itis_id = bm$itis_id,
      scientific_name = bm$scientific_name, common_name = bm$common_name,
      is_bird = isTRUE_vec(bm$is_bird), stringsAsFactors = FALSE))
    # mammals: resolve worms_id from overrides keyed by species_code
    r <- .apply_overrides(r, overrides, "calcofi_bird_mammal_census", bm$species_code)
    # coarse fallbacks for unidentified: bird -> Aves (itis 174371), mammal -> Mammalia (worms 1837)
    unid <- isTRUE_vec(bm$is_unidentified)
    r$itis_id[unid & isTRUE_vec(bm$is_bird)]   <- 174371L
    r$worms_id[unid & isTRUE_vec(bm$is_mammal)] <- 1837L
    arms$bird_mammal <- r
  }

  # --- composite measurement types (cufes / phyllosoma / euphausiids) ---------
  if (!is.null(measurement_taxon) && nrow(measurement_taxon)) {
    mt <- measurement_taxon
    mt <- mt[!is.na(mt$worms_id) | !is.na(mt$itis_id), , drop = FALSE]
    # one taxon per (dataset_key, resolved id)
    mt$k <- ifelse(!is.na(mt$worms_id), paste0("w", mt$worms_id), paste0("i", mt$itis_id))
    mt <- mt[!duplicated(paste(mt$dataset_key, mt$k)), , drop = FALSE]
    if (nrow(mt)) arms$measurement <- .as_taxon_rows(data.frame(
      dataset_key = mt$dataset_key, ds_prefix = mt$dataset_key,
      ds_taxa_code = tolower(gsub("[^A-Za-z0-9]+", "_", mt$taxon_scientific_name)),
      ds_scientific_name = mt$taxon_scientific_name, worms_id = mt$worms_id,
      itis_id = mt$itis_id, scientific_name = mt$taxon_scientific_name,
      is_bird = FALSE, stringsAsFactors = FALSE))
  }

  if (!length(arms)) stop(".taxon_norm_sources(): no taxon source tables found.")
  rows <- dplyr::bind_rows(arms)

  # global taxon_key + dataset-local fallback where no authority id resolves
  rows$taxon_key <- taxon_key_of(rows$worms_id, rows$itis_id, rows$is_bird)
  local_fb <- is.na(rows$taxon_key)
  rows$taxon_key[local_fb] <- paste0(rows$dataset_key[local_fb], ":", rows$ds_taxa_code[local_fb])
  # ds_taxon_key = "<prefix>:<local code>"
  rows$ds_taxon_key <- paste0(rows$ds_prefix, ":", rows$ds_taxa_code)
  rows
}

# build_dataset_taxon ----------------------------------------------------------

#' Build the `dataset_taxon` crosswalk (per-dataset vocabulary -> `taxon`)
#'
#' One row per (dataset, local taxon): the dataset's own `ds_taxon_key`
#' (`"<dataset-or-known-list>:<local id>"`, all lowercase — e.g. `calcofi:19`
#' for the shared CalCOFI species list, `cce-lter_zoodb:3` otherwise), its
#' `ds_scientific_name` / `ds_common_name` / `ds_taxa_code`, and the global
#' `taxon_key` it resolves to. Deduped on `ds_taxon_key`.
#'
#' @param con a DuckDB connection with the per-dataset taxon tables loaded
#' @param measurement_taxon optional data.frame of the composite-type crosswalk
#'   (`metadata/measurement_taxon.csv`) so cufes/phyllosoma/euphausiid taxa,
#'   which live in `measurement_type` names not a taxon table, are included
#' @param overrides optional data.frame of manual id resolution
#'   (`metadata/taxon_override.csv`) for coarse taxa (phyto groups, mammals)
#' @param tbl target table name (default `"dataset_taxon"`)
#' @return (invisibly) the row count written
#' @export
#' @concept taxonomy
build_dataset_taxon <- function(con, measurement_taxon = NULL, overrides = NULL,
                                tbl = "dataset_taxon") {
  rows <- .taxon_norm_sources(con, measurement_taxon, overrides)
  out <- rows |>
    dplyr::transmute(
      ds_taxon_key = .data$ds_taxon_key, dataset_key = .data$dataset_key,
      taxon_key = .data$taxon_key, ds_scientific_name = .data$ds_scientific_name,
      ds_common_name = .data$ds_common_name, ds_taxa_code = .data$ds_taxa_code) |>
    dplyr::distinct(.data$ds_taxon_key, .keep_all = TRUE) |>
    dplyr::arrange(.data$dataset_key, .data$ds_taxon_key) |>
    as.data.frame()
  .replace_table(con, tbl, out)
}

# build_taxon_reference --------------------------------------------------------

#' Build the unified `taxon` reference table
#'
#' Assembles one authoritative row per distinct `taxon_key` across every dataset's
#' local taxa **plus the WoRMS lineage ancestors** (from the pre-built `taxon`
#' hierarchy table, so `parent_taxon_key` chains resolve for descendant
#' expansion). Duplicate taxa across datasets collapse — e.g. Appendicularia
#' (AphiaID 146421) in both `zoodb_taxon` and `zooscan_taxon` becomes one
#' `worms:146421` row. Names/rank/lineage are coalesced with source priority
#' (WoRMS hierarchy > CalCOFI species / seabird-mammal > per-dataset lineage >
#' composite crosswalk). `rank_order` folds in the old `taxa_rank` lookup.
#'
#' @inheritParams build_dataset_taxon
#' @param tbl target table name (default `"taxon"`)
#' @return (invisibly) the row count written
#' @export
#' @concept taxonomy
build_taxon_reference <- function(con, measurement_taxon = NULL, overrides = NULL,
                                  tbl = "taxon") {
  # 1. dataset-local taxa (read BEFORE we overwrite `taxon`)
  rows <- .taxon_norm_sources(con, measurement_taxon, overrides)
  rows$.prio <- dplyr::case_when(
    rows$dataset_key %in% c("calcofi", "calcofi_bird_mammal_census") ~ 2L,
    rows$dataset_key %in% c("cce-lter_zoodb", "cce-lter_zooscan",
                            "calcofi_phytoplankton") ~ 3L, TRUE ~ 4L)

  # 2. WoRMS lineage ancestors from the existing `taxon` hierarchy (authority)
  hier <- .read_cols(con, "taxon",
    c("taxonID", "parentNameUsageID", "scientificName", "taxonRank", "taxonomicStatus"))
  if (!is.null(hier)) {
    hrows <- .as_taxon_rows(data.frame(
      dataset_key = NA_character_, worms_id = hier$taxonID,
      scientific_name = hier$scientificName, rank = hier$taxonRank,
      taxonomic_status = hier$taxonomicStatus, parent_worms_id = hier$parentNameUsageID,
      is_bird = FALSE, stringsAsFactors = FALSE))
    hrows$taxon_key <- taxon_key_of(hrows$worms_id, hrows$itis_id, FALSE)
    hrows$.prio <- 1L
    rows <- dplyr::bind_rows(rows, hrows[!is.na(hrows$taxon_key), ])
  }

  # 3. rank ordering (fold in taxa_rank)
  rank_ord <- .read_cols(con, "taxa_rank", c("taxonRank", "rank_order"))

  first_nn <- function(x, p) { o <- order(p); x <- x[o]; x <- x[!is.na(x)]; if (length(x)) x[1] else NA }

  taxon <- rows |>
    dplyr::filter(!is.na(.data$taxon_key)) |>
    dplyr::group_by(.data$taxon_key) |>
    dplyr::summarise(
      worms_id         = first_nn(.data$worms_id,         .data$.prio),
      itis_id          = first_nn(.data$itis_id,          .data$.prio),
      gbif_id          = first_nn(.data$gbif_id,          .data$.prio),
      scientific_name  = first_nn(.data$scientific_name,  .data$.prio),
      common_name      = first_nn(.data$common_name,      .data$.prio),
      rank             = first_nn(.data$rank,             .data$.prio),
      taxonomic_status = first_nn(.data$taxonomic_status, .data$.prio),
      parent_worms_id  = first_nn(.data$parent_worms_id,  .data$.prio),
      kingdom          = first_nn(.data$kingdom,          .data$.prio),
      phylum           = first_nn(.data$phylum,           .data$.prio),
      class            = first_nn(.data$class,            .data$.prio),
      order_taxon      = first_nn(.data$order_taxon,      .data$.prio),
      family           = first_nn(.data$family,           .data$.prio),
      .groups = "drop") |>
    dplyr::mutate(
      ncbi_id = NA_integer_, inat_id = NA_integer_,
      parent_taxon_key = ifelse(is.na(.data$parent_worms_id), NA_character_,
                                paste0("worms:", .data$parent_worms_id)))
  if (!is.null(rank_ord))
    taxon <- dplyr::left_join(taxon, rank_ord, by = c("rank" = "taxonRank"))
  else taxon$rank_order <- NA_integer_

  taxon <- taxon |>
    dplyr::select(
      "taxon_key", "worms_id", "itis_id", "gbif_id", "ncbi_id", "inat_id",
      "scientific_name", "common_name", "rank", "rank_order", "taxonomic_status",
      "parent_taxon_key", "kingdom", "phylum", "class", "order_taxon", "family") |>
    dplyr::arrange(.data$taxon_key) |>
    as.data.frame()
  .replace_table(con, tbl, taxon)
}

# build_taxon_group ------------------------------------------------------------

#' Build the `taxon_group` grouping table (many taxa per group)
#'
#' Seeds portable, cross-dataset groupings (`taxon_group_key` =
#' `"<dataset-or-known-list>:<group>"`, lowercase; a `description`; many
#' `taxon_key`) from the groupings each dataset already carries — phytoplankton
#' functional groups (`phyto_taxon.taxa`, e.g. `diatom, centric`) and the
#' seabird/mammal `is_bird`/`is_mammal` flags. Curated `calcofi:*` groups (e.g.
#' `forage_fish`) can be appended later.
#'
#' @inheritParams build_dataset_taxon
#' @param tbl target table name (default `"taxon_group"`)
#' @return (invisibly) the row count written
#' @export
#' @concept taxonomy
build_taxon_group <- function(con, measurement_taxon = NULL, overrides = NULL,
                              tbl = "taxon_group") {
  rows <- .taxon_norm_sources(con, measurement_taxon, overrides)
  grp <- list()

  # phytoplankton functional groups (from ds_common_name = phyto `taxa`)
  ph <- rows[rows$dataset_key == "calcofi_phytoplankton" & !is.na(rows$ds_common_name), , drop = FALSE]
  if (nrow(ph)) grp$phyto <- data.frame(
    taxon_group_key = paste0("calcofi_phytoplankton:",
                             gsub("[^a-z0-9]+", "_", tolower(ph$ds_common_name))),
    description = paste0("Phytoplankton functional group: ", ph$ds_common_name),
    taxon_key = ph$taxon_key, stringsAsFactors = FALSE)

  # seabird vs marine-mammal split (from the bird_mammal arm)
  bm <- rows[rows$dataset_key == "calcofi_bird_mammal_census", , drop = FALSE]
  if (nrow(bm)) grp$bm <- data.frame(
    taxon_group_key = ifelse(bm$is_bird, "calcofi:seabirds", "calcofi:marine_mammals"),
    description = ifelse(bm$is_bird, "Seabirds (CalCOFI seabird & mammal census)",
                                     "Marine mammals (CalCOFI seabird & mammal census)"),
    taxon_key = bm$taxon_key, stringsAsFactors = FALSE)

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
