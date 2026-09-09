# The species catalog record (plan 2026-09-09 "Landing follow-ups", S1): one entry
# per taxon with an observation at or below it, built from the release's own
# `taxon` / `dataset_taxon` / `taxon_group` / `obs_bio`.
#
# The fixture is a synthetic release in an in-memory DuckDB, small enough that
# every number below is arithmetic a reader can redo by hand:
#
#   worms:1  Biota                                    (root)
#    ├ worms:10 Actinopterygii (Class)
#    │   └ worms:20 Clupeiformes (Order)
#    │       ├ worms:30 Alosidae (Family)
#    │       │   └ worms:40 Sardinops (Genus)
#    │       │       ├ worms:50 Sardinops sagax        (Species, observed: 4)
#    │       │       ├ worms:51 Sardinops melanostictus(Species, observed: 2)
#    │       │       └ worms:52 Sardinops neopilchardus(Species, VOCABULARY ONLY)
#    │       └ worms:35 Gonostomatidae (Family)
#    │           └ worms:60 Cyclothone (Genus, observed: 1)
#    └ itis:174371 Aves (Class)
#        └ itis:1255048 Ardenna pacifica (Species, observed: 5)
#   ds_beta:218  a dataset-local class                (root, observed: 2)
#
# 14 obs_bio rows carry a taxon_key (a 15th carries none); 12 of the 13 taxon
# rows get an entry. Each `dataset_taxon` row exercises exactly one flag rule.

tfx <- function(...) testthat::test_path("fixtures", "taxa", ...)

taxa_fixture_record <- function() jsonlite::fromJSON(tfx("datasets.json"), simplifyVector = FALSE)

# a synthetic release: the four tables build_taxa_catalog() reads, plus `dataset`
new_taxa_fixture <- function() {
  testthat::skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")

  taxon <- data.frame(
    taxon_key = c("worms:1", "worms:10", "worms:20", "worms:30", "worms:35", "worms:40",
                  "worms:50", "worms:51", "worms:52", "worms:60",
                  "itis:174371", "itis:1255048", "ds_beta:218"),
    worms_id  = c(1L, 10L, 20L, 30L, 35L, 40L, 50L, 51L, 52L, 60L, NA, NA, NA),
    # worms:50 carries WoRMS's own ITIS cross-reference, so a source hint can disagree
    itis_id   = c(NA, NA, NA, NA, NA, NA, 161744L, NA, NA, NA, 174371L, 1255048L, NA),
    gbif_id   = c(rep(NA_integer_, 6), 5017L, rep(NA_integer_, 6)),
    ncbi_id   = NA_integer_,
    inat_id   = NA_integer_,
    scientific_name = c("Biota", "Actinopterygii", "Clupeiformes", "Alosidae", "Gonostomatidae",
                        "Sardinops", "Sardinops sagax", "Sardinops melanostictus",
                        "Sardinops neopilchardus", "Cyclothone", "Aves", "Ardenna pacifica", NA),
    common_name = c(NA, NA, NA, NA, NA, NA, "Pacific sardine", NA, NA, NA, "birds",
                    "Wedge-tailed Shearwater", NA),
    rank = c(NA, "Class", "Order", "Family", "Family", "Genus", "Species", "Species",
             "Species", "Genus", "Class", "Species", NA),
    rank_order = c(NA, 60L, 100L, 140L, 140L, 180L, 220L, 220L, 220L, 180L, 60L, 220L, NA),
    taxonomic_status = c(rep("accepted", 7), "unaccepted", "accepted", "accepted",
                         "accepted", "accepted", NA),
    status_checked = c(rep("2026-01-01", 12), NA),
    parent_taxon_key = c(NA, "worms:1", "worms:10", "worms:20", "worms:20", "worms:30",
                         "worms:40", "worms:40", "worms:40", "worms:35",
                         "worms:1", "itis:174371", NA),
    kingdom = c(NA, rep("Animalia", 11), NA),
    phylum  = c(NA, rep("Chordata", 11), NA),
    class   = c(NA, rep("Actinopterygii", 9), "Aves", "Aves", NA),
    order_taxon = c(NA, NA, "Clupeiformes", "Clupeiformes", "Clupeiformes", "Clupeiformes",
                    "Clupeiformes", "Clupeiformes", "Clupeiformes", "Stomiiformes",
                    NA, "Procellariiformes", NA),
    family = c(NA, NA, NA, "Alosidae", "Gonostomatidae", "Alosidae", "Alosidae", "Alosidae",
               "Alosidae", "Gonostomatidae", NA, "Procellariidae", NA),
    notes = c(rep(NA_character_, 11),
              "itis:174550 deprecated in ITIS -> itis:1255048 (Ardenna pacifica)", NA),
    stringsAsFactors = FALSE)

  # one row per flag rule, plus an accepted row, a second dataset on one taxon and
  # a vocabulary-only row
  dataset_taxon <- data.frame(
    ds_taxon_key = paste0("dt", 1:7),
    dataset_key  = c("ds_alpha", "ds_beta", "ds_beta", "ds_alpha", "ds_beta", "ds_beta", "ds_alpha"),
    taxon_key    = c("worms:50", "worms:50", "worms:51", "worms:60", "itis:1255048",
                     "ds_beta:218", "worms:52"),
    ds_scientific_name = c("Sardinops sagax", "Sardinops sagax", "Sardinops caeruleus",
                           "Cyclothone sp.", "Ardenna pacifica", NA, "Sardinops neopilchardus"),
    ds_common_name = c("Pacific sardine", NA, NA, "bristlemouth", "Wedge-tailed Shearwater",
                       "other, unidentified", NA),
    ds_taxa_code = c("SAR", "sard", "sarc", "CYC", "WTSH", "218", "SNE"),
    # dt1 agrees on the key authority and offers no secondary id -> no flag
    # dt2 agrees on worms (the key) and disagrees on itis (secondary) -> id_conflict
    # dt5 disagrees on itis, which IS the key of itis:1255048           -> rekeyed
    ds_source_json = c('{"worms_id":50}', '{"worms_id":50,"itis_id":161743}', NA, NA,
                       '{"itis_id":174550}', NA, NA),
    stringsAsFactors = FALSE)

  taxon_group <- data.frame(
    taxon_group_key = c("calcofi:forage_fish", "calcofi:seabirds"),
    description     = c("forage fish", "seabirds"),
    taxon_key       = c("worms:50", "itis:1255048"),
    stringsAsFactors = FALSE)

  obs_bio <- data.frame(
    obs_id      = 1:15,
    dataset_key = c(rep("ds_alpha", 3), "ds_beta", "ds_beta", "ds_beta", "ds_alpha",
                    rep("ds_beta", 5), "ds_beta", "ds_beta", "ds_alpha"),
    taxon_key   = c(rep("worms:50", 3), "worms:50", "worms:51", "worms:51", "worms:60",
                    rep("itis:1255048", 5), "ds_beta:218", "ds_beta:218", NA),
    life_stage  = c("egg", "larva", "larva", NA, NA, NA, NA,
                    "adult", "adult", "adult", NA, NA, NA, NA, NA),
    year        = c(2001L, 2001L, 2002L, 2003L, 2004L, 2005L, 2002L,
                    2010L, 2010L, 2011L, 2011L, 2011L, NA, NA, 2001L),
    sample_key  = c("s1", "s1", "s2", "s3", "s4", "s5", "s6",
                    "s7", "s7", "s8", "s9", "s10", "s11", "s12", "s13"),
    stringsAsFactors = FALSE)

  dataset <- data.frame(
    dataset_key = c("ds_alpha", "ds_beta"),
    dataset_name_short = c("Alpha Nets", "Beta Transects"),
    category = c("Fish Eggs & Larvae", "Seabirds & Marine Mammals"),
    color = c("#1155ff", "#aa3366"),
    stringsAsFactors = FALSE)

  DBI::dbWriteTable(con, "taxon", taxon)
  DBI::dbWriteTable(con, "dataset_taxon", dataset_taxon)
  DBI::dbWriteTable(con, "taxon_group", taxon_group)
  DBI::dbWriteTable(con, "obs_bio", obs_bio)
  DBI::dbWriteTable(con, "dataset", dataset)
  con
}

# a taxon entry by key, and its dataset block by dataset_key
tx_of <- function(rec, key) {
  i <- which(vapply(rec$taxa, function(t) t$taxon_key, "") == key)
  testthat::expect_length(i, 1)
  rec$taxa[[i]]
}
tx_ds <- function(t, key) t$datasets[[which(vapply(t$datasets, function(d) d$dataset_key, "") == key)]]
rec_ds <- function(rec, key) rec$datasets[[which(vapply(rec$datasets, function(d) d$dataset_key, "") == key)]]
# every flag in the record, as one table
all_flags <- function(rec) table(unlist(lapply(rec$taxa, function(t)
  unlist(lapply(t$datasets, function(d) unlist(lapply(d$sources, function(s) as.character(s$flags))))))))

fixture_taxa_record <- function() {
  con <- new_taxa_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  build_taxa_catalog(con, taxa_fixture_record())
}

# the vocabulary ---------------------------------------------------------------------

test_that("taxa_source_flags() is the schema's enum", {
  expect_equal(taxa_source_flags(), c("synonym", "sp_to_genus", "rekeyed", "id_conflict", "no_name"))
  schema <- jsonlite::fromJSON(
    system.file("schema", "taxa.schema.json", package = "calcofi4db"), simplifyVector = TRUE)
  enum <- schema$definitions$taxon$properties$datasets$items$properties$sources$items$properties$flags$items$enum
  expect_setequal(enum, taxa_source_flags())
})

test_that("taxa_catalog_checks() levels every check check_taxa_catalog() reports", {
  rec <- fixture_taxa_record()
  d <- check_taxa_catalog(rec)
  expect_true(all(d$check %in% names(taxa_catalog_checks())))
  expect_true(all(!is.na(d$level)))
})

# the record's shape and counts -------------------------------------------------------

test_that("build_taxa_catalog() counts the release, not the taxon table", {
  rec <- fixture_taxa_record()
  expect_equal(rec$schema_version, "1.0")
  expect_equal(rec$release, list(version = "v2026.01.01", release_date = "2026-01-01"))
  # 13 taxon rows, 5 observed, 12 with an observation at or below them
  expect_equal(rec$counts$taxon_rows, 13L)
  expect_equal(rec$counts$taxa_observed, 5L)
  expect_equal(rec$counts$species_observed, 3L)   # worms:50, worms:51, itis:1255048
  expect_equal(rec$counts$pages, 12L)
  expect_equal(rec$counts$datasets, 2L)
  expect_equal(rec$counts$obs_bio_rows, 14L)      # the 15th obs_bio row carries no taxon_key
  expect_length(rec$taxa, 12L)
})

test_that("a taxon with no observation anywhere gets no entry, only a vocabulary row", {
  rec <- fixture_taxa_record()
  # Sardinops neopilchardus sits under an observed genus and is still not a page
  expect_false("worms:52" %in% vapply(rec$taxa, function(t) t$taxon_key, ""))
  vo <- rec_ds(rec, "ds_alpha")$vocabulary_only
  expect_length(vo, 1)
  expect_equal(vo[[1]], list(taxon_key = "worms:52", name = "Sardinops neopilchardus", code = "SNE"))
  expect_length(rec_ds(rec, "ds_beta")$vocabulary_only, 0)
})

test_that("the slug is the taxon_key with its first ':' written '-'", {
  rec <- fixture_taxa_record()
  expect_equal(tx_of(rec, "worms:50")$slug, "worms-50")
  expect_equal(tx_of(rec, "itis:1255048")$slug, "itis-1255048")
  expect_equal(tx_of(rec, "ds_beta:218")$slug, "ds_beta-218")
  slugs <- vapply(rec$taxa, function(t) t$slug, "")
  expect_equal(anyDuplicated(slugs), 0L)
})

# direct counts, years and life stages -------------------------------------------------

test_that("direct{} counts only the observations keyed to the taxon itself", {
  rec <- fixture_taxa_record()
  t50 <- tx_of(rec, "worms:50")
  # 3 rows in ds_alpha (samples s1, s1, s2) + 1 in ds_beta (s3)
  expect_equal(t50$direct$n_obs, 4L)
  expect_equal(t50$direct$n_samples, 3L)
  expect_equal(t50$direct$n_datasets, 2L)
  expect_equal(t50$direct$year_min, 2001L)
  expect_equal(t50$direct$year_max, 2003L)
  # ordered by observations, so larva (2) before egg (1)
  expect_equal(as.character(t50$direct$life_stages), c("larva", "egg"))
  # a Family with no observation of its own
  t30 <- tx_of(rec, "worms:30")
  expect_equal(t30$direct$n_obs, 0L)
  expect_equal(t30$direct$n_datasets, 0L)
  expect_true(is.na(t30$direct$year_min))
  expect_length(t30$direct$life_stages, 0)
  expect_length(t30$datasets, 0)
})

test_that("a dataset block carries that dataset's own years and stages", {
  rec <- fixture_taxa_record()
  t50 <- tx_of(rec, "worms:50")
  expect_equal(vapply(t50$datasets, function(d) d$dataset_key, ""), c("ds_alpha", "ds_beta"))
  a <- tx_ds(t50, "ds_alpha")
  expect_equal(a$n_obs, 3L); expect_equal(a$n_samples, 2L)
  expect_equal(a$year_min, 2001L); expect_equal(a$year_max, 2002L)
  expect_equal(a$years, list(`2001` = 2L, `2002` = 1L))
  expect_equal(as.character(a$life_stages), c("larva", "egg"))
  b <- tx_ds(t50, "ds_beta")
  expect_equal(b$n_obs, 1L); expect_equal(b$years, list(`2003` = 1L))
  expect_length(b$life_stages, 0)
  # a dataset whose observations carry no year keeps `years` an empty OBJECT, not
  # an empty array: the schema types it object, and a page indexes into it
  loc <- tx_ds(tx_of(rec, "ds_beta:218"), "ds_beta")
  expect_equal(loc$years, stats::setNames(list(), character()))
  expect_true(grepl('"years":\\{\\}', jsonlite::toJSON(loc, auto_unbox = TRUE, na = "null")))
})

# the rollup ---------------------------------------------------------------------------

test_that("rollup{} is the taxon and every descendant, and the roots hold everything", {
  rec <- fixture_taxa_record()
  roll <- function(k) tx_of(rec, k)$rollup

  # the genus: two observed species, 4 + 2 observations, both datasets
  expect_equal(roll("worms:40")[c("n_obs", "n_taxa", "n_species", "n_datasets")],
               list(n_obs = 6L, n_taxa = 2L, n_species = 2L, n_datasets = 2L))
  expect_equal(roll("worms:40")$year_min, 2001L)
  expect_equal(roll("worms:40")$year_max, 2005L)
  # the family above it adds nothing (worms:52 is not a page)
  expect_equal(roll("worms:30")$n_obs, 6L)
  expect_equal(roll("worms:30")$n_taxa, 2L)
  # the sister family: a Genus is observed but is not a species
  expect_equal(roll("worms:35")[c("n_obs", "n_taxa", "n_species", "n_datasets")],
               list(n_obs = 1L, n_taxa = 1L, n_species = 0L, n_datasets = 1L))
  # the order: 6 + 1
  expect_equal(roll("worms:20")[c("n_obs", "n_taxa", "n_species")],
               list(n_obs = 7L, n_taxa = 3L, n_species = 2L))
  expect_equal(roll("itis:174371")[c("n_obs", "n_taxa", "n_species", "n_datasets")],
               list(n_obs = 5L, n_taxa = 1L, n_species = 1L, n_datasets = 1L))
  # the root over the authority keys
  expect_equal(roll("worms:1")[c("n_obs", "n_taxa", "n_species", "n_datasets")],
               list(n_obs = 12L, n_taxa = 4L, n_species = 3L, n_datasets = 2L))
  expect_equal(roll("worms:1")$year_min, 2001L)
  expect_equal(roll("worms:1")$year_max, 2011L)
  # the dataset-local root, which has no year at all
  expect_equal(roll("ds_beta:218")[c("n_obs", "n_taxa", "n_species", "n_datasets")],
               list(n_obs = 2L, n_taxa = 1L, n_species = 0L, n_datasets = 1L))
  expect_true(is.na(roll("ds_beta:218")$year_min))

  # every observation is under exactly one root
  roots <- Filter(function(t) is.na(t$parent_taxon_key), rec$taxa)
  expect_equal(sum(vapply(roots, function(t) t$rollup$n_obs, 0L)), rec$counts$obs_bio_rows)
})

# lineage, ids, groups, local ----------------------------------------------------------

test_that("a taxon carries its lineage, ids, groups and notes", {
  rec <- fixture_taxa_record()
  t50 <- tx_of(rec, "worms:50")
  expect_equal(t50$scientific_name, "Sardinops sagax")
  expect_equal(t50$common_name, "Pacific sardine")
  expect_equal(t50$rank, "Species")
  expect_equal(t50$rank_order, 220L)
  expect_equal(t50$parent_taxon_key, "worms:40")
  expect_equal(t50$lineage, list(kingdom = "Animalia", phylum = "Chordata",
                                 class = "Actinopterygii", order = "Clupeiformes",
                                 family = "Alosidae"))
  expect_equal(t50$ids, list(worms_id = 50L, itis_id = 161744L, gbif_id = 5017L,
                             ncbi_id = NA_integer_, inat_id = NA_integer_))
  expect_equal(as.character(t50$groups), "calcofi:forage_fish")
  expect_true(is.na(t50$notes))
  expect_null(t50$local)
  # the status pill's input, and the note that explains a re-key
  expect_equal(tx_of(rec, "worms:51")$taxonomic_status, "unaccepted")
  expect_match(tx_of(rec, "itis:1255048")$notes, "deprecated in ITIS")
})

test_that("a dataset-local class carries local{} and no other taxon does", {
  rec <- fixture_taxa_record()
  loc <- tx_of(rec, "ds_beta:218")
  expect_equal(loc$local, list(dataset_key = "ds_beta", name = NA_character_,
                               common_name = "other, unidentified", code = "218"))
  expect_true(is.na(loc$rank))
  expect_true(is.na(loc$parent_taxon_key))
  has_local <- vapply(rec$taxa, function(t) !is.null(t$local), logical(1))
  keys <- vapply(rec$taxa, function(t) t$taxon_key, "")
  expect_equal(keys[has_local], "ds_beta:218")
})

# the flags ---------------------------------------------------------------------------

test_that("a source row that uses the accepted name carries no flag", {
  rec <- fixture_taxa_record()
  s <- tx_ds(tx_of(rec, "worms:50"), "ds_alpha")$sources
  expect_length(s, 1)
  expect_equal(s[[1]]$name, "Sardinops sagax")
  expect_equal(s[[1]]$common_name, "Pacific sardine")
  expect_equal(s[[1]]$code, "SAR")
  # the key authority's id agrees and no secondary id is offered
  expect_equal(s[[1]]$ids, list(worms_id = 50L, itis_id = NA_integer_, gbif_id = NA_integer_))
  expect_length(s[[1]]$flags, 0)
})

test_that("synonym: the dataset's name is not the accepted name", {
  rec <- fixture_taxa_record()
  s <- tx_ds(tx_of(rec, "worms:51"), "ds_beta")$sources[[1]]
  expect_equal(s$name, "Sardinops caeruleus")
  expect_equal(as.character(s$flags), "synonym")
})

test_that("sp_to_genus: a '… sp.' name on a Genus is less precise, not wrong", {
  rec <- fixture_taxa_record()
  s <- tx_ds(tx_of(rec, "worms:60"), "ds_alpha")$sources[[1]]
  expect_equal(s$name, "Cyclothone sp.")
  # exactly one flag: sp_to_genus REPLACES synonym
  expect_equal(as.character(s$flags), "sp_to_genus")
})

test_that("sp_to_genus needs the Genus: a 'sp.' name keyed to a species is a synonym", {
  # regression: "Delphinus sp." keyed to Delphinus delphis is a different name, not
  # a genus rollup — the rank of the taxon it resolved to is what decides
  expect_equal(calcofi4db:::.taxa_flags_of("worms:137094", "Delphinus sp.", NA, "Species",
                                           "Delphinus delphis", NA, NA), "synonym")
  expect_equal(calcofi4db:::.taxa_flags_of("worms:137094", "Delphinus sp.", NA, "Genus",
                                           "Delphinus", NA, NA), "sp_to_genus")
  # the three name shapes the plan measured
  expect_true(calcofi4db:::.taxa_is_sp_name("Cyclothone sp."))
  expect_true(calcofi4db:::.taxa_is_sp_name("Thysanoessa spp."))
  expect_true(calcofi4db:::.taxa_is_sp_name("Abraliopsis sp A"))
  expect_false(calcofi4db:::.taxa_is_sp_name("Sardinops sagax"))
})

test_that("rekeyed: the KEY authority's id was deprecated and the row follows it", {
  rec <- fixture_taxa_record()
  s <- tx_ds(tx_of(rec, "itis:1255048"), "ds_beta")$sources[[1]]
  expect_equal(s$name, "Ardenna pacifica")          # the name agrees; only the id moved
  expect_equal(s$ids$itis_id, 174550L)              # what the source supplied
  expect_equal(tx_of(rec, "itis:1255048")$ids$itis_id, 1255048L)   # where it is keyed
  # the taxon is keyed `itis:`, so a differing itis_id IS a re-key
  expect_equal(as.character(s$flags), "rekeyed")
})

test_that("id_conflict: a SECONDARY authority's id disagrees, and nothing was re-keyed", {
  # the ichthyoplankton case: a worms:-keyed taxon whose source ITIS hint differs from
  # the itis_id WoRMS publishes as its external link. Until 4.9.0 this read `rekeyed`,
  # which told a reader an id had moved when none had.
  rec <- fixture_taxa_record()
  s <- tx_ds(tx_of(rec, "worms:50"), "ds_beta")$sources[[1]]
  expect_equal(s$name, "Sardinops sagax")    # the accepted name
  expect_equal(s$ids$worms_id, 50L)          # the KEY authority agrees
  expect_equal(s$ids$itis_id, 161743L)       # the secondary one does not
  expect_equal(tx_of(rec, "worms:50")$ids$itis_id, 161744L)
  expect_equal(as.character(s$flags), "id_conflict")
})

test_that("which authority moved decides which flag", {
  F <- calcofi4db:::.taxa_flags_of
  # a worms: key, worms id moved -> rekeyed
  expect_equal(F("worms:50", "Sardinops sagax", '{"worms_id":49}', "Species",
                 "Sardinops sagax", 50L, NA), "rekeyed")
  # a worms: key, itis hint differs -> id_conflict
  expect_equal(F("worms:50", "Sardinops sagax", '{"itis_id":161743}', "Species",
                 "Sardinops sagax", 50L, 161744L), "id_conflict")
  # an itis: key, the mirror image
  expect_equal(F("itis:1255048", "Ardenna pacifica", '{"itis_id":174550}', "Species",
                 "Ardenna pacifica", NA, 1255048L), "rekeyed")
  expect_equal(F("itis:1255048", "Ardenna pacifica", '{"worms_id":1}', "Species",
                 "Ardenna pacifica", 2L, 1255048L), "id_conflict")
  # both at once, both reported
  expect_equal(F("worms:50", "Sardinops sagax", '{"worms_id":49,"itis_id":161743}', "Species",
                 "Sardinops sagax", 50L, 161744L), c("rekeyed", "id_conflict"))
  # a dataset-local class is keyed by no authority, so it can never be re-keyed
  expect_equal(F("ds_beta:218", "a local class", '{"worms_id":49}', NA, "a local class",
                 50L, NA), "id_conflict")
  # an id the source did not supply, or the taxon does not carry, is not a disagreement
  expect_length(F("worms:50", "Sardinops sagax", '{"worms_id":50}', "Species",
                  "Sardinops sagax", 50L, 161744L), 0)
  expect_length(F("worms:50", "Sardinops sagax", NA, "Species", "Sardinops sagax",
                  50L, 161744L), 0)
  # a gbif_id keys nothing and is never flagged
  expect_length(F("worms:50", "Sardinops sagax", '{"worms_id":50,"gbif_id":999}', "Species",
                  "Sardinops sagax", 50L, NA), 0)
})

test_that("no_name: the dataset carries only a code", {
  rec <- fixture_taxa_record()
  s <- tx_ds(tx_of(rec, "ds_beta:218"), "ds_beta")$sources[[1]]
  expect_true(is.na(s$name))
  expect_equal(s$code, "218")
  expect_equal(as.character(s$flags), "no_name")
})

test_that("every flag rule fires exactly once over the fixture", {
  rec <- fixture_taxa_record()
  expect_equal(as.list(all_flags(rec)),
               list(id_conflict = 1L, no_name = 1L, rekeyed = 1L, sp_to_genus = 1L, synonym = 1L))
  expect_true(all(names(all_flags(rec)) %in% taxa_source_flags()))
})

# datasets[] --------------------------------------------------------------------------

test_that("datasets[] takes its name, colour and category from the record", {
  rec <- fixture_taxa_record()
  expect_equal(vapply(rec$datasets, function(d) d$dataset_key, ""), c("ds_alpha", "ds_beta"))
  a <- rec_ds(rec, "ds_alpha")
  expect_equal(a$dataset_name_short, "Alpha Nets")
  expect_equal(a$color, "#1155ff")
  # the record's category block carries prose and `registered`; the taxa schema
  # admits only these five keys
  expect_equal(a$category, list(key = NA_character_, name = "Fish Eggs & Larvae",
                                icon = "cat-ichthyo", realm = "bio", order = 10L))
  expect_equal(a$n_obs, 4L)     # 3 of worms:50 + 1 of worms:60
  expect_equal(a$n_taxa, 2L)
  b <- rec_ds(rec, "ds_beta")
  expect_equal(b$n_obs, 10L)    # 1 + 2 + 5 + 2
  expect_equal(b$n_taxa, 4L)
  expect_equal(a$n_obs + b$n_obs, rec$counts$obs_bio_rows)
})

# validate and check -------------------------------------------------------------------

test_that("the record validates against taxa.schema.json and round-trips", {
  skip_if_not_installed("jsonvalidate")
  rec <- fixture_taxa_record()
  expect_true(validate_taxa_catalog(rec))
  dir <- withr::local_tempdir()
  p <- write_taxa_catalog(rec, dir)
  expect_equal(basename(p), "taxa.json")
  expect_true(file.exists(p))
  expect_true(validate_taxa_catalog(p))
  back <- jsonlite::fromJSON(p, simplifyVector = FALSE)
  expect_equal(back$counts$pages, 12L)
  expect_equal(back$taxa[[which(vapply(back$taxa, function(t) t$taxon_key, "") == "worms:50")]]$rollup$n_obs, 4L)
})

test_that("check_taxa_catalog() passes on the release it was built from", {
  con <- new_taxa_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  record <- taxa_fixture_record()
  rec <- build_taxa_catalog(con, record)
  d <- check_taxa_catalog(rec, con, record)
  expect_true(all(d$ok), info = paste(d$check[!d$ok], collapse = ", "))
  expect_true(all(c("taxa_observed", "species_observed", "obs_bio_rows", "pages", "observed_paged",
                    "parents_resolve", "rollup_total", "slugs_unique", "datasets_known",
                    "dataset_meta", "flags_known", "local_blocks") %in% d$check))
  expect_silent(assert_taxa_catalog(d, quiet = TRUE))
})

test_that("check_taxa_catalog() catches a broken record, and assert stops on it", {
  con <- new_taxa_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  rec <- build_taxa_catalog(con, taxa_fixture_record())

  # a page dropped: the count, the observed set and the parent chain all notice
  short <- rec; short$taxa <- short$taxa[-1]
  d <- check_taxa_catalog(short, con)
  expect_false(d$ok[d$check == "pages"])
  expect_error(assert_taxa_catalog(d, quiet = TRUE), "blocking finding")

  # a rollup that no longer adds up
  bent <- rec
  i <- which(vapply(bent$taxa, function(t) t$taxon_key, "") == "worms:1")
  bent$taxa[[i]]$rollup$n_obs <- 99L
  expect_false(check_taxa_catalog(bent, con)$ok[check_taxa_catalog(bent, con)$check == "rollup_total"])

  # a parent with no entry
  orphan <- rec
  j <- which(vapply(orphan$taxa, function(t) t$taxon_key, "") == "worms:50")
  orphan$taxa[[j]]$parent_taxon_key <- "worms:999"
  dd <- check_taxa_catalog(orphan, con)
  expect_false(dd$ok[dd$check == "parents_resolve"])

  # a dataset the release record does not know
  stray <- rec
  stray$datasets[[1]]$dataset_key <- "ds_gamma"
  ds <- check_taxa_catalog(stray, con, taxa_fixture_record())
  expect_false(ds$ok[ds$check == "datasets_known"])

  # a flag outside the enum
  odd <- rec
  k <- which(vapply(odd$taxa, function(t) t$taxon_key, "") == "worms:51")
  odd$taxa[[k]]$datasets[[1]]$sources[[1]]$flags <- I("made_up")
  df <- check_taxa_catalog(odd, con)
  expect_false(df$ok[df$check == "flags_known"])
})

test_that("build_taxa_catalog() refuses a lineage that never reaches a root", {
  con <- new_taxa_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  # a cycle: the genus's parent becomes one of its own species
  DBI::dbExecute(con, "UPDATE taxon SET parent_taxon_key = 'worms:50' WHERE taxon_key = 'worms:40'")
  expect_error(build_taxa_catalog(con, taxa_fixture_record()),
               "does not reach a root")
})

test_that("build_taxa_catalog() takes the release version from the record, or the argument", {
  con <- new_taxa_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  rec <- build_taxa_catalog(con, taxa_fixture_record(), "v2026.02.02", "2026-02-02")
  expect_equal(rec$release, list(version = "v2026.02.02", release_date = "2026-02-02"))
  expect_error(build_taxa_catalog(con, list(datasets = list())), "no release version")
})
