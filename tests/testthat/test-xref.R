# The authority cross-reference: a bird gains a worms_id WITHOUT losing its itis:
# key, a deprecated id is re-keyed onto the accepted one, and the provenance is
# recorded once and only once. Everything here runs from a fixture CSV — no
# network, so `worrms`/`taxize` need not be installed.

# a pre-populated xref cache standing in for what fetch_taxon_xref() would write.
# GRCO: the Great Cormorant's source TSN, valid in ITIS, crosswalked to WoRMS.
# BADT: a deprecated TSN (Puffinus griseus 174553 -> Ardenna grisea 1255050).
# BLWH: the reverse direction, a WoRMS-keyed mammal gaining its itis_id.
xref_fixture_csv <- function(dir, today = "2026-08-05") {
  p <- file.path(dir, "taxon_xref.csv")
  utils::write.csv(data.frame(
    query_type   = c("tsn", "tsn", "aphia", "name"),
    query_value  = c("174715", "174553", "137090", "Bathophilus"),
    worms_id     = c(137179L, 137202L, 137090L, 126203L),
    itis_id      = c(174715L, 1255050L, 180528L, NA_integer_),
    matched_name = c("Phalacrocorax carbo", "Puffinus griseus",
                     "Balaenoptera musculus", "Bathophilus"),
    accepted_name = c("Phalacrocorax carbo", "Ardenna grisea",
                      "Balaenoptera musculus", "Bathophilus"),
    rank   = c("Species", "Species", "Species", "Genus"),
    status = c("accepted", "accepted", "accepted", "accepted"),
    checked_date = today,
    notes = c(
      paste0(today, ": worms_id 137179 via WoRMS TSN crosswalk (status accepted)"),
      paste0(today, ": worms_id 137202 via WoRMS TSN crosswalk (status accepted); ",
             "itis:174553 deprecated in ITIS -> itis:1255050 (Ardenna grisea)"),
      paste0(today, ": itis_id 180528 via WoRMS external link"),
      paste0(today, ": worms_id 126203 matched by name \"Bathophilus\"")),
    stringsAsFactors = FALSE), p, row.names = FALSE, na = "")
  p
}

# a bird whose TSN is fine, a bird whose TSN is deprecated, a mammal, and a
# mesopelagic fish whose source name carries the " sp." suffix
new_xref_fixture <- function() {
  testthat::skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  DBI::dbExecute(con, "CREATE TABLE bird_mammal_species AS
    SELECT 'GRCO' species_code,'Great Cormorant' common_name,'Phalacrocorax carbo' scientific_name,
           174715 itis_id, TRUE is_bird, FALSE is_mammal, FALSE is_unidentified, TRUE include_flag
    UNION ALL SELECT 'BADT','Sooty Shearwater','Puffinus griseus',174553,TRUE,FALSE,FALSE,TRUE
    UNION ALL SELECT 'BLWH','Blue Whale','Balaenoptera musculus',180528,FALSE,TRUE,FALSE,TRUE")
  DBI::dbExecute(con, "CREATE TABLE mesopelagic_fish_taxon AS
    SELECT 'Bathophilus sp.' scientific_name, NULL::INTEGER worms_id, NULL::VARCHAR rank")
  # the staged classification: the class (not a source flag) decides itis: (D2).
  # BADT is looked up by its ACCEPTED TSN, which is what the cross-reference
  # re-keys it onto before the class is read.
  DBI::dbExecute(con, "CREATE TABLE _taxon_lineage_flat AS
    SELECT 174715 requested_id, 'ITIS' authority, 'Species' AS \"rank\", 174712 parent_id,
           'Phalacrocorax carbo' scientific_name, 'Animalia' kingdom, 'Chordata' phylum,
           'Aves' AS \"class\", 'Pelecaniformes' order_taxon, 'Phalacrocoracidae' AS \"family\"
    UNION ALL SELECT 1255050,'ITIS','Species',1255018,'Ardenna grisea','Animalia','Chordata','Aves','Procellariiformes','Procellariidae'")
  con
}

bm_override <- function() data.frame(
  dataset_key = "farallon_bird-mammal", match_column = "species_code",
  match_value = "BLWH", worms_id = 137090L, itis_id = NA_integer_,
  scientific_name = "Balaenoptera musculus", rank = "Species",
  stringsAsFactors = FALSE)

stage_xref <- function(con, csv) {
  x <- utils::read.csv(csv, stringsAsFactors = FALSE, na.strings = c("", "NA"))
  DBI::dbWriteTable(con, "_taxon_xref", x, overwrite = TRUE)
}


test_that("clean_taxon_name strips open nomenclature without mangling real names", {
  expect_equal(clean_taxon_name("Bathophilus sp."),       "Bathophilus")
  expect_equal(clean_taxon_name("Cyclothone sp"),         "Cyclothone")
  expect_equal(clean_taxon_name("Meringosphaera spp."),   "Meringosphaera")
  expect_equal(clean_taxon_name("Phaeocystis cf pouchetti"), "Phaeocystis pouchetti")
  expect_equal(clean_taxon_name("Pterosperma sp. a"),     "Pterosperma")
  expect_equal(clean_taxon_name("indistinguished Pterosperma spp."), "Pterosperma")
  expect_equal(clean_taxon_name("Uria aalge (Pontoppidan, 1763)"), "Uria aalge")
  # untouched: a clean binomial and a legitimate trinomial
  expect_equal(clean_taxon_name("Uria aalge"), "Uria aalge")
  expect_equal(clean_taxon_name("Pterodroma phaeopygia sandwichensis"),
               "Pterodroma phaeopygia sandwichensis")
  expect_true(is.na(clean_taxon_name(NA_character_)))
})

test_that("a bird gains worms_id but KEEPS its itis: key", {
  # the regression that started this: matching on worms_id returned zero rows for
  # every seabird. Filling it must not flip the key onto the lagging authority.
  con <- new_xref_fixture(); on.exit(close_duckdb(con))
  stage_xref(con, xref_fixture_csv(withr::local_tempdir()))
  build_taxon_reference(con, overrides = bm_override())
  resolve_dataset_taxon(con, overrides = bm_override())

  tx <- DBI::dbGetQuery(con, "SELECT * FROM taxon")
  grco <- tx[tx$taxon_key == "itis:174715", ]
  expect_equal(nrow(grco), 1L)
  expect_equal(grco$worms_id, 137179L)   # the cross-reference is populated
  expect_equal(grco$itis_id,  174715L)   # ...and the key authority is unchanged
  expect_false(any(tx$taxon_key == "worms:137179"))

  # taxon_key_of() itself: both ids present still keys itis: when the class is Aves
  expect_equal(taxon_key_of(137179L, 174715L, class = "Aves"), "itis:174715")
})

test_that("a deprecated TSN is re-keyed onto the ITIS-accepted id", {
  con <- new_xref_fixture(); on.exit(close_duckdb(con))
  stage_xref(con, xref_fixture_csv(withr::local_tempdir()))
  build_taxon_reference(con, overrides = bm_override())
  resolve_dataset_taxon(con, overrides = bm_override())

  dt <- DBI::dbGetQuery(con, "SELECT * FROM dataset_taxon")
  badt <- dt[dt$ds_taxa_code == "BADT", ]
  expect_equal(badt$taxon_key, "itis:1255050")     # not the deprecated 174553
  expect_equal(badt$ds_scientific_name, "Puffinus griseus")  # source name kept

  tx <- DBI::dbGetQuery(con, "SELECT * FROM taxon")
  expect_false(any(tx$taxon_key == "itis:174553"))
  expect_equal(tx$worms_id[tx$taxon_key == "itis:1255050"], 137202L)
})

test_that("the re-key is recorded in an append-only note, once", {
  con <- new_xref_fixture(); on.exit(close_duckdb(con))
  stage_xref(con, xref_fixture_csv(withr::local_tempdir()))
  build_taxon_reference(con, overrides = bm_override())
  n1 <- DBI::dbGetQuery(con,
    "SELECT notes, status_checked, taxonomic_status FROM taxon WHERE taxon_key='itis:1255050'")

  expect_match(n1$notes, "deprecated in ITIS -> itis:1255050")
  expect_match(n1$notes, "^2026-08-05: ")           # datestamped
  expect_equal(n1$status_checked, "2026-08-05")     # status carries its check date
  expect_equal(n1$taxonomic_status, "accepted")     # fetched, not stamped

  # re-running with the same (warm) cross-reference must not duplicate the line
  build_taxon_reference(con, overrides = bm_override())
  n2 <- DBI::dbGetQuery(con, "SELECT notes FROM taxon WHERE taxon_key='itis:1255050'")
  expect_equal(n2$notes, n1$notes)
  expect_equal(lengths(regmatches(n2$notes, gregexpr("deprecated in ITIS", n2$notes))), 1L)
})

test_that("a WoRMS-keyed taxon gains its itis_id without changing key", {
  con <- new_xref_fixture(); on.exit(close_duckdb(con))
  stage_xref(con, xref_fixture_csv(withr::local_tempdir()))
  build_taxon_reference(con, overrides = bm_override())

  blwh <- DBI::dbGetQuery(con, "SELECT * FROM taxon WHERE taxon_key='worms:137090'")
  expect_equal(nrow(blwh), 1L)
  expect_equal(blwh$itis_id, 180528L)
})

test_that("a ' sp.' name resolves by cleaned name, and ds_taxa_code is NOT rewritten", {
  # ds_taxa_code IS the obs join key for mesopelagic fish; cleaning it would
  # orphan every observation of these six taxa.
  con <- new_xref_fixture(); on.exit(close_duckdb(con))
  stage_xref(con, xref_fixture_csv(withr::local_tempdir()))
  resolve_dataset_taxon(con, overrides = bm_override())

  mf <- DBI::dbGetQuery(con,
    "SELECT * FROM dataset_taxon WHERE dataset_key='sio_mesopelagic-fish'")
  expect_equal(mf$ds_taxa_code, "Bathophilus sp.")        # verbatim source header
  expect_equal(mf$taxon_key,    "worms:126203")           # ...but resolved
})

test_that("without a staged cross-reference nothing is re-keyed", {
  con <- new_xref_fixture(); on.exit(close_duckdb(con))
  resolve_dataset_taxon(con, overrides = bm_override())
  dt <- DBI::dbGetQuery(con, "SELECT * FROM dataset_taxon")

  # the deprecated TSN never reached the lineage, so no class says Aves and no
  # AphiaID resolved: the honest outcome is the dataset-local key (which
  # check_dataset_taxon() refuses), not a key minted on a deprecated id
  expect_equal(dt$taxon_key[dt$ds_taxa_code == "BADT"], "farallon_bird-mammal:BADT")
  expect_equal(dt$taxon_key[dt$ds_taxa_code == "GRCO"], "itis:174715")   # class Aves is staged
  expect_equal(dt$taxon_key[dt$ds_taxa_code == "Bathophilus sp."],
               "sio_mesopelagic-fish:Bathophilus sp.")
})

test_that("fetch_taxon_xref serves a warm cache offline and scopes its return", {
  csv <- xref_fixture_csv(withr::local_tempdir())
  out <- fetch_taxon_xref(itis_ids = c(174715L, 174553L), cache_csv = csv,
                          verbose = FALSE)
  expect_equal(sort(out$query_value), c("174553", "174715"))
  expect_equal(out$worms_id[out$query_value == "174553"], 137202L)
  # the aphia/name rows are in the cache but were not asked for
  expect_false(any(out$query_type %in% c("aphia", "name")))
})
