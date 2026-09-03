# taxon lineage: the fix for taxa that reached the release with a key and a name
# and nothing else. No network here — every test pre-seeds the cache CSV, which is
# also the mechanism an offline re-run relies on.

lineage_fixture_csv <- function(dir) {
  p <- file.path(dir, "taxon_lineage.csv")
  # Sardinops sagax (217452) and Metacarcinus magister (440388) WoRMS chains,
  # plus one ITIS chain for a seabird (Aves keys on itis: — see taxon_key_of())
  utils::write.csv(data.frame(
    requested_id = c(rep(217452L, 4), rep(440388L, 4), rep(174715L, 3)),
    authority    = c(rep("WoRMS", 8), rep("ITIS", 3)),
    taxonID      = c(2L, 1821L, 125464L, 217452L,
                     2L, 1130L, 106673L, 440388L,
                     174371L, 174712L, 174715L),
    parentNameUsageID = c(NA, 2L, 1821L, 125464L,
                          NA, 2L, 1130L, 106673L,
                          NA, 174371L, 174712L),
    scientificName = c("Animalia", "Clupeiformes", "Sardinops", "Sardinops sagax",
                       "Animalia", "Decapoda", "Cancridae", "Metacarcinus magister",
                       "Aves", "Pelecaniformes", "Phalacrocorax carbo"),
    taxonRank = c("Kingdom", "Order", "Genus", "Species",
                  "Kingdom", "Order", "Family", "Species",
                  "Class", "Order", "Species"),
    stringsAsFactors = FALSE), p, row.names = FALSE, na = "")
  p
}

test_that("a cached id is not re-fetched (so a re-run is offline and free)", {
  d <- withr::local_tempdir()
  p <- lineage_fixture_csv(d)
  # no network reachable in tests; if it tried to fetch, this would error or hang
  lin <- fetch_taxon_lineage(c(217452L, 440388L), 174715L, cache_csv = p,
                             verbose = FALSE)
  expect_equal(nrow(lin), 11L)
  expect_setequal(unique(lin$requested_id), c(217452L, 440388L, 174715L))
})

test_that("the flattened classification picks the five headline ranks", {
  d <- withr::local_tempdir()
  lin <- utils::read.csv(lineage_fixture_csv(d), stringsAsFactors = FALSE)
  flat <- calcofi4db:::.lineage_flat(lin)

  crab <- flat[flat$requested_id == 440388, ]
  expect_equal(crab$rank, "Species")
  expect_equal(crab$scientific_name, "Metacarcinus magister")
  expect_equal(crab$kingdom, "Animalia")
  expect_equal(crab$order_taxon, "Decapoda")
  expect_equal(crab$family, "Cancridae")       # populated by no dataset before
  expect_true(is.na(crab$phylum))              # absent from this chain, not invented
  expect_equal(crab$parent_id, 106673L)        # its own row's parent
})

test_that("ensure_taxon_lineage stages the hierarchy build_taxon_reference reads", {
  skip_if_not_installed("duckdb")
  d <- withr::local_tempdir()
  p <- lineage_fixture_csv(d)
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE species AS
    SELECT 19::SMALLINT species_id, 'Sardinops sagax' scientific_name,
           'Pacific sardine' common_name, 217452 worms_id, 161729 itis_id, NULL gbif_id")

  out <- ensure_taxon_lineage(con, NULL, NULL, cache_csv = p, verbose = FALSE)
  expect_gt(out$n_rows, 0L)

  # DwC shape, which is exactly what build_taxon_reference() consumes
  h <- DBI::dbGetQuery(con, "SELECT * FROM taxon ORDER BY taxonID")
  expect_true(all(c("taxonID", "parentNameUsageID", "scientificName",
                    "taxonRank", "authority") %in% names(h)))
  expect_true(125464L %in% h$taxonID)   # the ancestor, not just the requested taxon

  n <- build_taxon_reference(con, NULL, NULL)
  tx <- DBI::dbGetQuery(con, "SELECT * FROM taxon ORDER BY taxon_key")

  sard <- tx[tx$taxon_key == "worms:217452", ]
  expect_equal(sard$rank, "Species")
  expect_equal(sard$parent_taxon_key, "worms:125464")   # was NULL before the fix
  expect_equal(sard$kingdom, "Animalia")
  expect_equal(sard$order_taxon, "Clupeiformes")
  # the ancestors are rows of their own, so the chain can be walked
  expect_true(all(c("worms:2", "worms:1821", "worms:125464") %in% tx$taxon_key))
  # and every parent resolves
  expect_equal(sum(!is.na(tx$parent_taxon_key) &
                   !tx$parent_taxon_key %in% tx$taxon_key), 0L)
})

test_that("an ITIS-keyed taxon gets an ITIS parent, not a minted worms: key", {
  skip_if_not_installed("duckdb")
  d <- withr::local_tempdir()
  p <- lineage_fixture_csv(d)
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  # a seabird carrying only a TSN: pass (a) classifies it by that TSN (class
  # Aves), pass (b) keeps the ITIS chain, so the key is itis: and its parent is
  # too — pasting "worms:<parentNameUsageID>" would mint a key resolving to nothing
  DBI::dbExecute(con, "CREATE TABLE bird_mammal_species AS
    SELECT 'GRCO' species_code, 'Great Cormorant' common_name,
           'Phalacrocorax carbo' scientific_name, 174715 itis_id,
           TRUE is_bird, FALSE is_mammal, FALSE is_unidentified, TRUE include_flag")

  ensure_taxon_lineage(con, NULL, NULL, cache_csv = p, verbose = FALSE)
  build_taxon_reference(con, NULL, NULL)
  tx <- DBI::dbGetQuery(con, "SELECT * FROM taxon")

  gr <- tx[tx$taxon_key == "itis:174715", ]
  expect_equal(nrow(gr), 1L)
  expect_equal(gr$parent_taxon_key, "itis:174712")
  expect_equal(gr$class, "Aves")
  expect_false(any(grepl("^worms:1747", tx$taxon_key)))
  expect_equal(sum(!is.na(tx$parent_taxon_key) &
                   !tx$parent_taxon_key %in% tx$taxon_key), 0L)
})

test_that("the Aves rule is derived from the lineage: a bird with both ids keys itis: and stages only the ITIS chain", {
  skip_if_not_installed("duckdb")
  d <- withr::local_tempdir()
  p <- lineage_fixture_csv(d)
  # the bird's WoRMS chain, as pass (a) would fetch it by AphiaID: class Aves
  lin <- utils::read.csv(p, stringsAsFactors = FALSE)
  lin <- rbind(lin, data.frame(
    requested_id = 137179L, authority = "WoRMS",
    taxonID = c(2L, 1836L, 137071L, 137179L), parentNameUsageID = c(NA, 2L, 1836L, 137071L),
    scientificName = c("Animalia", "Aves", "Phalacrocorax", "Phalacrocorax carbo"),
    taxonRank = c("Kingdom", "Class", "Genus", "Species"), stringsAsFactors = FALSE))
  utils::write.csv(lin, p, row.names = FALSE, na = "")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  # after ensure_taxon_xref() the bird carries BOTH ids; no source flag says "bird"
  append_dataset_taxon(con, "demo_ds", data.frame(
    ds_taxa_code = "GRCO", ds_scientific_name = "Phalacrocorax carbo",
    worms_id = 137179L, itis_id = 174715L, stringsAsFactors = FALSE))

  out <- ensure_taxon_lineage(con, NULL, NULL, cache_csv = p, verbose = FALSE)
  flat <- DBI::dbGetQuery(con, "SELECT * FROM _taxon_lineage_flat")
  # the class was learned from WoRMS, the staged chain is the ITIS one
  expect_true(174715L %in% flat$requested_id[flat$authority == "ITIS"])
  expect_false(137179L %in% flat$requested_id[flat$authority == "WoRMS"])
  expect_false(1836L %in% flat$requested_id)      # no worms: Aves ancestor row

  resolve_dataset_taxon(con)
  build_taxon_reference(con)
  expect_equal(DBI::dbGetQuery(con, "SELECT taxon_key FROM dataset_taxon")$taxon_key, "itis:174715")
  tx <- DBI::dbGetQuery(con, "SELECT taxon_key, worms_id, class FROM taxon WHERE taxon_key = 'itis:174715'")
  expect_equal(tx$class, "Aves")
  expect_equal(tx$worms_id, 137179L)               # the cross-reference survives
  expect_false(any(grepl("^worms:1371", DBI::dbGetQuery(con, "SELECT taxon_key FROM taxon")$taxon_key)))
})

test_that("a non-bird with both ids stages its WoRMS chain and keys worms:", {
  skip_if_not_installed("duckdb")
  d <- withr::local_tempdir()
  p <- lineage_fixture_csv(d)
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))
  append_dataset_taxon(con, "demo_ds", data.frame(
    ds_taxa_code = "SARD", ds_scientific_name = "Sardinops sagax",
    worms_id = 217452L, itis_id = 161729L, stringsAsFactors = FALSE))
  ensure_taxon_lineage(con, NULL, NULL, cache_csv = p, verbose = FALSE)
  flat <- DBI::dbGetQuery(con, "SELECT * FROM _taxon_lineage_flat")
  expect_true(217452L %in% flat$requested_id[flat$authority == "WoRMS"])
  expect_false(161729L %in% flat$requested_id)    # ITIS never asked: not Aves
  resolve_dataset_taxon(con)
  expect_equal(DBI::dbGetQuery(con, "SELECT taxon_key FROM dataset_taxon")$taxon_key, "worms:217452")
})

test_that("a hierarchy already in the connection is merged, not replaced", {
  skip_if_not_installed("duckdb")
  d <- withr::local_tempdir()
  p <- lineage_fixture_csv(d)
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  # swfsc_ichthyo builds its own via build_taxon_hierarchy() and must keep it
  DBI::dbExecute(con, "CREATE TABLE taxon AS
    SELECT 'WoRMS' authority, 999999 taxonID, NULL::INTEGER parentNameUsageID,
           'Preexisting taxon' scientificName, 'Species' taxonRank,
           'accepted' taxonomicStatus, NULL::VARCHAR scientificNameAuthorship")
  DBI::dbExecute(con, "CREATE TABLE species AS
    SELECT 19::SMALLINT species_id, 'Sardinops sagax' scientific_name,
           'Pacific sardine' common_name, 217452 worms_id, NULL itis_id, NULL gbif_id")

  ensure_taxon_lineage(con, NULL, NULL, cache_csv = p, verbose = FALSE)
  h <- DBI::dbGetQuery(con, "SELECT taxonID, scientificName FROM taxon")
  expect_true(999999L %in% h$taxonID)     # kept
  expect_true(217452L %in% h$taxonID)     # gained
})

test_that("unresolvable ids stay bare rather than aborting the rest", {
  d <- withr::local_tempdir()
  p <- lineage_fixture_csv(d)
  # ask for a cached id plus one that is not cached; with no network the second
  # simply does not resolve, and the first still comes back
  lin <- suppressMessages(fetch_taxon_lineage(
    217452L, integer(), cache_csv = p, verbose = FALSE))
  expect_true(217452L %in% lin$requested_id)
  expect_equal(nrow(lin), 4L)    # its own chain, whole — and only its own
})

test_that("only the REQUESTED ids come back, not the whole cache", {
  # regression: the cache is global and grows across datasets, so returning all
  # of it put every dataset's lineage into every shard — calcofi_phyllosoma went
  # from 1 taxon to 2,101. It looked correct on swfsc_ichthyo only because that
  # notebook prunes afterwards.
  d <- withr::local_tempdir()
  p <- lineage_fixture_csv(d)   # cache holds 217452, 440388 (WoRMS) + 174715 (ITIS)

  lin <- fetch_taxon_lineage(440388L, integer(), cache_csv = p, verbose = FALSE)
  expect_equal(unique(lin$requested_id), 440388L)
  expect_false(217452L %in% lin$requested_id)
  expect_false(174715L %in% lin$requested_id)

  # asking for the WoRMS id must not drag in the ITIS one, and vice versa
  lin_i <- fetch_taxon_lineage(integer(), 174715L, cache_csv = p, verbose = FALSE)
  expect_equal(unique(lin_i$authority), "ITIS")

  # and the cache file itself is left whole
  still <- utils::read.csv(p, stringsAsFactors = FALSE)
  expect_setequal(unique(still$requested_id), c(217452L, 440388L, 174715L))
})


# Ancestors are real taxa a consumer can select and roll up on. .lineage_flat()
# used to emit only the REQUESTED ids, so every ancestor reached the release with
# a key, a name and a rank and no classification at all.

test_that(".lineage_flat classifies ancestors, not just the requested taxa", {
  # Sardinops sagax's chain, root -> self, deliberately NOT in depth order (the
  # cache is sorted by taxonID, which is what broke any positional assumption)
  lin <- data.frame(
    requested_id      = 217452L,
    authority         = "WoRMS",
    taxonID           = c(125464L, 2L, 217452L, 1821L, 152352L),
    parentNameUsageID = c(152352L, NA, 125464L, 2L, 1821L),
    scientificName    = c("Sardinops", "Animalia", "Sardinops sagax",
                          "Chordata", "Clupeidae"),
    taxonRank         = c("Genus", "Kingdom", "Species", "Phylum", "Family"),
    stringsAsFactors  = FALSE)

  f <- calcofi4db:::.lineage_flat(lin)
  expect_equal(nrow(f), 5L)                        # one row per taxon, not one
  row <- function(id) f[f$requested_id == id, , drop = FALSE]

  # the requested taxon, as before
  expect_equal(row(217452L)$family,  "Clupeidae")
  expect_equal(row(217452L)$kingdom, "Animalia")
  expect_equal(row(217452L)$rank,    "Species")

  # the ANCESTORS now carry their own classification
  expect_equal(row(125464L)$family,  "Clupeidae")   # genus is in the family
  expect_equal(row(125464L)$kingdom, "Animalia")
  expect_equal(row(152352L)$family,  "Clupeidae")   # the family is its own family
  expect_equal(row(152352L)$phylum,  "Chordata")

  # ...and a taxon ABOVE family rank correctly has none
  expect_true(is.na(row(1821L)$family))             # Chordata
  expect_equal(row(1821L)$kingdom, "Animalia")
  expect_true(is.na(row(2L)$phylum))                # Animalia, the root
})

test_that(".lineage_flat aliases a requested id the authority deprecated", {
  # requested 174553 (Puffinus griseus) but ITIS answered with 1255050
  lin <- data.frame(
    requested_id      = 174553L,
    authority         = "ITIS",
    taxonID           = c(202423L, 1255018L, 1255050L, 174532L),
    parentNameUsageID = c(NA, 174532L, 1255018L, 202423L),
    scientificName    = c("Animalia", "Ardenna", "Ardenna grisea", "Procellariidae"),
    taxonRank         = c("Kingdom", "Genus", "Species", "Family"),
    stringsAsFactors  = FALSE)

  f <- calcofi4db:::.lineage_flat(lin)
  ali <- f[f$requested_id == 174553L, , drop = FALSE]
  expect_equal(nrow(ali), 1L)
  expect_equal(ali$scientific_name, "Ardenna grisea")   # the chain's leaf
  expect_equal(ali$family, "Procellariidae")
  # the accepted id keeps its own row too
  expect_equal(f$family[f$requested_id == 1255050L], "Procellariidae")
})
