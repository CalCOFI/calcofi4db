# synthetic fixture exercising the unified taxon model: a shared CalCOFI species
# list (fish) + WoRMS hierarchy, the same taxon (Appendicularia, AphiaID 146421)
# in BOTH zoodb and zooscan (dedup), a seabird (itis) + a marine mammal (worms via
# override), and a coarse phyto functional group (worms via override).
new_taxa_fixture <- function() {
  testthat::skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")

  DBI::dbExecute(con, "CREATE TABLE species AS
    SELECT 19::SMALLINT species_id, 'Sardinops sagax' scientific_name, 'Pacific sardine' common_name,
           217452 worms_id, 161729 itis_id, 999 gbif_id
    UNION ALL SELECT 31,'Engraulis mordax','Northern anchovy',272286,161828,888")
  DBI::dbExecute(con, "CREATE TABLE taxon AS
    SELECT 'WoRMS' authority, 217452 taxonID, 125464 parentNameUsageID,
           'Sardinops sagax' scientificName, 'Species' taxonRank, 'accepted' taxonomicStatus,
           NULL scientificNameAuthorship
    UNION ALL SELECT 'WoRMS',125464,125463,'Sardinops','Genus','accepted',NULL")
  DBI::dbExecute(con, "CREATE TABLE taxa_rank AS
    SELECT 'Species' taxonRank, 260::SMALLINT rank_order
    UNION ALL SELECT 'Genus',180 UNION ALL SELECT 'Class',80")
  # same AphiaID 146421 in both zooplankton datasets -> must collapse to one taxon
  DBI::dbExecute(con, "CREATE TABLE zoodb_taxon AS
    SELECT 3 taxon_id,'APPENDICULARIA' taxon_zoodb,146421 aphia_id,'Appendicularia' scientific_name,
           'Class' rank,'Animalia' kingdom")
  DBI::dbExecute(con, "CREATE TABLE zooscan_taxon AS
    SELECT 1 taxon_id,'appendicularia' taxon_zooscan,146421 aphia_id,'Appendicularia' scientific_name,
           'Class' rank,'Animalia' kingdom")
  # a seabird (keys on ITIS) + a marine mammal (keys on WoRMS via override)
  DBI::dbExecute(con, "CREATE TABLE bird_mammal_species AS
    SELECT 'GRCO' species_code,'Great Cormorant' common_name,'Phalacrocorax carbo' scientific_name,
           174715 itis_id, TRUE is_bird, FALSE is_mammal, FALSE is_fish, FALSE is_unidentified, TRUE include_flag
    UNION ALL SELECT 'BLWH','Blue Whale','Balaenoptera musculus',180528,FALSE,TRUE,FALSE,FALSE,TRUE")
  # a resolved phyto genus + a coarse functional group (NULL aphia -> override on `taxa`)
  DBI::dbExecute(con, "CREATE TABLE phyto_taxon AS
    SELECT '316' species_code,'diatom, centric' taxa,'x' species, NULL aphia_id,
           NULL scientific_name_accepted, NULL rank, NULL kingdom, NULL phylum
    UNION ALL SELECT '600','diatom, centric','y',196347,'Actinocyclus','Genus','Chromista','x'")
  con
}

taxa_overrides <- function() data.frame(
  dataset_key    = c("calcofi_bird_mammal_census", "calcofi_phytoplankton"),
  match_column   = c("species_code", "taxa"),
  match_value    = c("BLWH", "diatom, centric"),
  worms_id       = c(137090L, 148899L),
  itis_id        = c(NA_integer_, NA_integer_),
  scientific_name = c("Balaenoptera musculus", "Bacillariophyceae"),
  rank           = c("Species", "Class"),
  stringsAsFactors = FALSE)

taxa_measurement <- function() data.frame(
  dataset_key = "swfsc_cufes", raw_measurement_type = "sardine_eggs", target = "obs",
  measurement_type = "abundance", taxon_scientific_name = "Sardinops sagax",
  worms_id = 217452L, itis_id = NA_integer_, life_stage = "egg", bin_value = NA_real_,
  review = FALSE, note = "", stringsAsFactors = FALSE)


test_that("taxon_key_of applies the worms-default / Aves-itis rule", {
  expect_equal(taxon_key_of(217452L, 161729L), "worms:217452")           # fish -> worms
  expect_equal(taxon_key_of(137179L, 174715L, is_bird = TRUE), "itis:174715")  # bird -> itis
  expect_equal(taxon_key_of(NA_integer_, 174715L), "itis:174715")        # no worms -> itis
  expect_true(is.na(taxon_key_of(NA_integer_, NA_integer_)))             # neither -> NA
})

test_that("build_dataset_taxon mints prefixed ds_taxon_keys resolving to global taxon_key", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  build_dataset_taxon(con, measurement_taxon = taxa_measurement(), overrides = taxa_overrides())
  dt <- DBI::dbGetQuery(con, "SELECT * FROM dataset_taxon")

  key <- function(k) dt[dt$ds_taxon_key == k, , drop = FALSE]
  # shared CalCOFI species list -> "calcofi:<species_id>", used by swfsc_ichthyo
  expect_equal(key("calcofi:19")$taxon_key, "worms:217452")
  expect_equal(key("calcofi:19")$dataset_key, "swfsc_ichthyo")
  # both zooplankton datasets resolve Appendicularia to the SAME global key
  expect_equal(key("cce-lter_zoodb:3")$taxon_key,   "worms:146421")
  expect_equal(key("cce-lter_zooscan:1")$taxon_key, "worms:146421")
  # seabird -> itis:, marine mammal -> worms: (override)
  expect_equal(key("calcofi_bird_mammal_census:GRCO")$taxon_key, "itis:174715")
  expect_equal(key("calcofi_bird_mammal_census:BLWH")$taxon_key, "worms:137090")
  # coarse phyto functional group resolved via override
  expect_equal(key("calcofi_phytoplankton:316")$taxon_key, "worms:148899")
  # composite cufes egg type contributes a taxon crosswalk row too
  expect_true(any(dt$dataset_key == "swfsc_cufes" & dt$taxon_key == "worms:217452"))
})

test_that("build_taxon_reference dedups cross-dataset taxa and keeps the WoRMS lineage", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  build_taxon_reference(con, measurement_taxon = taxa_measurement(), overrides = taxa_overrides())
  tx <- DBI::dbGetQuery(con, "SELECT * FROM taxon")

  # Appendicularia (146421) appears exactly once despite being in zoodb AND zooscan
  expect_equal(sum(tx$taxon_key == "worms:146421"), 1L)
  # sardine row carries authoritative name/rank/rank_order + parent from the hierarchy
  sar <- tx[tx$taxon_key == "worms:217452", ]
  expect_equal(sar$scientific_name, "Sardinops sagax")
  expect_equal(sar$rank, "Species")
  expect_equal(sar$rank_order, 260)
  expect_equal(sar$parent_taxon_key, "worms:125464")
  # WoRMS ancestor (Sardinops genus) is present with its own parent link
  expect_equal(tx[tx$taxon_key == "worms:125464", ]$parent_taxon_key, "worms:125463")
  # seabird keyed on itis, mammal + phyto-class keyed on worms
  expect_true("itis:174715"  %in% tx$taxon_key)   # Great Cormorant
  expect_true("worms:137090" %in% tx$taxon_key)   # Blue Whale
  expect_true("worms:148899" %in% tx$taxon_key)   # Bacillariophyceae
  # placeholder id columns exist
  expect_true(all(c("gbif_id", "ncbi_id", "inat_id") %in% names(tx)))
})

test_that("every dataset_taxon.taxon_key resolves to a taxon row (FK integrity)", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  build_dataset_taxon(con, measurement_taxon = taxa_measurement(), overrides = taxa_overrides())
  build_taxon_reference(con, measurement_taxon = taxa_measurement(), overrides = taxa_overrides())
  orphans <- DBI::dbGetQuery(con,
    "SELECT COUNT(*) n FROM dataset_taxon dt
      LEFT JOIN taxon t USING (taxon_key) WHERE t.taxon_key IS NULL")$n
  expect_equal(orphans, 0L)
})

test_that("build_taxon_group seeds phyto + seabird/mammal groupings", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  build_taxon_group(con, overrides = taxa_overrides())
  g <- DBI::dbGetQuery(con, "SELECT * FROM taxon_group")

  expect_true(any(grepl("^calcofi_phytoplankton:diatom", g$taxon_group_key)))
  expect_true("calcofi:seabirds"       %in% g$taxon_group_key)
  expect_true("calcofi:marine_mammals" %in% g$taxon_group_key)
  # the mammal group holds the blue whale's global key
  mam <- g[g$taxon_group_key == "calcofi:marine_mammals", ]
  expect_true("worms:137090" %in% mam$taxon_key)
})
