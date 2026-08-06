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
  dataset_key    = c("farallon_bird-mammal", "calcofi_phytoplankton"),
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
  expect_equal(key("farallon_bird-mammal:GRCO")$taxon_key, "itis:174715")
  expect_equal(key("farallon_bird-mammal:BLWH")$taxon_key, "worms:137090")
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

# prune_taxon_shard ------------------------------------------------------------

test_that("prune_taxon_shard keeps this dataset's vocabulary plus its ancestors", {
  con <- new_taxa_fixture()
  on.exit(close_duckdb(con))

  build_taxon_reference(con, taxa_measurement(), taxa_overrides())
  build_dataset_taxon(con,   taxa_measurement(), taxa_overrides())
  build_taxon_group(con,     taxa_measurement(), taxa_overrides())

  before <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM dataset_taxon")$n
  dangling_before <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) n FROM taxon c LEFT JOIN taxon p ON c.parent_taxon_key = p.taxon_key
    WHERE c.parent_taxon_key IS NOT NULL AND p.taxon_key IS NULL")$n
  expect_gt(length(unique(DBI::dbGetQuery(
    con, "SELECT dataset_key FROM dataset_taxon")$dataset_key)), 1L)

  n <- prune_taxon_shard(con, "swfsc_ichthyo")

  ds <- DBI::dbGetQuery(con, "SELECT dataset_key, taxon_key FROM dataset_taxon")
  expect_equal(unique(ds$dataset_key), "swfsc_ichthyo")
  expect_lt(nrow(ds), before)
  expect_equal(n$dataset_taxon, nrow(ds))

  # the sardine's WoRMS lineage ancestor (Sardinops, 125464) is NOT in
  # dataset_taxon but must survive, or descendant expansion breaks its chain
  tx <- DBI::dbGetQuery(con, "SELECT taxon_key FROM taxon ORDER BY taxon_key")$taxon_key
  expect_true("worms:217452" %in% tx)   # directly referenced
  expect_true("worms:125464" %in% tx)   # ancestor only
  # another dataset's taxon is gone
  expect_false("worms:146421" %in% tx)  # Appendicularia (zoodb/zooscan)
  expect_false("itis:174715"  %in% tx)  # Great Cormorant (bird_mammal)

  # pruning must not ORPHAN a parent that was resolvable before it. (The fixture
  # starts with one dangling parent of its own: Sardinops' parent 125463 is not in
  # the synthetic hierarchy at all, so it could never be kept.)
  expect_equal(DBI::dbGetQuery(con, "
    SELECT COUNT(*) n FROM taxon c LEFT JOIN taxon p ON c.parent_taxon_key = p.taxon_key
    WHERE c.parent_taxon_key IS NOT NULL AND p.taxon_key IS NULL")$n, dangling_before)

  # taxon_group is trimmed to the surviving taxa
  expect_equal(DBI::dbGetQuery(con, "
    SELECT COUNT(*) n FROM taxon_group g LEFT JOIN taxon t USING (taxon_key)
    WHERE t.taxon_key IS NULL")$n, 0L)
})

test_that("prune_taxon_shard errors rather than silently no-op without the refs", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))
  expect_error(prune_taxon_shard(con, "swfsc_ichthyo"), "dataset_taxon")
})


# taxon_override.csv is generic — the declared match_column is honored, and a row
# nobody can claim is an error rather than a silent no-op. It used to be read for
# only 2 of the 7 arms, with `match_column` never consulted anywhere in R/.

test_that("an override reaches an arm that was previously never consulted", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  ov <- rbind(taxa_overrides(), data.frame(
    dataset_key = "cce-lter_zoodb", match_column = "taxon_id",
    match_value = "3", worms_id = 111111L, itis_id = NA_integer_,
    scientific_name = "Appendicularia", rank = "Class", stringsAsFactors = FALSE))
  build_dataset_taxon(con, overrides = ov)
  dt <- DBI::dbGetQuery(con, "SELECT * FROM dataset_taxon")
  expect_equal(dt$taxon_key[dt$ds_taxon_key == "cce-lter_zoodb:3"], "worms:111111")
})

test_that("an override naming a match_column the source lacks errors", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  ov <- data.frame(
    dataset_key = "farallon_bird-mammal", match_column = "specieds_code",  # typo
    match_value = "BLWH", worms_id = 137090L, itis_id = NA_integer_,
    scientific_name = "Balaenoptera musculus", rank = "Species",
    stringsAsFactors = FALSE)
  expect_error(build_dataset_taxon(con, overrides = ov), "match_column")
})

test_that("an override naming an unknown dataset_key errors", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  ov <- data.frame(
    dataset_key = "sio_mesopelagic_fish",   # underscore, not hyphen
    match_column = "scientific_name", match_value = "Bathophilus sp.",
    worms_id = 126203L, itis_id = NA_integer_,
    scientific_name = "Bathophilus", rank = "Genus", stringsAsFactors = FALSE)
  expect_error(build_dataset_taxon(con, overrides = ov), "match no taxon source")
})

test_that("an override for a dataset absent from THIS connection is fine", {
  # every ingest reads the whole registry while loading only its own vocabulary
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  ov <- rbind(taxa_overrides(), data.frame(
    dataset_key = "sio_mesopelagic-fish", match_column = "scientific_name",
    match_value = "Bathophilus sp.", worms_id = 126203L, itis_id = NA_integer_,
    scientific_name = "Bathophilus", rank = "Genus", stringsAsFactors = FALSE))
  expect_no_error(build_dataset_taxon(con, overrides = ov))
})

test_that("check_taxon_ids gates unresolved taxa but allows declared ones", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  # a non-taxonomic operational class: no id anywhere -> dataset-local key
  DBI::dbExecute(con, "INSERT INTO zooscan_taxon
    SELECT 16,'nauplii',NULL,NULL,NULL,NULL")
  build_dataset_taxon(con, overrides = taxa_overrides())
  build_taxon_reference(con, overrides = taxa_overrides())

  expect_error(check_taxon_ids(con, verbose = FALSE), "cce-lter_zooscan:16")
  rpt <- check_taxon_ids(con, allow = "cce-lter_zooscan:16", verbose = FALSE)
  expect_true(rpt$n_local_key[rpt$dataset_key == "cce-lter_zooscan"] == 1L)
  expect_true(all(rpt$n_taxa > 0))
})


# rank_order used to come ONLY from a `taxa_rank` table that a single ingest
# built, so every other dataset's taxa — all 169 ITIS-keyed ones among them —
# released with it NULL. taxa_rank_reference() is the floor now.

test_that("taxa_rank_reference orders both authorities' rank vocabularies", {
  rr <- taxa_rank_reference()
  expect_true(all(c("taxonRank", "rank_order") %in% names(rr)))
  expect_equal(anyDuplicated(rr$taxonRank), 0L)          # one row per rank, or joins fan out
  expect_false(is.unsorted(rr$rank_order))
  ord <- function(x) rr$rank_order[match(x, rr$taxonRank)]
  expect_lt(ord("Kingdom"), ord("Phylum"))
  expect_lt(ord("Family"),  ord("Genus"))
  expect_lt(ord("Genus"),   ord("Species"))
  expect_lt(ord("Species"), ord("Subspecies"))
  # the ITIS/WoRMS ranks the release carries that the old inline vector lacked
  for (r in c("Gigaclass", "Infrakingdom", "Megaclass", "Parvphylum",
              "Phylum (Division)", "Subphylum (Subdivision)", "Subterclass",
              "Superdomain", "Section", "Subsection"))
    expect_false(is.na(ord(r)), info = r)
  # WoRMS nests Section/Subsection below Infraorder for decapods (Brachyura >
  # Eubrachyura > Heterotremata > Cancroidea), not between order and family
  expect_lt(ord("Infraorder"),  ord("Section"))
  expect_lt(ord("Section"),     ord("Subsection"))
  expect_lt(ord("Subsection"),  ord("Superfamily"))
})

test_that("taxa_rank_reference covers every rank the release actually carries", {
  # the vocabulary is only useful if it is COMPLETE — a rank it lacks silently
  # releases with rank_order NULL, which is how 100% of ITIS taxa went unnoticed
  release_ranks <- c(
    "Class", "Family", "Genus", "Gigaclass", "Infraclass", "Infrakingdom",
    "Infraorder", "Infraphylum", "Kingdom", "Megaclass", "Order", "Parvphylum",
    "Phylum", "Phylum (Division)", "Section", "Species", "Subclass",
    "Subfamily", "Subkingdom", "Suborder", "Subphylum",
    "Subphylum (Subdivision)", "Subsection", "Subspecies", "Subterclass",
    "Superclass", "Superdomain", "Superfamily", "Superorder", "Tribe")
  expect_equal(setdiff(release_ranks, taxa_rank_reference()$taxonRank), character(0))
})

test_that("rank_order is populated without a taxa_rank table in the connection", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  DBI::dbExecute(con, "DROP TABLE taxa_rank")     # the non-ichthyo case
  build_taxon_reference(con, overrides = taxa_overrides())
  tx <- DBI::dbGetQuery(con, "SELECT rank, rank_order FROM taxon WHERE rank IS NOT NULL")

  expect_gt(nrow(tx), 0)
  expect_true(all(!is.na(tx$rank_order)))
  expect_lt(tx$rank_order[tx$rank == "Genus"][1], tx$rank_order[tx$rank == "Species"][1])
})

test_that("a connection-local taxa_rank still wins, and never fans out", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  # a rank carrying BOTH an order and a NULL is what doubled every tree row
  DBI::dbExecute(con, "INSERT INTO taxa_rank SELECT 'Species', NULL")
  build_taxon_reference(con, overrides = taxa_overrides())
  tx <- DBI::dbGetQuery(con, "SELECT taxon_key, rank, rank_order FROM taxon")

  expect_equal(anyDuplicated(tx$taxon_key), 0L)        # no fan-out
  # `which()`, not a bare logical: a taxon with rank NA (in the species list but
  # not the hierarchy) subsets as NA and would smuggle an NA into the comparison
  expect_equal(unique(tx$rank_order[which(tx$rank == "Species")]), 260L)
})
