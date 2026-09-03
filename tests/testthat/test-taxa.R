# synthetic fixture exercising the unified taxon model: a shared CalCOFI species
# list (fish) + WoRMS hierarchy, the same taxon (Appendicularia, AphiaID 146421)
# in BOTH zoodb and zooscan (dedup), a seabird (itis) + a marine mammal (worms via
# override), and a coarse phyto functional group (worms via override).
#
# The seven per-dataset arms still serve unstaged datasets in 3.29.0
# (coexistence with the staged path, see test-dataset_taxon.R). The key
# authority is decided by the CLASS from the staged lineage, so the fixture
# carries `_taxon_lineage_flat` for the taxa whose class matters.
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
  # the flattened classification ensure_taxon_lineage() stages: the class is
  # what decides the key authority (taxon plan D2)
  DBI::dbExecute(con, "CREATE TABLE _taxon_lineage_flat AS
    SELECT 174715 requested_id, 'ITIS' authority, 'Species' AS \"rank\", 174712 parent_id,
           'Phalacrocorax carbo' scientific_name, 'Animalia' kingdom, 'Chordata' phylum,
           'Aves' AS \"class\", 'Pelecaniformes' order_taxon, 'Phalacrocoracidae' AS \"family\"
    UNION ALL SELECT 137090,'WoRMS','Species',137013,'Balaenoptera musculus','Animalia','Chordata','Mammalia','Cetartiodactyla','Balaenopteridae'")
  con
}

taxa_overrides <- function() data.frame(
  dataset_key    = c("farallon_bird-mammal", "calcofi_phytoplankton"),
  match_column   = c("species_code", "species_code"),
  match_value    = c("BLWH", "316"),
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

# today's groups, as metadata/taxon_group.csv declares them
taxa_group_rules <- function() data.frame(
  taxon_group_key = c("calcofi:seabirds", "calcofi:marine_mammals",
                      "calcofi_phytoplankton:diatom_centric"),
  description = c("Seabirds", "Marine mammals", "Phytoplankton functional group: diatom, centric"),
  rule        = c("class", "class", "dataset_taxon"),
  rule_value  = c("Aves", "Mammalia", NA),
  dataset_key = c(NA, NA, "calcofi_phytoplankton"),
  match_column = c(NA, NA, "ds_common_name"),
  match_value  = c(NA, NA, "diatom, centric"),
  stringsAsFactors = FALSE)


test_that("taxon_key_of: itis: iff class Aves and a TSN; else worms:; else NA", {
  expect_equal(taxon_key_of(217452L, 161729L), "worms:217452")              # fish -> worms
  expect_equal(taxon_key_of(137179L, 174715L, class = "Aves"), "itis:174715")  # bird -> itis
  expect_equal(taxon_key_of(137179L, NA_integer_, class = "Aves"), "worms:137179")  # bird, no TSN
  expect_equal(taxon_key_of(137090L, 180528L, class = "Mammalia"), "worms:137090")  # mammal
  expect_true(is.na(taxon_key_of(NA_integer_, 174715L)))                   # only a TSN, not Aves
  expect_true(is.na(taxon_key_of(NA_integer_, 174715L, class = "Mammalia")))
  expect_true(is.na(taxon_key_of(NA_integer_, NA_integer_)))               # neither
  # vectorised, recycling the scalar class
  expect_equal(taxon_key_of(c(1L, NA), c(2L, 3L), class = c("Aves", "Aves")),
               c("itis:2", "itis:3"))
  expect_equal(taxon_key_of(c(1L, 4L), c(2L, 3L)), c("worms:1", "worms:4"))
  # zero-length in, zero-length out (the empty measurement_taxon case)
  expect_equal(taxon_key_of(integer(), integer(), character()), character())
})

test_that("resolve_dataset_taxon mints prefixed ds_taxon_keys resolving to global taxon_key", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  resolve_dataset_taxon(con, measurement_taxon = taxa_measurement(), overrides = taxa_overrides())
  dt <- DBI::dbGetQuery(con, "SELECT * FROM dataset_taxon")

  key <- function(k) dt[dt$ds_taxon_key == k, , drop = FALSE]
  # shared CalCOFI species list -> "calcofi:<species_id>", used by swfsc_ichthyo
  expect_equal(key("calcofi:19")$taxon_key, "worms:217452")
  expect_equal(key("calcofi:19")$dataset_key, "swfsc_ichthyo")
  # both zooplankton datasets resolve Appendicularia to the SAME global key
  expect_equal(key("cce-lter_zoodb:3")$taxon_key,   "worms:146421")
  expect_equal(key("cce-lter_zooscan:1")$taxon_key, "worms:146421")
  # seabird -> itis: (class Aves from the lineage), marine mammal -> worms: (override)
  expect_equal(key("farallon_bird-mammal:GRCO")$taxon_key, "itis:174715")
  expect_equal(key("farallon_bird-mammal:BLWH")$taxon_key, "worms:137090")
  # coarse phyto functional group resolved via override
  expect_equal(key("calcofi_phytoplankton:316")$taxon_key, "worms:148899")
  # composite cufes egg type contributes a taxon crosswalk row too
  expect_true(any(dt$dataset_key == "swfsc_cufes" & dt$taxon_key == "worms:217452"))
  # arm rows carry the source's claims too, so the released column is uniform
  expect_equal(key("calcofi:19")$ds_source_json,
               '{"worms_id":217452,"itis_id":161729,"gbif_id":999}')
  expect_equal(key("farallon_bird-mammal:GRCO")$ds_source_json, '{"itis_id":174715}')
  expect_equal(key("farallon_bird-mammal:BLWH")$ds_source_json, '{"itis_id":180528}')  # pre-override
  expect_true(is.na(key("calcofi_phytoplankton:316")$ds_source_json))
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
  # the class rides along from the lineage
  expect_equal(tx$class[tx$taxon_key == "itis:174715"], "Aves")
  # placeholder id columns exist
  expect_true(all(c("gbif_id", "ncbi_id", "inat_id") %in% names(tx)))
})

test_that("every dataset_taxon.taxon_key resolves to a taxon row (FK integrity)", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  resolve_dataset_taxon(con, measurement_taxon = taxa_measurement(), overrides = taxa_overrides())
  build_taxon_reference(con, measurement_taxon = taxa_measurement(), overrides = taxa_overrides())
  orphans <- DBI::dbGetQuery(con,
    "SELECT COUNT(*) n FROM dataset_taxon dt
      LEFT JOIN taxon t USING (taxon_key) WHERE t.taxon_key IS NULL")$n
  expect_equal(orphans, 0L)
})

# build_taxon_group ------------------------------------------------------------
# Groups come from metadata/taxon_group.csv (taxon plan D4): a `class` rule over
# the released classification, a `dataset_taxon` rule over one dataset's
# vocabulary. No dataset name in the package.

test_that("build_taxon_group applies class and dataset_taxon rules from the registry", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  build_taxon_reference(con, overrides = taxa_overrides())
  resolve_dataset_taxon(con, overrides = taxa_overrides())
  build_taxon_group(con, rules = taxa_group_rules())
  g <- DBI::dbGetQuery(con, "SELECT * FROM taxon_group ORDER BY taxon_group_key, taxon_key")

  expect_equal(g$taxon_key[g$taxon_group_key == "calcofi:seabirds"],       "itis:174715")
  expect_equal(g$taxon_key[g$taxon_group_key == "calcofi:marine_mammals"], "worms:137090")
  # both phyto rows carry taxa = 'diatom, centric' -> both in the functional group
  expect_setequal(g$taxon_key[g$taxon_group_key == "calcofi_phytoplankton:diatom_centric"],
                  c("worms:148899", "worms:196347"))
  expect_equal(g$description[g$taxon_group_key == "calcofi:seabirds"], "Seabirds")
})

test_that("a class rule only groups vocabulary taxa, never a bare lineage ancestor", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  # an Aves-class ancestor (the order) that no dataset observes
  DBI::dbExecute(con, "INSERT INTO _taxon_lineage_flat
    SELECT 174712,'ITIS','Order',174371,'Pelecaniformes','Animalia','Chordata','Aves','Pelecaniformes',NULL")
  build_taxon_reference(con, overrides = taxa_overrides())
  resolve_dataset_taxon(con, overrides = taxa_overrides())
  build_taxon_group(con, rules = taxa_group_rules())
  g <- DBI::dbGetQuery(con, "SELECT taxon_key FROM taxon_group WHERE taxon_group_key = 'calcofi:seabirds'")
  expect_equal(g$taxon_key, "itis:174715")
})

test_that("a group rule naming a match_column the vocabulary lacks errors; an absent dataset is skipped", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  build_taxon_reference(con, overrides = taxa_overrides())
  resolve_dataset_taxon(con, overrides = taxa_overrides())
  r <- taxa_group_rules(); r$match_column[3] <- "taxa"           # the old source column
  expect_error(build_taxon_group(con, rules = r), "taxa")
  r <- taxa_group_rules(); r$dataset_key[3] <- "sio_mesopelagic-fish"  # not in this connection
  expect_no_error(build_taxon_group(con, rules = r))
  g <- DBI::dbGetQuery(con, "SELECT DISTINCT taxon_group_key FROM taxon_group")
  expect_false("calcofi_phytoplankton:diatom_centric" %in% g$taxon_group_key)
})

test_that("read_taxon_group_rules validates the registry shape", {
  d <- withr::local_tempdir()
  p <- file.path(d, "taxon_group.csv")
  utils::write.csv(taxa_group_rules(), p, row.names = FALSE, na = "")
  r <- read_taxon_group_rules(p)
  expect_equal(nrow(r), 3L)
  expect_true(is.na(r$dataset_key[1]))          # empty cell -> NA, never the string "NA"
  bad <- taxa_group_rules(); bad$rule[1] <- "klass"
  utils::write.csv(bad, p, row.names = FALSE, na = "")
  expect_error(read_taxon_group_rules(p), "klass")
  bad <- taxa_group_rules(); bad$rule_value[1] <- NA
  utils::write.csv(bad, p, row.names = FALSE, na = "")
  expect_error(read_taxon_group_rules(p), "rule_value")
  bad <- taxa_group_rules(); bad$match_value[3] <- NA
  utils::write.csv(bad, p, row.names = FALSE, na = "")
  expect_error(read_taxon_group_rules(p), "match_value")
  expect_error(read_taxon_group_rules(file.path(d, "nope.csv")), "not found")
})

test_that("build_taxon_group requires the two references it groups over", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  expect_error(build_taxon_group(con, rules = taxa_group_rules()), "dataset_taxon")
})

test_that("the pre-3.29 positional call is deprecated, not silently misread", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  build_taxon_reference(con, overrides = taxa_overrides())
  resolve_dataset_taxon(con, overrides = taxa_overrides())
  # build_taxon_group(con, mt_taxon, tx_over): a measurement frame is not a rule set
  expect_error(
    suppressWarnings(build_taxon_group(con, taxa_measurement(), taxa_overrides())),
    "taxon_group.csv")
})

# prune_taxon_shard ------------------------------------------------------------

test_that("prune_taxon_shard keeps this dataset's vocabulary plus its ancestors", {
  con <- new_taxa_fixture()
  on.exit(close_duckdb(con))

  build_taxon_reference(con, taxa_measurement(), taxa_overrides())
  resolve_dataset_taxon(con, taxa_measurement(), taxa_overrides())
  build_taxon_group(con, rules = taxa_group_rules())

  before <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM dataset_taxon")$n
  # the taxa whose parent RESOLVES before the prune — none of the survivors may
  # lose that (the fixture also has dangling parents of its own, e.g. Sardinops'
  # 125463, which is not in the synthetic hierarchy and could never be kept)
  resolved_before <- DBI::dbGetQuery(con, "
    SELECT c.taxon_key FROM taxon c JOIN taxon p ON c.parent_taxon_key = p.taxon_key")$taxon_key
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

  # pruning must not ORPHAN a parent that was resolvable before it
  resolved_after <- DBI::dbGetQuery(con, "
    SELECT c.taxon_key FROM taxon c JOIN taxon p ON c.parent_taxon_key = p.taxon_key")$taxon_key
  expect_setequal(intersect(resolved_before, tx), resolved_after)

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


# taxon_override.csv is generic — the declared match_column is honored. There is
# no hard-coded list of datasets it may name (taxon plan D5): a row for a dataset
# absent from this connection is another ingest's business and is left alone; a
# row for a PRESENT dataset that names a column the vocabulary lacks errors. The
# release-time check_taxon_registries() is where a dataset nobody supplies fails.

test_that("an override reaches an arm that was previously never consulted", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  ov <- rbind(taxa_overrides(), data.frame(
    dataset_key = "cce-lter_zoodb", match_column = "taxon_id",
    match_value = "3", worms_id = 111111L, itis_id = NA_integer_,
    scientific_name = "Appendicularia", rank = "Class", stringsAsFactors = FALSE))
  resolve_dataset_taxon(con, overrides = ov)
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
  expect_error(resolve_dataset_taxon(con, overrides = ov), "match_column")
})

test_that("an override for a dataset absent from THIS connection is left to that dataset's ingest", {
  # every ingest reads the whole registry while loading only its own vocabulary
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  ov <- rbind(taxa_overrides(), data.frame(
    dataset_key = "sio_mesopelagic-fish", match_column = "scientific_name",
    match_value = "Bathophilus sp.", worms_id = 126203L, itis_id = NA_integer_,
    scientific_name = "Bathophilus", rank = "Genus", stringsAsFactors = FALSE))
  expect_no_error(resolve_dataset_taxon(con, overrides = ov))
  # ...and so is a misspelled one — per-ingest there is no list to check it
  # against; check_taxon_registries() catches it where every dataset is present
  ov$dataset_key[3] <- "sio_mesopelagic_fish"
  expect_no_error(resolve_dataset_taxon(con, overrides = ov))
  expect_error(check_taxon_registries(con, overrides = ov), "sio_mesopelagic_fish")
})

test_that("check_taxon_ids gates unresolved taxa but allows declared ones", {
  con <- new_taxa_fixture(); on.exit(close_duckdb(con))
  # a non-taxonomic operational class: no id anywhere -> dataset-local key
  DBI::dbExecute(con, "INSERT INTO zooscan_taxon
    SELECT 16,'nauplii',NULL,NULL,NULL,NULL")
  resolve_dataset_taxon(con, overrides = taxa_overrides())
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
