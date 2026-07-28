# regression tests for the arms realigned with the validated release projection.
# each of these encodes a grain/decomposition rule that had drifted between
# .obs_arm_sql() and the inline arms in release_database.qmd; a break here means
# the two projections have separated again.

# bird_mammal: the headline is per (transect, species) --------------------------
# the source records one row per (transect, species, BEHAVIOR). Riding behavior
# on the obs headline counted the same birds once per behavior code.

new_bird_fixture <- function() {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  DBI::dbExecute(con, "CREATE TABLE bird_mammal_transect AS
    SELECT 'CAC19870502F10'::VARCHAR gis_key, 'st1-ln1'::VARCHAR grid_key,
           '1987-05-33CC'::VARCHAR cruise_key, 33.1::DOUBLE latitude, -118.4::DOUBLE longitude,
           TIMESTAMP '1987-05-02 09:00:00' datetime_start_utc")
  # one species seen under two behaviors (3 + 5) plus a second species (2)
  DBI::dbExecute(con, "CREATE TABLE bird_mammal_observation AS
    SELECT 1::INTEGER observation_id, 'CAC19870502F10'::VARCHAR gis_key,
           'LHSP'::VARCHAR species_code, 1::INTEGER behavior_code, 3::INTEGER count
    UNION ALL SELECT 2,'CAC19870502F10','LHSP',2,5
    UNION ALL SELECT 3,'CAC19870502F10','UNSP',1,2")
  DBI::dbExecute(con, "CREATE TABLE bird_mammal_behavior AS
    SELECT 1::INTEGER behavior_code, 'Flying'::VARCHAR description
    UNION ALL SELECT 2,'Sitting on water'")
  DBI::dbExecute(con, "CREATE TABLE dataset_taxon AS
    SELECT 'calcofi_bird_mammal_census:LHSP'::VARCHAR ds_taxon_key,
           'calcofi_bird_mammal_census'::VARCHAR dataset_key,
           'itis:176754'::VARCHAR taxon_key, 'Hydrobates leucorhous'::VARCHAR ds_scientific_name,
           'Leach''s Storm-Petrel'::VARCHAR ds_common_name, 'LHSP'::VARCHAR ds_taxa_code
    UNION ALL SELECT 'calcofi_bird_mammal_census:UNSP','calcofi_bird_mammal_census',
           'itis:174371','Aves','Unidentified storm-petrel','UNSP'")
  con
}

test_that("bird_mammal obs headline sums counts across behaviors, one row per species", {
  con <- new_bird_fixture()
  on.exit(close_duckdb(con))

  emit_core_tables(con, "calcofi_bird_mammal_census", taxa = FALSE)

  o <- DBI::dbGetQuery(con,
    "SELECT taxon_key, measurement_type, measurement_value, life_stage
     FROM obs ORDER BY taxon_key")
  # 3 source rows -> 2 headline rows (one per species), NOT 3
  expect_equal(nrow(o), 2L)
  expect_equal(o$measurement_type, c("count", "count"))
  # LHSP: 3 (flying) + 5 (sitting) = 8, counted once
  expect_equal(o$measurement_value[o$taxon_key == "itis:176754"], 8)
  expect_equal(o$measurement_value[o$taxon_key == "itis:174371"], 2)
  # behavior must NOT ride on the headline
  expect_true(all(is.na(o$life_stage)))
})

test_that("bird_mammal keeps unresolved species apart instead of merging them", {
  # only 156 of the 207 observed species codes resolve to a taxon. Grouping the
  # headline by taxon_key alone would sum every unresolved species into ONE
  # NULL-taxon row per transect, merging distinct species.
  con <- new_bird_fixture()
  on.exit(close_duckdb(con))

  # two more species on the same transect, neither present in dataset_taxon
  DBI::dbExecute(con, "INSERT INTO bird_mammal_observation VALUES
    (4, 'CAC19870502F10', 'XXXX', 1, 7),
    (5, 'CAC19870502F10', 'YYYY', 1, 11)")

  emit_core_tables(con, "calcofi_bird_mammal_census", taxa = FALSE)

  o <- DBI::dbGetQuery(con,
    "SELECT taxon_key, measurement_value FROM obs WHERE taxon_key IS NULL
     ORDER BY measurement_value")
  # two unresolved species -> two rows (7 and 11), NOT one row of 18
  expect_equal(nrow(o), 2L)
  expect_equal(o$measurement_value, c(7, 11))
  expect_false(18 %in% o$measurement_value)
})

test_that("bird_mammal behavior breakdown lands in obs_attribute with its label", {
  con <- new_bird_fixture()
  on.exit(close_duckdb(con))

  core <- emit_core_tables(con, "calcofi_bird_mammal_census", taxa = FALSE)
  expect_equal(core$obs_attribute, 3L)   # one per source (transect, species, behavior)

  a <- DBI::dbGetQuery(con,
    "SELECT taxon_key, measurement_type, bin_label, count FROM obs_attribute
     ORDER BY taxon_key, bin_label")
  expect_true(all(a$measurement_type == "behavior"))
  expect_equal(sort(a$bin_label), sort(c("Flying", "Sitting on water", "Flying")))
  # the attribute counts must reconstruct the headline exactly
  tot <- DBI::dbGetQuery(con, "
    SELECT o.taxon_key, o.measurement_value headline, SUM(a.count) attributed
    FROM obs o JOIN obs_attribute a USING (sample_key, taxon_key)
    GROUP BY 1, 2")
  expect_equal(tot$headline, tot$attributed)
})

# phytoplankton: region-pooled arm existed only in the release ------------------

test_that("phytoplankton projects into obs at region_pool grain with NULL grid_key", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE phyto_sample AS
    SELECT 1::INTEGER phyto_sample_id, '2018-04-33RR'::VARCHAR cruise_key,
           'north'::VARCHAR region_key, 33.5::DOUBLE latitude, -120.2::DOUBLE longitude")
  DBI::dbExecute(con, "CREATE TABLE phyto_measurement AS
    SELECT 1::INTEGER phyto_measurement_id, 1::INTEGER phyto_sample_id,
           'PH01'::VARCHAR species_code, 'cell_abundance'::VARCHAR measurement_type,
           1500.0::DOUBLE measurement_value")
  DBI::dbExecute(con, "CREATE TABLE dataset_taxon AS
    SELECT 'calcofi_phytoplankton:PH01'::VARCHAR ds_taxon_key,
           'calcofi_phytoplankton'::VARCHAR dataset_key, 'worms:163025'::VARCHAR taxon_key,
           'Chaetoceros'::VARCHAR ds_scientific_name, 'diatom, centric'::VARCHAR ds_common_name,
           'PH01'::VARCHAR ds_taxa_code")

  core <- emit_core_tables(con, "calcofi_phytoplankton", taxa = FALSE)
  expect_equal(core$sample, 1L)
  expect_equal(core$obs, 1L)          # was 0 — the arm was missing entirely

  o <- DBI::dbGetQuery(con,
    "SELECT realm, grid_key, cruise_key, datetime, taxon_key, measurement_value FROM obs")
  expect_equal(o$realm, "bio")
  expect_true(is.na(o$grid_key))      # region-pooled: no station grid cell
  expect_true(is.na(o$datetime))
  expect_equal(o$cruise_key, "2018-04-33RR")
  expect_equal(o$taxon_key, "worms:163025")

  s <- DBI::dbGetQuery(con, "SELECT sample_key, sample_type FROM sample")
  expect_equal(s$sample_key, "calcofi_phytoplankton:region_pool:1")
  expect_equal(s$sample_type, "region_pool")
})

# cufes / phyllosoma: taxon baked into the measurement_type name ----------------

test_that("cufes decomposes the taxon out of the measurement_type name", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE cufes_sample AS
    SELECT 1::INTEGER sample_id, 'st1-ln1'::VARCHAR grid_key, '2015-04-33RR'::VARCHAR cruise_key,
           33.0::DOUBLE latitude, -119.0::DOUBLE longitude,
           TIMESTAMP '2015-04-05 12:00:00' datetime_start_utc")
  DBI::dbExecute(con, "CREATE TABLE cufes_measurement AS
    SELECT 1::INTEGER cufes_measurement_id, 1::INTEGER sample_id,
           'sardine_eggs'::VARCHAR measurement_type, 12.0::DOUBLE measurement_value,
           NULL::VARCHAR measurement_qual
    UNION ALL SELECT 2,1,'anchovy_eggs',4.0,NULL")

  mt <- data.frame(
    dataset_key           = "swfsc_cufes",
    raw_measurement_type  = c("sardine_eggs", "anchovy_eggs"),
    target                = "obs",
    measurement_type      = "abundance",
    taxon_scientific_name = c("Sardinops sagax", "Engraulis mordax"),
    worms_id              = c(127023L, 127160L),
    itis_id               = NA_integer_,
    life_stage            = "egg",
    bin_value             = NA_real_,
    stringsAsFactors      = FALSE)

  core <- emit_core_tables(con, "swfsc_cufes", measurement_taxon = mt, taxa = FALSE)
  expect_equal(core$obs, 2L)

  o <- DBI::dbGetQuery(con,
    "SELECT taxon_key, measurement_type, life_stage, measurement_value
     FROM obs ORDER BY measurement_value")
  # the raw type name is replaced by the canonical type + a real taxon_key
  expect_equal(o$measurement_type, c("abundance", "abundance"))
  expect_equal(o$taxon_key, c("worms:127160", "worms:127023"))
  expect_equal(o$life_stage, c("egg", "egg"))
})

test_that("phyllosoma splits the total (obs) from the stage bins (obs_attribute)", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE phyllosoma_tow AS
    SELECT 1::INTEGER tow_id, 'st1-ln1'::VARCHAR grid_key, '1960-05-31CC'::VARCHAR cruise_key,
           32.5::DOUBLE latitude, -118.0::DOUBLE longitude,
           TIMESTAMP '1960-05-11 03:00:00' datetime_start_utc, 140.0::DOUBLE max_tow_depth_m")
  DBI::dbExecute(con, "CREATE TABLE phyllosoma_measurement AS
    SELECT 1::INTEGER phyllosoma_measurement_id, 1::INTEGER tow_id,
           'total_phyllosoma'::VARCHAR measurement_type, 9.0::DOUBLE measurement_value,
           NULL::VARCHAR measurement_qual
    UNION ALL SELECT 2,1,'phyllosoma_stage_1',4.0,NULL
    UNION ALL SELECT 3,1,'phyllosoma_stage_2',5.0,NULL
    UNION ALL SELECT 4,1,'phyllosoma_stage_3',0.0,NULL")

  mt <- data.frame(
    dataset_key           = "calcofi_phyllosoma",
    raw_measurement_type  = c("total_phyllosoma", "phyllosoma_stage_1",
                              "phyllosoma_stage_2", "phyllosoma_stage_3"),
    target                = c("obs", "attribute", "attribute", "attribute"),
    measurement_type      = c("abundance", "stage", "stage", "stage"),
    taxon_scientific_name = "Panulirus interruptus",
    worms_id              = 396116L,
    itis_id               = NA_integer_,
    life_stage            = "phyllosoma",
    bin_value             = c(NA, 1, 2, 3),
    stringsAsFactors      = FALSE)

  core <- emit_core_tables(con, "calcofi_phyllosoma", measurement_taxon = mt, taxa = FALSE)
  # only the total is the headline; the three stage rows are attribution
  expect_equal(core$obs, 1L)
  o <- DBI::dbGetQuery(con, "SELECT measurement_type, measurement_value, taxon_key FROM obs")
  expect_equal(o$measurement_type, "abundance")
  expect_equal(o$measurement_value, 9)
  expect_equal(o$taxon_key, "worms:396116")

  # zero-count stage bins are dropped, so 2 of 3 survive
  expect_equal(core$obs_attribute, 2L)
  a <- DBI::dbGetQuery(con,
    "SELECT measurement_type, bin_value, count FROM obs_attribute ORDER BY bin_value")
  expect_equal(a$bin_value, c(1, 2))
  expect_equal(a$count, c(4L, 5L))
  expect_true(all(a$measurement_type == "stage"))
})

# euphausiids: guard the species-resolved BTEDB path ---------------------------
# the release arm still decomposed via measurement_taxon, which collapses every
# species to family Euphausiidae. This asserts the dataset_taxon path survives.

test_that("euphausiids obs keeps species x life-stage resolution", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE euphausiids_tow AS
    SELECT 1::INTEGER tow_id, 'st1-ln1'::VARCHAR grid_key, '1998-02-33JD'::VARCHAR cruise_key,
           32.8::DOUBLE latitude, -117.9::DOUBLE longitude,
           TIMESTAMP '1998-02-14 21:00:00' datetime_start_utc")
  DBI::dbExecute(con, "CREATE TABLE euphausiids_measurement AS
    SELECT 1::INTEGER euphausiids_measurement_id, 1::INTEGER tow_id, 5::INTEGER taxon_id,
           'adult'::VARCHAR life_stage, 'euphausiid_abundance'::VARCHAR measurement_type,
           0.844::DOUBLE measurement_value, NULL::VARCHAR measurement_qual
    UNION ALL SELECT 2,1,5,'juvenile','euphausiid_abundance',0.211,NULL
    UNION ALL SELECT 3,1,6,'adult','euphausiid_abundance',1.5,NULL")
  DBI::dbExecute(con, "CREATE TABLE dataset_taxon AS
    SELECT 'cce-lter_euphausiids:5'::VARCHAR ds_taxon_key,
           'cce-lter_euphausiids'::VARCHAR dataset_key, 'worms:110683'::VARCHAR taxon_key,
           'Euphausia brevis'::VARCHAR ds_scientific_name, NULL::VARCHAR ds_common_name,
           '5'::VARCHAR ds_taxa_code
    UNION ALL SELECT 'cce-lter_euphausiids:6','cce-lter_euphausiids','worms:221056',
           'Euphausia diomedeae',NULL,'6'")

  # a measurement_taxon registry that WOULD collapse everything to the family is
  # supplied on purpose: the arm must ignore it for this dataset.
  mt <- data.frame(
    dataset_key = "cce-lter_euphausiids", raw_measurement_type = "euphausiid_abundance",
    target = "obs", measurement_type = "abundance",
    taxon_scientific_name = "Euphausiidae", worms_id = 110513L, itis_id = NA_integer_,
    life_stage = NA_character_, bin_value = NA_real_, stringsAsFactors = FALSE)

  core <- emit_core_tables(con, "cce-lter_euphausiids", measurement_taxon = mt, taxa = FALSE)
  expect_equal(core$obs, 3L)   # one row per (tow, species, life_stage)

  o <- DBI::dbGetQuery(con,
    "SELECT taxon_key, life_stage, measurement_value FROM obs ORDER BY taxon_key, life_stage")
  expect_equal(sort(unique(o$taxon_key)), c("worms:110683", "worms:221056"))
  expect_false("worms:110513" %in% o$taxon_key)   # NOT flattened to the family
  expect_equal(sort(unique(o$life_stage)), c("adult", "juvenile"))
})

# _measurement_taxon plumbing --------------------------------------------------

test_that("composite arms project zero rows (not an error) with no registry", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE cufes_sample AS
    SELECT 1::INTEGER sample_id, 'st1-ln1'::VARCHAR grid_key, '2015-04-33RR'::VARCHAR cruise_key,
           33.0::DOUBLE latitude, -119.0::DOUBLE longitude,
           TIMESTAMP '2015-04-05 12:00:00' datetime_start_utc")
  DBI::dbExecute(con, "CREATE TABLE cufes_measurement AS
    SELECT 1::INTEGER cufes_measurement_id, 1::INTEGER sample_id,
           'sardine_eggs'::VARCHAR measurement_type, 12.0::DOUBLE measurement_value,
           NULL::VARCHAR measurement_qual")

  core <- emit_core_tables(con, "swfsc_cufes", taxa = FALSE)
  expect_equal(core$sample, 1L)
  expect_equal(core$obs, 0L)
})

test_that("_measurement_taxon is restricted to the emitting dataset", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  mt <- data.frame(
    dataset_key = c("swfsc_cufes", "calcofi_phyllosoma"),
    raw_measurement_type = c("sardine_eggs", "total_phyllosoma"),
    target = "obs", measurement_type = "abundance",
    taxon_scientific_name = c("Sardinops sagax", "Panulirus interruptus"),
    worms_id = c(127023L, 396116L), itis_id = NA_integer_,
    life_stage = c("egg", "phyllosoma"), bin_value = NA_real_,
    stringsAsFactors = FALSE)

  calcofi4db:::.ensure_measurement_taxon(con, mt, dataset_key = "swfsc_cufes")
  got <- DBI::dbGetQuery(con,
    "SELECT dataset_key, raw_measurement_type, taxon_key FROM _measurement_taxon")
  expect_equal(nrow(got), 1L)
  expect_equal(got$dataset_key, "swfsc_cufes")
  expect_equal(got$taxon_key, "worms:127023")
})

test_that("core_output_tables() lists only non-empty core shards", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE picoplankton_bacteria_bottle AS
    SELECT 1::INTEGER bottle_id, '2004-11-33RR'::VARCHAR cruise_key, 'st1-ln1'::VARCHAR grid_key,
           33.4::DOUBLE latitude, -118.1::DOUBLE longitude,
           TIMESTAMP '2004-11-02 18:20:00' datetime_utc, 20.0::DOUBLE depth_m")
  DBI::dbExecute(con, "CREATE TABLE picoplankton_bacteria_measurement AS
    SELECT 1::INTEGER measurement_id, 1::INTEGER bottle_id,
           'synechococcus'::VARCHAR measurement_type, 41000.0::DOUBLE measurement_value")

  emit_core_tables(con, "cce-lter_picoplankton-bacteria", taxa = FALSE)
  tbls <- core_output_tables(con)
  expect_true(all(c("sample", "obs") %in% tbls))
  # env-only dataset: no attribution, no effort, no taxa -> no empty parquet files
  expect_false("obs_attribute" %in% tbls)
  expect_false("sample_measurement" %in% tbls)
  expect_false("taxon" %in% tbls)
})
