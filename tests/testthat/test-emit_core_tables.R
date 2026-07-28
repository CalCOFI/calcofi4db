
test_that("emit_core_tables projects a bio dataset without a prebuilt dataset_taxon", {
  skip_if_not_installed("duckdb")

  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE euphausiids_tow AS
    SELECT 1 tow_id, 'st1-ln1' grid_key, '2015-04-33RR' cruise_key,
           33.0 latitude, -120.0 longitude, '090.0 060.0' site_key,
           TIMESTAMP '2015-04-05 09:13:00' datetime_start_utc")
  DBI::dbExecute(con, "CREATE TABLE euphausiids_measurement AS
    SELECT 1 tow_id, 3 taxon_id, 'adult' life_stage,
           'euphausiid_abundance' measurement_type, 12.5 measurement_value,
           NULL::VARCHAR measurement_qual")

  # dataset_taxon is built centrally by the release, not by the ingest
  expect_false("dataset_taxon" %in% DBI::dbListTables(con))

  core <- emit_core_tables(con, "cce-lter_euphausiids")
  expect_equal(core$obs, 1L)

  got <- DBI::dbGetQuery(con,
    "SELECT realm, dataset_key, sample_key, life_stage, taxon_key,
            measurement_type, measurement_value FROM obs")
  expect_equal(got$realm, "bio")
  expect_equal(got$sample_key, "cce-lter_euphausiids:tow:1")
  # life_stage now rides the headline (species x stage grain)
  expect_equal(got$life_stage, "adult")
  # unresolved locally, filled in at release time
  expect_true(is.na(got$taxon_key))
})

test_that("emit_core_tables resolves taxon_key when dataset_taxon is present", {
  skip_if_not_installed("duckdb")

  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE euphausiids_tow AS
    SELECT 1 tow_id, 'st1-ln1' grid_key, '2015-04-33RR' cruise_key,
           33.0 latitude, -120.0 longitude, '090.0 060.0' site_key,
           TIMESTAMP '2015-04-05 09:13:00' datetime_start_utc")
  DBI::dbExecute(con, "CREATE TABLE euphausiids_measurement AS
    SELECT 1 tow_id, 3 taxon_id, 'furcilia F3' life_stage,
           'euphausiid_abundance' measurement_type, 12.5 measurement_value,
           NULL::VARCHAR measurement_qual")
  DBI::dbExecute(con, "CREATE TABLE dataset_taxon AS
    SELECT 'cce-lter_euphausiids:3' ds_taxon_key,
           'cce-lter_euphausiids' dataset_key, 'worms:110673' taxon_key,
           'Euphausia pacifica' ds_scientific_name,
           NULL::VARCHAR ds_common_name, '3' ds_taxa_code")

  emit_core_tables(con, "cce-lter_euphausiids")

  got <- DBI::dbGetQuery(con, "SELECT taxon_key, life_stage FROM obs")
  expect_equal(got$taxon_key, "worms:110673")
  expect_equal(got$life_stage, "furcilia F3")
})
