
test_that("mesopelagic-fish projects into sample + obs at tow grain", {
  skip_if_not_installed("duckdb")

  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE mesopelagic_fish_tow AS
    SELECT 1 tow_id, '2010-01-32NM' cruise_key, 'st1-ln1' grid_key, '090.0 060.0' site_key,
           32.9 latitude, -117.3 longitude,
           TIMESTAMP '2010-01-12 07:45:00' datetime_start_utc, 210.0 depth_m")
  DBI::dbExecute(con, "CREATE TABLE mesopelagic_fish_measurement AS
    SELECT 1 mesopelagic_fish_measurement_id, 1 tow_id,
           'Cyclothone acclinidens' scientific_name,
           'tally' measurement_type, 7.0 measurement_value")
  DBI::dbExecute(con, "CREATE TABLE dataset_taxon AS
    SELECT 'ucsd_sio_mesopelagic-fish:Cyclothone acclinidens' ds_taxon_key,
           'ucsd_sio_mesopelagic-fish' dataset_key, 'worms:272233' taxon_key,
           'Cyclothone acclinidens' ds_scientific_name,
           NULL::VARCHAR ds_common_name, 'Cyclothone acclinidens' ds_taxa_code")

  core <- emit_core_tables(con, "ucsd_sio_mesopelagic-fish")
  expect_equal(core$sample, 1L)
  expect_equal(core$obs, 1L)

  s <- DBI::dbGetQuery(con, "SELECT sample_key, sample_type, root_sample_key FROM sample")
  expect_equal(s$sample_key, "ucsd_sio_mesopelagic-fish:tow:1")
  expect_equal(s$sample_type, "tow")
  expect_equal(s$root_sample_key, s$sample_key)

  o <- DBI::dbGetQuery(con,
    "SELECT realm, taxon_key, measurement_type, depth_max_m, sample_key FROM obs")
  expect_equal(o$realm, "bio")
  expect_equal(o$taxon_key, "worms:272233")
  expect_equal(o$measurement_type, "tally")
  expect_equal(o$depth_max_m, 210)
  # obs.sample_key must FK into sample
  expect_true(o$sample_key %in% s$sample_key)
})

test_that("picoplankton-bacteria projects into sample + obs at bottle grain, env realm", {
  skip_if_not_installed("duckdb")

  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE picoplankton_bacteria_bottle AS
    SELECT 1 bottle_id, '2004-11-33RR' cruise_key, 'st1-ln1' grid_key, '090.0 060.0' site_key,
           33.4 latitude, -118.1 longitude,
           TIMESTAMP '2004-11-02 18:20:00' datetime_utc, 20.0 depth_m")
  DBI::dbExecute(con, "CREATE TABLE picoplankton_bacteria_measurement AS
    SELECT 1 measurement_id, 1 bottle_id, 'synechococcus' measurement_type,
           41000.0 measurement_value")

  core <- emit_core_tables(con, "cce-lter_picoplankton-bacteria")
  expect_equal(core$sample, 1L)
  expect_equal(core$obs, 1L)

  s <- DBI::dbGetQuery(con, "SELECT sample_key, sample_type FROM sample")
  expect_equal(s$sample_key, "cce-lter_picoplankton-bacteria:bottle:1")
  expect_equal(s$sample_type, "bottle")

  o <- DBI::dbGetQuery(con,
    "SELECT realm, taxon_key, measurement_type, depth_min_m, depth_max_m, sample_key FROM obs")
  # FCM cell counts are an environmental measurement vocabulary, not taxa
  expect_equal(o$realm, "env")
  expect_true(is.na(o$taxon_key))
  expect_equal(o$measurement_type, "synechococcus")
  expect_equal(o$depth_min_m, 20)
  expect_equal(o$depth_max_m, 20)
  expect_true(o$sample_key %in% s$sample_key)
})

test_that("build_sample_reference keeps the new datasets namespaced apart", {
  skip_if_not_installed("duckdb")

  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  # same integer id in both datasets must not collide on sample_key
  DBI::dbExecute(con, "CREATE TABLE mesopelagic_fish_tow AS
    SELECT 1 tow_id, 'c' cruise_key, 'g' grid_key, 's' site_key, 1.0 latitude, 1.0 longitude,
           TIMESTAMP '2010-01-12 07:45:00' datetime_start_utc, 1.0 depth_m")
  DBI::dbExecute(con, "CREATE TABLE picoplankton_bacteria_bottle AS
    SELECT 1 bottle_id, 'c' cruise_key, 'g' grid_key, 's' site_key, 1.0 latitude, 1.0 longitude,
           TIMESTAMP '2004-11-02 18:20:00' datetime_utc, 1.0 depth_m")

  n <- build_sample_reference(con)
  expect_equal(n, 2L)
  keys <- DBI::dbGetQuery(con, "SELECT sample_key FROM sample ORDER BY sample_key")$sample_key
  expect_equal(keys, c(
    "cce-lter_picoplankton-bacteria:bottle:1",
    "ucsd_sio_mesopelagic-fish:tow:1"))
})
