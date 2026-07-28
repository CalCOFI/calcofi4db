
setup_mets_fixture <- function(con) {
  DBI::dbExecute(con, "CREATE TABLE mets_sample AS
    SELECT * FROM (VALUES
      ('u1', '1704SH', '2017-04-3322', 'st1-ln1', 33.0, -120.0, TIMESTAMP '2017-04-05 09:00:00'),
      ('u2', '1704SH', '2017-04-3322', 'st1-ln1', 33.1, -120.1, TIMESTAMP '2017-04-05 09:01:00'),
      ('u3', '1704SH', '2017-04-3322', NULL,      33.2, -120.2, TIMESTAMP '2017-04-05 10:00:00'))
      t(mets_sample_uuid, cruise_code, cruise_key, grid_key, latitude, longitude, datetime_start_utc)")
  # only u1 and u3 survive thinning; u2 is an un-retained 1-minute sample
  DBI::dbExecute(con, "CREATE TABLE mets_thin AS
    SELECT * FROM (VALUES
      ('m1', 'u1', '2017-04-3322', 'sst_c', 14.5, 'grid'),
      ('m2', 'u3', '2017-04-3322', 'sst_c', 15.1, 'inflection'))
      t(mets_measurement_uuid, mets_sample_uuid, cruise_key,
        measurement_type, measurement_value, retained_reason)")
}

test_that("METS projects into sample at the underway grain, thinned only", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:"); on.exit(close_duckdb(con))
  setup_mets_fixture(con)

  core <- emit_core_tables(con, "calcofi_mets")

  s <- DBI::dbGetQuery(con,
    "SELECT sample_key, sample_type, root_sample_key, datetime FROM sample ORDER BY sample_key")
  # u2 is not referenced by mets_thin, so it must not become a sample row —
  # `sample` stays proportionate to `obs` rather than carrying the full 1-min series
  expect_equal(nrow(s), 2L)
  expect_equal(s$sample_key,
               c("calcofi_mets:underway:u1", "calcofi_mets:underway:u3"))
  expect_true(all(s$sample_type == "underway"))
  expect_equal(s$root_sample_key, s$sample_key)
  expect_false(any(is.na(s$datetime)))
  expect_equal(core$sample, 2L)
})

test_that("METS obs is env realm, fed by mets_thin, and FKs into sample", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:"); on.exit(close_duckdb(con))
  setup_mets_fixture(con)

  emit_core_tables(con, "calcofi_mets")

  o <- DBI::dbGetQuery(con,
    "SELECT realm, taxon_key, sample_key, measurement_type, measurement_value,
            depth_min_m, depth_max_m, hex_id FROM obs ORDER BY measurement_value")
  # u3 has a NULL grid_key so it is excluded, as every other arm excludes them
  expect_equal(nrow(o), 1L)
  expect_equal(o$realm, "env")
  expect_true(is.na(o$taxon_key))
  expect_equal(o$measurement_type, "sst_c")
  expect_equal(o$measurement_value, 14.5)
  expect_equal(o$depth_min_m, 0)
  expect_false(is.na(o$hex_id))

  s <- DBI::dbGetQuery(con, "SELECT sample_key FROM sample")
  expect_true(all(o$sample_key %in% s$sample_key))
})

test_that("METS sample_key does not collide with the other underway dataset", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:"); on.exit(close_duckdb(con))
  setup_mets_fixture(con)
  # swfsc_cufes also uses sample_type 'underway'; namespacing must keep them apart
  DBI::dbExecute(con, "CREATE TABLE cufes_sample AS
    SELECT 'u1' sample_id, '2017-04-3322' cruise_key, 'st1-ln1' grid_key,
           33.0 latitude, -120.0 longitude,
           TIMESTAMP '2017-04-05 09:00:00' datetime_start_utc")

  build_sample_reference(con)
  keys <- DBI::dbGetQuery(con, "SELECT sample_key FROM sample ORDER BY sample_key")$sample_key
  expect_true("calcofi_mets:underway:u1" %in% keys)
  expect_true("swfsc_cufes:underway:u1" %in% keys)
})
