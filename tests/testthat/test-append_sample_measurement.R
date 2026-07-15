test_that("append_sample_measurement projects net effort as event-level rows", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  n <- append_sample_measurement(con, ich_sample_measurement_sql)
  expect_equal(n, 15L)   # 3 nets x 5 effort types

  # canonical event-level type vocabulary (design measurement_type changes)
  types <- DBI::dbGetQuery(con,
    "SELECT DISTINCT measurement_type FROM sample_measurement ORDER BY 1")$measurement_type
  expect_setequal(types, c(
    "prop_sorted", "small_plankton_biomass", "std_haul_factor",
    "total_plankton_biomass", "volume_sampled"))

  # effort is stored once per net (the leaf event), not repeated per taxon
  per_net <- DBI::dbGetQuery(con,
    "SELECT sample_key, COUNT(*) n FROM sample_measurement GROUP BY 1")
  expect_true(all(per_net$n == 5))
  expect_true(all(grepl("^swfsc_ichthyo:net:", per_net$sample_key)))
})
