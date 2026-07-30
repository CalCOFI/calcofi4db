# derive_measurement_type_datasets() must attribute each measurement_type to the
# datasets that actually record it -- not to every dataset present in the table.

test_that("attribution is per (dataset_key, measurement_type), not per table", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbWriteTable(con, "obs", data.frame(
    dataset_key = c("swfsc_ichthyo", "calcofi_ctd-cast", "calcofi_ctd-cast"),
    measurement_type = c("abundance", "temperature_ave", "btl_ammonium"),
    stringsAsFactors = FALSE), overwrite = TRUE)

  got <- derive_measurement_type_datasets(
    con, list(obs = c("swfsc_ichthyo", "calcofi_ctd-cast")))

  # the regression: abundance must NOT pick up ctd-cast just for sharing a table
  expect_equal(got$abundance, "swfsc_ichthyo")
  expect_equal(got$temperature_ave, "calcofi_ctd-cast")
  expect_equal(got$btl_ammonium, "calcofi_ctd-cast")
})

test_that("a type shared by two datasets lists both, sorted", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbWriteTable(con, "obs", data.frame(
    dataset_key = c("ucsd_sio_mesopelagic-fish", "swfsc_ichthyo"),
    measurement_type = c("abundance", "abundance"),
    stringsAsFactors = FALSE), overwrite = TRUE)

  got <- derive_measurement_type_datasets(con, list(obs = c("a", "b")))
  expect_equal(got$abundance, c("swfsc_ichthyo", "ucsd_sio_mesopelagic-fish"))
})

test_that("a table with measurement_type but no dataset_key falls back to table-level", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbWriteTable(con, "ref", data.frame(
    measurement_type = "stage", stringsAsFactors = FALSE), overwrite = TRUE)

  got <- derive_measurement_type_datasets(con, list(ref = c("ds1", "ds2")))
  expect_equal(got$stage, c("ds1", "ds2"))
})

test_that("a table without measurement_type is skipped rather than erroring", {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbWriteTable(con, "sample", data.frame(
    dataset_key = "ds1", sample_key = "S1", stringsAsFactors = FALSE), overwrite = TRUE)

  expect_equal(length(derive_measurement_type_datasets(con, list(sample = "ds1"))), 0L)
})
