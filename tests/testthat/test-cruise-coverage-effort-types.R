# `effort_only_types` exempts sample types that are an inventory rather than an
# analyzed event. The case it exists for: cdfw_dungeness-crab's `tow` rows are a
# 60-year log of which archived jars EXIST, only ~11% of which were ever
# examined, while its `subsample` rows are lab-examined aliquots that all yield
# obs. Raising max_orphan_cruises would hide both — this hides only the first.

fixture <- function() {
  con <- get_duckdb_con(":memory:")
  DBI::dbExecute(con, "
    CREATE TABLE sample (dataset_key VARCHAR, sample_type VARCHAR,
                         cruise_key VARCHAR, sample_key VARCHAR)")
  DBI::dbExecute(con, "CREATE TABLE obs (sample_key VARCHAR)")
  DBI::dbExecute(con, "
    INSERT INTO sample VALUES
      -- inventory tows on a cruise where nothing was examined
      ('ds_a','tow','c1','a1'), ('ds_a','tow','c1','a2'),
      -- examined subsamples, both observed
      ('ds_a','subsample','c2','a3'), ('ds_a','subsample','c2','a4'),
      -- another dataset whose `tow` IS its observing type, one cruise lost
      ('ds_b','tow','c3','b1'), ('ds_b','tow','c4','b2')")
  DBI::dbExecute(con, "INSERT INTO obs VALUES ('a3'), ('a4'), ('b1')")
  con
}

test_that("without the exemption, inventory tows are reported as orphan cruises", {
  con <- fixture(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  r <- suppressWarnings(check_cruise_coverage(con, halt = FALSE, verbose = FALSE))
  expect_equal(r$cruises_no_obs[r$dataset_key == "ds_a"], 1L)  # c1
  expect_equal(r$orphan_samples[r$dataset_key == "ds_a"], 2L)
})

test_that("effort_only_types drops the inventory type from the calculation", {
  con <- fixture(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  r <- suppressWarnings(check_cruise_coverage(
    con, effort_only_types = c(ds_a = "tow"), halt = FALSE, verbose = FALSE))
  expect_equal(r$cruises_no_obs[r$dataset_key == "ds_a"], 0L)
  expect_equal(r$orphan_samples[r$dataset_key == "ds_a"], 0L)
})

test_that("the exemption is per dataset, not per sample_type globally", {
  # ds_b also uses 'tow', but as its observing type — exempting ds_a's must not
  # silence ds_b's genuinely lost cruise (c4)
  con <- fixture(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  r <- suppressWarnings(check_cruise_coverage(
    con, effort_only_types = c(ds_a = "tow"), halt = FALSE, verbose = FALSE))
  expect_equal(r$cruises_no_obs[r$dataset_key == "ds_b"], 1L)
})

test_that("an exempted dataset still fails on its observing sample types", {
  con <- fixture(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "DELETE FROM obs WHERE sample_key IN ('a3','a4')")
  expect_error(
    check_cruise_coverage(con, effort_only_types = c(ds_a = "tow"),
                          halt = TRUE, verbose = FALSE),
    "carry samples but no obs")
})

test_that("effort_only_types must be named by dataset_key", {
  con <- fixture(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  expect_error(
    check_cruise_coverage(con, effort_only_types = "tow", halt = FALSE),
    "must be named by dataset_key")
})
