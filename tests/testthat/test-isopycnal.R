# build_isopycnal(): the release's isopycnal table is calcofi4r::cc_isopycnal() over every CTD cast,
# renamed to the release's conventions, never a second implementation of the rule.

iso_db <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  # two cruises, one down cast each with its up cast; sigma-theta and temperature on 1 m bins
  rows <- function(k, cr, shift) {
    z <- 0:30
    data.frame(sample_key = k, cruise_key = cr, dataset_key = "calcofi_ctd-cast", depth_min_m = rep(z, 3),
               measurement_type = rep(c("sigma_theta_1", "sigma_theta_2", "temperature_ave"), each = length(z)),
               measurement_value = c(24 + z / 10 + shift, 24 + z / 10 + shift, 20 - z / 10),
               measurement_qual = NA_character_)
  }
  ks <- c("calcofi_ctd-cast:cast:0107_001d", "calcofi_ctd-cast:cast:0107_001u",
          "calcofi_ctd-cast:cast:0207_001d")
  DBI::dbWriteTable(con, "obs_ctd_full", rbind(
    rows(ks[1], "2001-07-33XX", 0), rows(ks[2], "2001-07-33XX", 0.05), rows(ks[3], "2002-07-33XX", 0.1)))
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = ks, sample_type = "cast", dataset_key = "calcofi_ctd-cast",
    cruise_key = c("2001-07-33XX", "2001-07-33XX", "2002-07-33XX"), site_key = "093.3 030.0",
    grid_key = "st30-ln93.3", longitude = -118, latitude = 33,
    datetime = as.POSIXct(c("2001-07-10", "2001-07-10 01:00", "2002-07-10"), tz = "UTC"),
    data_stage = "final"))
  DBI::dbWriteTable(con, "cruise", data.frame(
    cruise_key = c("2001-07-33XX", "2002-07-33XX"), month_median = 7L,
    date_median = as.Date(c("2001-07-10", "2002-07-10"))))
  DBI::dbExecute(con, "CREATE TABLE obs (realm VARCHAR, sample_key VARCHAR, grid_key VARCHAR,
    cruise_key VARCHAR, datetime TIMESTAMP, depth_min_m DOUBLE, measurement_type VARCHAR,
    measurement_value DOUBLE, measurement_qual VARCHAR, dataset_key VARCHAR)")
  con
}

test_that("build_isopycnal(): cc_isopycnal() over every cast, one row per cast x surface x iso_ type", {
  skip_if_not(requireNamespace("calcofi4r", quietly = TRUE) && utils::packageVersion("calcofi4r") >= "1.34.0")
  con <- iso_db()
  sig <- c(24.5, 25.5, 28)
  n <- build_isopycnal(con, sigma = sig, variables = "temperature_ave", batch = 1)
  iso <- DBI::dbGetQuery(con, "SELECT * FROM isopycnal")
  expect_equal(n, nrow(iso))
  expect_equal(names(iso), c("dataset_key", "sample_key", "cruise_key", "site_key", "grid_key", "longitude",
                             "latitude", "datetime", "data_stage", "year", "month", "sigma_theta", "status",
                             "measurement_type", "measurement_value"))
  # the down casts only, every surface keeps its depth row
  expect_setequal(unique(iso$sample_key), c("calcofi_ctd-cast:cast:0107_001d", "calcofi_ctd-cast:cast:0207_001d"))
  expect_equal(sum(iso$measurement_type == "iso_depth_m"), 2 * length(sig))
  expect_setequal(unique(iso$measurement_type), c("iso_depth_m", "iso_temperature_ave"))
  # the same values as calcofi4r::cc_isopycnal(), batch by batch or all at once
  ref <- calcofi4r::cc_isopycnal(con, sigma = sig, variables = "temperature_ave")
  m <- merge(iso, transform(ref, measurement_type = calcofi4r::cc_isopycnal_type(variable)),
             by = c("sample_key", "sigma_theta", "measurement_type"))
  expect_equal(nrow(m), nrow(iso))
  expect_equal(m$measurement_value, m$value)
  expect_equal(m$status.x, m$status.y)
  # 28 is never reached: a status, no value
  expect_true(all(iso$status[iso$sigma_theta == 28] == "not_reached"))
  expect_true(all(is.na(iso$measurement_value[iso$sigma_theta == 28])))
  # 24.5 at 5 m on the first cast (24 + z/10)
  expect_equal(iso$measurement_value[iso$sample_key == "calcofi_ctd-cast:cast:0107_001d" &
                                       iso$sigma_theta == 24.5 & iso$measurement_type == "iso_depth_m"], 5)
  expect_equal(unique(iso$month), 7L)
  # the key is unique and is the registered sort key, so the table exports
  sk <- release_sort_keys()$isopycnal
  expect_equal(sk$order_by, c("sample_key", "sigma_theta", "measurement_type"))
  expect_equal(core_relationships("isopycnal")$primary_keys$isopycnal, sk$order_by)
  # and it feeds the surface baseline
  build_climatology(con, qual_ok_sql = "TRUE", min_cruises = 2L, isopycnal_tbl = "isopycnal")
  cl <- DBI::dbGetQuery(con, "SELECT * FROM climatology WHERE sigma_theta = 24.5 AND measurement_type = 'iso_depth_m'")
  expect_equal(cl$clim_mean, mean(c(5, 4)))
  expect_equal(cl$n_cruises, 2L)
})
