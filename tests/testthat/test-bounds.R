# `valid_min`/`valid_max` were documentation, not enforcement: emitted as netCDF
# attributes and shown on the schema site while nothing compared a value to them.
# v2026.08.07 shipped ~31k impossible CTD values as a result. These pin the three
# things that matter — a violation is counted and attributed to the right bound,
# a type with NO declared bound is reported as a finding rather than passing
# silently, and the enforcement path deletes exactly what the check counted.

bounds_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  con
}

# 3 types: one bounded and violated on both ends, one bounded and clean, one with
# no bounds at all (the 67%-of-the-release case).
bounds_fixture <- function(con) {
  DBI::dbWriteTable(con, "obs", data.frame(
    dataset_key = "test_ds",
    measurement_type = c(rep("temperature", 6), rep("salinity", 3),
                         rep("abundance", 4)),
    measurement_value = c(-50, -1, 5, 20, 45, 60,     # 2 low, 2 high, 2 inside
                          33, 34, 35,                 # all inside
                          -3, 0, 10, 1e6),            # undeclared
    depth_m = c(0, 10, 20, 30, 300, 500, 0, 10, 20, 0, 10, 20, 30)),
    overwrite = TRUE)
  DBI::dbWriteTable(con, "measurement_type", data.frame(
    measurement_type  = c("temperature", "salinity", "abundance"),
    valid_min         = c(0, 20, NA),
    valid_max         = c(40, 45, NA),
    valid_depth_min_m = c(NA, NA, NA),
    valid_depth_max_m = c(200, NA, NA),
    units             = c("degC", "psu", "count")),
    overwrite = TRUE)
  con
}

test_that("check_measurement_bounds() counts violations and attributes the bound", {
  con <- bounds_fixture(bounds_con())
  b <- check_measurement_bounds(con, "obs")

  expect_setequal(b$measurement_type, c("temperature", "salinity", "abundance"))
  # worst first: the violated type leads
  expect_identical(b$measurement_type[1], "temperature")

  tmp <- b[b$measurement_type == "temperature", ]
  expect_identical(tmp$status, "out_of_range")
  expect_equal(tmp$n_total, 6)
  # -50 and -1 are below 0; 45 and 60 are above 40 — split, not just totalled,
  # because "too low" and "too high" usually have different causes (a sentinel
  # vs a scaling error)
  expect_equal(tmp$n_low, 2)
  expect_equal(tmp$n_high, 2)
  expect_equal(tmp$n_bad, 4)
  expect_equal(tmp$pct_bad, 66.6667, tolerance = 1e-3)
  expect_equal(c(tmp$v_min, tmp$v_max), c(-50, 60))

  sal <- b[b$measurement_type == "salinity", ]
  expect_identical(sal$status, "ok")
  expect_equal(sal$n_bad, 0)
})

test_that("a type with no declared bound is a finding, not a pass", {
  con <- bounds_fixture(bounds_con())
  b <- check_measurement_bounds(con, "obs")

  ab <- b[b$measurement_type == "abundance", ]
  expect_identical(ab$status, "undeclared")
  # it must NOT be reported as clean: n_bad is 0 only because nothing was checked
  expect_equal(ab$n_bad, 0)
  expect_true(is.na(ab$valid_min) && is.na(ab$valid_max))
  # the observed range is what lets someone propose a bound, and the finding text
  # is what goes in a questions.csv `context` cell
  expect_equal(c(ab$v_min, ab$v_max), c(-3, 1e6))
  expect_match(ab$finding, "No valid_min/valid_max declared", fixed = TRUE)
  expect_match(ab$finding, "physically possible")

  # opt out for a violations-only view
  b2 <- check_measurement_bounds(con, "obs", include_undeclared = FALSE)
  expect_false("abundance" %in% b2$measurement_type)
})

test_that("one-sided bounds work — 'never negative' without knowing the ceiling", {
  con <- bounds_fixture(bounds_con())
  DBI::dbExecute(con, "UPDATE measurement_type SET valid_min = 0
                       WHERE measurement_type = 'abundance'")
  b <- check_measurement_bounds(con, "obs")
  ab <- b[b$measurement_type == "abundance", ]

  expect_identical(ab$status, "out_of_range")
  expect_equal(ab$n_low, 1)     # the -3
  expect_equal(ab$n_high, 0)    # 1e6 is not a violation: no ceiling declared
  expect_equal(ab$n_bad, 1)
})

test_that("the depth window catches a type emitted where it is not defined", {
  con <- bounds_fixture(bounds_con())
  b <- check_measurement_bounds(con, "obs", depth_col = "depth_m")

  # temperature is declared for 0-200 m; the fixture has values at 300 and 500
  tmp <- b[b$measurement_type == "temperature", ]
  expect_equal(tmp$n_outside_depth, 2)
  # a type with no depth window is NA, not 0 — "not asked" differs from "none"
  expect_true(is.na(b$n_outside_depth[b$measurement_type == "salinity"]))
})

test_that("dataset_key filters, and a missing column is an error not a silent pass", {
  con <- bounds_fixture(bounds_con())
  DBI::dbExecute(con, "INSERT INTO obs VALUES ('other_ds', 'temperature', -999, 0)")

  expect_equal(
    check_measurement_bounds(con, "obs", dataset_key = "test_ds")$n_bad[1], 4)
  expect_equal(
    check_measurement_bounds(con, "obs", dataset_key = "other_ds")$n_bad[1], 1)

  expect_error(check_measurement_bounds(con, "obs", value_col = "nope"),
               "no column `nope`")
})

test_that("a corrupted registry cannot masquerade as 'undeclared'", {
  con <- bounds_fixture(bounds_con())
  # the write_csv(na = "NA") round trip: as.numeric("NA") is NA *with a warning*,
  # so a corrupted bound would quietly become "nothing declared" and this check
  # would report coverage it does not have
  mt <- data.frame(measurement_type = "temperature",
                   valid_min = "NA", valid_max = "40")
  expect_error(check_measurement_bounds(con, "obs", mt = mt), "sentinel strings")
})

test_that("drop_out_of_bounds() deletes exactly what the check counted", {
  con <- bounds_fixture(bounds_con())
  before <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM obs")$n

  acted <- drop_out_of_bounds(con, "obs", quiet = TRUE)
  after <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM obs")$n

  expect_identical(acted$measurement_type, "temperature")
  expect_equal(acted$n_bad, 4)
  expect_equal(before - after, 4)

  # the undeclared type is untouched — no bound, no licence to delete
  expect_equal(
    DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM obs
                          WHERE measurement_type = 'abundance'")$n, 4)

  # and the survivors are all inside the bound
  rng <- DBI::dbGetQuery(con, "SELECT MIN(measurement_value) lo,
                                      MAX(measurement_value) hi FROM obs
                               WHERE measurement_type = 'temperature'")
  expect_true(rng$lo >= 0 && rng$hi <= 40)

  # idempotent: a second pass finds nothing
  expect_identical(nrow(drop_out_of_bounds(con, "obs", quiet = TRUE)), 0L)
})

test_that("an empty table and a registry-less connection both fail loudly enough", {
  con <- bounds_con()
  DBI::dbExecute(con, "CREATE TABLE obs (
                         measurement_type VARCHAR, measurement_value DOUBLE)")
  expect_error(check_measurement_bounds(con, "obs"), "measurement_type` table")

  DBI::dbWriteTable(con, "measurement_type", data.frame(
    measurement_type = "temperature", valid_min = 0, valid_max = 40))
  expect_identical(nrow(check_measurement_bounds(con, "obs")), 0L)
})
