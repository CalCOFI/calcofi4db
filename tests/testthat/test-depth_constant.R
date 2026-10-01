# a derived series can be physically possible, unflagged and still not a profile: the
# provider's EstNO3_CruiseCorr holds ONE value per cast over 0-520 m (2026-10-01). These
# pin what is flagged and, as importantly, what is NOT judged.

depth_const_fixture <- function() {
  z8 <- seq(0, 350, by = 50)                       # 8 values over 350 m
  rbind(
    # constant over a full profile: flagged
    data.frame(cast = "const",  measurement_type = "est_nitrate", depth_m = z8,
               measurement_value = 12.5),
    # varies with depth: not flagged
    data.frame(cast = "vary",   measurement_type = "est_nitrate", depth_m = z8,
               measurement_value = seq(1, 40, length.out = 8)),
    # constant but only 5 values over 400 m: too short to judge
    data.frame(cast = "few",    measurement_type = "est_nitrate", depth_m = seq(0, 400, by = 100),
               measurement_value = 3),
    # constant, 8 values but only 35 m deep: too shallow to judge
    data.frame(cast = "shallow", measurement_type = "est_nitrate", depth_m = seq(0, 35, by = 5),
               measurement_value = 3),
    # the same constant cast under another type: flagged separately, per type
    data.frame(cast = "const",  measurement_type = "temperature", depth_m = z8,
               measurement_value = seq(15, 5, length.out = 8)),
    # constant to 1e-12 (inside the tolerance)
    data.frame(cast = "eps",    measurement_type = "est_nitrate", depth_m = z8,
               measurement_value = 7 + c(0, 1e-12, 0, 1e-12, 0, 1e-12, 0, 1e-12)))
}

test_that("a constant profile is flagged, a varying one is not", {
  r <- check_depth_constant_series(depth_const_fixture(), cast_col = "cast")
  expect_setequal(r$cast, c("const", "eps"))
  expect_true(all(r$measurement_type == "est_nitrate"))
  k <- r[r$cast == "const", ]
  expect_equal(k$n, 8)
  expect_equal(k$span_m, 350)
  expect_equal(k$depth_min_m, 0)
  expect_equal(k$depth_max_m, 350)
  expect_equal(k$value, 12.5)
  expect_false("vary" %in% r$cast)
})

test_that("a short cast (< min_n values or < min_span_m metres) is not judged", {
  d <- depth_const_fixture()
  r <- check_depth_constant_series(d, cast_col = "cast")
  expect_false(any(c("few", "shallow") %in% r$cast))
  # they are not judged at all, so they do not count in the denominator either
  j <- attr(r, "judged")
  expect_equal(unname(j["est_nitrate"]), 3L)  # const, vary, eps

  # the thresholds are arguments: relax them and both are judged and flagged
  r2 <- check_depth_constant_series(d, cast_col = "cast", min_n = 5, min_span_m = 30)
  expect_true(all(c("few", "shallow") %in% r2$cast))
  # tighten the span and the full profile (350 m) is no longer judged
  r3 <- check_depth_constant_series(d, cast_col = "cast", min_span_m = 400)
  expect_equal(nrow(r3), 0)
})

test_that("tol decides what counts as constant", {
  d <- depth_const_fixture()
  r <- check_depth_constant_series(d, cast_col = "cast", tol = 1e-15)
  expect_false("eps" %in% r$cast)
  expect_true("const" %in% r$cast)
})

test_that("NaN, Inf and NULL are not values; types restricts what is judged", {
  z <- seq(0, 350, by = 50)
  d <- data.frame(cast = "c", measurement_type = "x", depth_m = c(z, 400, 450),
                  measurement_value = c(rep(5, 8), NaN, Inf))
  r <- check_depth_constant_series(d, cast_col = "cast")
  expect_equal(r$n, 8)               # the NaN and the Inf are not counted
  expect_equal(r$depth_max_m, 350)   # nor do they stretch the span
  # one NaN-only series has nothing to judge
  expect_equal(nrow(check_depth_constant_series(
    data.frame(cast = "c", measurement_type = "x", depth_m = z, measurement_value = NaN),
    cast_col = "cast")), 0)
  expect_equal(nrow(check_depth_constant_series(
    depth_const_fixture(), cast_col = "cast", types = "temperature")), 0)
})

test_that("it reads a DBI connection by table name with the CTD defaults", {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))
  d <- depth_const_fixture()
  names(d)[1] <- "ctd_cast_uuid"
  DBI::dbWriteTable(con, "ctd_measurement", d)
  r <- check_depth_constant_series(con)
  expect_setequal(r$ctd_cast_uuid, c("const", "eps"))
  expect_error(check_depth_constant_series(con, tbl = "ctd_measurement", cast_col = "nope"),
               "no column")
})
