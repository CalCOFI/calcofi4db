# an exact 0 that holds for tens of metres inside a varying cast is a fill or a failed
# regression (the CTD team's station-corrected oxygen: 0.0 from 322 to 517 m beside a
# sensor reading 26 umol/kg), not a measurement (CalCOFI/workflows#131). These pin
# what is a run and what is not.

zr <- function(cast, depth_m, measurement_value, type = "oxygen_sta_corr")
  data.frame(cast = cast, measurement_type = type, depth_m = depth_m,
             measurement_value = measurement_value)

test_that("60 m of zeros inside a varying cast is a run; 40 m is not", {
  z <- 0:200
  v60 <- seq(250, 20, length.out = length(z)); v60[z >= 100 & z <= 160] <- 0
  v40 <- seq(250, 20, length.out = length(z)); v40[z >= 100 & z <= 140] <- 0
  d <- rbind(zr("r60", z, v60), zr("r40", z, v40))
  r <- check_zero_runs(d, cast_col = "cast")
  expect_equal(nrow(r), 1)
  expect_equal(r$cast, "r60")
  expect_equal(r$depth_min_m, 100)
  expect_equal(r$depth_max_m, 160)
  expect_equal(r$span_m, 60)
  expect_equal(r$n, 61)
  expect_equal(r$run, 1L)
  # the threshold is an argument
  expect_true("r40" %in% check_zero_runs(d, cast_col = "cast", min_span_m = 40)$cast)
})

test_that("zeros separated by a real value are two runs; a small gap does not break one", {
  z <- 0:300
  v <- rep(0, length(z)); v[z < 50] <- 100; v[z == 180] <- 3.2   # zeros 50-179 and 181-300
  r <- check_zero_runs(zr("two", z, v), cast_col = "cast")
  expect_equal(nrow(r), 2)
  expect_equal(r$run, 1:2)
  expect_equal(r$depth_min_m, c(50, 181))
  expect_equal(r$depth_max_m, c(179, 300))

  # a missing bin or four (<= max_gap_m) is not a break; a 10 m hole is
  zg <- setdiff(0:120, 61:64)                     # 60 -> 65: a 5 m step
  expect_equal(nrow(check_zero_runs(zr("g", zg, 0 * zg), cast_col = "cast")), 1)
  zh <- setdiff(0:120, 51:59)                     # 50 -> 60: a 10 m step
  r_h <- check_zero_runs(zr("h", zh, 0 * zh), cast_col = "cast", min_span_m = 50)
  expect_equal(r_h$depth_min_m, c(0, 60))         # 0-50 and 60-120: two runs
  expect_equal(r_h$depth_max_m, c(50, 120))
  # with a 10 m allowance the hole no longer breaks it: one run from the surface
  expect_equal(check_zero_runs(zr("h", zh, 0 * zh), cast_col = "cast", max_gap_m = 10)$depth_min_m, 0)
})

test_that("a cast of all zeros is found by both tests; applied in turn it is counted once", {
  z <- seq(0, 350, by = 1)
  d <- rbind(zr("allzero", z, 0), zr("vary", z, seq(250, 20, length.out = length(z))))
  whole <- check_depth_constant_series(d, cast_col = "cast")
  runs  <- check_zero_runs(d, cast_col = "cast")
  expect_equal(whole$cast, "allzero")
  expect_equal(runs$cast, "allzero")
  # the caller withholds the whole-cast hits first; the run test then finds nothing more
  rest <- d[!d$cast %in% whole$cast, ]
  expect_equal(nrow(check_zero_runs(rest, cast_col = "cast")), 0)
  expect_equal(sum(whole$n), length(z))
})

test_that("a zero surface bin or two, or values approaching zero, are not a run", {
  z <- 0:200
  v <- seq(0, 30, length.out = length(z)); v[z <= 1] <- 0      # 2 zero bins at the surface
  expect_equal(nrow(check_zero_runs(zr("surf", z, v), cast_col = "cast")), 0)
  near <- c(rep(0.001, 120), seq(0.5, 10, length.out = 81))    # near zero, never exactly
  expect_equal(nrow(check_zero_runs(zr("near", z, near), cast_col = "cast")), 0)
  # NaN / Inf are not values and do not break or make a run
  vn <- rep(0, length(z)); vn[z == 100] <- NaN; vn[z < 20] <- 50
  r <- check_zero_runs(zr("nan", z, vn), cast_col = "cast")
  expect_equal(nrow(r), 1)
  expect_equal(r$n, length(20:200) - 1)
})

test_that("types, a composite cast key and a DBI connection", {
  z <- 0:100
  d <- rbind(
    data.frame(cruise_key = "2408SR", cast_key = "2408_033d", cast_dir = "D",
               measurement_type = "oxygen_sta_corr", depth_m = z, measurement_value = 0),
    data.frame(cruise_key = "2408SR", cast_key = "2408_033d", cast_dir = "D",
               measurement_type = "est_nitrate", depth_m = z, measurement_value = 0))
  r <- check_zero_runs(d, cast_col = c("cruise_key", "cast_key", "cast_dir"),
                       types = "oxygen_sta_corr")
  expect_equal(names(r)[1:5], c("cruise_key", "cast_key", "cast_dir", "measurement_type", "run"))
  expect_equal(r$measurement_type, "oxygen_sta_corr")

  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbWriteTable(con, "m", d)
  rc <- check_zero_runs(con, tbl = "m", cast_col = c("cruise_key", "cast_key", "cast_dir"))
  expect_equal(nrow(rc), 2)
  expect_error(check_zero_runs(con, tbl = "m", cast_col = "nope"), "no column")
  expect_error(check_zero_runs(d, cast_col = "cast_key", max_gap_m = 0), "max_gap_m")
})
