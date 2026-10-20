# bottle derived products (calcofi_bottle-derived): spice and TEOS-10 sigma-theta per bottle,
# from the bottle's own temperature and salinity; one fixture per rule / branch

# reference values computed independently, straight from gsw (the recipe Ben Best sent
# Nastassia Patin on 2026-10-19): p = p_from_z(-z, lat); SA = SA_from_SP; CT = CT_from_t
ref <- function(t, s, z, lon, lat) {
  p  <- gsw::gsw_p_from_z(-z, lat)
  sa <- gsw::gsw_SA_from_SP(s, p, lon, lat)
  ct <- gsw::gsw_CT_from_t(sa, t, p)
  list(p = p, spice = gsw::gsw_spiciness0(sa, ct), sig0 = gsw::gsw_sigma0(sa, ct))
}

test_that("bottle_spice_sigma0: matches the gsw recipe at the surface and at depth", {
  t <- c(15.2, 4.1); s <- c(33.45, 34.32); z <- c(10, 1000); lon <- c(-121.5, -124); lat <- c(33.8, 31)
  r <- bottle_spice_sigma0(t, s, z, lon, lat)
  e <- ref(t, s, z, lon, lat)
  expect_named(r, c("pressure_dbar", "spiciness0", "sigma_theta_teos10"))
  expect_equal(r$pressure_dbar, e$p)
  expect_equal(r$spiciness0, e$spice)
  expect_equal(r$sigma_theta_teos10, e$sig0)
  # sanity on magnitude: California Current surface water near 24.7 kg/m3, 1000 m near 27.3
  expect_equal(round(r$sigma_theta_teos10, 1), c(24.7, 27.3))
  # pressure exceeds depth at 1000 m (about 1 % more), equals it to < 0.1 dbar at 10 m
  expect_gt(r$pressure_dbar[2], 1000)
  expect_lt(abs(r$pressure_dbar[1] - 10), 0.2)
})

test_that("bottle_spice_sigma0: spice is the same rule as ctd_spice() at the same pressure", {
  r <- bottle_spice_sigma0(12, 33.8, 150, -120, 33)
  expect_equal(r$spiciness0, ctd_spice(12, 33.8, r$pressure_dbar, -120, 33))
})

test_that("bottle_spice_sigma0: a flagged (8/9) temperature or salinity yields neither value", {
  r <- bottle_spice_sigma0(rep(15, 6), rep(33.5, 6), 10, -120, 33,
                           q_temperature = c(NA, "8", "9", NA, NA, "8.0"),
                           q_salinity    = c(NA, NA, NA, "8", "9.0", NA))
  expect_false(is.na(r$spiciness0[1]))
  expect_true(all(is.na(r$spiciness0[2:6])))
  expect_true(all(is.na(r$sigma_theta_teos10[2:6])))
  expect_true(all(is.na(r$pressure_dbar[2:6])))
})

test_that("bottle_spice_sigma0: code 6 (ok, taken from CTD) and other codes are kept", {
  r <- bottle_spice_sigma0(rep(15, 4), rep(33.5, 4), 10, -120, 33,
                           q_temperature = c("6", "0", "", NA), q_salinity = c("6", "4", "6.0", ""))
  expect_false(anyNA(r$spiciness0))
  expect_false(anyNA(r$sigma_theta_teos10))
  expect_equal(length(unique(r$sigma_theta_teos10)), 1)
})

test_that("bottle_spice_sigma0: a missing or non-finite input is NA, never an error", {
  r <- bottle_spice_sigma0(c(NA, 15, 15, 15, 15), c(33.5, NA, 33.5, NaN, 33.5),
                           c(10, 10, NA, 10, 10), -120, c(33, 33, 33, 33, NA))
  expect_true(all(is.na(r$spiciness0)))
  expect_true(all(is.na(r$sigma_theta_teos10)))
  expect_equal(nrow(r), 5)
})

test_that("bottle_spice_sigma0: longitude and latitude recycle; an all-NA input is all NA", {
  r <- bottle_spice_sigma0(c(15, 10), c(33.5, 34), c(10, 200), -120, 33)
  e <- ref(c(15, 10), c(33.5, 34), c(10, 200), -120, 33)
  expect_equal(r$sigma_theta_teos10, e$sig0)
  r0 <- bottle_spice_sigma0(NA_real_, NA_real_, NA_real_, NA_real_, NA_real_)
  expect_equal(nrow(r0), 1)
  expect_true(is.na(r0$sigma_theta_teos10))
})

# merge_source_datasets ----

test_that("merge_source_datasets: adds once, keeps the others and their order", {
  expect_identical(
    merge_source_datasets(
      c(NA, "", "calcofi_ctd-derived", "calcofi_ctd-derived;calcofi_bottle-derived",
        "calcofi_bottle-derived; calcofi_ctd-derived"),
      "calcofi_bottle-derived"),
    c("calcofi_bottle-derived", "calcofi_bottle-derived",
      "calcofi_ctd-derived;calcofi_bottle-derived",
      "calcofi_ctd-derived;calcofi_bottle-derived",
      "calcofi_bottle-derived;calcofi_ctd-derived"))
  expect_identical(merge_source_datasets(character(), "x"), character())
  expect_error(merge_source_datasets("a", c("x", "y")))
})

# add_measurement_source_dataset ----

test_that("add_measurement_source_dataset: appends the dataset, touches nothing else, refuses unknown types", {
  p <- withr::local_tempfile(fileext = ".csv")
  writeLines(c(
    "measurement_type,description,units,valid_min,valid_max,derivation,_source_datasets",
    "spiciness0,Spice,kg/m3,-25,18,ctd rule,calcofi_ctd-derived",
    "temperature,Water temperature,degC,-2,40,,calcofi_bottle"), p)
  before <- read_measurement_type(p)
  d <- add_measurement_source_dataset("spiciness0", "calcofi_bottle-derived", p, quiet = TRUE)
  expect_identical(d$`_source_datasets`,
                   c("calcofi_ctd-derived;calcofi_bottle-derived", "calcofi_bottle"))
  expect_identical(d[, setdiff(names(d), "_source_datasets")],
                   before[, setdiff(names(before), "_source_datasets")])
  # empty cells stay empty on disk (na = "")
  expect_false(any(grepl("\\bNA\\b", readLines(p))))
  # idempotent: a second call changes nothing
  txt <- readLines(p)
  add_measurement_source_dataset("spiciness0", "calcofi_bottle-derived", p, quiet = TRUE)
  expect_identical(readLines(p), txt)
  expect_error(add_measurement_source_dataset("spice_typo", "x", p, quiet = TRUE), "unknown")
})
