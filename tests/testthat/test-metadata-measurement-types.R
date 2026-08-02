# The `measurement_types` block of the release sidecar is what the schema site
# and the netCDF writers read for units, ranges and provenance. These pin that an
# empty registry cell is OMITTED rather than emitted as null: a published
# `valid_max: null` reads as "no upper bound", which is an assertion the registry
# never made.

mt_csv <- function(env = parent.frame(), ...) {
  d <- tibble::tibble(
    measurement_type  = c("temperature_ave", "est_chlorophyll_a_sta_corr"),
    description       = c("Average temperature", "Est. chlorophyll-a station-corrected"),
    units             = c("degC", "ug/L"),
    valid_min         = c(-2, NA),
    valid_max         = c(40, NA),
    valid_depth_min_m = c(NA, 0),
    valid_depth_max_m = c(NA, 200),
    derivation        = c(NA_character_, "Fluorometer regression over 0-200 m only."),
    is_canonical      = c(TRUE, FALSE),
    `_source_datasets` = c("calcofi_ctd-cast", "calcofi_ctd-cast"))
  path <- withr::local_tempfile(fileext = ".csv", .local_envir = env)
  readr::write_csv(d, path, na = "")
  path
}

merged <- function(env = parent.frame()) {
  out <- withr::local_tempfile(fileext = ".json", .local_envir = env)
  merge_metadata_json(
    paths = character(), output_path = out,
    measurement_type_csv = mt_csv(env))
  jsonlite::read_json(out, simplifyVector = FALSE)$measurement_types
}

test_that("the depth range and derivation reach the release sidecar", {
  mt <- merged()

  chl <- mt$est_chlorophyll_a_sta_corr
  expect_equal(chl$valid_depth_min_m[[1]], 0)
  expect_equal(chl$valid_depth_max_m[[1]], 200)
  expect_match(chl$derivation[[1]], "0-200 m")

  tmp <- mt$temperature_ave
  expect_equal(tmp$valid_min[[1]], -2)
  expect_equal(tmp$valid_max[[1]], 40)
})

test_that("an empty registry cell is omitted, never emitted as null", {
  mt <- merged()

  # temperature has a value range but no depth range and no derivation
  expect_false("valid_depth_min_m" %in% names(mt$temperature_ave))
  expect_false("valid_depth_max_m" %in% names(mt$temperature_ave))
  expect_false("derivation"        %in% names(mt$temperature_ave))
  # and the chlorophyll type the other way round
  expect_false("valid_min" %in% names(mt$est_chlorophyll_a_sta_corr))
  expect_false("valid_max" %in% names(mt$est_chlorophyll_a_sta_corr))
})

test_that("a registry with none of the optional columns still merges", {
  # 15 other registries and every older release predate these columns
  d <- tibble::tibble(
    measurement_type = "abundance", description = "Specimen count",
    units = "count", is_canonical = TRUE)
  path <- withr::local_tempfile(fileext = ".csv")
  readr::write_csv(d, path, na = "")
  out <- withr::local_tempfile(fileext = ".json")

  expect_no_error(merge_metadata_json(
    paths = character(), output_path = out, measurement_type_csv = path))
  mt <- jsonlite::read_json(out, simplifyVector = FALSE)$measurement_types
  expect_equal(mt$abundance$units[[1]], "count")
  expect_false("derivation" %in% names(mt$abundance))
})
