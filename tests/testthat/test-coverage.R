test_that("format_bbox writes hemisphere suffixes in geographic order", {
  # the CalCOFI case: all north, all west. West edge first, so the string reads
  # west -> east even though the underlying numbers descend in magnitude.
  expect_equal(format_bbox(29.8, 37.8, -126.5, -117.3),
               "29.8–37.8°N, 126.5–117.3°W")
  # eastern longitudes ascend
  expect_equal(format_bbox(1, 2, 10.25, 20.75, digits = 2),
               "1.00–2.00°N, 10.25–20.75°E")
})

test_that("format_bbox labels both ends when a span crosses a hemisphere", {
  # a single trailing suffix would mislabel half the range as the wrong side
  expect_equal(format_bbox(-3.2, 12.7, -5, 5),
               "3.2°S–12.7°N, 5.0°W–5.0°E")
})

test_that("format_bbox refuses a partial or non-finite box", {
  # NaN is the one that matters: it survives IS NOT NULL upstream, so it can
  # reach here looking like a real number
  expect_true(is.na(format_bbox(NaN, 37.8, -126.5, -117.3)))
  expect_true(is.na(format_bbox(29.8, Inf, -126.5, -117.3)))
  expect_true(is.na(format_bbox(29.8, 37.8, NA, -117.3)))
  expect_true(is.na(format_bbox(29.8, 37.8, -126.5, NULL)))
})

test_that("observed_coverage measures temporal and spatial extent per dataset", {
  con <- get_duckdb_con(":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "sample", data.frame(
    dataset_key = c("a_x", "a_x", "b_y", "b_y"),
    latitude    = c(30.0, 34.0, 10.0, 12.0),
    longitude   = c(-125.0, -118.0, -100.0, -99.0),
    datetime    = as.POSIXct(c("2001-03-04", "2005-11-02",
                               "1999-01-09", "2000-06-30"), tz = "UTC"),
    stringsAsFactors = FALSE))

  d <- observed_coverage(con, tables = "sample")
  expect_equal(d$dataset_key, c("a_x", "b_y"))
  expect_equal(d$coverage_temporal_observed,
               c("2001-03 to 2005-11", "1999-01 to 2000-06"))
  expect_equal(d$coverage_spatial_observed,
               c("30.0–34.0°N, 125.0–118.0°W", "10.0–12.0°N, 100.0–99.0°W"))
  expect_equal(d$lat_max, c(34.0, 12.0))
})

test_that("observed_coverage excludes NaN coordinates from the bounding box", {
  # regression: NaN survives IS NOT NULL and min()/max() propagate it, so one
  # poisoned row would blow the whole dataset's bbox out to NaN with every
  # nullity check still passing. isfinite() is the filter, not IS NOT NULL.
  con <- get_duckdb_con(":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "sample", data.frame(
    dataset_key = c("a_x", "a_x", "a_x"),
    latitude    = c(30.0, 34.0, NaN),
    longitude   = c(-125.0, -118.0, NaN),
    datetime    = as.POSIXct(c("2001-03-04", "2005-11-02", "2003-01-01"),
                             tz = "UTC"),
    stringsAsFactors = FALSE))

  d <- observed_coverage(con, tables = "sample")
  expect_equal(d$coverage_spatial_observed, "30.0–34.0°N, 125.0–118.0°W")
  expect_false(is.na(d$lat_min))
})

test_that("observed_coverage measures the halves independently", {
  # calcofi_phytoplankton is region-pooled: real coordinates, no datetime at
  # all. It must still yield a spatial extent, and must NOT invent a temporal
  # one -- the caller falls back to a declared static value for that half.
  con <- get_duckdb_con(":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "sample", data.frame(
    dataset_key = c("pooled", "pooled"),
    latitude    = c(31.0, 35.0),
    longitude   = c(-124.0, -120.0),
    datetime    = as.POSIXct(c(NA, NA), tz = "UTC"),
    stringsAsFactors = FALSE))

  d <- observed_coverage(con, tables = "sample")
  expect_equal(nrow(d), 1L)
  expect_true(is.na(d$coverage_temporal_observed))
  expect_equal(d$coverage_spatial_observed, "31.0–35.0°N, 124.0–120.0°W")
})

test_that("observed_coverage unions the grains and skips absent tables", {
  con <- get_duckdb_con(":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "sample", data.frame(
    dataset_key = "a_x", latitude = 30.0, longitude = -125.0,
    datetime = as.POSIXct("2001-03-04", tz = "UTC"), stringsAsFactors = FALSE))
  # obs carries denormalized coordinates and can extend the box beyond sample
  DBI::dbWriteTable(con, "obs", data.frame(
    dataset_key = "a_x", latitude = 38.0, longitude = -130.0,
    datetime = as.POSIXct("2010-08-15", tz = "UTC"), stringsAsFactors = FALSE))

  d <- observed_coverage(con, tables = c("sample", "obs", "does_not_exist"))
  expect_equal(d$coverage_temporal_observed, "2001-03 to 2010-08")
  expect_equal(d$coverage_spatial_observed, "30.0–38.0°N, 130.0–125.0°W")
})
