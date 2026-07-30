# Empty descriptions and units are invisible until a consumer hits them: they
# travel from an ingest's metadata.json into the release sidecar and out through
# cc_describe_table() / cc_db_catalog() as blank documentation. Nothing else
# surfaced them, so these assertions are the whole safety net.

md_fx <- function() list(
  tables = list(
    obs    = list(description_md = "core observations"),
    sample = list(description_md = ""),
    taxon  = list(description_md = NULL)),
  columns = list(
    `obs.temperature_ave` = list(description_md = "average temp", units = "degree_C"),
    `obs.oxygen`          = list(description_md = "oxygen",       units = NULL),
    `obs.sample_key`      = list(description_md = "",             units = NULL),
    `sample.latitude`     = list(description_md = "latitude",     units = NULL),
    `sample.datetime`     = list(description_md = "when",         units = NULL)))

test_that("empty and NULL descriptions are both counted as gaps", {
  g <- scan_metadata_gaps(md_fx())
  expect_setequal(g$tables_no_desc, c("sample", "taxon"))
  expect_equal(g$columns_no_desc, "obs.sample_key")
  expect_equal(g$n_tables, 3L)
  expect_equal(g$n_columns, 5L)
})

test_that("a missing unit counts only where a unit could exist", {
  # reporting every *_key, name and timestamp as unit-less buries the real gaps
  g <- scan_metadata_gaps(md_fx())
  expect_equal(g$columns_no_units, "obs.oxygen")
  expect_false("obs.sample_key"  %in% g$columns_no_units)   # a key has no units
  expect_false("sample.latitude" %in% g$columns_no_units)   # coordinate, not a measurement
  expect_false("sample.datetime" %in% g$columns_no_units)
})

test_that("a fully documented sidecar reports complete", {
  g <- scan_metadata_gaps(list(
    tables  = list(obs = list(description_md = "obs")),
    columns = list(`obs.temp` = list(description_md = "t", units = "degree_C"))))
  expect_equal(length(g$tables_no_desc), 0L)
  expect_equal(length(g$columns_no_desc), 0L)
  expect_equal(length(g$columns_no_units), 0L)
  expect_output(print(g), "complete")
})

test_that("the printed report names the offenders and how to fix them", {
  expect_output(print(scan_metadata_gaps(md_fx())), "sample, taxon")
  expect_output(print(scan_metadata_gaps(md_fx())), "flds_redefine.csv")
  expect_output(print(scan_metadata_gaps(md_fx())), "cc_describe_table")
})

test_that("scan_metadata_gaps reads a path as well as a list", {
  f <- withr::local_tempfile(fileext = ".json")
  jsonlite::write_json(md_fx(), f, auto_unbox = TRUE, null = "null")
  g <- scan_metadata_gaps(f)
  expect_setequal(g$tables_no_desc, c("sample", "taxon"))
  expect_error(scan_metadata_gaps(file.path(tempdir(), "nope.json")), "not found")
})

test_that("an empty sidecar is not an error", {
  g <- scan_metadata_gaps(list())
  expect_equal(g$n_tables, 0L)
  expect_equal(g$n_columns, 0L)
})

test_that("a units gap in one table is not reported against another table", {
  # REGRESSION: the exemption was matched on the bare column name and then the
  # surviving names were re-looked-up across ALL columns, so every table sharing a
  # bare name got flagged. `obs.depth_min_m` carries units "m" and was still
  # listed, because `sample.depth_min_m` did not.
  g <- scan_metadata_gaps(list(
    tables = list(obs = list(description_md = "o"), sample = list(description_md = "s")),
    columns = list(
      `obs.depth_min_m`    = list(description_md = "d", units = "m"),
      `sample.depth_min_m` = list(description_md = "d", units = NULL))))

  expect_equal(g$columns_no_units, "sample.depth_min_m")
  expect_false("obs.depth_min_m" %in% g$columns_no_units)
})

test_that("long-format value columns are not reported as missing units", {
  # the unit lives in measurement_type, one per row — that is the point of the
  # shape, so telling a maintainer to add a unit here would be actively wrong
  g <- scan_metadata_gaps(list(
    tables = list(obs = list(description_md = "o")),
    columns = list(
      `obs.measurement_value` = list(description_md = "value", units = NULL),
      `obs.measurement_prec`  = list(description_md = "prec",  units = NULL),
      `obs.realm`             = list(description_md = "realm", units = NULL),
      `obs._source_column`    = list(description_md = "hint",  units = NULL),
      `obs.temperature`       = list(description_md = "temp",  units = NULL))))
  expect_equal(g$columns_no_units, "obs.temperature")
})
