# The shared metadata registries are hand-edited AND written back by ingest
# notebooks. write_csv() defaults to na = "NA", so an empty cell round-trips to the
# 2-character string "NA" — invisible from R (read_csv reads it back to NA) but NOT
# from DuckDB's read_csv_auto, which shipped 161 such rows into the released
# measurement_type table. These pin both halves: writes emit empty cells, and reads
# refuse a corrupted registry.

mt_fixture <- function(env = parent.frame()) {
  d <- tibble::tibble(
    measurement_type  = c("abundance", "body_length"),
    description       = c("Specimen count", "Larva body length"),
    units             = c("count", "mm"),
    is_canonical      = c(TRUE, TRUE),
    `_qual_column`    = c(NA_character_, NA_character_),
    grain             = c("obs", "attribute"))
  path <- withr::local_tempfile(fileext = ".csv", .local_envir = env)
  readr::write_csv(d, path, na = "")
  path
}

test_that("check_registry_na_strings() passes a clean registry and names the culprit", {
  clean <- tibble::tibble(a = c("x", NA_character_), b = c(NA_character_, "y"))
  expect_silent(check_registry_na_strings(clean))
  expect_identical(check_registry_na_strings(clean), clean)

  dirty <- tibble::tibble(a = c("x", "NA"), b = c("NULL", "y"))
  expect_error(check_registry_na_strings(dirty, path = "reg.csv"),
               "sentinel strings")
  # the error must say WHERE, so it is actionable
  expect_error(check_registry_na_strings(dirty, path = "reg.csv"), "reg\\.csv")
  expect_error(check_registry_na_strings(dirty), "\\ba\\b")
  expect_error(check_registry_na_strings(dirty), "\\bb\\b")
  # and it must point at the cause
  expect_error(check_registry_na_strings(dirty), 'na = \\\\?""')

  # non-character columns are not candidates, and " NA " still counts
  expect_silent(check_registry_na_strings(tibble::tibble(n = c(1, 2))))
  expect_error(check_registry_na_strings(tibble::tibble(a = " NA ")), "sentinel")
})

test_that("read_measurement_type() errors on the write_csv(na='NA') round trip", {
  path <- mt_fixture()
  expect_s3_class(read_measurement_type(path), "data.frame")

  # reproduce the exact corruption: rewrite with readr's DEFAULT na
  readr::write_csv(readr::read_csv(path, show_col_types = FALSE), path)
  expect_error(read_measurement_type(path), "sentinel strings")
  # and confirm this is invisible to a plain read — which is why the guard exists
  expect_true(all(is.na(
    readr::read_csv(path, show_col_types = FALSE)$`_qual_column`)))
  # escape hatch for inspecting a known-broken file
  expect_s3_class(read_measurement_type(path, validate = FALSE), "data.frame")
})

test_that("register_measurement_types() writes empty cells, not 'NA'", {
  path <- mt_fixture()
  new <- tibble::tibble(
    measurement_type = "carapace_length", description = "Carapace length",
    units = "mm", is_canonical = TRUE, `_qual_column` = NA_character_,
    grain = "attribute")

  out <- register_measurement_types(new, path, quiet = TRUE)
  expect_true("carapace_length" %in% out$measurement_type)
  expect_equal(nrow(out), 3)

  # the file on disk must be readable by a NULL-strict reader: no literal "NA"
  raw <- readLines(path)
  expect_false(any(grepl("(^|,)NA(,|$)", raw)))
  expect_silent(read_measurement_type(path))
})

test_that("register_measurement_types() is idempotent and does not duplicate", {
  path <- mt_fixture()
  before <- readLines(path)

  # an existing type is not re-added, and the file is left untouched
  same <- tibble::tibble(measurement_type = "abundance", description = "changed")
  out <- register_measurement_types(same, path, quiet = TRUE)
  expect_equal(nrow(out), 2)
  expect_identical(readLines(path), before)
  # existing rows are never overwritten by a same-named candidate
  expect_equal(out$description[out$measurement_type == "abundance"],
               "Specimen count")

  # duplicates within new_types collapse to one row
  dup <- tibble::tibble(measurement_type = c("zoea_other", "zoea_other"),
                        description = c("first", "second"))
  out2 <- register_measurement_types(dup, path, quiet = TRUE)
  expect_equal(sum(out2$measurement_type == "zoea_other"), 1)

  # NULL / zero-row input is a no-op returning the registry
  expect_equal(nrow(register_measurement_types(NULL, path, quiet = TRUE)), 3)
  expect_equal(nrow(register_measurement_types(dup[0, ], path, quiet = TRUE)), 3)
})

test_that("register_measurement_types() will not silently widen the registry", {
  path <- mt_fixture()
  new <- tibble::tibble(measurement_type = "settled_volume_ml",
                        description = "Settled volume", bogus_col = "x")
  expect_warning(out <- register_measurement_types(new, path, quiet = TRUE),
                 "bogus_col")
  expect_false("bogus_col" %in% names(out))
})

test_that("a corrupted registry is visible to DuckDB but not to readr", {
  skip_if_not_installed("duckdb")
  path <- mt_fixture()
  readr::write_csv(readr::read_csv(path, show_col_types = FALSE), path)  # na = "NA"

  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con), add = TRUE)
  n <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(*) AS n FROM read_csv_auto('%s') WHERE \"_qual_column\" = 'NA'",
    path))$n
  # this is the bug: DuckDB sees literal 'NA', readr sees NA
  expect_gt(n, 0)
})
