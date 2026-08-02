# Flagged-row CSVs are committed and reviewed in diffs, so a write that only
# re-stamps `_ingested_at` is pure noise — data/flagged/invalid_egg_stages.csv
# churned the same 790 rows on every ingest run, which hides the diff that would
# actually matter.

fixture <- function(ts = "2026-08-02T10:00:00Z", n = 3) {
  tibble::tibble(
    net_uuid         = sprintf("N%02d", seq_len(n)),
    species_id       = seq_len(n),
    stage            = 12L,
    tally            = 1L,
    `_ingested_at`   = ts)
}

test_that("an unchanged flag set does not rewrite the file", {
  p <- withr::local_tempfile(fileext = ".csv")
  flag_invalid_rows(fixture(), p, "egg stages")
  before <- file.mtime(p)
  txt_before <- readLines(p)

  Sys.setFileTime(p, before - 60)   # so an actual rewrite is detectable
  # same rows, new ingest timestamp — the only thing that changed
  expect_message(
    flag_invalid_rows(fixture(ts = "2026-08-03T11:22:33Z"), p, "egg stages"),
    "unchanged")
  expect_identical(readLines(p), txt_before)
})

test_that("a real change still writes", {
  p <- withr::local_tempfile(fileext = ".csv")
  flag_invalid_rows(fixture(), p, "egg stages")

  # one more flagged row is a real change, even with the same timestamp
  expect_message(flag_invalid_rows(fixture(n = 4), p, "egg stages"), "Flagged 4 rows")
  expect_equal(nrow(readr::read_csv(p, show_col_types = FALSE)), 4L)

  # and so is a changed value in a non-volatile column
  d <- fixture(n = 4); d$stage <- 99L
  expect_message(flag_invalid_rows(d, p, "egg stages"), "Flagged 4 rows")
  expect_true(all(readr::read_csv(p, show_col_types = FALSE)$stage == 99L))
})

test_that("column order and type round-tripping do not fake a change", {
  # the on-disk copy has been through a CSV round trip and the in-memory tibble
  # has not, so a typed comparison would see integer 1 vs "1" and rewrite forever
  p <- withr::local_tempfile(fileext = ".csv")
  flag_invalid_rows(fixture(), p, "egg stages")
  d <- fixture(ts = "2026-09-09T09:09:09Z")[, c(5, 3, 1, 4, 2)]   # reordered
  expect_message(flag_invalid_rows(d, p, "egg stages"), "unchanged")
})

test_that("volatile_cols = character() forces the write, and append is unaffected", {
  p <- withr::local_tempfile(fileext = ".csv")
  flag_invalid_rows(fixture(), p, "egg stages")
  expect_message(
    flag_invalid_rows(fixture(ts = "2026-12-25T00:00:00Z"), p, "egg stages",
                      volatile_cols = character()),
    "Flagged 3 rows")
  # read back as character: read_csv would otherwise parse the ISO string to
  # POSIXct and the comparison would be about type, not about what was written
  expect_equal(
    readr::read_csv(p, show_col_types = FALSE,
                    col_types = readr::cols(.default = readr::col_character())
                    )$`_ingested_at`[1],
    "2026-12-25T00:00:00Z")

  # append never takes the skip path — it is additive by definition
  p2 <- withr::local_tempfile(fileext = ".csv")
  flag_invalid_rows(fixture(), p2, "egg stages")
  flag_invalid_rows(fixture(), p2, "egg stages", append = TRUE)
  expect_equal(nrow(readr::read_csv(p2, show_col_types = FALSE)), 6L)
})

test_that("empty cells are written empty, never as the string NA", {
  # same trap as the metadata registries: DuckDB's read_csv_auto does not treat
  # "NA" as NULL, so readr's default na = "NA" would ship a literal value
  p <- withr::local_tempfile(fileext = ".csv")
  d <- fixture(); d$stage <- NA_integer_
  flag_invalid_rows(d, p, "egg stages")
  expect_false(any(grepl(",NA,|,NA$", readLines(p))))
})
