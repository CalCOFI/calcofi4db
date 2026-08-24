
# small synthetic ship/cruise refs + a target table, so the cruise_key rule
# (YYYY-MM-NODC) is asserted exactly rather than inferred from a real ingest
setup_cruise_key_fixture <- function(con, table_name, ship_name = TRUE) {
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key  = c("33RR", "32NM"),
    ship_name = c("ROGER REVELLE", "NEW HORIZON"),
    ship_nodc = c("33RR", "32NM"),
    stringsAsFactors = FALSE))
  # no observed span on this fixture: resolution falls through to the month rule,
  # which is what these tests pin (see test-resolve_cruise_key.R for the span)
  DBI::dbWriteTable(con, "cruise", data.frame(
    cruise_key = c("2004-11-33RR", "2010-01-32NM"),
    ship_key   = c("33RR", "32NM"),
    date_min   = as.Date(c(NA, NA)),
    date_max   = as.Date(c(NA, NA)),
    stringsAsFactors = FALSE))

  d <- data.frame(
    ship_code    = c("33RR", "32NM", "99ZZ"),
    datetime_utc = as.POSIXct(
      c("2004-11-02 10:00:00", "2010-01-15 06:30:00", "2011-05-01 00:00:00"),
      tz = "UTC"),
    stringsAsFactors = FALSE)
  if (ship_name)
    d$ship_name <- c("ROGER REVELLE", "NEW HORIZON", "MYSTERY BOAT")
  DBI::dbWriteTable(con, table_name, d)
}

test_that("derive_cruise_key_on_casts derives YYYY-MM-NODC on the default casts table", {
  skip_if_not_installed("duckdb")

  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))
  setup_cruise_key_fixture(con, "casts")

  res <- derive_cruise_key_on_casts(con, fetch_ices = FALSE)

  got <- DBI::dbGetQuery(con,
    "SELECT ship_code, ship_key, cruise_key FROM casts ORDER BY ship_code")
  expect_equal(got$cruise_key, c("2010-01-32NM", "2004-11-33RR", NA_character_))
  expect_equal(got$ship_key,   c("32NM", "33RR", NA_character_))
  expect_true(is.list(res))
  expect_equal(nrow(res$unmatched_report), 1L)
  expect_equal(res$unmatched_report$ship_code, "99ZZ")
})

test_that("derive_cruise_key_on_casts honors table_name for a non-casts table", {
  skip_if_not_installed("duckdb")

  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))
  setup_cruise_key_fixture(con, "picoplankton_bacteria_bottle")

  derive_cruise_key_on_casts(
    con, fetch_ices = FALSE, table_name = "picoplankton_bacteria_bottle")

  got <- DBI::dbGetQuery(con,
    "SELECT ship_code, cruise_key FROM picoplankton_bacteria_bottle
     ORDER BY ship_code")
  expect_equal(got$cruise_key, c("2010-01-32NM", "2004-11-33RR", NA_character_))
  # the default table must be left alone / not required to exist
  expect_false("casts" %in% DBI::dbListTables(con))
})

test_that("derive_cruise_key_on_casts works when the target has no ship_name column", {
  skip_if_not_installed("duckdb")

  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))
  setup_cruise_key_fixture(con, "mets_wide", ship_name = FALSE)

  derive_cruise_key_on_casts(
    con, fetch_ices = FALSE, table_name = "mets_wide")

  got <- DBI::dbGetQuery(con,
    "SELECT ship_code, cruise_key FROM mets_wide ORDER BY ship_code")
  expect_equal(got$cruise_key, c("2010-01-32NM", "2004-11-33RR", NA_character_))
})

test_that("derive_cruise_key_on_casts fails loudly on a missing table or column", {
  skip_if_not_installed("duckdb")

  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))
  setup_cruise_key_fixture(con, "casts")

  expect_error(
    derive_cruise_key_on_casts(con, fetch_ices = FALSE, table_name = "nope"),
    "target table required")

  DBI::dbExecute(con, "CREATE TABLE no_code AS SELECT datetime_utc FROM casts")
  expect_error(
    derive_cruise_key_on_casts(con, fetch_ices = FALSE, table_name = "no_code"),
    "ship_code")
})
