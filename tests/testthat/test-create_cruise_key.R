# create_cruise_key(): YYYY-MM-NODC, and it must refuse to mint a malformed key
# from a blank/NULL ship_nodc rather than warn (WS-B — the July 2019 Bold
# Horizon cruise shipped as cruise_key "2019-07-" because DuckDB's CONCAT()
# treats NULL as '', and nothing stopped it).

cck_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  con
}

test_that("happy path: cruise_key is YYYY-MM-NODC for every ship with a NODC", {
  con <- cck_con()
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key = c("31BD", "31JD"), ship_nodc = c("31BD", "31JD"),
    stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "cruise", data.frame(
    ship_key = c("31BD", "31JD"),
    date_ym  = as.Date(c("1955-08-01", "1984-03-01")),
    stringsAsFactors = FALSE))
  suppressMessages(create_cruise_key(con))
  got <- DBI::dbGetQuery(con, "SELECT ship_key, cruise_key FROM cruise ORDER BY ship_key")
  expect_equal(got$cruise_key, c("1955-08-31BD", "1984-03-31JD"))
})

test_that("a blank NODC errors rather than minting 'YYYY-MM-' (regression for 2019-07-)", {
  con <- cck_con()
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key = c("31BD", "BH"), ship_nodc = c("31BD", ""),
    stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "cruise", data.frame(
    ship_key = c("31BD", "BH"),
    date_ym  = as.Date(c("1955-08-01", "2019-07-01")),
    stringsAsFactors = FALSE))
  expect_error(create_cruise_key(con), "blank/NULL ship_nodc")
  # nothing about the good row's key is corrupted by the failed run — the UPDATE
  # already ran (cruise_key is set for the well-formed row), only the caller is
  # stopped before treating the table as usable
  got <- DBI::dbGetQuery(con, "SELECT ship_key, cruise_key FROM cruise ORDER BY ship_key")
  expect_equal(got$cruise_key[got$ship_key == "31BD"], "1955-08-31BD")
  expect_equal(got$cruise_key[got$ship_key == "BH"], "2019-07-")
})

test_that("a NULL ship_nodc errors the same way as a blank one", {
  con <- cck_con()
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key = "BH", ship_nodc = NA_character_, stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "cruise", data.frame(
    ship_key = "BH", date_ym = as.Date("2019-07-01"), stringsAsFactors = FALSE))
  expect_error(create_cruise_key(con), "blank/NULL ship_nodc")
})

test_that("a missing ship_key (no match in ship_tbl) still only warns", {
  # different failure mode: the cruise's ship_key matches no row in `ship` at
  # all, so cruise_key is never set by the UPDATE (stays NULL) rather than
  # being minted malformed — that is the pre-existing NULL-cruise_key warning,
  # not the new hard stop, and it must keep working.
  con <- cck_con()
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key = "31BD", ship_nodc = "31BD", stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "cruise", data.frame(
    ship_key = "ZZZZ", date_ym = as.Date("1960-01-01"), stringsAsFactors = FALSE))
  expect_warning(create_cruise_key(con), "NULL cruise_key")
  got <- DBI::dbGetQuery(con, "SELECT cruise_key FROM cruise")
  expect_true(is.na(got$cruise_key))
})

test_that("a malformed cruise_key (bad NODC width) also errors", {
  # a ship_nodc that IS populated but is not the expected 4-character NODC code
  # (a data-entry slip, say) must not silently pass — the format check catches
  # what the blank check alone would miss.
  con <- cck_con()
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key = "BH", ship_nodc = "39C", stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "cruise", data.frame(
    ship_key = "BH", date_ym = as.Date("2019-07-01"), stringsAsFactors = FALSE))
  expect_error(create_cruise_key(con), "malformed")
})
