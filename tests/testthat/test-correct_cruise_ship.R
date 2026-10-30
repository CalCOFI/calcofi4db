# correct_cruise_ship(): a ship code corrected on ONE source cruise, leaving the
# ship's other cruises alone (the July 2009 McArthur / McArthur II case)

ccs_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  # bottle's `cruise` arrives as a DOUBLE (YYYYMM.0)
  DBI::dbWriteTable(con, "casts", data.frame(
    cast_id   = 1:4,
    cruise    = c(198502, 200907, 200907, 200901),
    ship_code = c("31M4", "31M4", "31M4", "32NM"),
    ship_name = c("MCARTHUR", "MCARTHUR", "MCARTHUR", "RV NEW HORIZON")))
  con
}

ccs_csv <- function(rows, env = parent.frame()) {
  f <- withr::local_tempfile(fileext = ".csv", .local_envir = env)
  readr::write_csv(rows, f, na = "")
  f
}

ccs_row <- function(dataset_key = "calcofi_bottle", cruise_ym = "200907")
  data.frame(ship_code = "31M4", cruise_ym = cruise_ym, ship_code_new = "33MH",
             ship_name_new = "MCARTHUR II", dataset_key = dataset_key,
             evidence = "SWFSC reference")

test_that("only the named cruise changes; the ship's other cruise keeps its code", {
  con <- ccs_con()
  out <- suppressMessages(correct_cruise_ship(
    con, "casts", ccs_csv(ccs_row()), dataset_key = "calcofi_bottle",
    ship_name_col = "ship_name"))
  expect_equal(out$n_updated, 2L)
  got <- DBI::dbGetQuery(con, "SELECT ship_code, ship_name FROM casts ORDER BY cast_id")
  expect_equal(got$ship_code, c("31M4", "33MH", "33MH", "32NM"))
  expect_equal(got$ship_name, c("MCARTHUR", "MCARTHUR II", "MCARTHUR II", "RV NEW HORIZON"))
})

test_that("a VARCHAR YYYYMM designation matches too, and the name is left alone by default", {
  con <- ccs_con()
  DBI::dbExecute(con, "ALTER TABLE casts ALTER cruise TYPE VARCHAR")
  DBI::dbExecute(con, "UPDATE casts SET cruise = CAST(CAST(cruise AS DOUBLE) AS INTEGER)")
  suppressMessages(correct_cruise_ship(
    con, "casts", ccs_csv(ccs_row()), dataset_key = "calcofi_bottle"))
  got <- DBI::dbGetQuery(con, "SELECT ship_code, ship_name FROM casts ORDER BY cast_id")
  expect_equal(got$ship_code, c("31M4", "33MH", "33MH", "32NM"))
  expect_equal(got$ship_name[2], "MCARTHUR")
})

test_that("a row for another dataset does not apply", {
  con <- ccs_con()
  out <- suppressMessages(correct_cruise_ship(
    con, "casts", ccs_csv(ccs_row(dataset_key = "cce-lter_picoplankton-bacteria")),
    dataset_key = "calcofi_bottle"))
  expect_equal(nrow(out), 0L)
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM casts WHERE ship_code = '33MH'")$n, 0)
})

test_that("a semicolon list of datasets applies to each listed one", {
  con <- ccs_con()
  out <- suppressMessages(correct_cruise_ship(
    con, "casts",
    ccs_csv(ccs_row(dataset_key = "calcofi_bottle;cce-lter_picoplankton-bacteria")),
    dataset_key = "calcofi_bottle"))
  expect_equal(out$n_updated, 2L)
})

test_that("a listed row that matches nothing is an error (stale registry or changed source)", {
  con <- ccs_con()
  expect_error(suppressMessages(correct_cruise_ship(
    con, "casts", ccs_csv(ccs_row(cruise_ym = "200807")), dataset_key = "calcofi_bottle")),
    "matched no row.*31M4@200807")
})

test_that("a malformed cruise_ym or a missing column is an error", {
  con <- ccs_con()
  expect_error(correct_cruise_ship(
    con, "casts", ccs_csv(ccs_row(cruise_ym = "2009-07")), dataset_key = "calcofi_bottle"),
    "YYYYMM")
  expect_error(correct_cruise_ship(
    con, "casts", ccs_csv(ccs_row()[, -6]), dataset_key = "calcofi_bottle"),
    "evidence")
})
