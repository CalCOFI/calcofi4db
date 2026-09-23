# infer_ship_by_occupation(): a ship-less event's ship from the station
# occupation it matches. CalCOFI/workflows#109: v2026.09.11 released 97 of the
# CDFW Dungeness crab sorting log's 216 examined tows with a NULL cruise_key,
# because the log records no ship and its designations ('CALCOFI 0604') name a
# month with two or three ships at sea. Each tow is a CalCOFI tow, so its
# (site_key, time) matches exactly one ichthyo station occupation — the ship.

io_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  con
}

# April 2006: three ships at sea (the real spans from the ichthyo reference),
# plus the March 1988 Jordan cruise that ran into April
io_refs <- function(con) {
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key  = c("JD", "NH", "OD"),
    ship_name = c("DAVID STARR JORDAN", "NEW HORIZON", "OCEAN STARR"),
    ship_nodc = c("31JD", "32NM", "33OA"), stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "cruise", data.frame(
    cruise_key = c("2006-04-31JD", "2006-04-32NM", "2006-04-33OA", "1988-03-31JD"),
    ship_key   = c("JD", "NH", "OD", "JD"),
    date_ym    = as.Date(c("2006-04-01", "2006-04-01", "2006-04-01", "1988-03-01")),
    date_min   = as.Date(c("2006-04-06", "2006-04-01", "2006-04-12", "1988-02-23")),
    date_max   = as.Date(c("2006-04-26", "2006-04-18", "2006-04-30", "1988-04-10")),
    stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "sample", data.frame(
    dataset_key = c(rep("swfsc_ichthyo", 6), "sio_pic-zooplankton"),
    sample_type = c(rep("site", 6), "tow"),
    cruise_key  = c("2006-04-31JD", "2006-04-33OA", "2006-04-32NM", "2006-04-33OA",
                    "1988-03-31JD", "2099-01-99XX", "2006-04-33OA"),
    site_key    = c("060.0 050.0", "063.3 055.0", "070.0 060.0", "070.0 060.0",
                    "068.8 055.8", "080.0 080.0", "040.0 035.0"),
    datetime    = as.POSIXct(c(
      "2006-04-22 10:00:00",   # JD at 060.0 050.0
      "2006-04-14 03:00:00",   # OA at 063.3 055.0
      "2006-04-15 12:00:00",   # NM and OA both at 070.0 060.0 within a day
      "2006-04-15 20:00:00",
      "1988-04-04 02:00:00",   # JD at 068.8 055.8, ~8 h after the log's clock
      "2006-04-20 00:00:00",   # a cruise the reference cannot place on a ship
      "2006-04-23 16:55:00"),  # a NON-ichthyo occupation: ignored by default
      tz = "UTC"),
    stringsAsFactors = FALSE))
}

io_events <- function(con) {
  DBI::dbWriteTable(con, "log", data.frame(
    id       = 1:8,
    site_key = c("060.0 050.0", "063.3 055.0", "070.0 060.0", "068.8 055.8",
                 "060.0 050.0", "080.0 080.0", "040.0 035.0", NA),
    cruise   = c("200604", "200604", "200604", "198803", "200604", "200604",
                 "200604", "200604"),
    datetime = as.POSIXct(c(
      "2006-04-22 10:05:00",   # 1: JD's occupation -> JD
      "2006-04-14 03:00:00",   # 2: OA's occupation -> OA (same month, other ship)
      "2006-04-15 16:00:00",   # 3: NM and OA both there -> ambiguous
      "1988-04-03 18:20:00",   # 4: 7.7 h off the reference -> still JD
      "2006-04-24 11:00:00",   # 5: right station, 49 h late -> none
      "2006-04-20 01:00:00",   # 6: only candidate has no ship -> ambiguous
      "2006-04-23 16:55:00",   # 7: only a pic tow is there -> none (ichthyo only)
      "2006-04-22 10:05:00"),  # 8: no site_key -> none
      tz = "UTC"),
    stringsAsFactors = FALSE))
}

test_that("a unique station occupation names the ship, and the span then names the cruise", {
  con <- io_con(); io_refs(con); io_events(con)
  st <- infer_ship_by_occupation(con, "log", datetime_col = "datetime")
  got <- DBI::dbGetQuery(con, "
    SELECT id, ship_key, ship_key_method, cruise_key_candidates FROM log ORDER BY id")
  expect_equal(got$ship_key[c(1, 2, 4)], c("JD", "OD", "JD"))
  expect_equal(got$ship_key_method[c(1, 2, 4)], rep("occupation", 3))
  expect_equal(st$n[st$outcome == "occupation"], 3)

  # the ladder does the keying: span containment on the inferred ship
  resolve_cruise_key(con, "log", datetime_col = "datetime",
                     cruise_ym_col = "cruise", require_in_cruise = TRUE)
  ck <- DBI::dbGetQuery(con, "SELECT id, cruise_key, cruise_key_method FROM log ORDER BY id")
  # regression #109: one designation (0604), two ships, two different cruises
  expect_equal(ck$cruise_key[1:2], c("2006-04-31JD", "2006-04-33OA"))
  # the March cruise that ran into April stays on March
  expect_equal(ck$cruise_key[4], "1988-03-31JD")
  expect_equal(ck$cruise_key_method[c(1, 2, 4)], rep("span", 3))
})

test_that("two ships at one station within the window stay ambiguous, with their candidates", {
  con <- io_con(); io_refs(con); io_events(con)
  st <- infer_ship_by_occupation(con, "log", datetime_col = "datetime")
  got <- DBI::dbGetQuery(con, "
    SELECT ship_key, ship_key_method, cruise_key_candidates FROM log WHERE id = 3")
  expect_true(is.na(got$ship_key))
  expect_true(is.na(got$ship_key_method))
  expect_equal(got$cruise_key_candidates, "2006-04-32NM,2006-04-33OA")
  # and resolve_cruise_key() cannot key a ship-less row, so it is never guessed
  resolve_cruise_key(con, "log", datetime_col = "datetime", cruise_ym_col = "cruise")
  expect_true(is.na(DBI::dbGetQuery(con, "SELECT cruise_key FROM log WHERE id = 3")[[1]]))
  expect_equal(st$n[st$outcome == "ambiguous"], 2)
})

test_that("a lone candidate cruise with no ship in the reference is ambiguous, not inferred", {
  con <- io_con(); io_refs(con); io_events(con)
  infer_ship_by_occupation(con, "log", datetime_col = "datetime")
  got <- DBI::dbGetQuery(con, "
    SELECT ship_key, cruise_key_candidates FROM log WHERE id = 6")
  expect_true(is.na(got$ship_key))
  expect_equal(got$cruise_key_candidates, "2099-01-99XX")
})

test_that("outside the window, at another station, or with no site: no ship", {
  con <- io_con(); io_refs(con); io_events(con)
  st <- infer_ship_by_occupation(con, "log", datetime_col = "datetime")
  got <- DBI::dbGetQuery(con, "
    SELECT id, ship_key, cruise_key_candidates FROM log WHERE id IN (5, 7, 8) ORDER BY id")
  expect_true(all(is.na(got$ship_key)))
  expect_true(all(is.na(got$cruise_key_candidates)))
  expect_equal(st$n[st$outcome == "none"], 3)
  # widening the window reaches row 5 (49 h late)
  infer_ship_by_occupation(con, "log", datetime_col = "datetime", tolerance_hours = 50)
  expect_equal(DBI::dbGetQuery(con, "SELECT ship_key FROM log WHERE id = 5")[[1]], "JD")
})

test_that("occupation_sql chooses the reference: a pic tow counts only when asked for", {
  con <- io_con(); io_refs(con); io_events(con)
  infer_ship_by_occupation(
    con, "log", datetime_col = "datetime",
    occupation_sql = "SELECT cruise_key, site_key, datetime FROM sample")
  expect_equal(DBI::dbGetQuery(con, "SELECT ship_key FROM log WHERE id = 7")[[1]], "OD")
})

test_that("a ship the source recorded is never overwritten", {
  con <- io_con(); io_refs(con); io_events(con)
  DBI::dbExecute(con, "ALTER TABLE log ADD COLUMN ship_key TEXT")
  DBI::dbExecute(con, "UPDATE log SET ship_key = 'NH' WHERE id = 1")
  st <- infer_ship_by_occupation(con, "log", datetime_col = "datetime")
  got <- DBI::dbGetQuery(con, "SELECT ship_key, ship_key_method FROM log WHERE id = 1")
  expect_equal(got$ship_key, "NH")
  expect_true(is.na(got$ship_key_method))
  expect_equal(st$n[st$outcome == "source"], 1)
  expect_equal(st$n[st$outcome == "occupation"], 2)
})

test_that("a re-run clears the previous inference instead of keeping it", {
  con <- io_con(); io_refs(con); io_events(con)
  infer_ship_by_occupation(con, "log", datetime_col = "datetime")
  st <- infer_ship_by_occupation(con, "log", datetime_col = "datetime",
                                 tolerance_hours = 0)
  got <- DBI::dbGetQuery(con, "SELECT id, ship_key, ship_key_method FROM log ORDER BY id")
  # only the exact-time match (row 2) survives a zero-hour window
  expect_equal(got$ship_key[2], "OD")
  expect_true(all(is.na(got$ship_key[c(1, 4)])))
  expect_true(all(is.na(got$ship_key_method[c(1, 4)])))
  expect_equal(st$n[st$outcome == "source"], 0)
  expect_equal(sum(st$n), 8)
})

test_that("bad arguments fail loudly", {
  con <- io_con(); io_refs(con); io_events(con)
  expect_error(infer_ship_by_occupation(con, "nope", "datetime"), "target table")
  expect_error(infer_ship_by_occupation(con, "log", "dt"), "datetime_col")
  expect_error(infer_ship_by_occupation(con, "log", "datetime", site_key_col = "stn"),
               "site_key_col")
  expect_error(infer_ship_by_occupation(con, "log", "datetime", tolerance_hours = -1),
               "tolerance_hours")
})
