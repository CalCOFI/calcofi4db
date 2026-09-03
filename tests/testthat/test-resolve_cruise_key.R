# cruise_key resolution: the designated month, not the event's month.
# v2026.08.14 keyed every bottle cast by its own calendar month, so cruise 5508BD
# (7 Aug - 25 Sep 1955) shipped as 1955-08-31BD + 1955-09-31BD — and the second
# is a REAL ichthyo cruise, so no FK ever objected. These fixtures pin the ladder
# span -> source designation -> month, and the boundary case the old rule broke.

ck_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  con
}

ck_refs <- function(con) {
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key = c("31BD", "31JD"), ship_name = c("BLACK DOUGLAS", "DAVID STARR JORDAN"),
    ship_nodc = c("31BD", "31JD"), stringsAsFactors = FALSE))
  # 5508BD spans 7 Aug - 3 Sep; 5509BD 8 Sep - 25 Sep (no overlap, 5 day gap);
  # 8403JD is designated March but started 9 Feb; 5511BD has no sites (NULL span)
  DBI::dbWriteTable(con, "cruise", data.frame(
    cruise_key = c("1955-08-31BD", "1955-09-31BD", "1984-03-31JD", "1955-11-31BD"),
    ship_key   = c("31BD", "31BD", "31JD", "31BD"),
    date_ym    = as.Date(c("1955-08-01", "1955-09-01", "1984-03-01", "1955-11-01")),
    date_min   = as.Date(c("1955-08-07", "1955-09-08", "1984-02-09", NA)),
    date_max   = as.Date(c("1955-09-03", "1955-09-25", "1984-03-29", NA)),
    stringsAsFactors = FALSE))
}

ck_events <- function(con, tbl = "casts") {
  DBI::dbWriteTable(con, tbl, data.frame(
    id       = 1:7,
    ship_key = c("31BD", "31BD", "31BD", "31JD", "31BD", "31BD", NA),
    cruise   = c("195508", "195508", "195509", "198402", "195511", NA, "195508"),
    datetime_utc = as.POSIXct(c(
      "1955-08-20 12:00:00",   # 1: inside 5508BD, August
      "1955-09-03 20:12:00",   # 2: inside 5508BD but in SEPTEMBER (the bug)
      "1955-09-10 00:00:00",   # 3: inside 5509BD
      "1984-02-10 00:00:00",   # 4: source says 8402, reference span says 8403
      "1955-11-15 00:00:00",   # 5: no span (no sites) -> source designation
      "1955-12-15 00:00:00",   # 6: no span, no designation -> month rule
      "1955-08-20 00:00:00"),  # 7: no ship -> no key
      tz = "UTC"),
    stringsAsFactors = FALSE))
}

test_that("span containment wins over the event's own month", {
  con <- ck_con(); ck_refs(con); ck_events(con)
  st <- resolve_cruise_key(con, "casts", datetime_col = "datetime_utc",
                           cruise_ym_col = "cruise")
  got <- DBI::dbGetQuery(con,
    "SELECT id, cruise_key, cruise_key_method FROM casts ORDER BY id")
  # the September cast of the August cruise stays on the August cruise
  expect_equal(got$cruise_key[1:3], c("1955-08-31BD", "1955-08-31BD", "1955-09-31BD"))
  expect_equal(got$cruise_key_method[1:3], rep("span", 3))
  # reference designation beats the source's when the span is known
  expect_equal(got$cruise_key[4], "1984-03-31JD")
  # no span: the source designation is used as-is
  expect_equal(got$cruise_key[5], "1955-11-31BD")
  expect_equal(got$cruise_key_method[5], "source")
  # no span and no designation: month rule (and the key need not be a known cruise)
  expect_equal(got$cruise_key[6], "1955-12-31BD")
  expect_equal(got$cruise_key_method[6], "month")
  expect_true(is.na(got$cruise_key[7]))
  expect_true(is.na(got$cruise_key_method[7]))
  expect_setequal(st$method, c("span", "source", "month", "none"))
  expect_equal(st$n[st$method == "span"], 4)
})

test_that("require_in_cruise leaves unknown keys NULL; no cruise_ym_col uses month", {
  con <- ck_con(); ck_refs(con); ck_events(con)
  resolve_cruise_key(con, "casts", datetime_col = "datetime_utc",
                     require_in_cruise = TRUE)
  got <- DBI::dbGetQuery(con, "SELECT id, cruise_key FROM casts ORDER BY id")
  expect_equal(got$cruise_key[2], "1955-08-31BD")     # span still wins
  expect_equal(got$cruise_key[5], "1955-11-31BD")     # month rule, and it exists
  expect_true(is.na(got$cruise_key[6]))               # 1955-12-31BD is not a cruise
})

test_that("tolerance_days extends the span; ties go to the nearest span; a malformed designation is ignored", {
  con <- ck_con(); ck_refs(con)
  DBI::dbWriteTable(con, "ev", data.frame(
    ship_key = "31BD", cruise = c("5508", "195513", NA),
    datetime_utc = as.POSIXct(c("1955-08-05 00:00:00", "1955-09-05 00:00:00",
                                "1955-09-06 00:00:00"), tz = "UTC"),
    stringsAsFactors = FALSE))
  resolve_cruise_key(con, "ev", datetime_col = "datetime_utc",
                     cruise_ym_col = "cruise", tolerance_days = 3L)
  got <- DBI::dbGetQuery(con, "SELECT cruise_key, cruise_key_method FROM ev")
  expect_equal(got$cruise_key[1], "1955-08-31BD")     # 2 days before date_min
  expect_equal(got$cruise_key_method[1], "span")
  # 5 Sep is within tolerance of BOTH (5508BD ended 3 Sep, 5509BD starts 8 Sep):
  # 2 days past one span vs 3 days before the other -> the nearer span
  expect_equal(got$cruise_key[2], "1955-08-31BD")
  # 6 Sep: 3 days past 5508BD, 2 days before 5509BD -> 5509BD
  expect_equal(got$cruise_key[3], "1955-09-31BD")
  # with no tolerance neither date is in a span; "195513" is not a month, so the
  # month rule applies to both
  resolve_cruise_key(con, "ev", datetime_col = "datetime_utc",
                     cruise_ym_col = "cruise", tolerance_days = 0L)
  got <- DBI::dbGetQuery(con, "SELECT cruise_key, cruise_key_method FROM ev")
  expect_equal(got$cruise_key[2:3], c("1955-09-31BD", "1955-09-31BD"))
  expect_equal(got$cruise_key_method[2:3], c("month", "month"))
})

test_that("a cruise reference without spans fails loudly", {
  con <- ck_con(); ck_refs(con); ck_events(con)
  DBI::dbExecute(con, "ALTER TABLE cruise DROP COLUMN date_min")
  expect_error(resolve_cruise_key(con, "casts", datetime_col = "datetime_utc"),
               "add_cruise_date_span")
})

test_that("add_cruise_date_span() writes spans and reports spill + overlap", {
  con <- ck_con(); ck_refs(con)
  DBI::dbExecute(con, "ALTER TABLE cruise DROP COLUMN date_min")
  DBI::dbExecute(con, "ALTER TABLE cruise DROP COLUMN date_max")
  DBI::dbWriteTable(con, "site", data.frame(
    cruise_key = c("1955-08-31BD", "1955-08-31BD", "1955-09-31BD", "1984-03-31JD", NA),
    datetime_start_utc = as.POSIXct(c("1955-08-07 10:00", "1955-09-03 20:12",
                                      "1955-09-08 00:00", "1984-02-09 00:00",
                                      "1955-08-08 00:00"), tz = "UTC"),
    stringsAsFactors = FALSE))
  sp <- add_cruise_date_span(
    con, "SELECT cruise_key, datetime_start_utc AS datetime FROM site")
  got <- DBI::dbGetQuery(con, "SELECT cruise_key, date_min, date_max FROM cruise ORDER BY 1")
  expect_equal(as.character(got$date_min), c("1955-08-07", "1955-09-08", NA, "1984-02-09"))
  expect_equal(as.character(got$date_max), c("1955-09-03", "1955-09-08", NA, "1984-02-09"))
  expect_equal(sp$n_events[sp$cruise_key == "1955-08-31BD"], 2)
  expect_true(sp$spills_month[sp$cruise_key == "1955-08-31BD"])     # Aug cruise into Sep
  expect_true(sp$spills_month[sp$cruise_key == "1984-03-31JD"])     # Mar cruise from Feb
  expect_false(sp$spills_month[sp$cruise_key == "1955-09-31BD"])
  expect_false(any(sp$overlaps))
  # an overlap is a reference-data error the caller must be able to assert on:
  # a September cruise whose first site precedes the August cruise's last one
  DBI::dbExecute(con, "INSERT INTO site VALUES ('1955-09-31BD', TIMESTAMP '1955-09-02 00:00:00')")
  sp2 <- add_cruise_date_span(
    con, "SELECT cruise_key, datetime_start_utc AS datetime FROM site")
  expect_true(sp2$overlaps[sp2$cruise_key == "1955-08-31BD"])
  expect_true(sp2$overlaps[sp2$cruise_key == "1955-09-31BD"])
  expect_false(sp2$overlaps[sp2$cruise_key == "1984-03-31JD"])
  expect_equal(as.character(sp2$date_min[sp2$cruise_key == "1955-09-31BD"]), "1955-09-02")
})

test_that("a blank-NODC ship yields NULL cruise_key/method, never 'YYYY-MM-'", {
  # WS-B: the July 2019 Bold Horizon cruise was released as cruise_key
  # "2019-07-" because DuckDB's CONCAT() treats NULL as '' and nothing refused
  # it. Steps 2 (source) and 3 (month) must not mint a key from a blank/NULL
  # ship_nodc; the row stays unresolved instead of shipping a malformed key.
  con <- ck_con()
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key = c("31BD", "BH"), ship_name = c("BLACK DOUGLAS", "BOLD HORIZON"),
    ship_nodc = c("31BD", ""), stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "cruise", data.frame(
    cruise_key = "1955-08-31BD", ship_key = "31BD",
    date_ym = as.Date("1955-08-01"),
    date_min = as.Date("1955-08-07"), date_max = as.Date("1955-09-03"),
    stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "ev", data.frame(
    ship_key = c("BH", "BH"),
    cruise   = c("201907", NA),
    datetime_utc = as.POSIXct(c("2019-07-15 00:00:00", "2019-07-20 00:00:00"),
                              tz = "UTC"),
    stringsAsFactors = FALSE))
  st <- resolve_cruise_key(con, "ev", datetime_col = "datetime_utc",
                           cruise_ym_col = "cruise")
  got <- DBI::dbGetQuery(con, "SELECT cruise_key, cruise_key_method FROM ev")
  # row 1 would resolve "source" (has a designation), row 2 "month" (no
  # designation) if ship_nodc were populated; both must stay NULL instead
  expect_true(all(is.na(got$cruise_key)))
  expect_true(all(is.na(got$cruise_key_method)))
  expect_false(any(grepl("^2019-07-$", got$cruise_key)))
  expect_equal(st$method[st$method == "none"], "none")
  expect_equal(st$n[st$method == "none"], 2)
})

test_that("a numeric (DOUBLE) designation column resolves like a character one", {
  # the bottle CSV reader typed all-digit `Cruise` as DOUBLE, so CAST gave
  # '195508.0' and 0 of 5,408 unspanned casts took the source step (2026-08-24)
  con <- ck_con(); ck_refs(con)
  DBI::dbWriteTable(con, "ev", data.frame(
    ship_key = c("31BD", "31BD", "31BD"),
    cruise   = c(195511, 195511.0, NA_real_),
    datetime_utc = as.POSIXct(c("1955-11-15 00:00:00", "1955-12-01 00:00:00",
                                "1955-12-01 00:00:00"), tz = "UTC"),
    stringsAsFactors = FALSE))
  expect_identical(DBI::dbGetQuery(con, "SELECT typeof(cruise) t FROM ev LIMIT 1")$t, "DOUBLE")
  resolve_cruise_key(con, "ev", datetime_col = "datetime_utc", cruise_ym_col = "cruise")
  got <- DBI::dbGetQuery(con, "SELECT cruise_key, cruise_key_method FROM ev")
  expect_equal(got$cruise_key, c("1955-11-31BD", "1955-11-31BD", "1955-12-31BD"))
  expect_equal(got$cruise_key_method, c("source", "source", "month"))
})
