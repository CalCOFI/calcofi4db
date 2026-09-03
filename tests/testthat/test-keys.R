# cruise_key integrity + provider UUIDs (WS-B). Three functions, one fixture
# family per function: complete_cruise_reference() fills the `cruise`
# reference; check_cruise_key_integrity() is the release gate (one violated
# check per fixture, everything else clean); match_station_occupation()
# stamps sample.station_uuid.

k_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  con
}

# ---- complete_cruise_reference() ------------------------------------------

test_that("a missing cruise_key gets a derived row; existing rows are stamped 'swfsc'", {
  con <- k_con()
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key = c("BD", "JD"), ship_nodc = c("31BD", "31JD"), stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "cruise", data.frame(
    cruise_key = "1955-08-31BD", ship_key = "BD",
    date_ym = as.Date("1955-08-01"),
    date_min = as.Date("1955-08-07"), date_max = as.Date("1955-08-20"),
    cruise_uuid = "11111111-1111-1111-1111-111111111111",
    stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = c("ds1:e:1", "ds1:e:2", "ds2:e:1"),
    dataset_key = c("ds1", "ds1", "ds2"),
    cruise_key = c("1955-08-31BD", "1955-08-31BD", "2020-01-31JD"),
    datetime = as.POSIXct(c("1955-08-10", "1955-08-12", "2020-01-06"), tz = "UTC"),
    stringsAsFactors = FALSE))

  added <- suppressMessages(complete_cruise_reference(con))
  expect_equal(nrow(added), 1L)
  expect_equal(added$cruise_key, "2020-01-31JD")
  expect_equal(added$ship_key, "JD")
  expect_equal(as.character(added$date_ym), "2020-01-01")
  expect_equal(as.character(added$date_min), "2020-01-06")
  expect_equal(as.character(added$date_max), "2020-01-06")

  got <- DBI::dbGetQuery(con, "SELECT * FROM cruise ORDER BY cruise_key")
  # ascending cruise_key: "1955-08-31BD" (swfsc) sorts before "2020-01-31JD" (derived)
  expect_equal(got$cruise_key_method, c("swfsc", "derived"))
  expect_true(is.na(got$cruise_uuid[got$cruise_key == "2020-01-31JD"]))
  expect_equal(got$cruise_key_datasets[got$cruise_key == "1955-08-31BD"], "ds1")
  expect_equal(got$cruise_key_datasets[got$cruise_key == "2020-01-31JD"], "ds2")
})

test_that("an unresolvable NODC errors, naming the offending key(s)", {
  con <- k_con()
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key = "BD", ship_nodc = "31BD", stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "cruise", data.frame(
    cruise_key = "1955-08-31BD", ship_key = "BD",
    date_ym = as.Date("1955-08-01"),
    date_min = as.Date("1955-08-07"), date_max = as.Date("1955-08-20"),
    cruise_uuid = "11111111-1111-1111-1111-111111111111",
    stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = "ds1:e:1", dataset_key = "ds1", cruise_key = "1999-01-ZZZZ",
    datetime = as.POSIXct("1999-01-01", tz = "UTC"), stringsAsFactors = FALSE))
  expect_error(complete_cruise_reference(con), "1999-01-ZZZZ")
})

test_that("existing rows are left alone: values, row count and PK survive", {
  con <- k_con()
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key = "BD", ship_nodc = "31BD", stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "cruise", data.frame(
    cruise_key = "1955-08-31BD", ship_key = "BD",
    date_ym = as.Date("1955-08-01"),
    date_min = as.Date("1955-08-07"), date_max = as.Date("1955-08-20"),
    cruise_uuid = "11111111-1111-1111-1111-111111111111",
    stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = "ds1:e:1", dataset_key = "ds1", cruise_key = "1955-08-31BD",
    datetime = as.POSIXct("1955-08-10", tz = "UTC"), stringsAsFactors = FALSE))

  suppressMessages(complete_cruise_reference(con))
  n1 <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM cruise")$n
  before <- DBI::dbGetQuery(con, "SELECT * FROM cruise")
  added2 <- suppressMessages(complete_cruise_reference(con))
  expect_equal(nrow(added2), 0L)
  n2 <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM cruise")$n
  expect_equal(n1, n2)
  after <- DBI::dbGetQuery(con, "SELECT * FROM cruise")
  expect_equal(before$cruise_uuid, after$cruise_uuid)
  expect_equal(before$cruise_key_method, after$cruise_key_method)
})

test_that("a cruise_tbl arriving as a VIEW is materialized into a table", {
  con <- k_con()
  DBI::dbWriteTable(con, "ship", data.frame(
    ship_key = "BD", ship_nodc = "31BD", stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "_cruise_base", data.frame(
    cruise_key = "1955-08-31BD", ship_key = "BD",
    date_ym = as.Date("1955-08-01"),
    date_min = as.Date("1955-08-07"), date_max = as.Date("1955-08-20"),
    cruise_uuid = "11111111-1111-1111-1111-111111111111",
    stringsAsFactors = FALSE))
  DBI::dbExecute(con, "CREATE VIEW cruise AS SELECT * FROM _cruise_base")
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = "ds1:e:1", dataset_key = "ds1", cruise_key = "1955-08-31BD",
    datetime = as.POSIXct("1955-08-10", tz = "UTC"), stringsAsFactors = FALSE))

  suppressMessages(complete_cruise_reference(con))
  kind <- DBI::dbGetQuery(con,
    "SELECT table_type FROM information_schema.tables WHERE table_name = 'cruise'")$table_type
  expect_match(kind, "TABLE", ignore.case = TRUE)
  got <- DBI::dbGetQuery(con, "SELECT cruise_key_method FROM cruise")
  expect_equal(got$cruise_key_method, "swfsc")
})

# ---- check_cruise_key_integrity() ------------------------------------------

# a baseline that PASSES every hard check and stays within every ratchet;
# each test below perturbs exactly one thing.
ck2_base <- function() {
  list(
    ship = data.frame(
      ship_key = c("BD", "JD"), ship_nodc = c("31BD", "31JD"),
      stringsAsFactors = FALSE),
    cruise = data.frame(
      cruise_key = c("1955-08-31BD", "1955-09-31BD", "2020-01-31JD"),
      ship_key   = c("BD", "BD", "JD"),
      date_ym    = as.Date(c("1955-08-01", "1955-09-01", "2020-01-01")),
      date_min   = as.Date(c("1955-08-07", "1955-09-08", "2020-01-05")),
      date_max   = as.Date(c("1955-08-20", "1955-09-20", "2020-01-10")),
      cruise_uuid = c("11111111-1111-1111-1111-111111111111",
                      "22222222-2222-2222-2222-222222222222", NA_character_),
      cruise_key_method   = c("swfsc", "swfsc", "derived"),
      cruise_key_datasets = c("ds1", "ds1", "ds2"),
      stringsAsFactors = FALSE),
    sample = data.frame(
      sample_key = c("ds1:e:1", "ds1:e:2", "ds2:e:1",
                     "swfsc_ichthyo:site:S1", "swfsc_ichthyo:tow:T1"),
      root_sample_key = c("ds1:e:1", "ds1:e:2", "ds2:e:1",
                          "swfsc_ichthyo:site:S1", "swfsc_ichthyo:site:S1"),
      dataset_key = c("ds1", "ds1", "ds2", "swfsc_ichthyo", "swfsc_ichthyo"),
      sample_type = c("e", "e", "e", "site", "tow"),
      cruise_key  = c("1955-08-31BD", "1955-09-31BD", "2020-01-31JD",
                      "1955-08-31BD", "1955-08-31BD"),
      datetime    = as.POSIXct(c(
        "1955-08-10 00:00:00", "1955-09-10 00:00:00", "2020-01-06 00:00:00",
        "1955-08-10 00:00:00", "1955-08-10 01:00:00"), tz = "UTC"),
      source_uuid = c(NA_character_, NA_character_, NA_character_,
                      "33333333-3333-3333-3333-333333333333",
                      "44444444-4444-4444-4444-444444444444"),
      stringsAsFactors = FALSE),
    obs = data.frame(
      sample_key  = c("ds1:e:1", "ds2:e:1"),
      dataset_key = c("ds1", "ds2"),
      cruise_key  = c("1955-08-31BD", "2020-01-31JD"),
      stringsAsFactors = FALSE))
}

ck2_con <- function(fx, env = parent.frame()) {
  con <- k_con(env)
  DBI::dbWriteTable(con, "ship", fx$ship)
  DBI::dbWriteTable(con, "cruise", fx$cruise)
  DBI::dbWriteTable(con, "sample", fx$sample)
  DBI::dbWriteTable(con, "obs", fx$obs)
  con
}

test_that("the baseline fixture passes every hard check and ratchet", {
  con <- ck2_con(ck2_base())
  out <- suppressMessages(check_cruise_key_integrity(
    con, manifest_ichthyo = 0L, halt = FALSE))
  fails <- out[(out$mode == "fail" & out$n > 0) |
               (out$mode == "ratchet" & out$n > out$allowance), ]
  expect_equal(nrow(fails), 0L)
  expect_silent(suppressMessages(check_cruise_key_integrity(
    con, manifest_ichthyo = 0L)))
})

test_that("check 1: a malformed cruise_key format fails", {
  fx <- ck2_base()
  fx$cruise$cruise_key[3] <- "2020-1-31JD"          # month not zero-padded
  fx$sample$cruise_key[fx$sample$cruise_key == "2020-01-31JD"] <- "2020-1-31JD"
  fx$obs$cruise_key[fx$obs$cruise_key == "2020-01-31JD"] <- "2020-1-31JD"
  con <- ck2_con(fx)
  expect_error(suppressMessages(check_cruise_key_integrity(con, manifest_ichthyo = 0L)),
               "cruise_key_format")
})

test_that("check 2: cruise.date_ym disagreeing with the key's month fails", {
  fx <- ck2_base()
  fx$cruise$date_ym[1] <- as.Date("1955-07-01")     # was 1955-08
  con <- ck2_con(fx)
  expect_error(suppressMessages(check_cruise_key_integrity(con, manifest_ichthyo = 0L)),
               "date_ym_mismatch")
})

test_that("check 3: the key's NODC disagreeing with the cruise's ship fails", {
  fx <- ck2_base()
  fx$cruise$ship_key[1] <- "JD"                     # 31BD key, ship now JD/31JD
  con <- ck2_con(fx)
  expect_error(suppressMessages(check_cruise_key_integrity(con, manifest_ichthyo = 0L)),
               "nodc_mismatch")
})

test_that("check 4: an orphan cruise_key (in sample or obs, not in cruise) fails", {
  fx <- ck2_base()
  fx$sample$cruise_key[1] <- "1949-01-31BD"         # not in cruise
  con <- ck2_con(fx)
  expect_error(suppressMessages(check_cruise_key_integrity(con, manifest_ichthyo = 0L)),
               "fk_orphan_sample")
})

test_that("check 5: a swfsc row with no UUID, or a derived row WITH one, fails", {
  fx <- ck2_base()
  fx$cruise$cruise_uuid[1] <- NA_character_          # swfsc row missing its UUID
  con <- ck2_con(fx)
  expect_error(suppressMessages(check_cruise_key_integrity(con, manifest_ichthyo = 0L)),
               "cruise_uuid_hygiene")

  fx2 <- ck2_base()
  fx2$cruise$cruise_uuid[3] <- "99999999-9999-9999-9999-999999999999"  # derived row WITH one
  con2 <- ck2_con(fx2)
  expect_error(suppressMessages(check_cruise_key_integrity(con2, manifest_ichthyo = 0L)),
               "cruise_uuid_hygiene")
})

test_that("check 6: an event 32d outside its cruise's span fails; 30d passes; a listed key is exempt", {
  fx <- ck2_base()
  fx$sample$datetime[1] <- as.POSIXct("1955-09-21 00:00:00", tz = "UTC")  # 32d past 1955-08-20
  con <- ck2_con(fx)
  expect_error(suppressMessages(check_cruise_key_integrity(con, manifest_ichthyo = 0L)),
               "event_outside_span")
  # exactly 30 days outside instead: passes
  fx2 <- ck2_base()
  fx2$sample$datetime[1] <- as.POSIXct("1955-09-19 00:00:00", tz = "UTC") # 30d past
  con2 <- ck2_con(fx2)
  expect_silent(suppressMessages(check_cruise_key_integrity(con2, manifest_ichthyo = 0L)))
  # 32d outside AGAIN, but this time the sample_key is named in known_outside_span
  con3 <- ck2_con(fx)
  expect_silent(suppressMessages(check_cruise_key_integrity(
    con3, manifest_ichthyo = 0L, known_outside_span = "ds1:e:1")))
  # an EIGHTH (unlisted) violator still fails even with an allowlist present
  fx4 <- ck2_base()
  fx4$sample$datetime[1] <- as.POSIXct("1955-09-21 00:00:00", tz = "UTC")
  fx4$sample$datetime[2] <- as.POSIXct("1955-11-01 00:00:00", tz = "UTC") # 42d past date_max, also far outside
  con4 <- ck2_con(fx4)
  expect_error(suppressMessages(check_cruise_key_integrity(
    con4, manifest_ichthyo = 0L, known_outside_span = "ds1:e:1")), "event_outside_span")
})

test_that("check 7: a missing or non-zero ichthyo manifest count fails, as does a missing source_uuid", {
  con <- ck2_con(ck2_base())
  expect_error(suppressMessages(check_cruise_key_integrity(con, manifest_ichthyo = NULL)),
               "ichthyo_uuid_check")
  expect_error(suppressMessages(check_cruise_key_integrity(con, manifest_ichthyo = 1L)),
               "ichthyo_uuid_check")

  fx <- ck2_base()
  fx$sample$source_uuid[fx$sample$sample_key == "swfsc_ichthyo:tow:T1"] <- NA_character_
  con2 <- ck2_con(fx)
  expect_error(suppressMessages(check_cruise_key_integrity(con2, manifest_ichthyo = 0L)),
               "ichthyo_uuid_check")
})

test_that("check 8 (ratchet): two cruises of one ship overlapping > 3d fails past the allowance", {
  fx <- ck2_base()
  fx$cruise$date_min[2] <- as.Date("1955-08-15")    # now overlaps cruise 1 (ends 08-20) by 5d
  con <- ck2_con(fx)
  expect_error(suppressMessages(check_cruise_key_integrity(
    con, manifest_ichthyo = 0L, ratchets = list(
      span_overlaps_max = 0L, derived_max = 152L, key_null_max = integer()))),
    "span_overlap")
  # the SAME overlap passes once the ratchet allows it
  expect_silent(suppressMessages(check_cruise_key_integrity(
    con, manifest_ichthyo = 0L, ratchets = list(
      span_overlaps_max = 1L, derived_max = 152L, key_null_max = integer()))))
})

test_that("check 9 (ratchet): the derived-row count is held to its allowance", {
  con <- ck2_con(ck2_base())  # 1 derived row
  expect_error(suppressMessages(check_cruise_key_integrity(
    con, manifest_ichthyo = 0L, ratchets = list(
      span_overlaps_max = 2L, derived_max = 0L, key_null_max = integer()))),
    "derived_rows")
  expect_silent(suppressMessages(check_cruise_key_integrity(
    con, manifest_ichthyo = 0L, ratchets = list(
      span_overlaps_max = 2L, derived_max = 1L, key_null_max = integer()))))
})

test_that("check 10 (ratchet): NULL cruise_key on a root sample is held per-dataset", {
  fx <- ck2_base()
  fx$sample <- rbind(fx$sample, data.frame(
    sample_key = "ds1:e:3", root_sample_key = "ds1:e:3", dataset_key = "ds1",
    sample_type = "e", cruise_key = NA_character_,
    datetime = as.POSIXct("1955-08-11", tz = "UTC"), source_uuid = NA_character_,
    stringsAsFactors = FALSE))
  con <- ck2_con(fx)
  expect_error(suppressMessages(check_cruise_key_integrity(
    con, manifest_ichthyo = 0L, ratchets = list(
      span_overlaps_max = 2L, derived_max = 152L, key_null_max = integer()))),
    "null_cruise_key")
  expect_silent(suppressMessages(check_cruise_key_integrity(
    con, manifest_ichthyo = 0L, ratchets = list(
      span_overlaps_max = 2L, derived_max = 152L, key_null_max = c(ds1 = 1L)))))
})

test_that("halt = FALSE returns the tibble with a warning instead of stopping", {
  fx <- ck2_base()
  fx$cruise$date_ym[1] <- as.Date("1955-07-01")
  con <- ck2_con(fx)
  expect_warning(
    suppressMessages(check_cruise_key_integrity(con, manifest_ichthyo = 0L, halt = FALSE)),
    "date_ym_mismatch")
  # expect_warning() returns the warning condition, not the expression's
  # value — get the actual tibble back on a second, silenced call
  out <- suppressWarnings(suppressMessages(
    check_cruise_key_integrity(con, manifest_ichthyo = 0L, halt = FALSE)))
  expect_true(any(out$check == "date_ym_mismatch" & out$n > 0))
})

# ---- match_station_occupation() --------------------------------------------

# self / order_occ / datetime / NULL, propagation to children, and the
# row-count + sample_key-uniqueness assertions.
ms_con <- function(env = parent.frame()) {
  con <- k_con(env)
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = c(
      "swfsc_ichthyo:site:S1", "swfsc_ichthyo:tow:T1",           # ichthyo site + its tow (child)
      "swfsc_ichthyo:site:S2",                                    # a second occupation, same station, no order_occ match target
      "calcofi_bottle:cast:1",   # order_occ match -> S1
      "calcofi_bottle:cast:2",   # datetime match (unique within 24h) -> S2
      "calcofi_bottle:cast:3",   # two candidates within 24h -> NULL
      "calcofi_bottle:cast:4",   # no datetime -> NULL
      "calcofi_bottle:cast:5"),  # no candidate at this station at all -> NULL
    root_sample_key = c(
      "swfsc_ichthyo:site:S1", "swfsc_ichthyo:site:S1",
      "swfsc_ichthyo:site:S2",
      "calcofi_bottle:cast:1", "calcofi_bottle:cast:2",
      "calcofi_bottle:cast:3", "calcofi_bottle:cast:4", "calcofi_bottle:cast:5"),
    dataset_key = c("swfsc_ichthyo", "swfsc_ichthyo", "swfsc_ichthyo",
                    "calcofi_bottle", "calcofi_bottle", "calcofi_bottle",
                    "calcofi_bottle", "calcofi_bottle"),
    sample_type = c("site", "tow", "site", "cast", "cast", "cast", "cast", "cast"),
    cruise_key = c("1955-08-31BD", "1955-08-31BD", "1955-08-31BD",
                   "1955-08-31BD", "1955-08-31BD", "1955-08-31BD",
                   "1955-08-31BD", "1955-09-31BD"),
    site_key = c("090.0 060.0", "090.0 060.0", "090.0 060.0",
                "090.0 060.0", "090.0 060.0", "090.0 060.0",
                "090.0 060.0", "090.0 060.0"),
    order_occ = c(1L, 1L, 2L, 1L, NA_integer_, NA_integer_, NA_integer_, 1L),
    datetime = as.POSIXct(c(
      "1955-08-10 00:00:00", "1955-08-10 01:00:00",  # S1, tow
      "1955-08-15 00:00:00",                           # S2
      "1955-08-10 02:00:00",                           # cast 1: order_occ match wins even off-time
      "1955-08-15 06:00:00",                           # cast 2: 6h from S2 -> unique datetime match
      "1955-08-12 12:00:00",                           # cast 3: no station within 24h of EITHER -> NULL
      NA,                                               # cast 4: no datetime
      "1955-09-15 00:00:00"), tz = "UTC"),              # cast 5: different cruise, no ichthyo site there
    source_uuid = c("s1-uuid", "t1-uuid", "s2-uuid",
                    NA_character_, NA_character_, NA_character_,
                    NA_character_, NA_character_),
    stringsAsFactors = FALSE))
  con
}

test_that("self: an ichthyo site's station_uuid is its own source_uuid, and its tow inherits it", {
  con <- ms_con()
  suppressMessages(match_station_occupation(con))
  got <- DBI::dbGetQuery(con,
    "SELECT sample_key, station_uuid, station_uuid_method FROM sample ORDER BY sample_key")
  s1 <- got[got$sample_key == "swfsc_ichthyo:site:S1", ]
  expect_equal(s1$station_uuid, "s1-uuid")
  expect_equal(s1$station_uuid_method, "self")
  t1 <- got[got$sample_key == "swfsc_ichthyo:tow:T1", ]
  expect_equal(t1$station_uuid, "s1-uuid")     # inherited from its root (S1), not its own UUID
  expect_equal(t1$station_uuid_method, "self")
})

test_that("order_occ beats datetime, even when the datetime is further off", {
  con <- ms_con()
  suppressMessages(match_station_occupation(con))
  got <- DBI::dbGetQuery(con,
    "SELECT station_uuid, station_uuid_method FROM sample WHERE sample_key = 'calcofi_bottle:cast:1'")
  expect_equal(got$station_uuid, "s1-uuid")
  expect_equal(got$station_uuid_method, "order_occ")
})

test_that("a unique datetime match within tolerance wins when order_occ does not resolve it", {
  con <- ms_con()
  suppressMessages(match_station_occupation(con))
  got <- DBI::dbGetQuery(con,
    "SELECT station_uuid, station_uuid_method FROM sample WHERE sample_key = 'calcofi_bottle:cast:2'")
  expect_equal(got$station_uuid, "s2-uuid")
  expect_equal(got$station_uuid_method, "datetime")
})

test_that("no candidate within tolerance, no datetime, or a different cruise all stay NULL", {
  con <- ms_con()
  suppressMessages(match_station_occupation(con))
  got <- DBI::dbGetQuery(con,
    "SELECT sample_key, station_uuid, station_uuid_method FROM sample
      WHERE sample_key IN ('calcofi_bottle:cast:3','calcofi_bottle:cast:4','calcofi_bottle:cast:5')
      ORDER BY sample_key")
  expect_true(all(is.na(got$station_uuid)))
  expect_true(all(is.na(got$station_uuid_method)))
})

test_that("a foreign row parented DIRECTLY to an ichthyo site gets method 'parent', not 'self'", {
  # the crab pattern: a subsample's parent_sample_key AND root_sample_key both
  # equal the ichthyo site's sample_key directly (never entering the `roots`
  # CTE, since its own sample_key != root_sample_key)
  con <- k_con()
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = c("swfsc_ichthyo:site:S1", "cdfw_dungeness-crab:subsample:9"),
    root_sample_key = c("swfsc_ichthyo:site:S1", "swfsc_ichthyo:site:S1"),
    dataset_key = c("swfsc_ichthyo", "cdfw_dungeness-crab"),
    sample_type = c("site", "subsample"),
    cruise_key = "1955-08-31BD", site_key = "090.0 060.0",
    order_occ = c(1L, NA_integer_),
    datetime = as.POSIXct(c("1955-08-10 00:00:00", "1955-08-10 00:00:00"), tz = "UTC"),
    source_uuid = c("s1-uuid", NA_character_),
    stringsAsFactors = FALSE))
  suppressMessages(match_station_occupation(con))
  got <- DBI::dbGetQuery(con,
    "SELECT sample_key, station_uuid, station_uuid_method FROM sample ORDER BY sample_key")
  crab <- got[got$sample_key == "cdfw_dungeness-crab:subsample:9", ]
  expect_equal(crab$station_uuid, "s1-uuid")
  expect_equal(crab$station_uuid_method, "parent")
  site <- got[got$sample_key == "swfsc_ichthyo:site:S1", ]
  expect_equal(site$station_uuid_method, "self")
  # and it does not appear in the root-only summary (it is not a root)
  rpt <- suppressMessages(match_station_occupation(con))
  expect_false("parent" %in% rpt$method)
})

test_that("row count and sample_key uniqueness are unchanged by the rebuild", {
  con <- ms_con()
  n_before <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM sample")$n
  suppressMessages(match_station_occupation(con))
  n_after <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM sample")$n
  expect_equal(n_before, n_after)
  dup <- DBI::dbGetQuery(con,
    "SELECT sample_key FROM sample GROUP BY 1 HAVING COUNT(*) > 1")
  expect_equal(nrow(dup), 0L)
})

test_that("a two-candidate datetime tie (both inside tolerance) stays NULL, not an arbitrary pick", {
  con <- k_con()
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = c("swfsc_ichthyo:site:A", "swfsc_ichthyo:site:B", "calcofi_bottle:cast:9"),
    root_sample_key = c("swfsc_ichthyo:site:A", "swfsc_ichthyo:site:B", "calcofi_bottle:cast:9"),
    dataset_key = c("swfsc_ichthyo", "swfsc_ichthyo", "calcofi_bottle"),
    sample_type = c("site", "site", "cast"),
    cruise_key = "1955-08-31BD",
    site_key = "090.0 060.0",
    order_occ = c(1L, 2L, NA_integer_),
    datetime = as.POSIXct(c("1955-08-10 00:00:00", "1955-08-10 04:00:00",
                            "1955-08-10 02:00:00"), tz = "UTC"),  # 2h from A, 2h from B
    source_uuid = c("a-uuid", "b-uuid", NA_character_),
    stringsAsFactors = FALSE))
  suppressMessages(match_station_occupation(con))
  got <- DBI::dbGetQuery(con,
    "SELECT station_uuid, station_uuid_method FROM sample WHERE sample_key = 'calcofi_bottle:cast:9'")
  expect_true(is.na(got$station_uuid))
  expect_true(is.na(got$station_uuid_method))
})
