test_that("match_cruise_by_track assigns the nearest same-day cruise", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  # two ships working the same day, ~110 km apart in latitude
  DBI::dbExecute(con, "CREATE TABLE trk AS SELECT * FROM (VALUES
    ('2000-01-10-AA', DATE '2000-01-10', -120.0, 33.0),
    ('2000-01-10-BB', DATE '2000-01-10', -120.0, 34.0)
  ) t(cruise_key, datetime_start_utc, longitude, latitude)")

  DBI::dbExecute(con, "CREATE TABLE dat AS SELECT * FROM (VALUES
    ('a', DATE '2000-01-10', -120.0, 33.02),
    ('b', DATE '2000-01-10', -120.0, 33.98)
  ) t(id, datetime_start_utc, longitude, latitude)")

  s <- match_cruise_by_track(con, "dat", "trk")

  got <- DBI::dbGetQuery(con, "SELECT id, cruise_key FROM dat ORDER BY id")
  expect_equal(got$cruise_key, c("2000-01-10-AA", "2000-01-10-BB"))
  expect_equal(s$matched, 2)
  expect_equal(s$pct, 100)
})

test_that("match_cruise_by_track leaves rows beyond max_km NULL", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE trk AS SELECT * FROM (VALUES
    ('c1', DATE '2000-01-10', -120.0, 33.0)
  ) t(cruise_key, datetime_start_utc, longitude, latitude)")

  # ~0.9 deg north = ~100 km away, well outside the 25 km default
  DBI::dbExecute(con, "CREATE TABLE dat AS SELECT * FROM (VALUES
    ('near', DATE '2000-01-10', -120.0, 33.1),
    ('far',  DATE '2000-01-10', -120.0, 33.9)
  ) t(id, datetime_start_utc, longitude, latitude)")

  match_cruise_by_track(con, "dat", "trk")

  got <- DBI::dbGetQuery(con, "SELECT id, cruise_key FROM dat ORDER BY id")
  expect_equal(got$cruise_key[got$id == "far"], NA_character_)
  expect_equal(got$cruise_key[got$id == "near"], "c1")
})

test_that("match_cruise_by_track respects window_days", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE trk AS SELECT * FROM (VALUES
    ('c1', DATE '2000-01-10', -120.0, 33.0)
  ) t(cruise_key, datetime_start_utc, longitude, latitude)")
  DBI::dbExecute(con, "CREATE TABLE dat AS SELECT * FROM (VALUES
    ('next_day', DATE '2000-01-11', -120.0, 33.0)
  ) t(id, datetime_start_utc, longitude, latitude)")

  # same-day default must not reach across the date boundary
  match_cruise_by_track(con, "dat", "trk")
  expect_true(is.na(DBI::dbGetQuery(con, "SELECT cruise_key FROM dat")$cruise_key))

  match_cruise_by_track(con, "dat", "trk", window_days = 1)
  expect_equal(DBI::dbGetQuery(con, "SELECT cruise_key FROM dat")$cruise_key, "c1")
})

test_that("group consensus outvotes a stray row and fills non-voting rows", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE trk AS SELECT * FROM (VALUES
    ('right', DATE '2000-01-10', -120.0, 33.0),
    ('wrong', DATE '2000-01-10', -120.0, 34.0)
  ) t(cruise_key, datetime_start_utc, longitude, latitude)")

  # 3 rows vote 'right', 1 stray row sits on the other ship's station, and
  # 'novote' is too far from either to vote at all
  DBI::dbExecute(con, "CREATE TABLE dat AS SELECT * FROM (VALUES
    ('r1',     'S1', DATE '2000-01-10', -120.0, 33.00),
    ('r2',     'S1', DATE '2000-01-10', -120.0, 33.01),
    ('r3',     'S1', DATE '2000-01-10', -120.0, 33.02),
    ('stray',  'S1', DATE '2000-01-10', -120.0, 34.00),
    ('novote', 'S1', DATE '2000-01-10', -110.0, 33.00)
  ) t(id, survey, datetime_start_utc, longitude, latitude)")

  s <- match_cruise_by_track(con, "dat", "trk", group_col = "survey")

  got <- DBI::dbGetQuery(con, "SELECT id, cruise_key FROM dat ORDER BY id")
  # every row of the group, including the stray and the non-voter, gets 'right'
  expect_equal(unique(got$cruise_key), "right")
  expect_equal(s$matched, 5)

  expect_equal(nrow(s$groups), 1)
  expect_equal(s$groups$cruise_key, "right")
  expect_equal(s$groups$votes, 3)
  expect_equal(s$groups$votes_total, 4)
  expect_equal(s$groups$n_rows, 5)
  expect_equal(s$groups$share, 0.75)
})

test_that("a group whose vote is too split stays NULL rather than guessing", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE trk AS SELECT * FROM (VALUES
    ('a', DATE '2000-01-10', -120.0, 33.0),
    ('b', DATE '2000-01-10', -120.0, 34.0)
  ) t(cruise_key, datetime_start_utc, longitude, latitude)")
  DBI::dbExecute(con, "CREATE TABLE dat AS SELECT * FROM (VALUES
    ('r1', 'S1', DATE '2000-01-10', -120.0, 33.0),
    ('r2', 'S1', DATE '2000-01-10', -120.0, 34.0)
  ) t(id, survey, datetime_start_utc, longitude, latitude)")

  # a 50/50 split cannot clear min_share = 0.6
  s <- match_cruise_by_track(con, "dat", "trk", group_col = "survey",
                             min_share = 0.6)

  expect_true(all(is.na(DBI::dbGetQuery(con, "SELECT cruise_key FROM dat")$cruise_key)))
  expect_equal(s$matched, 0)
  expect_true(is.na(s$groups$cruise_key))
  expect_equal(s$groups$n_rows, 2)
})

test_that("a group with no vote at all is reported and left NULL", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE trk AS SELECT * FROM (VALUES
    ('a', DATE '2000-01-10', -120.0, 33.0)
  ) t(cruise_key, datetime_start_utc, longitude, latitude)")
  # a survey that rode a non-CalCOFI cruise: right place, no cruise that day
  DBI::dbExecute(con, "CREATE TABLE dat AS SELECT * FROM (VALUES
    ('r1', 'CAC', DATE '2000-01-10', -120.0, 33.0),
    ('r2', 'OTH', DATE '2000-06-10', -120.0, 33.0)
  ) t(id, survey, datetime_start_utc, longitude, latitude)")

  s <- match_cruise_by_track(con, "dat", "trk", group_col = "survey")

  got <- DBI::dbGetQuery(con, "SELECT id, cruise_key FROM dat ORDER BY id")
  expect_equal(got$cruise_key, c("a", NA_character_))
  expect_equal(s$groups$cruise_key[s$groups$survey == "OTH"], NA_character_)
  expect_equal(s$groups$n_rows[s$groups$survey == "OTH"], 1)
})

test_that("NaN and Inf coordinates never match (they survive IS NOT NULL)", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE trk AS SELECT * FROM (VALUES
    ('a', DATE '2000-01-10', -120.0, 33.0)
  ) t(cruise_key, datetime_start_utc, longitude, latitude)")
  DBI::dbExecute(con, "CREATE TABLE dat AS SELECT * FROM (VALUES
    ('ok',  DATE '2000-01-10', -120.0, 33.0),
    ('nan', DATE '2000-01-10', 'NaN'::DOUBLE, 'NaN'::DOUBLE),
    ('inf', DATE '2000-01-10', 'Infinity'::DOUBLE, 33.0),
    ('nul', DATE '2000-01-10', NULL, NULL)
  ) t(id, datetime_start_utc, longitude, latitude)")

  match_cruise_by_track(con, "dat", "trk")

  got <- DBI::dbGetQuery(con, "SELECT id, cruise_key FROM dat ORDER BY id")
  expect_equal(got$cruise_key[got$id == "ok"], "a")
  expect_true(all(is.na(got$cruise_key[got$id != "ok"])))
})

test_that("an existing cruise_key column is reused, not duplicated", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE trk AS SELECT * FROM (VALUES
    ('a', DATE '2000-01-10', -120.0, 33.0)
  ) t(cruise_key, datetime_start_utc, longitude, latitude)")
  DBI::dbExecute(con, "CREATE TABLE dat AS SELECT * FROM (VALUES
    ('r1', DATE '2000-01-10', -120.0, 33.0)
  ) t(id, datetime_start_utc, longitude, latitude)")
  DBI::dbExecute(con, "ALTER TABLE dat ADD COLUMN cruise_key VARCHAR")

  match_cruise_by_track(con, "dat", "trk")

  expect_equal(sum(DBI::dbListFields(con, "dat") == "cruise_key"), 1L)
  expect_equal(DBI::dbGetQuery(con, "SELECT cruise_key FROM dat")$cruise_key, "a")
})

test_that("differently-named reference columns are honoured", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE trk AS SELECT * FROM (VALUES
    ('a', TIMESTAMP '2000-01-10 04:00:00', -120.0, 33.0)
  ) t(cruise_key, datetime, lon, lat)")
  DBI::dbExecute(con, "CREATE TABLE dat AS SELECT * FROM (VALUES
    ('r1', DATE '2000-01-10', -120.0, 33.0)
  ) t(id, date, longitude, latitude)")

  match_cruise_by_track(
    con, "dat", "trk",
    datetime_col     = "date",     lon_col     = "longitude", lat_col     = "latitude",
    ref_datetime_col = "datetime", ref_lon_col = "lon",       ref_lat_col = "lat")

  expect_equal(DBI::dbGetQuery(con, "SELECT cruise_key FROM dat")$cruise_key, "a")
})

test_that("a mismatched reference column name errors legibly, not as a binder error", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  # the real trap: ref_* default to the data_tbl names, and a `sample`-shard
  # track calls its timestamp `datetime` while the source table calls it `date`
  DBI::dbExecute(con, "CREATE TABLE trk AS SELECT * FROM (VALUES
    ('a', DATE '2000-01-10', -120.0, 33.0)
  ) t(cruise_key, datetime, longitude, latitude)")
  DBI::dbExecute(con, "CREATE TABLE dat AS SELECT * FROM (VALUES
    ('r1', DATE '2000-01-10', -120.0, 33.0)
  ) t(id, date, longitude, latitude)")

  expect_error(
    match_cruise_by_track(con, "dat", "trk", datetime_col = "date"),
    "trk has no column 'date'")

  # and names the group_col when that is what is missing
  expect_error(
    match_cruise_by_track(con, "dat", "trk", datetime_col = "date",
                          ref_datetime_col = "datetime", group_col = "survey"),
    "dat has no column 'survey'")
})
