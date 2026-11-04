# add_cruise_median_date(): a cruise's month is the calendar month of the median datetime of its root
# samples (Rasmus Swalethorp 2026-10-16 / Ben Best 2026-10-20 / agreed 2026-10-27), with the DJF
# seasons and December counting toward the following winter. One fixture per rule.
med_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  con
}
med_fixture <- function(con) {
  DBI::dbExecute(con, "CREATE TABLE cruise AS SELECT * FROM (VALUES
    ('1981-02-31JD'), ('2025-12-33XX'), ('2026-07-3322'), ('1950-05-31CR'), ('bad-key')
    ) t(cruise_key)")
  DBI::dbExecute(con, "CREATE TABLE sample AS SELECT * FROM (VALUES
    -- keyed February, centres on 1 March: five casts, median the 1 March one
    ('a1', NULL,  '1981-02-31JD', TIMESTAMP '1981-02-20 00:00'),
    ('a2', NULL,  '1981-02-31JD', TIMESTAMP '1981-02-27 00:00'),
    ('a3', NULL,  '1981-02-31JD', TIMESTAMP '1981-03-01 12:00'),
    ('a4', NULL,  '1981-02-31JD', TIMESTAMP '1981-03-05 00:00'),
    ('a5', NULL,  '1981-02-31JD', TIMESTAMP '1981-03-09 00:00'),
    -- a3's bottles: children never outvote their cast (24 bottles on one day would otherwise move the median)
    ('a3b1', 'a3', '1981-02-31JD', TIMESTAMP '1981-03-01 12:00'),
    ('a3b2', 'a3', '1981-02-31JD', TIMESTAMP '1981-03-01 12:00'),
    -- a December cruise: winter of the following year
    ('d1', NULL,  '2025-12-33XX', TIMESTAMP '2025-12-03 00:00'),
    ('d2', NULL,  '2025-12-33XX', TIMESTAMP '2025-12-10 00:00'),
    ('d3', NULL,  '2025-12-33XX', TIMESTAMP '2025-12-19 00:00'),
    -- July 2026 worked line 93.3 on 30 June; its median is July
    ('j1', NULL,  '2026-07-3322', TIMESTAMP '2026-06-30 22:00'),
    ('j2', NULL,  '2026-07-3322', TIMESTAMP '2026-07-04 00:00'),
    ('j3', NULL,  '2026-07-3322', TIMESTAMP '2026-07-12 00:00'),
    -- a cruise whose only dated rows are children (no dated root): their median counts
    ('c1', NULL,  '1950-05-31CR', NULL),
    ('c1b', 'c1', '1950-05-31CR', TIMESTAMP '1950-06-02 00:00'),
    -- an unparseable key with no date: nothing to stamp
    ('x1', NULL,  'bad-key', NULL)
    ) t(sample_key, root_sample_key, cruise_key, datetime)")
}

test_that("add_cruise_median_date(): the median root date's month, the key's month only as a fallback", {
  con <- med_con(); med_fixture(con)
  out <- add_cruise_median_date(con)
  expect_named(out, c("cruise_key", "month_key", "date_median", "month_median", "season", "season_year", "n_roots", "month_changed"))
  r <- function(k) out[out$cruise_key == k, ]
  # 1981-02 JD centres on 1 March: month 3, MAM, 1981; bottles did not outvote casts (5 roots)
  expect_equal(as.character(r("1981-02-31JD")$date_median), "1981-03-01")
  expect_equal(r("1981-02-31JD")$month_median, 3L)
  expect_equal(r("1981-02-31JD")$season, "MAM")
  expect_equal(r("1981-02-31JD")$season_year, 1981L)
  expect_equal(r("1981-02-31JD")$n_roots, 5L)
  expect_true(r("1981-02-31JD")$month_changed)
  # December 2025 is winter 2026
  expect_equal(r("2025-12-33XX")$month_median, 12L)
  expect_equal(r("2025-12-33XX")$season, "DJF")
  expect_equal(r("2025-12-33XX")$season_year, 2026L)
  expect_false(r("2025-12-33XX")$month_changed)
  # 2607: July, JJA, unchanged
  expect_equal(r("2026-07-3322")$month_median, 7L)
  expect_equal(r("2026-07-3322")$season, "JJA")
  expect_equal(r("2026-07-3322")$season_year, 2026L)
  # no dated root: the children's median (June) rather than the key's May
  expect_equal(r("1950-05-31CR")$month_median, 6L)
  expect_equal(r("1950-05-31CR")$season, "JJA")
  # no date and no parseable key: NULL throughout
  expect_true(is.na(r("bad-key")$date_median) && is.na(r("bad-key")$month_median) && is.na(r("bad-key")$season))
  # the columns are on the table, typed
  cols <- DBI::dbGetQuery(con, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'cruise' ORDER BY ordinal_position")
  expect_equal(cols$column_name, c("cruise_key", "date_median", "month_median", "season", "season_year"))
  expect_equal(cols$data_type, c("VARCHAR", "DATE", "TINYINT", "VARCHAR", "SMALLINT"))
})

test_that("add_cruise_median_date(): a cruise with no dated sample at all keeps its key's month and year", {
  con <- med_con()
  DBI::dbExecute(con, "CREATE TABLE cruise AS SELECT * FROM (VALUES ('1999-12-33XX'), ('2000-04-33XX')) t(cruise_key)")
  DBI::dbExecute(con, "CREATE TABLE sample AS SELECT * FROM (VALUES ('s1', NULL, '2000-04-33XX', TIMESTAMP '2000-04-10 00:00'))
    t(sample_key, root_sample_key, cruise_key, datetime)")
  out <- add_cruise_median_date(con)
  r <- out[out$cruise_key == "1999-12-33XX", ]
  expect_true(is.na(r$date_median))
  expect_equal(r$month_median, 12L)
  expect_equal(r$season, "DJF")
  expect_equal(r$season_year, 2000L, info = "December by the key still counts toward the next winter")
  expect_equal(r$n_roots, 0L)
  expect_false(r$month_changed)
  # re-running is idempotent (ADD COLUMN IF NOT EXISTS, UPDATE)
  out2 <- add_cruise_median_date(con)
  expect_equal(out2, out)
})

test_that("build_obs_slim(): year / quarter / cruise_month / season_year come from the cruise table and are appended after hex7", {
  con <- med_con()
  DBI::dbExecute(con, "CREATE TABLE obs AS SELECT * FROM (VALUES
    (1, 'env', 'calcofi_ctd-cast', 'k1', 'st30-ln93.3', '2025-12-33XX', 32.0, -118.0, TIMESTAMP '2025-12-03 00:00', 2.0, 2.0,
     NULL::VARCHAR, NULL::VARCHAR, 'temperature_ave', 15.0, NULL::VARCHAR, NULL::DOUBLE, 622000000000000000::UBIGINT),
    (2, 'env', 'calcofi_ctd-cast', 'k2', 'st30-ln93.3', '1981-02-31JD', 32.0, -118.0, TIMESTAMP '1981-02-20 00:00', 2.0, 2.0,
     NULL, NULL, 'temperature_ave', 15.0, NULL, NULL, 622000000000000000::UBIGINT),
    (3, 'env', 'calcofi_ctd-cast', 'k3', 'st30-ln93.3', '2026-07-3322', 32.0, -118.0, TIMESTAMP '2026-06-30 22:00', 2.0, 2.0,
     NULL, NULL, 'temperature_ave', 15.0, NULL, NULL, 622000000000000000::UBIGINT)
    ) t(obs_id, realm, dataset_key, sample_key, grid_key, cruise_key, latitude, longitude, datetime, depth_min_m, depth_max_m,
        taxon_key, life_stage, measurement_type, measurement_value, measurement_qual, measurement_prec, hex_id)")
  DBI::dbExecute(con, "CREATE TABLE sample AS SELECT * FROM (VALUES
    ('k1', '093.3 030.0', NULL::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR), ('k2', '093.3 030.0', NULL, NULL, NULL, NULL), ('k3', '093.3 030.0', NULL, NULL, NULL, NULL)
    ) t(sample_key, site_key, root_sample_key, depth_min_m, depth_max_m, tow_type)")
  DBI::dbExecute(con, "CREATE TABLE sample_root AS SELECT * FROM (VALUES
    ('k1', 1, NULL::DOUBLE, NULL::DOUBLE), ('k2', 2, NULL, NULL), ('k3', 3, NULL, NULL)) t(root_sample_key, root_id, depth_min_m, depth_max_m)")
  DBI::dbExecute(con, "CREATE TABLE sample_measurement (sample_key VARCHAR, measurement_type VARCHAR, measurement_value DOUBLE)")
  DBI::dbExecute(con, "CREATE TABLE measurement_type AS SELECT 'temperature_ave' AS measurement_type, 'degC' AS units")
  DBI::dbExecute(con, "CREATE TABLE cruise AS SELECT * FROM (VALUES
    ('2025-12-33XX', DATE '2025-12-10', 12::TINYINT, 'DJF', 2026::SMALLINT),
    ('1981-02-31JD', DATE '1981-03-01', 3, 'MAM', 1981)
    ) t(cruise_key, date_median, month_median, season, season_year)")
  build_obs_slim(con, "env", qual_ok_sql = "TRUE", density_sql = "NULL::DOUBLE AS density_per_10m2, NULL::DOUBLE AS density_per_1000m3, NULL::VARCHAR AS effort_class")
  o <- DBI::dbGetQuery(con, "SELECT obs_id, year, quarter, cruise_month, season_year FROM obs_env ORDER BY obs_id")
  expect_equal(o$year, c(2025L, 1981L, 2026L))
  expect_equal(o$quarter, c(1L, 2L, 3L), info = "DJF, MAM (by the median month, not the key's February), JJA")
  expect_equal(o$cruise_month, c(12L, 3L, 7L), info = "the third cruise is not in the table: its key's July")
  expect_equal(o$season_year, c(2026L, 1981L, 2026L), info = "December 2025 belongs to winter 2026")
  cols <- DBI::dbListFields(con, "obs_env")
  expect_equal(tail(cols, 3), c("hex7", "cruise_month", "season_year"), info = "appended, never inserted")
})
