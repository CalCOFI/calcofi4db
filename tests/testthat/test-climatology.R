# build_climatology(): the one baseline every anomaly subtracts, asserted rule by rule on a
# fixture small enough to hand-check every number.
clim_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  con
}
# the calcofi4r predicate, pinned as text so this package never depends on calcofi4r
CLIM_QUAL_OK <- "COALESCE(regexp_replace(o.measurement_qual, '\\.0+$', '') NOT IN ('8', '9'), TRUE)"

clim_fixture <- function(con) {
  # one CTD station: three July cruises inside the window at 2 m (10, 11, 12) -> one cell;
  # then one row per rule that must NOT reach that cell, or must land somewhere else
  DBI::dbExecute(con, "CREATE TABLE obs AS SELECT * FROM (VALUES
    -- the baseline cell: july, bin 0, three cruises
    (1,  'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  2.0, 'temperature_ave', 10.0, NULL, 'calcofi_ctd-cast'),
    (2,  'env', 'st60-ln90', '2001-07-33XX', TIMESTAMP '2001-07-11 12:00',  2.0, 'temperature_ave', 11.0, '1',  'calcofi_ctd-cast'),
    (3,  'env', 'st60-ln90', '2002-07-33XX', TIMESTAMP '2002-07-12 12:00',  9.9, 'temperature_ave', 12.0, '2',  'calcofi_ctd-cast'),
    -- outside the window: not a baseline year
    (4,  'env', 'st60-ln90', '2020-07-33XX', TIMESTAMP '2020-07-10 12:00',  2.0, 'temperature_ave', 99.0, NULL, 'calcofi_ctd-cast'),
    (5,  'env', 'st60-ln90', '1992-07-33XX', TIMESTAMP '1992-07-10 12:00',  2.0, 'temperature_ave', 99.0, NULL, 'calcofi_ctd-cast'),
    -- flagged questionable / bad: the quality predicate drops them
    (6,  'env', 'st60-ln90', '2001-07-33XX', TIMESTAMP '2001-07-11 12:00',  2.0, 'temperature_ave', 50.0, '8',  'calcofi_ctd-cast'),
    (7,  'env', 'st60-ln90', '2001-07-33XX', TIMESTAMP '2001-07-11 12:00',  2.0, 'temperature_ave', 50.0, '9.0', 'calcofi_ctd-cast'),
    -- another calendar month is another cell (and, with one cruise, below the floor)
    (8,  'env', 'st60-ln90', '2001-01-33XX', TIMESTAMP '2001-01-11 12:00',  2.0, 'temperature_ave', 5.0,  NULL, 'calcofi_ctd-cast'),
    -- a deeper bin: 12.5 m floors to 10; 600 m is past the cap
    (9,  'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00', 12.5, 'temperature_ave', 9.0,  NULL, 'calcofi_ctd-cast'),
    (10, 'env', 'st60-ln90', '2001-07-33XX', TIMESTAMP '2001-07-11 12:00', 19.9, 'temperature_ave', 9.0,  NULL, 'calcofi_ctd-cast'),
    (11, 'env', 'st60-ln90', '2002-07-33XX', TIMESTAMP '2002-07-12 12:00', 10.0, 'temperature_ave', 9.0,  NULL, 'calcofi_ctd-cast'),
    (12, 'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00', 600.0, 'temperature_ave', 4.0, NULL, 'calcofi_ctd-cast'),
    -- not a value / not placed / not env
    (13, 'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  2.0, 'temperature_ave', 'NaN'::DOUBLE, NULL, 'calcofi_ctd-cast'),
    (14, 'env', NULL,        '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  2.0, 'temperature_ave', 10.0, NULL, 'calcofi_ctd-cast'),
    (15, 'env', 'st60-ln90', '2000-07-33XX', NULL,                          2.0, 'temperature_ave', 10.0, NULL, 'calcofi_ctd-cast'),
    (16, 'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00', NULL, 'temperature_ave', 10.0, NULL, 'calcofi_ctd-cast'),
    (17, 'bio', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  2.0, 'abundance',       10.0, NULL, 'swfsc_ichthyo'),
    -- the bottle dataset in the same cell is its OWN row (a consumer filters or pools by n)
    (18, 'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  0.0, 'temperature', 20.0, NULL, 'calcofi_bottle'),
    (19, 'env', 'st60-ln90', '2001-07-33XX', TIMESTAMP '2001-07-11 12:00',  0.0, 'temperature', 20.0, NULL, 'calcofi_bottle'),
    (20, 'env', 'st60-ln90', '2002-07-33XX', TIMESTAMP '2002-07-12 12:00',  0.0, 'temperature', 20.0, NULL, 'calcofi_bottle'),
    (21, 'env', 'st60-ln90', '2003-07-33XX', TIMESTAMP '2003-07-12 12:00',  0.0, 'temperature', 20.0, NULL, 'calcofi_bottle'),
    -- st30-ln90: four casts from ONE cruise (90.30, 90.28, 90.27.7, 88.5/30.1 share the cell) —
    -- three observations, one cruise: below the floor
    (22, 'env', 'st30-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  2.0, 'temperature_ave', 15.0, NULL, 'calcofi_ctd-cast'),
    (23, 'env', 'st30-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 16:00',  2.0, 'temperature_ave', 16.0, NULL, 'calcofi_ctd-cast'),
    (24, 'env', 'st30-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 20:00',  2.0, 'temperature_ave', 17.0, NULL, 'calcofi_ctd-cast')
    ) t(obs_id, realm, grid_key, cruise_key, datetime, depth_min_m, measurement_type, measurement_value, measurement_qual, dataset_key)")
}

test_that("build_climatology(): the cell is a mean per dataset x station x month x 10 m bin x type", {
  con <- clim_con(); clim_fixture(con)
  n <- build_climatology(con, qual_ok_sql = CLIM_QUAL_OK)
  cl <- DBI::dbGetQuery(con, "SELECT * FROM climatology ORDER BY dataset_key, grid_key, month, depth_bin")
  expect_equal(n, nrow(cl))
  expect_named(cl, c("dataset_key", "grid_key", "month", "depth_bin", "measurement_type",
                     "clim_mean", "clim_sd", "clim_n", "n_cruises", "clim_yr_min", "clim_yr_max"))
  # exactly three cells survive: ctd bin 0, ctd bin 10, bottle bin 0 — all July at st60
  expect_equal(nrow(cl), 3)
  expect_true(all(cl$grid_key == "st60-ln90"), info = "st30's one-cruise cell is below the floor")
  expect_true(all(cl$month == 7L), info = "January has one cruise, so no January cell")

  ctd0 <- cl[cl$dataset_key == "calcofi_ctd-cast" & cl$depth_bin == 0, ]
  expect_equal(ctd0$clim_mean, 11)                    # (10 + 11 + 12) / 3: 99s, 50s, NaN, 5 all out
  expect_equal(ctd0$clim_sd, 1)
  expect_equal(ctd0$clim_n, 3L)
  expect_equal(ctd0$n_cruises, 3L)
  expect_equal(ctd0$measurement_type, "temperature_ave")

  ctd10 <- cl[cl$dataset_key == "calcofi_ctd-cast" & cl$depth_bin == 10, ]
  expect_equal(ctd10$clim_mean, 9)                    # 12.5, 19.9 and 10.0 all floor to bin 10
  expect_equal(ctd10$clim_n, 3L)

  btl <- cl[cl$dataset_key == "calcofi_bottle", ]
  expect_equal(btl$depth_bin, 0L)
  expect_equal(btl$clim_mean, 20)
  expect_equal(btl$n_cruises, 4L)
  expect_equal(btl$measurement_type, "temperature")
})

test_that("build_climatology(): the window is stamped on every row and honoured", {
  con <- clim_con(); clim_fixture(con)
  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK)
  cl <- DBI::dbGetQuery(con, "SELECT DISTINCT clim_yr_min, clim_yr_max FROM climatology")
  expect_equal(cl, data.frame(clim_yr_min = 1993L, clim_yr_max = 2013L))
  # a window that admits the 2020 row moves the mean; one that misses 2000-2002 empties the table
  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK, yr_min = 2000, yr_max = 2020, tbl = "c2")
  ctd0 <- DBI::dbGetQuery(con, "SELECT clim_mean, clim_n, clim_yr_max FROM c2 WHERE dataset_key = 'calcofi_ctd-cast' AND depth_bin = 0")
  expect_equal(ctd0$clim_mean, (10 + 11 + 12 + 99) / 4)
  expect_equal(ctd0$clim_yr_max, 2020L)
  expect_equal(build_climatology(con, qual_ok_sql = CLIM_QUAL_OK, yr_min = 2005, yr_max = 2013, tbl = "c3"), 0)
  expect_error(build_climatology(con, qual_ok_sql = CLIM_QUAL_OK, yr_min = 2013, yr_max = 1993))
  expect_error(build_climatology(con, qual_ok_sql = ""))
})

test_that("build_climatology(): the floor counts cruises, not observations", {
  con <- clim_con(); clim_fixture(con)
  # at min_cruises = 1 the one-cruise cells appear — st30's three same-cruise casts pooled into one
  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK, min_cruises = 1)
  st30 <- DBI::dbGetQuery(con, "SELECT clim_mean, clim_n, n_cruises FROM climatology WHERE grid_key = 'st30-ln90'")
  expect_equal(st30, data.frame(clim_mean = 16, clim_n = 3L, n_cruises = 1L))
  jan <- DBI::dbGetQuery(con, "SELECT clim_mean, n_cruises FROM climatology WHERE month = 1")
  expect_equal(jan, data.frame(clim_mean = 5, n_cruises = 1L))
  # at 4 only the bottle cell (four cruises) is left
  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK, min_cruises = 4)
  expect_equal(DBI::dbGetQuery(con, "SELECT dataset_key FROM climatology")$dataset_key, "calcofi_bottle")
})

test_that("build_climatology(): pooling datasets weighted by clim_n equals the mean over their observations", {
  con <- clim_con(); clim_fixture(con)
  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK)
  pooled <- DBI::dbGetQuery(con, "
    SELECT sum(clim_mean * clim_n) / sum(clim_n) AS v, sum(clim_n) AS n
    FROM climatology WHERE grid_key = 'st60-ln90' AND month = 7 AND depth_bin = 0")
  direct <- DBI::dbGetQuery(con, "
    SELECT avg(measurement_value) AS v, count(*) AS n FROM obs
    WHERE obs_id IN (1, 2, 3, 18, 19, 20, 21)")
  expect_equal(pooled$v, direct$v)
  expect_equal(pooled$n, direct$n)
})

test_that("climatology has a registered sort key that is a unique total order, so it exports", {
  sk <- release_sort_keys()$climatology
  expect_equal(sk$partition_by, "measurement_type")
  expect_equal(sk$order_by, c("measurement_type", "dataset_key", "grid_key", "month", "depth_bin"))
  con <- clim_con(); clim_fixture(con)
  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK)
  out <- withr::local_tempdir()
  f <- export_release_parquet(con, "climatology", file.path(out, "climatology"), sk$order_by,
                              partition_by = sk$partition_by)
  expect_setequal(basename(dirname(f$rel_path)), c("measurement_type=temperature", "measurement_type=temperature_ave"))
  back <- DBI::dbGetQuery(con, glue::glue(
    "SELECT count(*) AS n FROM read_parquet('{file.path(out, 'climatology')}/*/*.parquet', hive_partitioning = true)"))
  expect_equal(back$n, 3)
})
