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
  # then one row per rule that must NOT reach that cell, or must land somewhere else.
  # Each obs row names its sample; `sample` carries the station (site_key) — obs does not.
  DBI::dbExecute(con, "CREATE TABLE obs AS SELECT * FROM (VALUES
    -- the baseline cell: july, bin 0, three cruises
    (1,  'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  2.0, 'temperature_ave', 10.0, NULL, 'calcofi_ctd-cast', 'c1'),
    (2,  'env', 'st60-ln90', '2001-07-33XX', TIMESTAMP '2001-07-11 12:00',  2.0, 'temperature_ave', 11.0, '1',  'calcofi_ctd-cast', 'c2'),
    (3,  'env', 'st60-ln90', '2002-07-33XX', TIMESTAMP '2002-07-12 12:00',  9.9, 'temperature_ave', 12.0, '2',  'calcofi_ctd-cast', 'c3'),
    -- outside the window: not a baseline year
    (4,  'env', 'st60-ln90', '2020-07-33XX', TIMESTAMP '2020-07-10 12:00',  2.0, 'temperature_ave', 99.0, NULL, 'calcofi_ctd-cast', 'c4'),
    (5,  'env', 'st60-ln90', '1992-07-33XX', TIMESTAMP '1992-07-10 12:00',  2.0, 'temperature_ave', 99.0, NULL, 'calcofi_ctd-cast', 'c5'),
    -- flagged questionable / bad: the quality predicate drops them
    (6,  'env', 'st60-ln90', '2001-07-33XX', TIMESTAMP '2001-07-11 12:00',  2.0, 'temperature_ave', 50.0, '8',  'calcofi_ctd-cast', 'c2'),
    (7,  'env', 'st60-ln90', '2001-07-33XX', TIMESTAMP '2001-07-11 12:00',  2.0, 'temperature_ave', 50.0, '9.0', 'calcofi_ctd-cast', 'c2'),
    -- another calendar month is another cell (and, with one cruise, below the floor)
    (8,  'env', 'st60-ln90', '2001-01-33XX', TIMESTAMP '2001-01-11 12:00',  2.0, 'temperature_ave', 5.0,  NULL, 'calcofi_ctd-cast', 'c8'),
    -- a deeper bin: 12.5 m floors to 10; 600 m is past the cap
    (9,  'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00', 12.5, 'temperature_ave', 9.0,  NULL, 'calcofi_ctd-cast', 'c1'),
    (10, 'env', 'st60-ln90', '2001-07-33XX', TIMESTAMP '2001-07-11 12:00', 19.9, 'temperature_ave', 9.0,  NULL, 'calcofi_ctd-cast', 'c2'),
    (11, 'env', 'st60-ln90', '2002-07-33XX', TIMESTAMP '2002-07-12 12:00', 10.0, 'temperature_ave', 9.0,  NULL, 'calcofi_ctd-cast', 'c3'),
    (12, 'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00', 600.0, 'temperature_ave', 4.0, NULL, 'calcofi_ctd-cast', 'c1'),
    -- not a value / not placed / not env
    (13, 'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  2.0, 'temperature_ave', 'NaN'::DOUBLE, NULL, 'calcofi_ctd-cast', 'c1'),
    (14, 'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  2.0, 'temperature_ave', 10.0, NULL, 'calcofi_ctd-cast', 'nosite'),
    (15, 'env', 'st60-ln90', '2000-07-33XX', NULL,                          2.0, 'temperature_ave', 10.0, NULL, 'calcofi_ctd-cast', 'c1'),
    (16, 'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00', NULL, 'temperature_ave', 10.0, NULL, 'calcofi_ctd-cast', 'c1'),
    (17, 'bio', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  2.0, 'abundance',       10.0, NULL, 'swfsc_ichthyo',    'i1'),
    -- the bottle dataset in the same cell is its OWN row (a consumer filters or pools by n)
    (18, 'env', 'st60-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  0.0, 'temperature', 20.0, NULL, 'calcofi_bottle', 'b1'),
    (19, 'env', 'st60-ln90', '2001-07-33XX', TIMESTAMP '2001-07-11 12:00',  0.0, 'temperature', 20.0, NULL, 'calcofi_bottle', 'b2'),
    (20, 'env', 'st60-ln90', '2002-07-33XX', TIMESTAMP '2002-07-12 12:00',  0.0, 'temperature', 20.0, NULL, 'calcofi_bottle', 'b3'),
    (21, 'env', 'st60-ln90', '2003-07-33XX', TIMESTAMP '2003-07-12 12:00',  0.0, 'temperature', 20.0, NULL, 'calcofi_bottle', 'b4'),
    -- st30-ln90: three casts from ONE cruise at three stations that share the cell (90.30, 90.28,
    -- 90.27.7) — one cruise each: below the floor
    (22, 'env', 'st30-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  2.0, 'temperature_ave', 15.0, NULL, 'calcofi_ctd-cast', 's30'),
    (23, 'env', 'st30-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 16:00',  2.0, 'temperature_ave', 16.0, NULL, 'calcofi_ctd-cast', 's28'),
    (24, 'env', 'st30-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 20:00',  2.0, 'temperature_ave', 17.0, NULL, 'calcofi_ctd-cast', 's277'),
    -- st35-ln90: TWO stations (90.35, 90.37) sharing the cell, three cruises each, different water:
    -- two baseline rows, never one. 90.37's second cruise was logged one cell over (GPS jitter).
    (25, 'env', 'st35-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 12:00',  2.0, 'temperature_ave', 18.0, NULL, 'calcofi_ctd-cast', 'a1'),
    (26, 'env', 'st35-ln90', '2001-07-33XX', TIMESTAMP '2001-07-11 12:00',  2.0, 'temperature_ave', 18.0, NULL, 'calcofi_ctd-cast', 'a2'),
    (27, 'env', 'st35-ln90', '2002-07-33XX', TIMESTAMP '2002-07-12 12:00',  2.0, 'temperature_ave', 18.0, NULL, 'calcofi_ctd-cast', 'a3'),
    (28, 'env', 'st35-ln90', '2000-07-33XX', TIMESTAMP '2000-07-10 14:00',  2.0, 'temperature_ave', 14.0, NULL, 'calcofi_ctd-cast', 'z1'),
    (29, 'env', 'st40-ln90', '2001-07-33XX', TIMESTAMP '2001-07-11 14:00',  2.0, 'temperature_ave', 14.0, NULL, 'calcofi_ctd-cast', 'z2'),
    (30, 'env', 'st35-ln90', '2002-07-33XX', TIMESTAMP '2002-07-12 14:00',  2.0, 'temperature_ave', 14.0, NULL, 'calcofi_ctd-cast', 'z3')
    ) t(obs_id, realm, grid_key, cruise_key, datetime, depth_min_m, measurement_type, measurement_value, measurement_qual, dataset_key, sample_key)")
  DBI::dbExecute(con, "CREATE TABLE sample AS SELECT * FROM (VALUES
    ('c1', '090.0 060.0'), ('c2', '090.0 060.0'), ('c3', '090.0 060.0'), ('c4', '090.0 060.0'),
    ('c5', '090.0 060.0'), ('c8', '090.0 060.0'), ('i1', '090.0 060.0'),
    ('b1', '090.0 060.0'), ('b2', '090.0 060.0'), ('b3', '090.0 060.0'), ('b4', '090.0 060.0'),
    ('nosite', NULL),
    ('s30', '090.0 030.0'), ('s28', '090.0 028.0'), ('s277', '090.0 027.7'),
    ('a1', '090.0 035.0'), ('a2', '090.0 035.0'), ('a3', '090.0 035.0'),
    ('z1', '090.0 037.0'), ('z2', '090.0 037.0'), ('z3', '090.0 037.0')
    ) t(sample_key, site_key)")
}

test_that("build_climatology(): the cell is a mean per dataset x station x month x 10 m bin x type", {
  con <- clim_con(); clim_fixture(con)
  n <- build_climatology(con, qual_ok_sql = CLIM_QUAL_OK)
  cl <- DBI::dbGetQuery(con, "SELECT * FROM climatology ORDER BY dataset_key, site_key, month, depth_bin")
  expect_equal(n, nrow(cl))
  expect_named(cl, c("dataset_key", "site_key", "grid_key", "month", "depth_bin", "measurement_type",
                     "clim_mean", "clim_sd", "clim_n", "n_cruises", "clim_yr_min", "clim_yr_max"))
  # five cells survive: ctd bin 0 + bin 10 + bottle bin 0 at 90.60, and the two st35 stations
  expect_equal(nrow(cl), 5)
  expect_true(all(cl$month == 7L), info = "January has one cruise, so no January cell")
  expect_false(any(cl$site_key %in% c("090.0 030.0", "090.0 028.0", "090.0 027.7")),
               info = "st30's three one-cruise stations are each below the floor")

  st60 <- cl[cl$site_key == "090.0 060.0", ]
  expect_equal(nrow(st60), 3)
  expect_true(all(st60$grid_key == "st60-ln90"))

  ctd0 <- st60[st60$dataset_key == "calcofi_ctd-cast" & st60$depth_bin == 0, ]
  expect_equal(ctd0$clim_mean, 11)                    # (10 + 11 + 12) / 3: 99s, 50s, NaN, 5, nosite all out
  expect_equal(ctd0$clim_sd, 1)
  expect_equal(ctd0$clim_n, 3L)
  expect_equal(ctd0$n_cruises, 3L)
  expect_equal(ctd0$measurement_type, "temperature_ave")

  ctd10 <- st60[st60$dataset_key == "calcofi_ctd-cast" & st60$depth_bin == 10, ]
  expect_equal(ctd10$clim_mean, 9)                    # 12.5, 19.9 and 10.0 all floor to bin 10
  expect_equal(ctd10$clim_n, 3L)

  btl <- st60[st60$dataset_key == "calcofi_bottle", ]
  expect_equal(btl$depth_bin, 0L)
  expect_equal(btl$clim_mean, 20)
  expect_equal(btl$n_cruises, 4L)
  expect_equal(btl$measurement_type, "temperature")
})

# regression (2026-09-09): the baseline was grained on grid_key, and the inshore cells hold 2-4
# stations occupied every cruise (st30-ln90 = 90.30, 90.28, 90.27.7, 88.5/30.1), so one cell's mean
# blended stations 15-30 km apart. Two stations in one cell are two baseline rows.
test_that("build_climatology(): two stations in one grid cell are two rows; grid_key is the station's modal cell", {
  con <- clim_con(); clim_fixture(con)
  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK)
  st35 <- DBI::dbGetQuery(con, "
    SELECT site_key, grid_key, clim_mean, clim_n, n_cruises FROM climatology
    WHERE site_key IN ('090.0 035.0', '090.0 037.0') ORDER BY site_key")
  expect_equal(st35$site_key,  c("090.0 035.0", "090.0 037.0"))
  expect_equal(st35$clim_mean, c(18, 14))            # not one row at 16
  expect_equal(st35$n_cruises, c(3L, 3L))
  expect_equal(st35$grid_key,  c("st35-ln90", "st35-ln90"),
               info = "90.37 was logged in st40 once; its modal cell is st35")
  # a sample with a grid cell but no station contributes to nothing
  expect_equal(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM climatology WHERE site_key IS NULL")$n, 0)
  # the primary key core_relationships() declares is unique on the result
  pk <- core_relationships("climatology")$primary_keys$climatology
  expect_equal(pk, c("dataset_key", "site_key", "month", "depth_bin", "measurement_type"))
  expect_silent(check_core_pk_unique(con, "climatology"))
})

test_that("build_climatology(): the window is stamped on every row and honoured", {
  con <- clim_con(); clim_fixture(con)
  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK)
  cl <- DBI::dbGetQuery(con, "SELECT DISTINCT clim_yr_min, clim_yr_max FROM climatology")
  expect_equal(cl, data.frame(clim_yr_min = 1993L, clim_yr_max = 2013L))
  # a window that admits the 2020 row moves the mean; one that misses 2000-2002 empties the table
  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK, yr_min = 2000, yr_max = 2020, tbl = "c2")
  ctd0 <- DBI::dbGetQuery(con, "SELECT clim_mean, clim_n, clim_yr_max FROM c2 WHERE dataset_key = 'calcofi_ctd-cast' AND site_key = '090.0 060.0' AND depth_bin = 0")
  expect_equal(ctd0$clim_mean, (10 + 11 + 12 + 99) / 4)
  expect_equal(ctd0$clim_yr_max, 2020L)
  expect_equal(build_climatology(con, qual_ok_sql = CLIM_QUAL_OK, yr_min = 2005, yr_max = 2013, tbl = "c3"), 0)
  expect_error(build_climatology(con, qual_ok_sql = CLIM_QUAL_OK, yr_min = 2013, yr_max = 1993))
  expect_error(build_climatology(con, qual_ok_sql = ""))
})

test_that("build_climatology(): the floor counts cruises, not observations", {
  con <- clim_con(); clim_fixture(con)
  # at min_cruises = 1 the one-cruise cells appear — st30's three same-cruise casts are three
  # stations, one row each, never pooled into one
  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK, min_cruises = 1)
  st30 <- DBI::dbGetQuery(con, "SELECT site_key, clim_mean, clim_n, n_cruises FROM climatology WHERE grid_key = 'st30-ln90' ORDER BY site_key")
  expect_equal(st30, data.frame(site_key  = c("090.0 027.7", "090.0 028.0", "090.0 030.0"),
                                clim_mean = c(17, 16, 15), clim_n = c(1L, 1L, 1L), n_cruises = c(1L, 1L, 1L)))
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
    FROM climatology WHERE site_key = '090.0 060.0' AND month = 7 AND depth_bin = 0")
  direct <- DBI::dbGetQuery(con, "
    SELECT avg(measurement_value) AS v, count(*) AS n FROM obs
    WHERE obs_id IN (1, 2, 3, 18, 19, 20, 21)")
  expect_equal(pooled$v, direct$v)
  expect_equal(pooled$n, direct$n)
  # and a cell-level consumer pools the two st35 stations the same way
  cell <- DBI::dbGetQuery(con, "
    SELECT sum(clim_mean * clim_n) / sum(clim_n) AS v FROM climatology WHERE grid_key = 'st35-ln90'")
  expect_equal(cell$v, 16)
})

test_that("climatology has a registered sort key that is a unique total order, so it exports", {
  sk <- release_sort_keys()$climatology
  expect_equal(sk$partition_by, "measurement_type")
  expect_equal(sk$order_by, c("measurement_type", "dataset_key", "site_key", "month", "depth_bin"))
  con <- clim_con(); clim_fixture(con)
  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK)
  out <- withr::local_tempdir()
  f <- export_release_parquet(con, "climatology", file.path(out, "climatology"), sk$order_by,
                              partition_by = sk$partition_by)
  expect_setequal(basename(dirname(f$rel_path)), c("measurement_type=temperature", "measurement_type=temperature_ave"))
  back <- DBI::dbGetQuery(con, glue::glue(
    "SELECT count(*) AS n FROM read_parquet('{file.path(out, 'climatology')}/*/*.parquet', hive_partitioning = true)"))
  expect_equal(back$n, 5)
})

# regression (2026-09-05): production v2026.09.04 re-exported `climatology` with identical row
# counts but a different content hash on 60/71 partitions, and the same happened between two
# staging runs the same day — every other release table reproduces exactly. Cause: DuckDB computes
# avg()/stddev_samp() by combining per-thread partial sums, and floating-point addition is not
# associative, so the combine order (which varies run to run for no data reason) flips the last 1-2
# bits of the double. `clim_n`/`n_cruises` (integer counts) were never affected — only the two float
# aggregates. The fix rounds `clim_mean`/`clim_sd` to `round_digits` (6 decimal places) so the noise,
# which is ~9 orders of magnitude below the rounding grain, is discarded after the (correct, if
# unstable) aggregate is computed.
test_that("build_climatology(): clim_mean/clim_sd are stable under DuckDB's parallel avg()/stddev_samp(), so re-exports of unchanged data are byte-identical", {
  con <- clim_con()
  # enough distinct rows per cell to force DuckDB's morsel-driven parallel aggregation (8 threads on
  # this machine) to combine multiple per-thread partial sums per group -- a handful of rows, as in
  # clim_fixture(), never exercises this path
  DBI::dbExecute(con, "SET threads TO 8")
  DBI::dbExecute(con, "
    CREATE TABLE obs AS
    SELECT 'env' AS realm,
           'st' || (i % 40) || '-ln90'                       AS grid_key,
           'S' || (i % 40)                                    AS sample_key,
           'cruise' || (i % 11)                               AS cruise_key,
           TIMESTAMP '2000-07-01' + (i % 11) * INTERVAL 1 YEAR AS datetime,
           2.0                                                 AS depth_min_m,
           'temperature_ave'                                   AS measurement_type,
           12.3456789 + sin(i * 0.7919) * 3.7182818            AS measurement_value,
           NULL::VARCHAR                                       AS measurement_qual,
           'calcofi_ctd-cast'                                  AS dataset_key
    FROM range(400000) t(i)")
  DBI::dbExecute(con, "
    CREATE TABLE sample AS
    SELECT 'S' || i AS sample_key, printf('%05.1f %05.1f', 90.0, i::DOUBLE) AS site_key FROM range(40) t(i)")

  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK, tbl = "clim_a")
  build_climatology(con, qual_ok_sql = CLIM_QUAL_OK, tbl = "clim_b")

  ha <- calcofi4db:::.partition_content_hashes(con, "clim_a", "measurement_type")
  hb <- calcofi4db:::.partition_content_hashes(con, "clim_b", "measurement_type")
  expect_equal(ha, hb)

  a <- DBI::dbGetQuery(con, "SELECT * FROM clim_a ORDER BY grid_key")
  b <- DBI::dbGetQuery(con, "SELECT * FROM clim_b ORDER BY grid_key")
  expect_identical(a$clim_mean, b$clim_mean)
  expect_identical(a$clim_sd,   b$clim_sd)
  expect_true(nrow(a) > 0)

  # the rounded value is still the right one: a single-threaded, independently expressed aggregate
  # over the same rows agrees with build_climatology()'s (parallel, rounded) result to the rounding
  # grain -- rounding discarded only the parallel-combine noise, not correctness
  DBI::dbExecute(con, "SET threads TO 1")
  ref <- DBI::dbGetQuery(con, "
    SELECT grid_key, round(avg(measurement_value), 6) AS clim_mean, round(stddev_samp(measurement_value), 6) AS clim_sd
    FROM obs GROUP BY grid_key ORDER BY grid_key")
  expect_equal(a$clim_mean, ref$clim_mean)
  expect_equal(a$clim_sd,   ref$clim_sd)
})
