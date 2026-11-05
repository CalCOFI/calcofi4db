skip_if_not_installed("calcofi4r")

track_con <- function() {
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = parent.frame())
  con
}

test_that("append_sample_track computes the middle, the length and a LINESTRING", {
  con <- track_con()
  n <- append_sample_track(con, "
    SELECT 'ds:transect:1', 'ds', 'effort_segment',
           TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 02:00:00',
           -120.0, 34.0, -120.0, 35.0, NULL::DOUBLE
    UNION ALL
    SELECT 'ds:transect:2', 'ds', 'effort_segment',
           TIMESTAMP '2020-01-01 03:00:00', TIMESTAMP '2020-01-01 04:00:00',
           -121.0, 34.0, -120.0, 34.0, 99.5")
  expect_equal(n, 2L)
  d <- DBI::dbGetQuery(con, "
    SELECT sample_key, datetime_mid, longitude_mid, latitude_mid, length_km,
           ST_AsText(geom) AS wkt, ST_NPoints(geom) AS np
    FROM sample_track ORDER BY sample_key")

  # along a meridian the middle is the mean latitude, 1 degree = 111.195 km
  expect_equal(d$datetime_mid[1], as.POSIXct("2020-01-01 01:00:00", tz = "UTC"))
  expect_equal(d$longitude_mid[1], -120)
  expect_equal(d$latitude_mid[1], 34.5, tolerance = 1e-9)
  expect_equal(d$length_km[1], 111.195, tolerance = 1e-4)
  expect_equal(d$np, c(2L, 2L))
  expect_match(d$wkt[1], "^LINESTRING \\(-120 34, -120 35\\)$")

  # along a parallel the great-circle middle bulges poleward of the mean latitude
  expect_equal(d$longitude_mid[2], -120.5, tolerance = 1e-9)
  expect_gt(d$latitude_mid[2], 34)
  expect_lt(d$latitude_mid[2], 34.01)
  # a supplied length (the provider's own) is kept, not recomputed
  expect_equal(d$length_km[2], 99.5)
})

test_that("append_sample_track normalizes NaN and leaves geom NULL for a missing end", {
  con <- track_con()
  append_sample_track(con, "
    SELECT 'ds:sighting:1', 'ds', 'sight_line',
           TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 00:00:00',
           -120.0, 34.0, 'NaN'::DOUBLE, 34.1, NULL::DOUBLE
    UNION ALL
    SELECT 'ds:sighting:2', 'ds', 'sight_line',
           TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 00:00:00',
           -120.0, 34.0, -120.0, 34.01, NULL::DOUBLE")
  d <- DBI::dbGetQuery(con, "
    SELECT sample_key, longitude_end, geom IS NULL AS no_geom, datetime_mid
    FROM sample_track ORDER BY sample_key")
  # NaN is not NULL: it is normalized before a geometry is minted
  expect_true(is.na(d$longitude_end[1]))
  expect_equal(d$no_geom, c(TRUE, FALSE))
  # an instantaneous path: the middle time is the time
  expect_equal(d$datetime_mid[2], as.POSIXct("2020-01-01", tz = "UTC"))
})

test_that("append_sample_track refuses the wrong arity by name", {
  con <- track_con()
  expect_error(
    append_sample_track(con, "SELECT 'a', 'b', 'c'"),
    "must yield 10 columns")
})

test_that("sample_track is a core table keyed by sample_key with an FK to sample", {
  rels <- core_relationships(c("sample", "sample_track"))
  expect_equal(rels$primary_keys$sample_track, "sample_key")
  fk <- Filter(function(e) e$table == "sample_track", rels$foreign_keys)
  expect_length(fk, 1)
  expect_equal(fk[[1]]$ref_table, "sample")
  expect_equal(release_sort_keys()$sample_track$order_by, "sample_key")
})

test_that("cogo_true_bearing adds the angle to starboard and subtracts it to port", {
  # the lab's first CC2004-07 sightings: heading 272, 25 deg to port -> 247
  expect_equal(cogo_true_bearing(272, 25, "Port"), 247)
  expect_equal(cogo_true_bearing(241, 30, "Star"), 271)
  # wraps through north both ways
  expect_equal(cogo_true_bearing(350, 30, "starboard"), 20)
  expect_equal(cogo_true_bearing(10, 30, "P"), 340)
  # an unknown side is NA, never a guess
  expect_true(is.na(cogo_true_bearing(100, 30, NA)))
  expect_true(is.na(cogo_true_bearing(100, 30, "ahead")))
})

test_that("cogo_destination moves the distance along the bearing", {
  # 1 km due north: 1000 / 6371008.8 rad of latitude
  p <- cogo_destination(-120, 34, 0, 1000)
  expect_equal(p$longitude, -120)
  expect_equal(p$latitude, 34 + 1000 / 6371008.8 * 180 / pi, tolerance = 1e-12)
  # due east at 34N: the round trip by calcofi4r's haversine is the distance
  p <- cogo_destination(-120, 34, 90, 5000)
  con <- track_con()
  km <- DBI::dbGetQuery(con, paste("SELECT", calcofi4r::cc_distance_km_sql(
    "-120", "34", p$longitude, p$latitude), "AS km"))$km
  expect_equal(km, 5, tolerance = 1e-9)
  # longitude stays in [-180, 180) across the antimeridian; NA in, NA out
  expect_lt(cogo_destination(179.999, 0, 90, 1000)$longitude, -179)
  expect_true(is.na(cogo_destination(-120, 34, NA, 1000)$latitude))
})

test_that("assemble_core() leaves sample_track absent without shards, unions it when one exists, stops on a duplicate", {
  skip_if_not_installed("withr")
  root <- withr::local_tempdir()
  withr::local_envvar(c(CALCOFI_STAGE_DIR = root))
  con <- track_con()
  for (d in c("ds_a", "ds_b")) dir.create(file.path(root, "parquet", d), recursive = TRUE)
  p <- function(d, t) file.path(root, "parquet", d, paste0(t, ".parquet"))
  for (d in c("ds_a", "ds_b"))
    DBI::dbExecute(con, glue::glue(
      "COPY (SELECT '{d}:tow:1' sample_key, 'tow' sample_type, '{d}' dataset_key)
       TO '{p(d, 'sample')}' (FORMAT PARQUET)"))

  # a public release: no dataset writes a track, so the table is simply absent
  n0 <- assemble_core(con, root, supplemental = FALSE)
  expect_equal(n0$sample_track, 0L)
  expect_false("sample_track" %in% DBI::dbListTables(con))

  # one dataset writes one: only its rows, no other shard needed
  DBI::dbExecute(con, glue::glue(
    "COPY (SELECT 'ds_b:tow:1' sample_key, 'ds_b' dataset_key, 'effort_segment' track_type)
     TO '{p('ds_b', 'sample_track')}' (FORMAT PARQUET)"))
  # (a fresh connection per run: re-assembling over existing tables is not what the release does)
  con <- track_con()
  n1 <- assemble_core(con, root, supplemental = FALSE)
  expect_equal(n1$sample_track, 1)
  expect_equal(DBI::dbGetQuery(con, "SELECT sample_key FROM sample_track")$sample_key, "ds_b:tow:1")

  # two paths for one sample break the primary key
  DBI::dbExecute(con, glue::glue(
    "COPY (SELECT 'ds_b:tow:1' sample_key, 'ds_b' dataset_key, 'sight_line' track_type)
     TO '{p('ds_a', 'sample_track')}' (FORMAT PARQUET)"))
  con <- track_con()
  expect_error(assemble_core(con, root, supplemental = FALSE),
               "more than one sample_track row")
})
