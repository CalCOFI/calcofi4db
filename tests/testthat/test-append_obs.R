test_that("append_obs writes the abundance headline with a computed hex_id", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  n <- append_obs(con, ich_obs_sql)
  expect_equal(n, 1L)   # one base (measurement_type NULL) row -> one abundance obs

  o <- DBI::dbGetQuery(con,
    "SELECT measurement_type, measurement_value, life_stage, sample_key, hex_id FROM obs")
  expect_equal(o$measurement_type, "abundance")
  expect_equal(o$measurement_value, 10)
  expect_equal(o$sample_key, "swfsc_ichthyo:net:N1")
  expect_true("hex_id" %in% DBI::dbListFields(con, "obs"))
  expect_false(is.na(o$hex_id))          # hex_id computed from lat/lng

  # obs_id is minted and offset on a second append (stays unique)
  append_obs(con, ich_obs_sql)
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(DISTINCT obs_id) n FROM obs")$n,
               DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM obs")$n)
})

test_that("obs_attribute stage bins sum to the abundance headline; length bins are a subsample", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  append_obs(con, ich_obs_sql)
  append_obs_attribute(con, ich_obs_attribute_sql)

  d <- DBI::dbGetQuery(con, "
    SELECT
      (SELECT SUM(count) FROM obs_attribute
        WHERE measurement_type='stage')       AS stage_sum,
      (SELECT SUM(count) FROM obs_attribute
        WHERE measurement_type='body_length') AS length_sum,
      (SELECT SUM(measurement_value) FROM obs
        WHERE measurement_type='abundance')   AS abundance")

  expect_equal(d$stage_sum, d$abundance)   # 4 + 6 == 10  (design: stage sum == abundance)
  expect_lte(d$length_sum, d$abundance)    # 3 + 2 == 5 <= 10 (design: length is a subsample)

  # size -> body_length rename; stage bins carry a lookup label
  types <- DBI::dbGetQuery(con,
    "SELECT DISTINCT measurement_type FROM obs_attribute ORDER BY 1")$measurement_type
  expect_setequal(types, c("body_length", "stage"))
  lab <- DBI::dbGetQuery(con,
    "SELECT bin_label FROM obs_attribute WHERE measurement_type='stage' AND bin_value=2.0")$bin_label
  expect_equal(lab, "preflexion")
})

# NaN is not NULL — it survives IS NOT NULL, reaches the release, and yields no
# hex_id while still looking like a position. Releasing ungridded observations
# is what first exposed 9,030 such rows; before that the `grid_key IS NOT NULL`
# filter hid them, because a NaN coordinate cannot grid.
test_that("append_obs normalises NaN/Inf coordinates to NULL", {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))

  append_obs(con, "
    SELECT * FROM (VALUES
      ('bio','aa','aa:s:1',NULL,NULL, 33.0,        -120.0,       NULL::TIMESTAMP,
        0,0,NULL,NULL,'count',1.0,NULL,NULL),
      ('bio','aa','aa:s:2',NULL,NULL, 'nan'::DOUBLE,'nan'::DOUBLE,NULL::TIMESTAMP,
        0,0,NULL,NULL,'count',1.0,NULL,NULL),
      ('bio','aa','aa:s:3',NULL,NULL, 'inf'::DOUBLE, -120.0,      NULL::TIMESTAMP,
        0,0,NULL,NULL,'count',1.0,NULL,NULL))")

  d <- DBI::dbGetQuery(con, "SELECT sample_key, latitude, longitude, hex_id FROM obs ORDER BY sample_key")
  expect_equal(nrow(d), 3)                       # nothing is dropped
  expect_equal(d$latitude[1], 33)                # a good coordinate is untouched
  expect_false(is.na(d$hex_id[1]))               # ... and still gets a hex

  expect_true(is.na(d$latitude[2]));  expect_true(is.na(d$longitude[2]))
  expect_true(is.na(d$latitude[3]))              # Inf too, not just NaN

  # the contract test_release enforces: no row may carry lat/lng without a hex
  expect_equal(DBI::dbGetQuery(con, "
    SELECT COUNT(*) n FROM obs
    WHERE hex_id IS NULL AND latitude IS NOT NULL AND longitude IS NOT NULL")$n, 0)
})
