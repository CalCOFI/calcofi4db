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

test_that("obs_freq stage bins sum to the abundance headline; length bins are a subsample", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  append_obs(con, ich_obs_sql)
  append_obs_freq(con, ich_obs_freq_sql)

  d <- DBI::dbGetQuery(con, "
    SELECT
      (SELECT SUM(count) FROM obs_freq
        WHERE measurement_type='stage')       AS stage_sum,
      (SELECT SUM(count) FROM obs_freq
        WHERE measurement_type='body_length') AS length_sum,
      (SELECT SUM(measurement_value) FROM obs
        WHERE measurement_type='abundance')   AS abundance")

  expect_equal(d$stage_sum, d$abundance)   # 4 + 6 == 10  (design: stage sum == abundance)
  expect_lte(d$length_sum, d$abundance)    # 3 + 2 == 5 <= 10 (design: length is a subsample)

  # size -> body_length rename; stage bins carry a lookup label
  types <- DBI::dbGetQuery(con,
    "SELECT DISTINCT measurement_type FROM obs_freq ORDER BY 1")$measurement_type
  expect_setequal(types, c("body_length", "stage"))
  lab <- DBI::dbGetQuery(con,
    "SELECT bin_label FROM obs_freq WHERE measurement_type='stage' AND bin_value=2.0")$bin_label
  expect_equal(lab, "preflexion")
})
