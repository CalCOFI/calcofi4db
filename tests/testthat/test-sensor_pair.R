# combine_sensor_pair(): Rasmus's rule for the CTD's two sensors, one case per branch, and the
# SQL twin agreeing with the R form on every case.

SP <- data.frame(
  case = c("both good: mean", "s1 questionable: s2 alone", "s2 bad (9.0): s1 alone",
           "both bad: NULL", "use primary: s1", "use secondary: s2",
           "primary said on s2's flag: still s1", "1 and 2 disagree: mean",
           "use primary but s1 bad: s2 alone", "s1 NULL: s2", "both NULL: NULL",
           "numeric flags", "blank flag is good"),
  v1 = c(10, 10, 10, 10, 10, 10, 10, 10, 10, NA, NA, 10, 10),
  v2 = c(12, 12, 12, 12, 12, 12, 12, 12, 12, 12, NA, 12, 12),
  q1 = c(NA, "8", NA,  "8", "1", NA,  NA,  "1", "1", NA, NA, "1", ""),
  q2 = c(NA, NA,  "9.0", "9", NA, "2", "1", "2", "9", NA, NA, NA, ""),
  want = c(11, 12, 10, NA, 10, 12, 10, 11, 12, 12, NA, 10, 11),
  stringsAsFactors = FALSE)

test_that("combine_sensor_pair(): every branch of the rule", {
  got <- combine_sensor_pair(SP$v1, SP$v2, SP$q1, SP$q2)
  for (i in seq_len(nrow(SP))) expect_equal(got[i], SP$want[i], info = SP$case[i])
  # numeric flags and recycling of a scalar flag
  expect_equal(combine_sensor_pair(c(10, 10), c(12, 12), q1 = 8, q2 = NA), c(12, 12))
  expect_equal(combine_sensor_pair(numeric(0), numeric(0)), numeric(0))
})

test_that("combine_sensor_pair_sql(): the SQL twin agrees on every case", {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbWriteTable(con, "sp", SP)
  expr <- combine_sensor_pair_sql("v1", "v2", "q1", "q2")
  got <- DBI::dbGetQuery(con, glue::glue("SELECT {expr} AS v FROM sp"))$v
  expect_equal(got, SP$want)
  # over integer-typed flags too (a source that parsed them as numbers)
  DBI::dbExecute(con, "CREATE TABLE sp2 AS SELECT v1, v2, TRY_CAST(q1 AS DOUBLE) AS q1, TRY_CAST(q2 AS DOUBLE) AS q2, want FROM sp")
  got2 <- DBI::dbGetQuery(con, glue::glue("SELECT {expr} AS v FROM sp2"))$v
  expect_equal(got2, SP$want)
  expect_error(combine_sensor_pair_sql("a", "b", "c", c("d", "e")))
})
