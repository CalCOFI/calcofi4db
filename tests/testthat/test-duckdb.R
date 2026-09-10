test_that("get_duckdb_con() takes memory_limit and threads from the environment", {
  skip_if_not_installed("duckdb")
  withr::local_envvar(CALCOFI_DUCKDB_MEMORY_LIMIT = "1GB", CALCOFI_DUCKDB_THREADS = "2")
  con <- get_duckdb_con()
  withr::defer(close_duckdb(con))
  expect_equal(DBI::dbGetQuery(con, "SELECT current_setting('threads') AS t")$t, 2)
  ml <- DBI::dbGetQuery(con, "SELECT current_setting('memory_limit') AS m")$m
  expect_match(ml, "^(953\\.6 MiB|1\\.0 GiB|1000\\.0 MiB|1\\.0 GB)$")
})

test_that("an explicit config entry wins over the environment, and an empty variable is ignored", {
  skip_if_not_installed("duckdb")
  withr::local_envvar(CALCOFI_DUCKDB_THREADS = "2", CALCOFI_DUCKDB_MEMORY_LIMIT = "")
  con <- get_duckdb_con(config = list(threads = 1))
  withr::defer(close_duckdb(con))
  expect_equal(DBI::dbGetQuery(con, "SELECT current_setting('threads') AS t")$t, 1)
  expect_false(DBI::dbGetQuery(con, "SELECT current_setting('memory_limit') AS m")$m == "1GB")
})
