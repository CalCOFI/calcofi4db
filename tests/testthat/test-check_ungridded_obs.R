# Ungridded observations are released now, not dropped — so the thing that must
# not regress is that they stay VISIBLE. These pin the counting and the split
# between "off-grid but positioned" and "no position at all", which is the
# distinction a provider needs in order to answer.

make_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  DBI::dbWriteTable(con, "obs", data.frame(
    dataset_key = c(rep("aa", 4), rep("bb", 2)),
    grid_key    = c("g1", "g2", NA, NA, "g1", "g2"),
    latitude    = c(33, 34, 35, NA, 33, 34),
    longitude   = c(-120, -121, -122, NA, -120, -121)))
  con
}

test_that("it counts ungridded rows and separates the ones with no position", {
  rpt <- check_ungridded_obs(make_con(), verbose = FALSE)
  aa <- rpt[rpt$dataset_key == "aa", ]
  expect_equal(aa$n_obs, 4L)
  expect_equal(aa$n_ungridded, 2L)      # one positioned off-grid, one with no position
  expect_equal(aa$n_no_position, 1L)
  expect_equal(aa$pct_ungridded, 50)
})

test_that("a fully gridded dataset gets no finding to ask about", {
  rpt <- check_ungridded_obs(make_con(), verbose = FALSE)
  bb <- rpt[rpt$dataset_key == "bb", ]
  expect_equal(bb$n_ungridded, 0L)
  expect_true(is.na(bb$finding))
})

test_that("the finding names both numbers, so it can be pasted into questions.csv", {
  rpt <- check_ungridded_obs(make_con(), verbose = FALSE)
  f <- rpt$finding[rpt$dataset_key == "aa"]
  expect_match(f, "2 of 4 obs rows")
  expect_match(f, "no latitude/longitude at all")
  expect_match(f, "RELEASED rather than dropped")
})

test_that("it reports rather than filters — nothing is removed from obs", {
  con <- make_con()
  before <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM obs")$n
  invisible(check_ungridded_obs(con, verbose = FALSE))
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM obs")$n, before)
})
