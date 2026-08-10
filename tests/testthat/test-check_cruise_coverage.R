# A cruise can leave `obs` without leaving `sample`, and no foreign key notices:
# FK validation runs child -> parent, so a parent with no children is silent.
# These pin the four behaviors that make the check usable as a release gate.

# two datasets: `aa` observes 2 of its 3 cruises, `rr` is a registry (no obs)
make_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)

  smpl <- data.frame(
    dataset_key = c(rep("aa", 5), rep("rr", 2)),
    cruise_key  = c("c1", "c1", "c2", "c3", "c3", "r1", "r2"),
    sample_key  = c("aa:1", "aa:2", "aa:3", "aa:4", "aa:5", "rr:1", "rr:2"))
  # c3's two samples have no obs at all; c1 has one observed and one bare sample
  # (the CTD up/down-cast case, which must NOT count as a loss)
  obs <- data.frame(
    sample_key       = c("aa:1", "aa:3", "aa:3"),
    measurement_type = c("t", "t", "s"))

  DBI::dbWriteTable(con, "sample", smpl)
  DBI::dbWriteTable(con, "obs", obs)
  con
}

test_that("a cruise with samples and no obs is found, and a half-observed one is not", {
  con <- make_con()
  rpt <- check_cruise_coverage(con, halt = FALSE, verbose = FALSE) |>
    suppressWarnings()

  aa <- rpt[rpt$dataset_key == "aa", ]
  expect_equal(aa$cruises, 3L)
  # c3 only — c1 keeps one bare sample beside an observed one and is fine
  expect_equal(aa$cruises_no_obs, 1L)
  expect_equal(aa$orphan_samples, 2)
})

test_that("a dataset that emits no observations at all is exempt, not 587 failures", {
  con <- make_con()
  rpt <- check_cruise_coverage(con, halt = FALSE, verbose = FALSE) |>
    suppressWarnings()

  rr <- rpt[rpt$dataset_key == "rr", ]
  expect_false(rr$emits_obs)
  expect_equal(rr$cruises_no_obs, 0L)   # sio_pic-zooplankton is a tow registry
  expect_true(rpt$emits_obs[rpt$dataset_key == "aa"])
})

test_that("it halts by default and the per-dataset allowance ratchets it", {
  con <- make_con()
  expect_error(check_cruise_coverage(con, verbose = FALSE), "samples but no obs")
  expect_silent(
    check_cruise_coverage(con, max_orphan_cruises = c(aa = 1L), verbose = FALSE))
  # the ratchet is per dataset: an allowance for another key does not cover `aa`
  expect_error(
    check_cruise_coverage(con, max_orphan_cruises = c(zz = 9L), verbose = FALSE),
    "aa: 1 of 3")
})

test_that("a NULL cruise_key on obs cannot invent an orphan", {
  # obs.cruise_key is denormalized and NULL for tens of thousands of real rows,
  # so the join must go through sample_key. Drop the column entirely: if the
  # implementation ever reaches for it, this fails rather than silently passing.
  con <- make_con()
  DBI::dbExecute(con, "ALTER TABLE obs ADD COLUMN cruise_key VARCHAR")
  rpt <- check_cruise_coverage(con, halt = FALSE, verbose = FALSE) |>
    suppressWarnings()
  expect_equal(rpt$cruises_no_obs[rpt$dataset_key == "aa"], 1L)
})
