# tests for input fingerprinting (R/fingerprint.R)
#
# The whole mechanism decides whether an hour of download → parse → pivot is
# skipped, so the assertions that matter are the ones about what must NOT compare
# equal: a changed registry, and a deleted one.

test_that("the fingerprint is stable for unchanged inputs", {
  dir <- withr::local_tempdir()
  f1 <- file.path(dir, "a.csv"); writeLines("x,y\n1,2", f1)
  f2 <- file.path(dir, "b.csv"); writeLines("p,q\n3,4", f2)

  a <- input_fingerprint(c(f1, f2), values = c("u1", "u2"))
  b <- input_fingerprint(c(f1, f2), values = c("u1", "u2"))
  expect_identical(a$hash, b$hash)
  expect_equal(length(a$parts), 3L)  # two files + the values bundle
})

test_that("a changed file changes the fingerprint and is named", {
  dir <- withr::local_tempdir()
  f <- file.path(dir, "a.csv"); writeLines("x,y\n1,2", f)
  before <- input_fingerprint(f)

  writeLines("x,y\n1,3", f)
  after <- input_fingerprint(f)

  expect_false(identical(before$hash, after$hash))
  expect_equal(changed_inputs(after, list(parts = as.list(before$parts))), f)
})

test_that("a DELETED input changes the fingerprint", {
  # the failure this prevents: dropping cruise_key_corrections.csv would otherwise
  # look like "nothing changed" and the stale correction would stay baked into the
  # outputs
  dir <- withr::local_tempdir()
  f <- file.path(dir, "a.csv"); writeLines("x", f)
  before <- input_fingerprint(f)
  file.remove(f)
  after <- input_fingerprint(f)

  expect_false(identical(before$hash, after$hash))
  expect_equal(unname(after$parts[[1]]), "<missing>")
})

test_that("changed values (e.g. a new source zip) change the fingerprint", {
  expect_false(identical(
    input_fingerprint(values = c("a.zip", "b.zip"))$hash,
    input_fingerprint(values = c("a.zip", "b.zip", "c.zip"))$hash))
})

test_that("write/read round-trips and an absent or corrupt state reads as NULL", {
  dir <- withr::local_tempdir()
  p   <- file.path(dir, "state.json")
  f   <- file.path(dir, "a.csv"); writeLines("x", f)
  fp  <- input_fingerprint(f, values = "z")

  expect_null(read_input_fingerprint(p))
  write_input_fingerprint(p, fp)
  got <- read_input_fingerprint(p)
  expect_equal(got$hash, fp$hash)
  expect_equal(changed_inputs(fp, got), character(0))

  # a truncated state file must fall through to a full run, not error
  writeLines("{ not json", p)
  expect_null(read_input_fingerprint(p))
  expect_equal(changed_inputs(fp, NULL), names(fp$parts))
})
