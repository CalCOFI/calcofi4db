# Every pipeline target is format = "file", so targets hashes whatever path the
# command returns. A DIRECTORY output can never settle: any later write
# underneath it moves the hash, so the target is outdated forever and re-runs on
# every pass.
#
# release_database shipped like this. It declared `data/releases`, and
# test_release writes `data/releases/{version}/test_results.json` — as a SIDE
# EFFECT, not as its declared output, so no comparison of the `output:` fields
# could ever have related the two. Verified on v2026.08.08: the release's files
# landed 16:46-17:06, test_results.json at 17:08:47. Cost was a ~40 min freeze
# and a multi-GB re-upload of an already-promoted release on every invocation of
# anything downstream.

test_that("a DIRECTORY output is rejected — the case that actually bit us", {
  wf <- data.frame(
    target_name = c("release_database", "test_release"),
    output      = c("data/releases", "_output/test_release.html"))

  expect_error(check_nested_outputs(wf), "DIRECTORY")
  expect_error(check_nested_outputs(wf), "release_database")
  # the fix must be stated, not just the fault
  expect_error(check_nested_outputs(wf), "_release_stamp")

  # and the innocent target must not be listed as an offender
  msg <- tryCatch(check_nested_outputs(wf), error = conditionMessage)
  expect_false(grepl("test_release ->", msg, fixed = TRUE))
})

test_that("a directory is caught even when it does not exist yet", {
  # at parse time the path may not exist; the basename has no extension
  wf <- data.frame(target_name = "x", output = "data/not_created_yet")
  expect_error(check_nested_outputs(wf, root = tempdir()), "DIRECTORY")
})

test_that("statically nested outputs are rejected too", {
  wf <- data.frame(
    target_name = c("a", "b"),
    output      = c("out/a.json", "out/a.json/v1/b.json"))
  expect_error(check_nested_outputs(wf, root = tempdir()), "nest")
  expect_error(check_nested_outputs(wf, root = tempdir()), "\\ba\\b")
  expect_error(check_nested_outputs(wf, root = tempdir()), "\\bb\\b")
})

test_that("check_nested_outputs() accepts the shapes the pipeline actually uses", {
  ok <- data.frame(
    target_name = c("release_database", "test_release", "ingest_a", "ingest_b",
                    "publish_obis"),
    output      = c("data/releases/_release_stamp.json",   # the fix
                    "_output/test_release.html",
                    "data/parquet/a/manifest.json",
                    "data/parquet/b/manifest.json",
                    "data/darwincore/ichthyo_*.zip"))      # a glob is not a dir
  expect_silent(check_nested_outputs(ok, root = tempdir()))
  expect_identical(check_nested_outputs(ok, root = tempdir()), ok)
})

test_that("a shared name prefix is not mistaken for nesting", {
  # `out/releases_old/...` is NOT inside `out/releases.json` — a startsWith()
  # without the trailing separator would block a perfectly valid setup
  wf <- data.frame(
    target_name = c("a", "b"),
    output      = c("out/releases.json", "out/releases_old/x.json"))
  expect_silent(check_nested_outputs(wf, root = tempdir()))

  # identical outputs are a different (also bad) problem — not nesting, and not
  # this function's job to report
  wf2 <- data.frame(target_name = c("a", "b"), output = c("x/y.json", "x/y.json"))
  expect_silent(check_nested_outputs(wf2, root = tempdir()))
})
