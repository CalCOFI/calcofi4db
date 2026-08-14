# Guards on moving `latest.txt`, both written after 2026-08-14, when a release
# was promoted to a version that had no catalog.json and consumers resolving
# through `latest` got a 404 for an hour while the query suite showed 28/28.
#
# The tests stub `find_gcloud()` so nothing touches GCS: what is pinned here is
# the DECISION (promote / refuse) and the flags used, not the transport.

# Write a fake `gcloud` at `bin`: exits 0 for `storage ls` on a listed object and
# 1 otherwise, prints a version for `storage cat`, and appends every invocation
# to `log`. The caller owns `bin`/`log` lifetimes — creating them inside a mocked
# binding would delete them the moment that call returns.
write_fake_gcloud <- function(bin, log, present = character()) {
  pat <- if (length(present)) paste(present, collapse = "|") else "^$"
  writeLines(c(
    "#!/bin/sh",
    sprintf('echo "$@" >> %s', shQuote(log)),
    'case "$2" in',
    '  cat) echo "v2026.08.11"; exit 0 ;;',
    sprintf('  ls) echo "$3" | grep -Eq %s && exit 0 || exit 1 ;;', shQuote(pat)),
    '  cp) exit 0 ;;',
    'esac',
    "exit 0"), bin)
  Sys.chmod(bin, "0755")
  bin
}

# a test fixture: fake gcloud + log, with find_gcloud() mocked to return it
setup_gcloud <- function(present = character(), env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  bin <- file.path(dir, "gcloud"); log <- file.path(dir, "calls.log")
  file.create(log)
  write_fake_gcloud(bin, log, present)
  testthat::local_mocked_bindings(find_gcloud = function() bin, .env = env)
  log
}

calls <- function(log) if (file.exists(log)) readLines(log) else character()

ALL_PRESENT <- "catalog.json|metadata.json|relationships.json"

test_that("check_release_complete() fails when a required sidecar is absent", {
  setup_gcloud(present = "parquet")   # the 2026-08-14 shape: parquet up, JSON not
  expect_error(check_release_complete("v2026.08.14"), "is NOT complete")
  expect_error(check_release_complete("v2026.08.14"), "catalog\\.json")
})

test_that("check_release_complete() passes when all required objects exist", {
  setup_gcloud(present = ALL_PRESENT)
  out <- check_release_complete("v2026.08.14")
  expect_true(all(out$exists))
  expect_setequal(out$object, RELEASE_REQUIRED_OBJECTS)
})

test_that("check_release_complete(halt = FALSE) reports instead of stopping", {
  setup_gcloud(present = "metadata.json")
  out <- check_release_complete("v2026.08.14", halt = FALSE)
  expect_false(all(out$exists))
  expect_true(out$exists[out$object == "metadata.json"])
  expect_false(out$exists[out$object == "catalog.json"])
})

test_that("promote_release() refuses to move the pointer on an incomplete release", {
  log <- setup_gcloud(present = "parquet")
  expect_error(promote_release("v2026.08.14"), "is NOT complete")
  # and wrote nothing: the pointer must not move. This is the whole outage.
  expect_length(grep("^storage cp", calls(log), value = TRUE), 0)
})

test_that("promote_release() writes latest.txt with Cache-Control: no-cache", {
  log <- setup_gcloud(present = ALL_PRESENT)
  expect_message(promote_release("v2026.08.14"), "promoted v2026.08.14")
  cp <- grep("^storage cp", calls(log), value = TRUE)
  expect_length(cp, 1)
  # without this the edge caches the pointer for an hour and a rollback is invisible
  expect_match(cp, "--cache-control=no-cache,max-age=0", fixed = TRUE)
  expect_match(cp, "latest.txt")
})

test_that("read_promoted_release() reads through the API, not the cached URL", {
  log <- setup_gcloud()
  expect_equal(read_promoted_release(), "v2026.08.11")
  cl <- calls(log)
  expect_true(any(grepl("^storage cat", cl)))
  expect_false(any(grepl("storage.googleapis.com", cl)))  # never the CDN URL
})
