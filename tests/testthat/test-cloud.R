test_that("re_escape() escapes regex metacharacters with PCRE, not TRE", {
  # REGRESSION. sync_to_gcs() builds one `--exclude ^<name>$` per sidecar so that
  # --delete-unmatched-destination-objects cannot delete the release's schema
  # record. The escape was written with R's default TRE engine, which reads the
  # `{}` inside the character class as an interval quantifier and rejects the
  # whole pattern — so EVERY ingest died at the upload step, after all its work
  # had succeeded, with "Invalid contents of {}".
  # suppressWarnings: R raises a TRE compilation WARNING alongside the error, and
  # letting it through would leave the suite permanently at 1 warning
  expect_error(
    suppressWarnings(
      gsub("([.\\\\+*?\\[\\]^$(){}|])", "\\\\\\1", "manifest.json")),
    "Invalid contents",
    info = "the TRE form must still fail, or this regression test is vacuous")

  expect_equal(re_escape("manifest.json"), "manifest\\.json")
  expect_equal(re_escape("metadata.json"), "metadata\\.json")

  # the metacharacters that broke it, and the rest of the class
  expect_equal(re_escape("a{1}b"), "a\\{1\\}b")
  expect_equal(re_escape("a[2]b"), "a\\[2\\]b")
  expect_equal(re_escape("a+b*c?d"), "a\\+b\\*c\\?d")
  expect_equal(re_escape("^a$"),    "\\^a\\$")
  expect_equal(re_escape("a(b)c"),  "a\\(b\\)c")
  expect_equal(re_escape("a|b"),    "a\\|b")

  # vectorised, and a name with nothing to escape is returned untouched
  expect_equal(re_escape(c("a.json", "plain")), c("a\\.json", "plain"))
  expect_equal(re_escape(character()), character())
})

test_that("re_escape() output actually anchors to the literal filename", {
  # the escaped form is used as `^<name>$`; it must match the sidecar and nothing
  # else — in particular `manifest.json` must not match `manifestXjson`
  pat <- paste0("^", re_escape("manifest.json"), "$")
  expect_true(grepl(pat, "manifest.json"))
  expect_false(grepl(pat, "manifestXjson"))
  expect_false(grepl(pat, "my-manifest.json"))
  expect_false(grepl(pat, "manifest.json.bak"))
})


# 4.6.1: an upload is a hash comparison first ---------------------------------------------

test_that("local_md5_base64() is the base64 MD5 GCS reports", {
  f <- withr::local_tempfile(fileext = ".txt"); writeLines("calcofi", f)
  expect_equal(local_md5_base64(f), jsonlite::base64_enc(digest::digest(f, algo = "md5", file = TRUE, raw = TRUE)))
  expect_match(local_md5_base64(f), "^[A-Za-z0-9+/]+=*$")
})

test_that("put_gcs_file() skips an object whose MD5 matches, uploads otherwise", {
  f <- withr::local_tempfile(fileext = ".txt"); writeLines("calcofi", f)
  uploads <- character()
  local_mocked_bindings(
    gcs_object_md5 = function(uri) local_md5_base64(f),
    gcloud_upload  = function(local_path, bucket, gcs_path) { uploads <<- c(uploads, gcs_path); invisible(NULL) })
  expect_message(put_gcs_file(f, "gs://b/x/y.txt"), "unchanged")
  expect_length(uploads, 0)
  local_mocked_bindings(gcs_object_md5 = function(uri) NA_character_)
  put_gcs_file(f, "gs://b/x/y.txt")
  expect_equal(uploads, "x/y.txt")
  local_mocked_bindings(gcs_object_md5 = function(uri) "different==")
  put_gcs_file(f, "gs://b/x/z.txt")
  expect_equal(uploads, c("x/y.txt", "x/z.txt"))
  put_gcs_file(f, "gs://b/x/w.txt", skip_unchanged = FALSE)
  expect_equal(length(uploads), 3)
})
