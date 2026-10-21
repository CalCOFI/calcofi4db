md5_a <- strrep("a", 32)
md5_b <- strrep("b", 32)

test_that("source_archive_path() keys a source by content under provider/dataset", {
  expect_equal(
    source_archive_path(md5_a, "/x/y/20-0501JD_CTDFinalQC.zip", "calcofi", "ctd-cast"),
    paste0("archive/sources/calcofi/ctd-cast/", md5_a, "/20-0501JD_CTDFinalQC.zip"))
  expect_error(source_archive_path("not-a-hash", "f.zip", "calcofi", "ctd-cast"))
})

test_that("plan_source_archive(): present, copy from the mirror, else upload", {
  local <- tibble::tibble(
    file    = c("/d/a.zip", "/d/b.zip", "/d/c.zip"),
    md5     = c(md5_a, md5_b, md5_a),
    archive = source_archive_path(c(md5_a, md5_b, md5_a), c("a.zip", "b.zip", "c.zip"),
                                  "calcofi", "ctd-cast"))
  mirror <- tibble::tibble(name = c("_sync/x/b.zip", "_sync/x/c.zip"),
                           md5  = c(md5_b, md5_b))  # c.zip in the mirror has other bytes
  p <- plan_source_archive(local, archived = local$archive[1], mirror = mirror)
  expect_equal(p$action, c("present", "copy", "upload"))
  expect_equal(p$copy_from, c(NA, "_sync/x/b.zip", NA))
})

test_that("a re-published file with the same name is archived beside the old one", {
  old <- source_archive_path(md5_a, "20-2607SH_CTDPrelim.zip", "calcofi", "ctd-cast")
  new <- source_archive_path(md5_b, "20-2607SH_CTDPrelim.zip", "calcofi", "ctd-cast")
  p <- plan_source_archive(
    tibble::tibble(file = "20-2607SH_CTDPrelim.zip", md5 = md5_b, archive = new), archived = old)
  expect_equal(p$action, "upload")
  expect_false(identical(old, new))
})

test_that("md5_base64_to_hex() matches tools::md5sum for the same bytes", {
  f <- withr::local_tempfile(lines = "calcofi")
  expect_equal(calcofi4db:::md5_base64_to_hex(local_md5_base64(f)), unname(tools::md5sum(f)))
  expect_true(is.na(calcofi4db:::md5_base64_to_hex(NA_character_)))
})

test_that("sources_block() carries md5, url and archive when present", {
  x <- tibble::tibble(source = "a.zip", method = "file_mtime",
                      accessed = as.POSIXct("2026-10-21 08:00:00", tz = "UTC"), bytes = 10,
                      md5 = md5_a, url = NA_character_, archive = "gs://b/archive/sources/a.zip")
  b <- sources_block(x)[[1]]
  expect_equal(b$md5, md5_a)
  expect_equal(b$archive, "gs://b/archive/sources/a.zip")
  expect_null(b$url)
  # a plain stamp_source_access() table is unchanged
  expect_named(sources_block(x[, 1:4])[[1]], c("source", "method", "accessed", "bytes"))
})
