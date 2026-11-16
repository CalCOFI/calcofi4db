# tests for publish change detection (R/publish.R)
#
# What this guards is "never rebuild, never re-upload, what did not change" — and,
# more importantly, the converse: a change to ONE dataset's rows in a shared table
# must rebuild that dataset and only that dataset.

# a tiny content-addressed release: a shared `sample` holding two datasets, an `obs`
# hive-partitioned by dataset_key, and a vocabulary with no dataset_key column
.pub_fixture <- function(dir, b_value = 2) {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  w <- function(sql, path) {
    dir.create(dirname(file.path(dir, path)), recursive = TRUE, showWarnings = FALSE)
    DBI::dbExecute(con, glue::glue("COPY ({sql}) TO '{file.path(dir, path)}' (FORMAT PARQUET)"))
  }
  w(glue::glue("SELECT * FROM (VALUES ('a', 's1', 1.0), ('a', 's2', 1.5), ('b', 's3', {b_value}))
                t(dataset_key, sample_key, depth_m)"), "sample.parquet")
  w("SELECT * FROM (VALUES ('x', 10.0)) t(measurement_type, v)", "measurement_type.parquet")
  w("SELECT * FROM (VALUES ('s1', 5.0)) t(sample_key, value)", "obs_a.parquet")
  obj <- function(path, ch, ...) list(path = path, content_hash = ch, ...)
  list(tables = list(
    list(name = "sample", objects = list(obj("sample.parquet", paste0("samp-", b_value)))),
    list(name = "measurement_type", objects = list(obj("measurement_type.parquet", "mt-1"))),
    list(name = "obs", objects = list(
      obj("obs_a.parquet", "obs-a-1", partition_by = "dataset_key", partition_value = "a")))))
}

.pub_sigs <- function(dir, catalog, cache = NULL, owners = NULL) {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  publish_object_signatures(con, catalog, c("sample", "measurement_type", "obs"),
                            owners = owners, cache = cache, base_url = dir)
}

test_that("a partition object is its dataset's signature, and a vocabulary is everyone's", {
  dir <- withr::local_tempdir()
  s <- .pub_sigs(dir, .pub_fixture(dir))
  expect_equal(s$signature[s$table == "obs"], "obs-a-1")
  expect_equal(s$dataset_key[s$table == "obs"], "a")
  expect_equal(s$dataset_key[s$table == "measurement_type"], "*")
  expect_setequal(s$dataset_key[s$table == "sample"], c("a", "b"))
})

test_that("changing one dataset's rows in a shared table moves only that dataset", {
  dir1 <- withr::local_tempdir(); dir2 <- withr::local_tempdir()
  s1 <- .pub_sigs(dir1, .pub_fixture(dir1, b_value = 2))
  s2 <- .pub_sigs(dir2, .pub_fixture(dir2, b_value = 3))
  tbls <- c("sample", "measurement_type", "obs")
  expect_identical(publish_data_parts(s1, "a", tbls), publish_data_parts(s2, "a", tbls))
  b1 <- publish_data_parts(s1, "b", tbls); b2 <- publish_data_parts(s2, "b", tbls)
  expect_false(identical(b1[["data:sample"]], b2[["data:sample"]]))
  expect_identical(b1[["data:measurement_type"]], b2[["data:measurement_type"]])
})

test_that("a dataset with no rows in a table is <absent>, a table the release lacks <missing>", {
  dir <- withr::local_tempdir()
  s <- .pub_sigs(dir, .pub_fixture(dir))
  p <- publish_data_parts(s, "b", c("obs", "sample"))
  expect_equal(p[["data:obs"]], "<absent>")
  cat_ <- .pub_fixture(dir)
  con <- DBI::dbConnect(duckdb::duckdb()); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  s2 <- publish_object_signatures(con, cat_, "nope", base_url = dir)
  expect_equal(publish_data_parts(s2, "b", "nope")[["data:nope"]], "<missing>")
})

test_that("a cached object is never read again", {
  dir <- withr::local_tempdir()
  cache <- file.path(dir, "sig.csv")
  cat_ <- .pub_fixture(dir)
  s1 <- .pub_sigs(dir, cat_, cache = cache)
  expect_true(file.exists(cache))
  unlink(file.path(dir, "sample.parquet"))        # a read would now fail
  s2 <- .pub_sigs(dir, cat_, cache = cache)
  expect_identical(s1[s1$table == "sample", "signature"], s2[s2$table == "sample", "signature"])
})

test_that("a table only one dataset reads is signed by its content_hash, unread", {
  dir <- withr::local_tempdir()
  cat_ <- .pub_fixture(dir)
  unlink(file.path(dir, "sample.parquet"))
  s <- .pub_sigs(dir, cat_, owners = list(sample = "a"))
  expect_equal(s$signature[s$table == "sample"], "samp-2")
  expect_equal(s$dataset_key[s$table == "sample"], "*")
})

test_that("local tables sign per dataset, and a vocabulary only through the keys it reaches", {
  con <- DBI::dbConnect(duckdb::duckdb()); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "CREATE TABLE obs_bio AS SELECT * FROM (VALUES
    ('a', 't1', 1.0), ('b', 't2', 2.0)) t(dataset_key, taxon_key, value)")
  DBI::dbExecute(con, "CREATE TABLE taxon AS SELECT * FROM (VALUES
    ('t1', 'Engraulis'), ('t2', 'Sardinops')) t(taxon_key, scientific_name)")
  DBI::dbExecute(con, "CREATE TABLE cruise AS SELECT * FROM (VALUES ('c1', 'ship')) t(cruise_key, ship)")
  via <- list(taxon = c("obs_bio", "taxon_key"))
  tbls <- c("obs_bio", "taxon", "cruise", "not_here")
  s1 <- publish_table_signatures(con, tbls, via = via)
  expect_setequal(s1$dataset_key[s1$table == "taxon"], c("a", "b"))
  expect_equal(s1$dataset_key[s1$table == "cruise"], "*")          # no via: signed whole
  expect_false("not_here" %in% s1$table)
  expect_equal(publish_data_parts(s1, "a", "not_here")[["data:not_here"]], "<absent>")

  # renaming b's taxon moves b's taxon signature, never a's
  DBI::dbExecute(con, "UPDATE taxon SET scientific_name = 'Sardinops sagax' WHERE taxon_key = 't2'")
  s2 <- publish_table_signatures(con, tbls, via = via)
  expect_identical(publish_data_parts(s1, "a", tbls), publish_data_parts(s2, "a", tbls))
  expect_false(identical(publish_data_parts(s1, "b", "taxon"), publish_data_parts(s2, "b", "taxon")))
})

test_that("the record digest ignores key order and the volatile keys, not a value", {
  r1 <- list(dataset_key = "a", attribution = list(license = NULL, citation_main = "X"),
             objects = list(list(since = "v1")), status = list(updated = "t1"))
  r2 <- list(status = list(updated = "t2"), objects = list(list(since = "v2")),
             attribution = list(citation_main = "X", license = NULL), dataset_key = "a")
  expect_identical(publish_record_digest(r1), publish_record_digest(r2))
  r3 <- r1; r3$attribution$license <- "CC-BY-4.0"
  expect_false(identical(publish_record_digest(r1), publish_record_digest(r3)))
  expect_equal(publish_record_digest(NULL), "<missing>")
})

test_that("a notebook's prose and comments are not code; its chunks and options are", {
  dir <- withr::local_tempdir()
  f <- file.path(dir, "pub.qmd")
  qmd <- function(prose, comment, code, opt = "#| eval: true")
    writeLines(c("---", "title: x", "---", prose, "```{r}", opt, comment, code, "```", "tail"), f)
  qmd("Some prose.", "# a comment", "x <- 1"); a <- publish_code_parts(f)
  qmd("Other prose!", "# reworded", "x <- 1");  b <- publish_code_parts(f)
  expect_identical(a, b)
  qmd("Some prose.", "# a comment", "x <- 2"); expect_false(identical(a, publish_code_parts(f)))
  qmd("Some prose.", "# a comment", "x <- 1", "#| eval: false")
  expect_false(identical(a, publish_code_parts(f)))
  expect_equal(unname(publish_code_parts(file.path(dir, "gone.R"))), "<missing>")
  expect_named(a, "code:pub.qmd")
})

test_that("an unchanged fingerprint with outputs on disk is reused; anything else rebuilds", {
  dir <- withr::local_tempdir()
  out <- file.path(dir, "pkg.xml"); writeLines("<x/>", out)
  fp <- publish_fingerprint(data = c("data:sample" = "h1"), metadata = list(record = "r1"),
                            code = c("code:pub.qmd" = "c1"))
  prior <- list(hash = fp$hash, parts = as.list(fp$parts))
  expect_equal(publish_decide(fp, prior, out)$action, "reuse")
  expect_equal(publish_decide(fp, NULL, out)$action, "build")

  fp2 <- publish_fingerprint(data = c("data:sample" = "h2"), metadata = list(record = "r1"),
                             code = c("code:pub.qmd" = "c1"))
  d <- publish_decide(fp2, prior, out)
  expect_equal(d$action, "build")
  expect_equal(d$changed, "data:sample")

  unlink(out)
  d <- publish_decide(fp, prior, out)
  expect_equal(d$action, "build")
  expect_match(d$reason, "outputs missing: pkg.xml")
})

test_that("the upload status compares built bytes with deposited bytes", {
  s <- publish_upload_status(content_hash     = c(NA, "h1", "h1", "h2"),
                             uploaded_hash    = c(NA, NA,   "h1", "h1"),
                             uploaded_version = c(NA, NA,   "v1", "v1"))
  expect_equal(s$upload_status, c("not built", "never uploaded", "current", "changed since v1"))
  expect_equal(s$needs_upload, c(FALSE, TRUE, FALSE, TRUE))
})

# content-addressed published objects: the release tables' scheme (R/release.R) ----------

test_that("content_hash is data + metadata + format, never code; data_hash is data alone", {
  base <- publish_fingerprint(data = c("data:obs" = "a", "data:sample" = "b"), metadata = list(record = "r"),
                              code = c("code:x.qmd" = "c1"), format = "1")
  expect_match(base$content_hash, "^[0-9a-f]{32}$"); expect_match(base$data_hash, "^[0-9a-f]{32}$")
  code2 <- publish_fingerprint(data = c("data:obs" = "a", "data:sample" = "b"), metadata = list(record = "r"),
                               code = c("code:x.qmd" = "c2"), format = "1")
  expect_identical(code2$content_hash, base$content_hash)      # a code edit is not a new object...
  expect_false(identical(code2$hash, base$hash))                # ...but the fingerprint still says it moved
  fmt2 <- publish_fingerprint(data = c("data:obs" = "a", "data:sample" = "b"), metadata = list(record = "r"),
                              code = c("code:x.qmd" = "c1"), format = "2")
  expect_false(identical(fmt2$content_hash, base$content_hash)) # a format bump is
  expect_identical(fmt2$data_hash, base$data_hash)
  meta2 <- publish_fingerprint(data = c("data:obs" = "a", "data:sample" = "b"), metadata = list(record = "r2"),
                               code = c("code:x.qmd" = "c1"), format = "1")
  expect_false(identical(meta2$content_hash, base$content_hash)); expect_identical(meta2$data_hash, base$data_hash)
  dat2 <- publish_fingerprint(data = c("data:obs" = "A", "data:sample" = "b"), metadata = list(record = "r"),
                              code = c("code:x.qmd" = "c1"), format = "1")
  expect_false(identical(dat2$data_hash, base$data_hash))
  # part order does not matter
  rev <- publish_fingerprint(data = c("data:sample" = "b", "data:obs" = "a"), metadata = list(record = "r"),
                             code = c("code:x.qmd" = "c1"), format = "1")
  expect_identical(rev$content_hash, base$content_hash)
})

test_that("publish_decide() reuses on an unchanged content_hash even when only code moved", {
  dir <- withr::local_tempdir(); out <- file.path(dir, "o.zip"); writeLines("x", out)
  fp1 <- publish_fingerprint(data = c("data:obs" = "a"), code = c("code:x.qmd" = "c1"), format = "1")
  fp2 <- publish_fingerprint(data = c("data:obs" = "a"), code = c("code:x.qmd" = "c2"), format = "1")
  prior <- list(hash = fp1$hash, parts = as.list(fp1$parts), content_hash = fp1$content_hash)
  d <- publish_decide(fp2, prior, out)
  expect_equal(d$action, "reuse"); expect_match(d$reason, "only code changed: code:x.qmd")
  fp3 <- publish_fingerprint(data = c("data:obs" = "b"), code = c("code:x.qmd" = "c1"), format = "1")
  expect_equal(publish_decide(fp3, prior, out)$action, "build")
})

test_that("publish_object_path() mirrors canonical_path(): identity a directory, file name readable", {
  h <- "0123456789abcdef0123456789abcdef"
  expect_equal(publish_object_path("zip", "calcofi_dic", h, "calcofi_dic_v2026.11.14_csv.zip"),
               "publish/zip/calcofi_dic/0123456789abcdef01234567/calcofi_dic_v2026.11.14_csv.zip")
  expect_equal(publish_object_path("netcdf", "calcofi_dic", h, "calcofi_dic.nc", prefix = "netcdf")[1],
               "netcdf/netcdf/calcofi_dic/0123456789abcdef01234567/calcofi_dic.nc")
  expect_equal(publish_object_path("netcdf", "calcofi_dic", h, "calcofi_dic.nc", prefix = ""),
               "netcdf/calcofi_dic/0123456789abcdef01234567/calcofi_dic.nc")
  expect_equal(nchar(basename(publish_object_path("edi", "k", h))), 24)
  # the 24 characters canonical_path() uses
  expect_equal(basename(dirname(canonical_path("t", h))), basename(publish_object_path("edi", "k", h)))
  expect_error(publish_object_path("zip", "k", "short"))
})

test_that("publish_objects_plan(): an unchanged object exists (since and bytes inherited), a changed one uploads", {
  prev <- data.frame(dataset_key = c("a", "a", "b"), format = c("csv", "xlsx", "csv"),
                     content_hash = c("h1", "h1", "h9"), since = c("v2026.09.11", "v2026.09.11", "v2026.09.11"),
                     path = c("p/a/h1/a_csv.zip", "p/a/h1/a_xlsx.zip", "p/b/h9/b_csv.zip"),
                     bytes = c(10, 20, 30), sha256 = c("s1", "s2", "s3"), stringsAsFactors = FALSE)
  now <- data.frame(dataset_key = c("a", "a", "b"), format = c("csv", "xlsx", "csv"),
                    content_hash = c("h1", "h1", "h2"), path = c("p/a/h1/a_csv.zip", "p/a/h1/a_xlsx.zip", "p/b/h2/b_csv.zip"),
                    bytes = c(11, 21, 31), sha256 = c("x1", "x2", "x3"), stringsAsFactors = FALSE)
  p <- publish_objects_plan(now, prev, "v2026.11.14")
  expect_equal(p$action, c("exists", "exists", "upload"))
  expect_equal(p$since, c("v2026.09.11", "v2026.09.11", "v2026.11.14"))
  expect_equal(p$bytes, c(10, 20, 31)); expect_equal(p$sha256, c("s1", "s2", "x3"))   # the object that is there
  # the same content under another format or dataset is not the same object
  expect_equal(publish_objects_plan(transform(now, format = "parquet"), prev, "v2")$action, rep("upload", 3))
  # no previous manifest: everything is new, since this release
  p0 <- publish_objects_plan(now, NULL, "v2026.11.14")
  expect_equal(p0$action, rep("upload", 3)); expect_equal(unique(p0$since), "v2026.11.14")
  expect_error(publish_objects_plan(transform(now, path = "same"), NULL, "v"), "share one path")
})

test_that("publish_record_digest() ignores the source_accessed date every release re-measures", {
  r1 <- list(dataset_key = "k", attribution = list(license = "CC0-1.0", source_accessed = "2026-09-11"))
  r2 <- list(dataset_key = "k", attribution = list(license = "CC0-1.0", source_accessed = "2026-11-14"))
  expect_identical(publish_record_digest(r1), publish_record_digest(r2))
  r3 <- list(dataset_key = "k", attribution = list(license = "CC-BY-4.0", source_accessed = "2026-11-14"))
  expect_false(identical(publish_record_digest(r1), publish_record_digest(r3)))
})
