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
