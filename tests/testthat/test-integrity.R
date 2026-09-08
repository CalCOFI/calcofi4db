ri_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  con
}

ri_fixture <- function(con) {
  DBI::dbWriteTable(con, "cruise", data.frame(cruise_key = c("2000-01-XX", "2000-02-XX"), stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key  = c("a:cast:1", "a:cast:2", "b:tow:1"),
    cruise_key  = c("2000-01-XX", "2000-02-XX", NA),          # NA: a nullable edge, not an orphan
    stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "obs", data.frame(
    obs_id     = 1:4,
    sample_key = c("a:cast:1", "a:cast:1", "a:cast:2", "b:tow:1"),
    taxon_key  = c("worms:1", NA, "worms:1", NA),
    stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "taxon", data.frame(taxon_key = "worms:1", stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "sample_spatial", data.frame(          # a composite key
    root_sample_key = c("a:cast:1", "a:cast:1", "a:cast:2"),
    spatial_key     = c("mpa:1", "mpa:2", "mpa:1"),
    stringsAsFactors = FALSE))
}

ri_rels <- function() list(
  primary_keys = list(cruise = "cruise_key", sample = "sample_key", obs = "obs_id",
                      taxon = "taxon_key", sample_spatial = list("root_sample_key", "spatial_key"),
                      ghost = "ghost_id"),
  foreign_keys = list(
    list(table = "sample", column = "cruise_key",  ref_table = "cruise", ref_column = "cruise_key"),
    list(table = "obs",    column = "sample_key",  ref_table = "sample", ref_column = "sample_key"),
    list(table = "obs",    column = "taxon_key",   ref_table = "taxon",  ref_column = "taxon_key"),
    list(table = "casts",  column = "cruise_key",  ref_table = "cruise", ref_column = "cruise_key")))  # not released

test_that("check_release_relationships() measures every declared key and skips the unreleased", {
  con <- ri_con(); ri_fixture(con)
  res <- check_release_relationships(con, ri_rels(), halt = TRUE)
  expect_true(res$ok)
  pk <- res$primary_keys
  expect_equal(pk$status[pk$table == "ghost"], "skipped")
  expect_equal(pk$n_rows[pk$table == "obs"], 4)
  expect_equal(pk$n_distinct[pk$table == "sample_spatial"], 3)   # composite key, counted as a tuple
  expect_true(all(pk$n_dup[pk$status == "ok"] == 0))
  fk <- res$foreign_keys
  expect_equal(fk$status[fk$table == "casts"], "skipped")
  expect_equal(fk$n_null[fk$table == "sample" & fk$column == "cruise_key"], 1)   # NULL is permitted
  expect_equal(fk$n_orphan[fk$table == "obs" & fk$column == "taxon_key"], 0)
  expect_equal(res$n_skipped, 2)
})

test_that("a duplicate or NULL primary key fails the release, naming the table", {
  con <- ri_con(); ri_fixture(con)
  DBI::dbExecute(con, "INSERT INTO obs VALUES (4, 'a:cast:1', NULL)")
  expect_error(check_release_relationships(con, ri_rels()), "obs\\(obs_id\\): 1 duplicate")
  res <- check_release_relationships(con, ri_rels(), halt = FALSE)
  expect_false(res$ok)
  DBI::dbExecute(con, "DELETE FROM obs WHERE taxon_key IS NULL AND obs_id = 4")
  DBI::dbExecute(con, "INSERT INTO cruise VALUES (NULL)")
  expect_error(check_release_relationships(con, ri_rels()), "cruise\\(cruise_key\\): 0 duplicate, 1 NULL")
})

test_that("a non-NULL foreign key with no match is an orphan and fails; a NULL one is not", {
  con <- ri_con(); ri_fixture(con)
  DBI::dbExecute(con, "INSERT INTO obs VALUES (5, 'nowhere:cast:1', NULL)")
  expect_error(check_release_relationships(con, ri_rels()), "obs.sample_key -> sample.sample_key: 1 orphan")
  res <- check_release_relationships(con, ri_rels(), halt = FALSE)
  fk  <- res$foreign_keys
  expect_equal(fk$n_orphan[fk$table == "obs" & fk$column == "sample_key"], 1)
  expect_equal(fk$status[fk$table == "obs" & fk$column == "taxon_key"], "ok")
})

test_that("integrity.json is written, deterministic, and carries the version and the counts", {
  con <- ri_con(); ri_fixture(con)
  p <- withr::local_tempfile(fileext = ".json")
  check_release_relationships(con, ri_rels(), path = p, version = "v2026.09.99")
  doc <- jsonlite::fromJSON(p)
  expect_equal(doc$version, "v2026.09.99")
  expect_true(doc$ok)
  expect_equal(doc$n_primary_keys, 5)
  expect_equal(doc$n_foreign_keys, 3)
  expect_equal(doc$n_skipped, 2)
  expect_setequal(names(doc$foreign_keys), c("table", "column", "ref_table", "ref_column",
                                            "n_rows", "n_null", "n_orphan", "status"))
  h1 <- digest::digest(file = p)
  check_release_relationships(con, ri_rels(), path = p, version = "v2026.09.99")
  expect_equal(digest::digest(file = p), h1)
})

test_that("check_release_relationships() reads a relationships.json path", {
  con <- ri_con(); ri_fixture(con)
  p <- withr::local_tempfile(fileext = ".json")
  jsonlite::write_json(ri_rels(), p, auto_unbox = TRUE)
  res <- check_release_relationships(con, p, halt = FALSE)
  expect_equal(nrow(res$foreign_keys), 4)
})
