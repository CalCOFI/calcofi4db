# the `sample` adjacency list: one append per event level, each row keyed
# `dataset_key:sample_type:id`. These pin the generic machinery — the arms
# themselves are fixtures (see helper-fixtures.R), because each dataset's
# projection is owned by its ingest notebook.

test_that("appending one arm per event level materializes per-level counts", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  build_ich_sample(con)
  n <- function(type) DBI::dbGetQuery(
    con, glue::glue("SELECT COUNT(*) n FROM sample WHERE sample_type = '{type}'"))$n

  expect_equal(n("net"), 3L)
  expect_equal(n("tow"), 1L)
  expect_equal(n("site"), 1L)
})

test_that("tow_type rides onto tow + net rows and stays NULL on site", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  build_ich_sample(con)
  tt <- function(key) DBI::dbGetQuery(con, glue::glue(
    "SELECT tow_type FROM sample WHERE sample_key = '{key}'"))$tow_type

  # net gear (fixture tow is a 'CB' bongo) propagates to the tow + all its nets
  expect_equal(tt("swfsc_ichthyo:tow:T1"), "CB")
  expect_equal(tt("swfsc_ichthyo:net:N1"), "CB")
  expect_equal(tt("swfsc_ichthyo:net:N2"), "CB")
  # site has no gear
  expect_true(is.na(tt("swfsc_ichthyo:site:S1")))
})

test_that("sample_key namespacing reconstructs leaf -> parent -> root with no recursion", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  build_ich_sample(con)
  r <- DBI::dbGetQuery(con,
    "SELECT parent_sample_key, root_sample_key
       FROM sample WHERE sample_key = 'swfsc_ichthyo:net:N1'")

  expect_equal(r$parent_sample_key, "swfsc_ichthyo:tow:T1")
  expect_equal(r$root_sample_key,   "swfsc_ichthyo:site:S1")

  # two self-joins recover the whole chain (depth <= 3, no recursive CTE)
  chain <- DBI::dbGetQuery(con,
    "SELECT n.sample_key net_key, t.sample_key tow_key, s.sample_key site_key
       FROM sample n JOIN sample t ON n.parent_sample_key = t.sample_key
                     JOIN sample s ON t.parent_sample_key = s.sample_key
      WHERE n.sample_key = 'swfsc_ichthyo:net:N1'")
  expect_equal(chain$tow_key,  "swfsc_ichthyo:tow:T1")
  expect_equal(chain$site_key, "swfsc_ichthyo:site:S1")
})

test_that("a duplicated source id yields a duplicate sample_key the notebook must catch", {
  # `append_sample()` does not police uniqueness — it is an INSERT. The guard now
  # lives in each notebook's projection assertions ("sample_key must be globally
  # unique"), so this pins that a collision is DETECTABLE the way those queries
  # look for it, rather than silently invisible.
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))
  # duplicate a net_uuid so the namespaced sample_key collides
  DBI::dbExecute(con, "INSERT INTO net VALUES ('N1','T1',100,5,0.5,1,2)")
  build_ich_sample(con)

  dup <- DBI::dbGetQuery(con,
    "SELECT sample_key, COUNT(*) n FROM sample GROUP BY 1 HAVING COUNT(*) > 1")
  expect_equal(nrow(dup), 1L)
  expect_equal(dup$sample_key, "swfsc_ichthyo:net:N1")
  expect_equal(dup$n, 2L)
})

test_that("sample_arm_self qualifies caller-supplied column expressions", {
  # DuckDB resolves an unqualified `site_key AS site_key` against the alias being
  # defined in the same SELECT (lateral column alias) and errors rather than
  # reading the column, so bare identifiers must come back table-qualified.
  sql <- sample_arm_self("cce-lter_zoodb", "zoodb_sample", "sample_id", "tow",
                         site_expr = "site_key", depth_min = "min_depth_m",
                         depth_max = "max_depth_m")
  expect_match(sql, "_src\\.site_key AS site_key")
  expect_match(sql, "_src\\.min_depth_m AS depth_min_m")
  expect_match(sql, "FROM zoodb_sample AS _src")
  # an expression (not a bare identifier) is passed through untouched
  expect_match(sample_arm_self("d", "t", "i", "tow", ord_expr = "CAST(order_occ AS INTEGER)"),
               "CAST\\(order_occ AS INTEGER\\) AS order_occ", fixed = FALSE)
})
