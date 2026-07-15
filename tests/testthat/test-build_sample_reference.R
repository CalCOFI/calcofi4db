test_that("build_sample_reference materializes per-level sample counts", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  build_sample_reference(con)
  n <- function(type) DBI::dbGetQuery(
    con, glue::glue("SELECT COUNT(*) n FROM sample WHERE sample_type = '{type}'"))$n

  expect_equal(n("net"), 3L)
  expect_equal(n("tow"), 1L)
  expect_equal(n("site"), 1L)
})

test_that("sample_key namespacing reconstructs leaf -> parent -> root with no recursion", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  build_sample_reference(con)
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

test_that("build_sample_reference errors on duplicate sample_key", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))
  # duplicate a net_uuid so the namespaced sample_key collides
  DBI::dbExecute(con, "INSERT INTO net VALUES ('N1','T1',100,5,0.5,1,2)")
  expect_error(build_sample_reference(con), "duplicate sample_key")
})
