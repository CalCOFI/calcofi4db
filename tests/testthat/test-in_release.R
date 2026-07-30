# `calcofi.in_release: false` lets an in-progress ingest write its own parquet
# outputs without them entering the frozen release. These pin the two halves of
# that contract: the flag is opt-OUT (absence means "in the release", so no
# existing notebook changes behavior), and every release-side discovery step
# filters on it.

# a workflows dir holding three ingest notebooks: two in the release, one out
make_workflow_dir <- function(env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  qmd <- function(file, provider, dataset, in_release = NULL) {
    writeLines(c(
      "---",
      glue::glue("title: \"Ingest {provider} {dataset}\""),
      "calcofi:",
      glue::glue("  target_name: ingest_{provider}_{dataset}"),
      "  workflow_type: ingest",
      glue::glue("  output: data/parquet/{provider}_{dataset}/manifest.json"),
      glue::glue("  provider: {provider}"),
      glue::glue("  dataset: {dataset}"),
      if (!is.null(in_release)) glue::glue("  in_release: {in_release}"),
      "---", "", "## Body"),
      file.path(dir, file))
  }
  qmd("ingest_aa_one.qmd",   "aa", "one")                       # no key -> in
  qmd("ingest_bb_two.qmd",   "bb", "two",   in_release = "true")
  qmd("ingest_cc_three.qmd", "cc", "three", in_release = "false")
  dir
}

test_that("in_release is opt-out: absent key means the ingest is in the release", {
  dir <- make_workflow_dir()
  wf  <- parse_qmd_frontmatter(dir)

  expect_true(all(c("in_release") %in% names(wf)))
  expect_true(wf$in_release[wf$target_name == "ingest_aa_one"])
  expect_true(wf$in_release[wf$target_name == "ingest_bb_two"])
  expect_false(wf$in_release[wf$target_name == "ingest_cc_three"])
})

test_that("read_ingest_yaml() returns every ingest unless in_release_only", {
  dir <- make_workflow_dir()

  expect_setequal(names(read_ingest_yaml(dir)), c("aa_one", "bb_two", "cc_three"))
  expect_setequal(
    names(read_ingest_yaml(dir, in_release_only = TRUE)), c("aa_one", "bb_two"))
})

test_that("release_excluded_datasets() names only the flagged-out datasets", {
  dir <- make_workflow_dir()
  expect_equal(release_excluded_datasets(dir), "cc_three")

  # a dir where nothing is flagged out yields an empty vector, not NULL
  bare <- withr::local_tempdir()
  writeLines(c("---", "title: x", "calcofi:", "  provider: aa", "  dataset: one",
               "  workflow_type: ingest", "  output: data/parquet/aa_one/manifest.json",
               "---"),
             file.path(bare, "ingest_aa_one.qmd"))
  expect_identical(release_excluded_datasets(bare), character())
})

test_that("build_release_table_registry() omits flagged-out ingests", {
  dir <- make_workflow_dir()
  # a manifest per ingest, each owning one distinctly-named table
  for (ds in c("aa_one", "bb_two", "cc_three")) {
    pq <- file.path(dir, "data/parquet", ds)
    dir.create(pq, recursive = TRUE)
    jsonlite::write_json(
      list(tables = paste0(ds, "_tbl"), files = list(rows = 1)),
      file.path(pq, "manifest.json"), auto_unbox = TRUE)
  }

  reg <- build_release_table_registry(dir)
  expect_setequal(reg$table, c("aa_one_tbl", "bb_two_tbl"))
  expect_false("cc_three_tbl" %in% reg$table)
})

test_that("core shard discovery skips flagged-out datasets", {
  dir <- make_workflow_dir()
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con), add = TRUE)

  for (ds in c("aa_one", "cc_three")) {
    pq <- file.path(dir, "data/parquet", ds)
    dir.create(pq, recursive = TRUE)
    DBI::dbExecute(con, glue::glue(
      "COPY (SELECT 1 obs_id, '{ds}' dataset_key, '{ds}:tow:1' sample_key,
                    'g1' grid_key, 'abundance' measurement_type,
                    1.0 measurement_value, NULL::DOUBLE depth_min_m)
       TO '{file.path(pq, 'obs.parquet')}' (FORMAT PARQUET)"))
  }

  # default `exclude` reads the flag straight from the notebooks under `root`
  paths <- core_shard_paths("obs", root = dir)
  expect_length(paths, 1)
  expect_true(grepl("aa_one", paths))

  n <- assemble_core_table(con, "obs", root = dir, id_col = "obs_id",
                           order_by = "dataset_key")
  expect_equal(n, 1)
  expect_equal(
    DBI::dbGetQuery(con, "SELECT DISTINCT dataset_key FROM obs")$dataset_key,
    "aa_one")

  # an explicit empty `exclude` opts back in, so the flag is the only gate
  expect_length(core_shard_paths("obs", root = dir, exclude = character()), 2)
})

test_that("a flagged-out ingest is not an [auto] dependency of the release", {
  dir <- make_workflow_dir()
  writeLines(c(
    "---", "title: Release", "calcofi:", "  target_name: release_database",
    "  workflow_type: release", "  dependency:", "    - auto",
    "  output: data/releases", "---"),
    file.path(dir, "release_database.qmd"))

  tl  <- build_targets_list(dir, verbose = FALSE)
  rel <- Filter(function(t) t$settings$name == "release_database", tl)[[1]]
  body_txt <- paste(deparse(rel$command$expr), collapse = " ")

  # the release depends on the two in-release ingests, not the flagged-out one
  expect_match(body_txt, "ingest_aa_one")
  expect_match(body_txt, "ingest_bb_two")
  expect_false(grepl("ingest_cc_three", body_txt))

  # ...but the flagged-out ingest still has its own target
  expect_true("ingest_cc_three" %in% vapply(tl, function(t) t$settings$name, ""))
})
