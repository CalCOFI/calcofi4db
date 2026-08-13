# The display trio — `dataset_name_short`, `category`, `color` — is authored in
# the ingest front-matter and carried into the release `dataset` table so the
# consumer apps stop keeping their own per-dataset maps keyed on dataset_key.
#
# Two halves to pin, and the second is the one that matters: the fields reach the
# release when present, AND they are optional. Before this existed, a dataset the
# apps had never heard of rendered as a grey card labelled with the raw key and a
# human had to notice; a required field would just move that failure earlier.

make_dir <- function(env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  qmd <- function(file, provider, dataset, extra = character()) {
    writeLines(c(
      "---",
      glue::glue("title: \"Ingest {provider} {dataset}\""),
      "calcofi:",
      glue::glue("  target_name: ingest_{provider}_{dataset}"),
      "  workflow_type: ingest",
      glue::glue("  output: data/parquet/{provider}_{dataset}/manifest.json"),
      glue::glue("  provider: {provider}"),
      glue::glue("  dataset: {dataset}"),
      "  dataset_meta:",
      glue::glue("    dataset_name: The Formal {dataset} Name"),
      extra,
      "---", "", "## Body"),
      file.path(dir, file))
  }
  qmd("ingest_aa_one.qmd", "aa", "one", c(
    "    dataset_name_short: Short One",
    "    category: Zooplankton",
    "    color: \"#f76707\""))
  qmd("ingest_bb_two.qmd", "bb", "two")   # declares none of the three
  dir
}

test_that("the display trio reaches the dataset registry when declared", {
  d <- ingest_yaml_to_dataset_df(read_ingest_yaml(make_dir()))
  r <- d[d$provider == "aa", ]
  expect_equal(r$dataset_name_short, "Short One")
  expect_equal(r$category, "Zooplankton")
  expect_equal(r$color, "#f76707")
  # the formal name is untouched — short is additive, not a replacement
  expect_equal(r$dataset_name, "The Formal one Name")
})

test_that("the display trio is optional and degrades to NA, not to an error", {
  d <- ingest_yaml_to_dataset_df(read_ingest_yaml(make_dir()))
  r <- d[d$provider == "bb", ]
  expect_true(is.na(r$dataset_name_short))
  expect_true(is.na(r$category))
  expect_true(is.na(r$color))
  # and the row still exists, carrying enough for a consumer to fall back on
  expect_equal(nrow(r), 1L)
  expect_equal(r$dataset_name, "The Formal two Name")
})

test_that("the columns are always present, so a consumer can select them blindly", {
  # a release whose every dataset omits the trio must still emit the columns —
  # otherwise build_datasets.sql's SELECT is a binder error rather than NULLs
  dir <- withr::local_tempdir()
  writeLines(c(
    "---", "title: \"x\"", "calcofi:", "  target_name: ingest_x_y",
    "  workflow_type: ingest", "  output: data/parquet/x_y/manifest.json",
    "  provider: x", "  dataset: y", "---", "", "## Body"),
    file.path(dir, "ingest_x_y.qmd"))
  d <- ingest_yaml_to_dataset_df(read_ingest_yaml(dir))
  expect_true(all(c("dataset_name_short", "category", "color") %in% names(d)))
})
