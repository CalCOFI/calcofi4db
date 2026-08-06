test_that("cc_stage_dir reads CALCOFI_STAGE_DIR and falls back outside the repo", {
  withr::with_envvar(c(CALCOFI_STAGE_DIR = "/tmp/somewhere/calcofi"), {
    expect_equal(cc_stage_dir(), "/tmp/somewhere/calcofi")
  })
  # unset and blank both fall back
  withr::with_envvar(c(CALCOFI_STAGE_DIR = ""), {
    expect_equal(cc_stage_dir(), path.expand("~/_big/calcofi"))
  })
  withr::with_envvar(c(CALCOFI_STAGE_DIR = "   "), {
    expect_equal(cc_stage_dir(), path.expand("~/_big/calcofi"))
  })
  # the fallback must never resolve inside a git working tree -- the whole point
  # is that 24 GB of parquet stops living in one
  expect_false(grepl("/Github/", cc_stage_dir(), fixed = TRUE))
})

test_that("cc_stage_path joins and only creates when asked", {
  tmp <- withr::local_tempdir()
  withr::local_envvar(c(CALCOFI_STAGE_DIR = tmp))

  p <- cc_stage_path("parquet", "calcofi_dic")
  expect_equal(p, file.path(tmp, "parquet", "calcofi_dic"))
  expect_false(dir.exists(p))

  p2 <- cc_stage_path("parquet", "calcofi_dic", create = TRUE)
  expect_true(dir.exists(p2))
})

test_that("write_parquet_outputs sends bytes to the stage and sidecars to the repo", {
  tmp_stage <- withr::local_tempdir()
  repo_dir  <- withr::local_tempdir()
  withr::local_envvar(c(CALCOFI_STAGE_DIR = tmp_stage))

  con <- get_duckdb_con(":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "widget", data.frame(id = 1:3, v = c("a", "b", "c"),
                                              stringsAsFactors = FALSE))

  out_dir <- file.path(repo_dir, "calcofi_test")
  stats <- write_parquet_outputs(con, output_dir = out_dir, tables = "widget")

  # sidecar in the repo, parquet in the stage, and NEITHER in the other's place
  expect_true(file.exists(file.path(out_dir, "manifest.json")))
  expect_false(file.exists(file.path(out_dir, "widget.parquet")))
  expect_true(file.exists(
    file.path(tmp_stage, "parquet", "calcofi_test", "widget.parquet")))

  # manifest records a RELATIVE path: it is committed, so an absolute one would
  # bake one machine's home directory into the repo
  m <- jsonlite::read_json(file.path(out_dir, "manifest.json"))
  expect_equal(unlist(m$files$path), "widget.parquet")
  expect_false(any(grepl("^/", unlist(m$files$path))))
  expect_equal(stats$rows, 3L)
})

test_that("write_parquet_outputs still colocates when parquet_dir is given", {
  # the split is a default, not a mandate -- an explicit parquet_dir restores
  # the old single-directory layout
  tmp <- withr::local_tempdir()
  con <- get_duckdb_con(":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "widget", data.frame(id = 1:2))

  write_parquet_outputs(con, output_dir = tmp, parquet_dir = tmp,
                        tables = "widget")
  expect_true(file.exists(file.path(tmp, "widget.parquet")))
  expect_true(file.exists(file.path(tmp, "manifest.json")))
})

test_that("the content-hash skip still reads the prior manifest from the repo", {
  # manifest.json is the dedup ledger and now lives apart from the bytes it
  # describes; if write_parquet_outputs looked for it beside the parquet it
  # would never find one and would re-upload everything, every run
  tmp_stage <- withr::local_tempdir()
  repo_dir  <- withr::local_tempdir()
  withr::local_envvar(c(CALCOFI_STAGE_DIR = tmp_stage))

  con <- get_duckdb_con(":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "widget", data.frame(id = 1:3))

  out_dir <- file.path(repo_dir, "calcofi_test")
  write_parquet_outputs(con, output_dir = out_dir, tables = "widget")
  h1 <- jsonlite::read_json(file.path(out_dir, "manifest.json"))$data_hash

  write_parquet_outputs(con, output_dir = out_dir, tables = "widget")
  h2 <- jsonlite::read_json(file.path(out_dir, "manifest.json"))$data_hash
  expect_equal(h1, h2)
  expect_false(is.null(h1$widget))
})
