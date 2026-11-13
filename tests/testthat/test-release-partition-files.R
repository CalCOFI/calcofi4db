# one parquet object per partition value (calcofi4db 4.38.0). DuckDB 2.0.0 opens a new file for a
# partition once more than `partitioned_write_max_open_files` (default 100) are open, so a single
# COPY ... PARTITION_BY of obs_ctd_full (143 cruises) wrote data_0/data_1/data_2.parquet for 24
# cruises; release_objects() gave every file the partition's one release path and freeze_plan() one
# canonical path, the uploads overwrote each other and v2026.11.05 + v2026.11.09 published 22
# cruises short. The fixture reproduces it small: 10 partitions x 5,000 rows with the open-files
# limit at 2 and a flush threshold of 1,000 rows gives 22 files on the old code.

pf_fixture <- function(con, n_part = 10, n_per = 5000) {
  d <- data.frame(
    obs_id      = seq_len(n_part * n_per),
    cruise_key  = rep(sprintf("2015-%02d-32OC", seq_len(n_part)), each = n_per),
    depth_m     = rep(seq_len(n_per), n_part),
    value       = round(sin(seq_len(n_part * n_per)), 6))
  DBI::dbWriteTable(con, "obs_ctd_full", d[sample(nrow(d)), ], overwrite = TRUE)
  invisible(d)
}

test_that("export_release_parquet() writes one file per partition above the open-files limit", {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))
  set.seed(7); pf_fixture(con)
  DBI::dbExecute(con, "SET partitioned_write_max_open_files = 2")
  DBI::dbExecute(con, "SET partitioned_write_flush_threshold = 1000")
  d <- withr::local_tempdir()
  files <- export_release_parquet(con, "obs_ctd_full", file.path(d, "obs_ctd_full"),
                                  c("cruise_key", "depth_m", "obs_id"), partition_by = "cruise_key")
  on_disk <- list.files(file.path(d, "obs_ctd_full"), "[.]parquet$", recursive = TRUE)
  expect_length(on_disk, 10)
  expect_equal(nrow(files), 10)
  expect_equal(length(unique(dirname(files$rel_path))), 10)
  # every row is in the export, once
  n <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(*) AS n, COUNT(DISTINCT obs_id) AS u FROM read_parquet('%s/obs_ctd_full/*/*.parquet')", d))
  expect_equal(n$n, 50000); expect_equal(n$u, 50000)
  # the caller's settings come back
  s <- DBI::dbGetQuery(con, "SELECT current_setting('partitioned_write_max_open_files') AS v")$v
  expect_equal(as.integer(s), 2L)
  # release_objects() then yields one object, and one path, per partition
  o <- release_objects(con, "obs_ctd_full", d, files, "v2026.11.13", partition_by = "cruise_key")
  expect_equal(nrow(o), 10); expect_false(anyDuplicated(o$rel_path) > 0)
  p <- freeze_plan(o, NULL, "v2026.11.13", layout = "canonical")
  expect_false(anyDuplicated(p$path) > 0)
})

test_that(".check_one_file_per_partition() names every partition with more than one file", {
  rel <- c("t/cruise_key=A/data_0.parquet", "t/cruise_key=B/data_0.parquet",
           "t/cruise_key=B/data_1.parquet", "t/cruise_key=C%2FX/data_0.parquet",
           "t/cruise_key=C%2FX/data_1.parquet", "t/cruise_key=C%2FX/data_2.parquet")
  expect_error(calcofi4db:::.check_one_file_per_partition(rel, "t", "cruise_key"),
               "2 partition\\(s\\) of `t`.*B \\(2 files\\).*C/X \\(3 files\\)")
  expect_silent(calcofi4db:::.check_one_file_per_partition(rel[c(1, 2, 4)], "t", "cruise_key"))
})

test_that("release_objects() stops when two files map to one partition value", {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))
  set.seed(8); pf_fixture(con, n_part = 3, n_per = 100)
  d <- withr::local_tempdir()
  files <- export_release_parquet(con, "obs_ctd_full", file.path(d, "obs_ctd_full"),
                                  c("cruise_key", "depth_m", "obs_id"), partition_by = "cruise_key")
  # a second file beside a partition's first, as DuckDB 2.0.0 wrote them
  extra <- sub("data_0[.]parquet$", "data_1.parquet", files$rel_path[2])
  file.copy(file.path(d, files$rel_path[2]), file.path(d, extra))
  files2 <- rbind(files, tibble::tibble(rel_path = extra, bytes = files$bytes[2]))
  expect_error(release_objects(con, "obs_ctd_full", d, files2, "v2026.11.13", partition_by = "cruise_key"),
               "more than one file.*2015-02-32OC")
})

test_that("freeze_plan() stops on a duplicated path", {
  obj <- tibble::tibble(
    table = "t", partition_by = "k", partition_value = c("A", "B", "B"),
    rel_path = c("t/k=A/data_0.parquet", "t/k=B/data_0.parquet", "t/k=B/data_0.parquet"),
    local_path = c("a", "b0", "b1"), bytes = c(10, 20, 5), content_hash = c("h1", "h2", "h2"),
    sha256 = c("s1", "s2", "s3"), since = "v1")
  expect_error(freeze_plan(obj, NULL, "v1", layout = "canonical"), "duplicated path.*k=B")
  expect_error(freeze_plan(obj, NULL, "v1", layout = "compat"), "duplicated path.*k=B")
  expect_no_error(freeze_plan(obj[1:2, ], NULL, "v1", layout = "canonical"))
})
