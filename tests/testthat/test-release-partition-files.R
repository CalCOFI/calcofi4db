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

test_that("check_release_objects() flags repeated paths, extra objects and short published rows", {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))
  set.seed(9); pf_fixture(con, n_part = 3, n_per = 200)
  d <- withr::local_tempdir()
  files <- export_release_parquet(con, "obs_ctd_full", file.path(d, "obs_ctd_full"),
                                  c("cruise_key", "depth_m", "obs_id"), partition_by = "cruise_key")
  o <- release_objects(con, "obs_ctd_full", d, files, "v1", partition_by = "cruise_key")
  obj <- lapply(seq_len(nrow(o)), function(i) list(
    path = o$rel_path[i], bytes = o$bytes[i], sha256 = o$sha256[i], content_hash = o$content_hash[i],
    partition_by = "cruise_key", partition_value = o$partition_value[i]))
  whole <- list(version = "v1", tables = list(
    list(name = "obs_ctd_full", rows = 600, partitioned = TRUE, supplemental = TRUE, objects = obj)))
  r <- check_release_objects(whole, con, base_url = d)
  expect_equal(nrow(r), 3); expect_true(all(r$ok))
  expect_equal(r$n[r$check == "published_rows"], 600)
  # as v2026.11.09 published it: a partition listed twice and its object short
  short <- file.path(d, "short"); dir.create(short)
  DBI::dbExecute(con, sprintf(
    "COPY (SELECT * FROM read_parquet('%s') LIMIT 7) TO '%s/data_0.parquet' (FORMAT PARQUET)",
    file.path(d, o$rel_path[2]), short))
  bad_obj <- obj; bad_obj[[2]]$path <- "short/data_0.parquet"
  bad_obj[[4]] <- bad_obj[[2]]
  bad <- whole; bad$tables[[1]]$objects <- bad_obj
  r2 <- check_release_objects(bad, con, base_url = d)
  expect_false(any(r2$ok))
  expect_match(r2$detail[r2$check == "unique_paths"], "1 path\\(s\\) listed more than once: short/data_0.parquet")
  expect_match(r2$detail[r2$check == "one_object_per_partition"], "4 object\\(s\\) for 3 partition.*2015-02-32OC")
  expect_equal(r2$n[r2$check == "published_rows"], 407)
  expect_match(r2$detail[r2$check == "published_rows"], "published 407 rows, catalog says 600 \\(-193\\)")
  # without a connection, only the two catalog checks run
  expect_equal(nrow(check_release_objects(bad, tables = "obs_ctd_full")), 2)
  # a single-file table listed twice
  one <- list(tables = list(list(name = "sample", rows = 1, partitioned = FALSE,
    objects = list(list(path = "s.parquet"), list(path = "s2.parquet")))))
  r3 <- check_release_objects(one)
  expect_equal(r3$ok, c(TRUE, FALSE)); expect_match(r3$detail[2], "2 objects for a single-file table")
})

test_that("check_release_objects() counts a single-file twin on its own", {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))
  set.seed(10); pf_fixture(con, n_part = 2, n_per = 50)
  d <- withr::local_tempdir()
  files <- export_release_parquet(con, "obs_ctd_full", file.path(d, "p"), c("cruise_key", "depth_m", "obs_id"),
                                  partition_by = "cruise_key")
  export_release_parquet(con, "obs_ctd_full", file.path(d, "twin.parquet"), c("cruise_key", "depth_m", "obs_id"))
  pv <- sub("^.*cruise_key=([^/]+)/.*$", "\\1", files$rel_path)
  objs <- c(lapply(seq_along(pv), function(i) list(path = files$rel_path[i], partition_by = "cruise_key",
                                                   partition_value = pv[i])),
            list(list(path = "twin.parquet")))
  ct <- list(tables = list(list(name = "obs", rows = 100, partitioned = TRUE, objects = objs)))
  r <- check_release_objects(ct, con, base_url = d)
  expect_true(all(r$ok)); expect_equal(r$n[r$check == "published_rows"], 100)
  ct$tables[[1]]$rows <- 90
  r2 <- check_release_objects(ct, con, base_url = d)
  expect_match(r2$detail[r2$check == "published_rows"], "^published 100 rows.*; single-file twin: published 100")
  ct$tables[[1]]$rows <- 100; ct$tables[[1]]$objects[[4]] <- list(path = "twin2.parquet")
  expect_match(check_release_objects(ct)$detail[2], "2 single-file twins")
})
