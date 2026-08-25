# T1 determinism: the same rows always produce the same bytes (single file and
# partitioned, with GEOMETRY and NULLs in the sort key), a shuffled input too, and
# a non-unique sort key is refused. T2 identity: content_hash ignores row order and
# provenance columns and reacts to a single changed value, per partition.
# Catalog/plan: objects, `since`, upload-vs-copy decisions, canonical paths.

rl_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  load_duckdb_extension(con, "spatial")
  con
}

rl_fixture <- function(con, seed = 1) {
  set.seed(seed)
  n <- 5000
  d <- data.frame(
    obs_id = seq_len(n), dataset_key = sample(c("a_x", "b_y", "c_z"), n, TRUE),
    grid_key = sample(c("st10-ln80", "st20-ln80", NA), n, TRUE),
    depth_min_m = sample(c(0, 10, 50, NA), n, TRUE),
    measurement_type = sample(c("temperature", "salinity"), n, TRUE),
    datetime = as.POSIXct("2020-01-01", tz = "UTC") + sample(0:86400, n, TRUE),
    measurement_value = round(runif(n), 6),
    `_ingested_at` = as.POSIXct("2026-08-25", tz = "UTC"), check.names = FALSE)
  d <- d[sample(n), ]                           # shuffled insertion order
  DBI::dbWriteTable(con, "obs", d, overwrite = TRUE)
  DBI::dbWriteTable(con, "pts", data.frame(sample_key = sprintf("k%04d", sample(n)),
                                           lon = runif(n, -125, -117), lat = runif(n, 30, 38)),
                    overwrite = TRUE)
  DBI::dbExecute(con, "CREATE OR REPLACE TABLE sample AS
    SELECT sample_key, lon AS longitude, lat AS latitude, ST_Point(lon, lat) AS geom FROM pts")
  invisible(d)
}

ob <- release_sort_keys()$obs

test_that("export_release_parquet() is byte-stable, single file and partitioned, and across insertion order", {
  con <- rl_con(); rl_fixture(con, 1)
  d <- withr::local_tempdir()
  f1 <- export_release_parquet(con, "obs", file.path(d, "a.parquet"), ob$order_by)
  f2 <- export_release_parquet(con, "obs", file.path(d, "b.parquet"), ob$order_by)
  sha <- function(f) digest::digest(f, algo = "sha256", file = TRUE)
  expect_identical(sha(file.path(d, "a.parquet")), sha(file.path(d, "b.parquet")))
  expect_equal(f1$rel_path, "a.parquet")
  # provenance stripped
  cols <- DBI::dbGetQuery(con, glue::glue("DESCRIBE SELECT * FROM read_parquet('{file.path(d, 'a.parquet')}')"))$column_name
  expect_false("_ingested_at" %in% cols)
  # same rows inserted in a different order -> same bytes
  con2 <- rl_con(); rl_fixture(con2, 1)
  DBI::dbExecute(con2, "CREATE OR REPLACE TABLE obs AS SELECT * FROM obs ORDER BY random()")
  export_release_parquet(con2, "obs", file.path(d, "c.parquet"), ob$order_by)
  expect_identical(sha(file.path(d, "a.parquet")), sha(file.path(d, "c.parquet")))
  # partitioned: one file per partition, byte-stable tree
  p1 <- export_release_parquet(con, "obs", file.path(d, "p1"), ob$order_by, partition_by = "dataset_key")
  p2 <- export_release_parquet(con2, "obs", file.path(d, "p2"), ob$order_by, partition_by = "dataset_key")
  expect_equal(nrow(p1), 3)
  expect_setequal(p1$rel_path, c("p1/dataset_key=a_x/data_0.parquet", "p1/dataset_key=b_y/data_0.parquet",
                                 "p1/dataset_key=c_z/data_0.parquet"))
  tree <- function(files) vapply(sort(files), sha, "")
  expect_identical(unname(tree(file.path(d, p1$rel_path))), unname(tree(file.path(d, p2$rel_path))))
  # geometry column, byte-stable too
  export_release_parquet(con, "sample", file.path(d, "s1.parquet"), "sample_key")
  export_release_parquet(con2, "sample", file.path(d, "s2.parquet"), "sample_key")
  expect_identical(sha(file.path(d, "s1.parquet")), sha(file.path(d, "s2.parquet")))
  # a non-unique key is refused; a missing column is refused
  expect_error(export_release_parquet(con, "obs", file.path(d, "x.parquet"), "dataset_key"),
               "not a unique key")
  expect_error(export_release_parquet(con, "obs", file.path(d, "x.parquet"), "nope"), "no column")
  # threads restored
  expect_false(identical(DBI::dbGetQuery(con, "SELECT current_setting('threads') t")$t, "1"))
})

test_that("release_objects(): content_hash ignores order and provenance, per-partition hashes are local", {
  con <- rl_con(); rl_fixture(con, 2)
  d <- withr::local_tempdir()
  files <- export_release_parquet(con, "obs", file.path(d, "obs"), ob$order_by, partition_by = "dataset_key")
  o1 <- release_objects(con, "obs", d, files, "v2026.09.01", partition_by = "dataset_key")
  expect_equal(nrow(o1), 3)
  expect_setequal(o1$partition_value, c("a_x", "b_y", "c_z"))
  expect_true(all(nchar(o1$sha256) == 64), all(nchar(o1$content_hash) == 32))
  # reorder rows + bump provenance: same hashes
  DBI::dbExecute(con, "CREATE OR REPLACE TABLE obs AS SELECT * EXCLUDE (_ingested_at), TIMESTAMP '2030-01-01' AS _ingested_at FROM obs ORDER BY random()")
  files2 <- export_release_parquet(con, "obs", file.path(d, "obs2"), ob$order_by, partition_by = "dataset_key")
  o2 <- release_objects(con, "obs", d, files2, "v2026.09.02", partition_by = "dataset_key")
  expect_equal(o2$content_hash[order(o2$partition_value)], o1$content_hash[order(o1$partition_value)])
  expect_equal(o2$sha256[order(o2$partition_value)], o1$sha256[order(o1$partition_value)])
  # change one value in one partition: only that partition's hash moves
  DBI::dbExecute(con, "UPDATE obs SET measurement_value = measurement_value + 1 WHERE obs_id = (SELECT min(obs_id) FROM obs WHERE dataset_key = 'b_y')")
  files3 <- export_release_parquet(con, "obs", file.path(d, "obs3"), ob$order_by, partition_by = "dataset_key")
  o3 <- release_objects(con, "obs", d, files3, "v2026.09.03", partition_by = "dataset_key")
  ch <- function(o, pv) o$content_hash[o$partition_value == pv]
  expect_equal(ch(o3, "a_x"), ch(o1, "a_x")); expect_equal(ch(o3, "c_z"), ch(o1, "c_z"))
  expect_false(ch(o3, "b_y") == ch(o1, "b_y"))
  # single-file table
  fs <- export_release_parquet(con, "sample", file.path(d, "sample.parquet"), "sample_key")
  os <- release_objects(con, "sample", d, fs, "v2026.09.01")
  expect_equal(nrow(os), 1); expect_true(is.na(os$partition_by))
})

test_that("freeze_plan() + build_release_catalog(): since is inherited, unchanged objects are copied/exist", {
  con <- rl_con(); rl_fixture(con, 3)
  d <- withr::local_tempdir()
  files <- export_release_parquet(con, "obs", file.path(d, "obs"), ob$order_by, partition_by = "dataset_key")
  fs    <- export_release_parquet(con, "sample", file.path(d, "sample.parquet"), "sample_key")
  tables_df <- data.frame(name = c("obs", "sample"), rows = c(5000, 5000),
                          partitioned = c(TRUE, FALSE), supplemental = FALSE)
  # release 1: nothing before it -> everything uploads, since = v1
  o1 <- rbind(release_objects(con, "obs", d, files, "v1", "dataset_key"),
              release_objects(con, "sample", d, fs, "v1"))
  p1 <- freeze_plan(o1, NULL, "v1", "compat")
  expect_true(all(p1$action == "upload"))
  expect_equal(p1$path[p1$table == "sample"], "ducklake/releases/v1/parquet/sample.parquet")
  cat1 <- build_release_catalog("v1", tables_df, p1, "compat", "2026-09-01")
  # round-trip through JSON as consumers see it
  j <- jsonlite::fromJSON(jsonlite::toJSON(cat1, auto_unbox = TRUE), simplifyVector = TRUE)
  expect_equal(j$tables$name, c("obs", "sample"))
  expect_equal(nrow(j$tables$objects[[1]]), 3)
  expect_true(all(j$tables$objects[[1]]$since == "v1"))
  expect_equal(j$total_size, sum(p1$bytes))
  # release 2: one partition changed -> that one uploads, the rest COPY from v1 with since = v1
  DBI::dbExecute(con, "UPDATE obs SET measurement_value = 0 WHERE dataset_key = 'c_z'")
  files2 <- export_release_parquet(con, "obs", file.path(d, "obs_2"), ob$order_by, partition_by = "dataset_key")
  o2 <- rbind(release_objects(con, "obs", d, files2, "v2", "dataset_key", prev_catalog = j),
              release_objects(con, "sample", d, fs, "v2", prev_catalog = j))
  p2 <- freeze_plan(o2, j, "v2", "compat")
  expect_equal(p2$action[p2$partition_value %in% "c_z"], "upload")
  expect_equal(p2$since[p2$partition_value %in% "c_z"], "v2")
  expect_true(all(p2$action[!p2$partition_value %in% "c_z"] == "copy"))
  expect_true(all(p2$since[!p2$partition_value %in% "c_z"] == "v1"))
  expect_equal(p2$source[p2$table == "sample"], "ducklake/releases/v1/parquet/sample.parquet")
  expect_equal(p2$path[p2$table == "sample"], "ducklake/releases/v2/parquet/sample.parquet")
  # canonical layout: paths are content-addressed, compat_path kept, catalog says so
  p3 <- freeze_plan(o2, j, "v2", "canonical")
  expect_match(p3$path[p3$table == "sample"], "^ducklake/tables/sample/[0-9a-f]{24}/sample\\.parquet$")
  expect_match(p3$path[p3$partition_value %in% "a_x"], "^ducklake/tables/obs/dataset_key=a_x/[0-9a-f]{24}/data_0\\.parquet$")
  expect_true(all(p3$action == "upload"))   # previous release was compat-layout: nothing canonical exists yet
  cat3 <- build_release_catalog("v2", tables_df, p3, "canonical")
  expect_equal(cat3$layout, "canonical")
  expect_equal(cat3$tables[[2]]$compat_path, "ducklake/releases/v2/parquet/sample.parquet")
  # release 3 on canonical after release 2 on canonical: unchanged objects `exist`
  j3 <- jsonlite::fromJSON(jsonlite::toJSON(cat3, auto_unbox = TRUE), simplifyVector = TRUE)
  o4 <- rbind(release_objects(con, "obs", d, files2, "v3", "dataset_key", prev_catalog = j3),
              release_objects(con, "sample", d, fs, "v3", prev_catalog = j3))
  p4 <- freeze_plan(o4, j3, "v3", "canonical")
  expect_true(all(p4$action == "exists"))
  expect_true(all(p4$since %in% c("v1", "v2")))
  expect_equal(canonical_path("t", strrep("a", 40)), "ducklake/tables/t/aaaaaaaaaaaaaaaaaaaaaaaa/t.parquet")
  # dry-run upload prints, touches nothing
  expect_message(upload_release_objects(p4, d, "calcofi-db", dry_run = TRUE), "release objects")
})
