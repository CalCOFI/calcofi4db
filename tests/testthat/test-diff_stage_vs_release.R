# diff_stage_vs_release(): one small synthetic stage + release per rule.
# the stage dir mimics {stage}/parquet/{dataset_key}/ (obs partitioned by cruise_key);
# the release dir mimics a release's parquet (obs_env partitioned by measurement_type,
# with `value` in place of measurement_value).

DS <- "test_ds"

obs_rows <- function(sample_key = "test_ds:cast:1", depth = 10, type = "temperature",
                     value = 1, qual = NA_character_, cruise_key = "2020-01-NODC") {
  data.frame(
    obs_id = seq_along(sample_key), dataset_key = DS, sample_key = sample_key,
    depth_min_m = depth, depth_max_m = depth, taxon_key = NA_character_,
    life_stage = NA_character_, measurement_type = type, measurement_value = value,
    measurement_qual = qual, cruise_key = cruise_key, stringsAsFactors = FALSE)
}

write_pq <- function(con, df, path, partition = NULL) {
  duckdb::duckdb_register(con, "df_tmp", df, overwrite = TRUE)
  withr::defer(duckdb::duckdb_unregister(con, "df_tmp"))
  # keep the typed NULLs: an all-NA character column registers as logical
  cols <- c(taxon_key = "VARCHAR", life_stage = "VARCHAR", measurement_qual = "VARCHAR")
  sel <- vapply(names(df), function(k)
    if (k %in% names(cols)) sprintf('"%s"::%s AS "%s"', k, cols[[k]], k) else sprintf('"%s"', k), "")
  if (is.null(partition)) {
    DBI::dbExecute(con, sprintf("COPY (SELECT %s FROM df_tmp) TO '%s' (FORMAT parquet)",
                                paste(sel, collapse = ", "), path))
  } else {
    DBI::dbExecute(con, sprintf(
      "COPY (SELECT %s FROM df_tmp) TO '%s' (FORMAT parquet, PARTITION_BY (%s))",
      paste(sel, collapse = ", "), path, partition))
  }
}

# stage obs + release obs_env (value renamed), optional sample_measurement on each side
make_fixture <- function(stage_obs, release_obs, stage_sm = NULL, release_sm = NULL,
                         stage_sample = NULL, release_sample = NULL) {
  skip_if_not_installed("duckdb")
  root <- withr::local_tempdir(.local_envir = parent.frame())
  sd <- file.path(root, "stage", DS); rd <- file.path(root, "release")
  dir.create(sd, recursive = TRUE); dir.create(rd, recursive = TRUE)
  con <- get_duckdb_con()
  withr::defer(close_duckdb(con))
  if (!is.null(stage_obs)) write_pq(con, stage_obs, file.path(sd, "obs"), "cruise_key")
  if (!is.null(release_obs)) {
    names(release_obs)[names(release_obs) == "measurement_value"] <- "value"
    write_pq(con, release_obs, file.path(rd, "obs_env"), "measurement_type")
  }
  if (!is.null(stage_sm))       write_pq(con, stage_sm, file.path(sd, "sample_measurement.parquet"))
  if (!is.null(release_sm))     write_pq(con, release_sm, file.path(rd, "sample_measurement.parquet"))
  if (!is.null(stage_sample))   write_pq(con, stage_sample, file.path(sd, "sample.parquet"))
  if (!is.null(release_sample)) write_pq(con, release_sample, file.path(rd, "sample.parquet"))
  list(stage = sd, release = rd)
}

run_diff <- function(fx, ...) {
  diff_stage_vs_release(DS, release = fx$release, stage_dir = fx$stage, ...)
}

row_of <- function(d, type, tbl = "obs") d[d$table == tbl & d$measurement_type == type, ]

test_that("an identical stage and release is all unchanged", {
  o <- obs_rows(sample_key = c("a", "b"), value = c(1, 2))
  d <- run_diff(make_fixture(o, o))
  r <- row_of(d, "temperature")
  expect_equal(r$n_release, 2); expect_equal(r$n_stage, 2); expect_equal(r$n_unchanged, 2)
  expect_equal(r$n_changed + r$n_added + r$n_removed + r$n_filled, 0)
  expect_equal(nrow(diff_stage_vs_release_rows(d)), 0)
})

test_that("a changed value is counted with its largest change", {
  rel <- obs_rows(sample_key = c("a", "b", "c"), value = c(1, 2, 3))
  stg <- obs_rows(sample_key = c("a", "b", "c"), value = c(1, 2.5, 5))
  d <- run_diff(make_fixture(stg, rel))
  r <- row_of(d, "temperature")
  expect_equal(r$n_changed, 2); expect_equal(r$n_unchanged, 1)
  expect_equal(r$max_abs_change, 2)
  rows <- diff_stage_vs_release_rows(d)
  expect_equal(rows$sample_key, c("c", "b"))          # largest change first
  expect_equal(rows$status, c("changed", "changed"))
})

test_that("a value filled: release NULL -> stage number, and a row only in the stage is added", {
  rel <- obs_rows(sample_key = c("a"), value = NA_real_)
  stg <- obs_rows(sample_key = c("a", "b"), value = c(4, 5))
  d <- run_diff(make_fixture(stg, rel))
  r <- row_of(d, "temperature")
  expect_equal(r$n_filled, 1); expect_equal(r$n_added, 1)
  expect_equal(r$n_release, 1); expect_equal(r$n_stage, 2)
  expect_true(is.na(r$max_abs_change))
})

test_that("a value removed: a row only in the release, and a number -> NULL is blanked", {
  rel <- obs_rows(sample_key = c("a", "b"), value = c(1, 2))
  stg <- obs_rows(sample_key = c("a"), value = NA_real_)
  d <- run_diff(make_fixture(stg, rel))
  r <- row_of(d, "temperature")
  expect_equal(r$n_removed, 1); expect_equal(r$n_blanked, 1)
  expect_equal(r$n_unchanged, 0)
})

test_that("a qual-only change is counted apart from the value", {
  rel <- obs_rows(sample_key = c("a", "b"), value = c(1, 2), qual = c(NA, "2"))
  stg <- obs_rows(sample_key = c("a", "b"), value = c(1, 2), qual = c("8", "2"))
  d <- run_diff(make_fixture(stg, rel))
  r <- row_of(d, "temperature")
  expect_equal(r$n_qual_changed, 1); expect_equal(r$n_changed, 0)
  expect_equal(r$n_unchanged, 1)
  rows <- diff_stage_vs_release_rows(d)
  expect_equal(rows$status, "same"); expect_true(rows$qual_changed)
  expect_equal(rows$qual_stage, "8")
})

test_that("NaN is not NULL: NaN <-> NULL is nan_null, NaN -> number is filled, NaN == NaN is same", {
  rel <- obs_rows(sample_key = c("a", "b", "c", "d"), value = c(NaN, NA, NaN, 1))
  stg <- obs_rows(sample_key = c("a", "b", "c", "d"), value = c(NA, NaN, 3, NaN))
  d <- run_diff(make_fixture(stg, rel))
  r <- row_of(d, "temperature")
  expect_equal(r$n_nan_null, 2); expect_equal(r$n_filled, 1); expect_equal(r$n_blanked, 1)
  rel2 <- obs_rows(sample_key = "a", value = NaN)
  r2 <- row_of(run_diff(make_fixture(rel2, rel2)), "temperature")
  expect_equal(r2$n_unchanged, 1)
})

test_that("a duplicate key on one side is reported and its extra row counted", {
  rel <- obs_rows(sample_key = c("a", "b"), value = c(1, 2))
  stg <- obs_rows(sample_key = c("a", "a", "b"), value = c(1, 1, 2))
  d <- run_diff(make_fixture(stg, rel))
  r <- row_of(d, "temperature")
  expect_equal(r$n_dup_keys_stage, 1); expect_equal(r$n_dup_keys_release, 0)
  expect_equal(r$n_added, 1); expect_equal(r$n_unchanged, 2)
})

test_that("a type only in the stage and a type only in the release both appear", {
  rel <- obs_rows(sample_key = c("a", "a"), type = c("temperature", "salinity"), value = c(1, 33))
  stg <- obs_rows(sample_key = c("a", "a"), type = c("temperature", "oxygen"), value = c(1, 5))
  d <- run_diff(make_fixture(stg, rel))
  expect_setequal(d$measurement_type[d$table == "obs"], c("oxygen", "salinity", "temperature"))
  expect_equal(row_of(d, "oxygen")$n_added, 1)
  expect_equal(row_of(d, "oxygen")$n_release, 0)
  expect_equal(row_of(d, "salinity")$n_removed, 1)
  expect_equal(row_of(d, "salinity")$n_stage, 0)
})

test_that("a type named in measurement_type but on neither side still gets an all-zero row", {
  o <- obs_rows(sample_key = "a")
  d <- run_diff(make_fixture(o, o), measurement_type = c("temperature", "ph"))
  expect_equal(d$measurement_type, c("ph", "temperature"))
  expect_equal(row_of(d, "ph")$n_release, 0)
  expect_equal(row_of(d, "ph")$n_stage, 0)
})

test_that("tolerance is respected", {
  rel <- obs_rows(sample_key = c("a", "b"), value = c(1, 2))
  stg <- obs_rows(sample_key = c("a", "b"), value = c(1 + 1e-12, 2.001))
  fx <- make_fixture(stg, rel)
  expect_equal(row_of(run_diff(fx), "temperature")$n_changed, 1)
  expect_equal(row_of(run_diff(fx, tolerance = 0.01), "temperature")$n_changed, 0)
  expect_equal(row_of(run_diff(fx, tolerance = 0), "temperature")$n_changed, 2)
})

test_that("the cruise filter restricts both sides, sample_measurement through the samples", {
  rel <- obs_rows(sample_key = c("a", "b"), value = c(1, 2),
                  cruise_key = c("2020-01-NODC", "2021-01-NODC"))
  stg <- obs_rows(sample_key = c("a", "b"), value = c(1, 9),
                  cruise_key = c("2020-01-NODC", "2021-01-NODC"))
  sm <- data.frame(sample_measurement_id = 1:2, sample_key = c("a", "b"), dataset_key = DS,
                   measurement_type = "mld", measurement_value = c(10, 20),
                   measurement_qual = NA_character_)
  sm2 <- sm; sm2$measurement_value <- c(10, 25)
  smp <- data.frame(sample_key = c("a", "b"), cruise_key = c("2020-01-NODC", "2021-01-NODC"))
  fx <- make_fixture(stg, rel, stage_sm = sm2, release_sm = sm, release_sample = smp)
  d_all <- run_diff(fx)
  expect_equal(row_of(d_all, "temperature")$n_changed, 1)
  expect_equal(row_of(d_all, "mld", "sample_measurement")$n_changed, 1)
  d20 <- run_diff(fx, cruise_key = "2020-01-NODC")
  expect_equal(row_of(d20, "temperature")$n_release, 1)
  expect_equal(row_of(d20, "temperature")$n_changed, 0)
  expect_equal(row_of(d20, "mld", "sample_measurement")$n_release, 1)
  expect_equal(row_of(d20, "mld", "sample_measurement")$n_changed, 0)
  d21 <- run_diff(fx, cruise_key = "2021-01-NODC")
  expect_equal(row_of(d21, "temperature")$n_changed, 1)
  expect_equal(row_of(d21, "mld", "sample_measurement")$n_changed, 1)
})

test_that("batching by cruise gives the same answer as one pass", {
  rel <- obs_rows(sample_key = letters[1:6], value = 1:6,
                  cruise_key = rep(c("2020-01-NODC", "2020-04-NODC", "2020-07-NODC"), 2))
  stg <- rel; stg$measurement_value[c(2, 5)] <- c(20, 50); stg <- stg[-1, ]
  fx <- make_fixture(stg, rel)
  one <- run_diff(fx)
  many <- run_diff(fx, chunk_rows = 2)
  expect_equal(as.data.frame(one), as.data.frame(many), ignore_attr = TRUE)
  expect_equal(nrow(diff_stage_vs_release_rows(many)), 3)
})

test_that("a stage with no release counterpart diffs against nothing", {
  stg <- obs_rows(sample_key = c("a", "b"))
  d <- run_diff(make_fixture(stg, NULL))
  expect_equal(row_of(d, "temperature")$n_added, 2)
  expect_equal(row_of(d, "temperature")$n_release, 0)
})
