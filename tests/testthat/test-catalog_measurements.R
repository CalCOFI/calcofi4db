# The measurements catalog record (plan 2026-09-10 "Measurements catalog — the
# environment's Species", § D2/D4, Appendix A): one entry per measurement key
# built from the release's own `obs_env` and the metadata registries.
#
# The fixture is a synthetic release in an in-memory DuckDB, small enough that
# every number below is arithmetic a reader can redo by hand.
#
#   key            series (dataset · type)                 why it is here
#   temperature    ds_a · temp      (4 values)             a UNIFIED key: two
#                  ds_b · temp_ave  (3 values)             types, two datasets.
#                                                          `temp` declares -2..40
#                                                          and holds one 99
#   btl_temp       ds_b · btl_temp  (2)                    a single-series key,
#                                                          same P01 -> same_bottles
#   sst            ds_c · sst       (2)                    underway -> underway_vs_cast
#   temp_s1        ds_b · temp_s1   (2)                    a raw sensor beside the
#   temp_s2        ds_b · temp_s2   (2)                    mean -> sensor_vs_mean,
#                                                          and each other ->
#                                                          paired_sensors
#   alk            ds_b · alk       (2)                    the mean of a replicate
#   alk_rep1       ds_a · alk_rep1  (2)                    -> replicate_vs_mean
#   r_alk          ds_a · r_alk     (2)                    -> pre_qc_twin
#   alk_dic        ds_d · alk_dic   (2)                    another dataset's own
#                                                          cast -> same_casts
#   count_x        ds_c · count_x   (3, one with no year)  no P01, no bound, no flag
#
#   ds_b · temp_1  is in the registry, non-canonical, sourced from a supplemental
#                  table and never reaches obs_env
#                  -> datasets[ds_b].full_resolution_only[]
#   ds_a · pres    is in the registry, non-canonical, sourced from a table that is
#                  NOT a supplemental -> listed nowhere
#
# 26 rows of obs_env in 11 series over 10 keys.

mfx_categories <- function() data.frame(
  category = c("Physical Oceanography", "Carbonate System", "Picoplankton & Bacteria"),
  order    = c(1L, 3L, 7L),
  realm    = c("env", "env", "bio"),
  icon     = c("cat-physical", "cat-carbonate", "cat-picoplankton"),
  stringsAsFactors = FALSE)

p01 <- function(code) paste0("http://vocab.nerc.ac.uk/collection/P01/current/", code, "/")

mfx_measurement_type <- function() data.frame(
  measurement_type = c("temp", "temp_ave", "temp_1", "temp_s1", "temp_s2", "btl_temp", "sst",
                       "alk", "alk_rep1", "r_alk", "alk_dic", "count_x", "pres"),
  description = c("Water temperature (QC'd)", "Average temperature",
                  "Temperature, sensor 1 (raw)", "Temperature, sensor 1 (corrected)",
                  "Temperature, sensor 2 (corrected)",
                  "Bottle temperature", "Sea surface temperature",
                  "Total alkalinity", "Total alkalinity, replicate 1",
                  "Reported total alkalinity (pre-QC)", "Total alkalinity (DIC package)",
                  "Cell counts", "Pressure at the cast"),
  units = c("degC", "degC", "degC", "degC", "degC", "degC", "deg_C",
            "umol/kg", "umol/kg", "umol/kg", "umol/kg", "cells/mL", "dbar"),
  valid_min = c(-2, rep(NA, 12)),
  valid_max = c(40, rep(NA, 12)),
  derivation = c(NA, "Mean of the two temperature sensors.", rep(NA, 11)),
  is_canonical = c(TRUE, TRUE, FALSE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, FALSE),
  `_source_column` = c("t_degc", "temp_ave", "t1", "t1_corr", "t2_corr", "btl_t", "sst_c",
                       "alk", "alk1", "r_alk", "alk_dic", "cells", "pres"),
  `_qual_column` = c("t_qual", NA, NA, NA, NA, NA, NA, "a_qual", NA, NA, NA, NA, NA),
  `_source_table` = c("bottle", "ctd_raw", "ctd_raw", "ctd_raw", "ctd_raw", "ctd_raw",
                      "mets_measurement",
                      "ctd_raw", "bottle", "bottle", "dic_measurement", "pico", "casts"),
  `_source_datasets` = c("ds_a", "ds_b", "ds_b", "ds_b", "ds_b", "ds_b", "ds_c",
                         "ds_b", "ds_a", "ds_a", "ds_d", "ds_c", "ds_a"),
  category = c(rep("Physical Oceanography", 7), rep("Carbonate System", 4),
               "Picoplankton & Bacteria", "Physical Oceanography"),
  variable = c("temperature", "temperature", "temperature", rep(NA_character_, 10)),
  nerc_p01 = c(rep(p01("TEMPPR01"), 7), rep(p01("MDMAP014"), 4), NA, NA),
  units_nerc_p06 = c(rep("http://vocab.nerc.ac.uk/collection/P06/current/UPAA/", 7),
                     rep(NA_character_, 6)),
  check.names = FALSE, stringsAsFactors = FALSE)

# the one row of the label registry WS-M1 authors: only the unified key has a label
mfx_variable <- function() data.frame(
  variable = "temperature", label = "Temperature", units = "degC",
  nerc_p01 = p01("TEMPPR01"), is_unified = TRUE, stringsAsFactors = FALSE)

mfx_record <- function() list(
  schema_version = "1.1",
  release = list(version = "v2026.01.01", release_date = "2026-01-01"),
  datasets = list(
    list(dataset_key = "ds_a", dataset_name_short = "Alpha Bottles", color = "#4dabf7",
         category = list(name = "Physical Oceanography", realm = "env",
                         icon = "cat-physical", order = 1L)),
    list(dataset_key = "ds_b", dataset_name_short = "Beta Casts", color = "#3bc9db",
         category = list(name = "Physical Oceanography", realm = "env",
                         icon = "cat-physical", order = 1L)),
    list(dataset_key = "ds_c", dataset_name_short = "Gamma Underway", color = "#74c0fc",
         category = list(name = "Meteorology & Sea State", realm = "env",
                         icon = "cat-meteorology", order = 5L)),
    list(dataset_key = "ds_d", dataset_name_short = "Delta Carbonate", color = "#63e6be",
         category = list(name = "Carbonate System", realm = "env",
                         icon = "cat-carbonate", order = 3L))))

new_measurements_fixture <- function() {
  testthat::skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  obs_env <- data.frame(
    obs_id      = 1:26,
    dataset_key = c(rep("ds_a", 4), rep("ds_b", 3), rep("ds_b", 2), rep("ds_c", 2),
                    rep("ds_b", 2), rep("ds_b", 2), rep("ds_b", 2), rep("ds_a", 2),
                    rep("ds_a", 2), rep("ds_d", 2), rep("ds_c", 3)),
    measurement_type = c(rep("temp", 4), rep("temp_ave", 3), rep("btl_temp", 2),
                         rep("sst", 2), rep("temp_s1", 2), rep("temp_s2", 2),
                         rep("alk", 2), rep("alk_rep1", 2), rep("r_alk", 2),
                         rep("alk_dic", 2), rep("count_x", 3)),
    sample_key  = c("a1", "a1", "a2", "a2", "b1", "b1", "b2", "b3", "b3",
                    "c1", "c2", "b1", "b2", "b1", "b2", "b4", "b4", "a3", "a3", "a3", "a3",
                    "d1", "d2", "c3", "c4", "c4"),
    root_id     = c(1L, 1L, 2L, 2L, 3L, 3L, 4L, 5L, 5L, 6L, 7L, 3L, 4L, 3L, 4L, 8L, 8L,
                    9L, 9L, 9L, 9L, 12L, 13L, 10L, 11L, 11L),
    grid_key    = c("g1", "g1", "g2", "g2", "g3", "g3", "g3", "g4", "g4",
                    "g5", "g5", "g3", "g3", "g3", "g3", "g6", "g6", "g7", "g7", "g7", "g7",
                    "g9", "g9", "g8", "g8", "g8"),
    cruise_key  = c("cr1", "cr1", "cr2", "cr2", "cr3", "cr3", "cr4", "cr3", "cr3",
                    "cr5", "cr5", "cr3", "cr4", "cr3", "cr4", "cr6", "cr6", "cr6", "cr6",
                    "cr6", "cr6", "cr8", "cr8", "cr7", "cr7", "cr7"),
    year        = c(2000L, 2000L, 2001L, 2001L, 2010L, 2010L, 2011L, 2010L, 2011L,
                    2015L, 2015L, 2010L, 2011L, 2010L, 2011L, 2012L, 2013L, 2012L, 2013L,
                    2012L, 2013L, 2014L, 2015L, 2020L, 2020L, NA),
    depth_min_m = c(0, 20, 150, 600, 5, 60, 2500, 10, 10, 0, 0, 5, 60, 5, 60,
                    100, 100, 100, 100, 100, 100, 50, 50, 0, 0, 0),
    depth_max_m = c(0, 20, 150, 600, 5, 60, 2500, 10, 10, 0, 0, 5, 60, 5, 60,
                    100, 100, 100, 100, 100, 100, 50, 50, 0, 0, 0),
    value = c(5, 10, 15, 99, 6, 11, 16, 7, 12, 18, 19, 6.5, 11.5, 5.5, 10.5,
              2200, 2250, 2210, 2260, 2205, 2255, 2215, 2265, 100, 200, 300),
    measurement_qual = c(NA, "6", "8", NA, rep(NA_character_, 22)),
    qual_ok = c(TRUE, TRUE, FALSE, TRUE, rep(TRUE, 22)),
    stringsAsFactors = FALSE)
  obs_env$datetime <- as.POSIXct(
    ifelse(is.na(obs_env$year), NA_character_,
           sprintf("%04d-%02d-15 12:00:00", obs_env$year,
                   c(1L, 1L, 4L, 4L, 2L, 2L, 5L, 2L, 5L, 7L, 7L, 2L, 5L, 2L, 5L,
                     3L, 3L, 3L, 3L, 3L, 3L, 6L, 6L, 11L, 11L, 1L))),
    tz = "UTC")
  climatology <- data.frame(measurement_type = c("temp", "temp_ave"), stringsAsFactors = FALSE)
  DBI::dbWriteTable(con, "obs_env", obs_env)
  DBI::dbWriteTable(con, "climatology", climatology)
  con
}

fixture_measurements_record <- function(variable = mfx_variable()) {
  con <- new_measurements_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  build_measurements_catalog(
    con, mfx_record(), measurement_type = mfx_measurement_type(),
    variable = variable, category = mfx_categories(),
    underway_datasets = "ds_c")
}

mm_of <- function(rec, key) {
  i <- which(vapply(rec$measurements, function(m) m$key, "") == key)
  testthat::expect_length(i, 1)
  rec$measurements[[i]]
}
mm_ser <- function(m, dataset_key)
  m$series[[which(vapply(m$series, function(s) s$dataset_key, "") == dataset_key)]]
mm_ds <- function(rec, key)
  rec$datasets[[which(vapply(rec$datasets, function(d) d$dataset_key, "") == key)]]
mm_related <- function(m) stats::setNames(
  vapply(m$related, function(r) r$why, ""), vapply(m$related, function(r) r$key, ""))

# ---------------------------------------------------------------------------------

test_that("the record's counts are the fixture's own arithmetic", {
  rec <- fixture_measurements_record()
  expect_identical(rec$schema_version, "1.0")
  expect_identical(rec$release$version, "v2026.01.01")
  expect_identical(rec$counts$measurements, 10L)
  expect_identical(rec$counts$pages, 10L)
  expect_identical(rec$counts$series, 11L)
  expect_identical(rec$counts$datasets, 4L)
  expect_identical(rec$counts$obs_env_rows, 26L)
  # the fixture has neither obs_*_full table nor a supplemental_rows argument
  expect_true(is.na(rec$counts$full_rows))
  n <- sum(vapply(rec$measurements, function(m)
    sum(vapply(m$series, function(s) s$n_values, 0L)), 0L))
  expect_identical(n, 26L)
})

test_that("full_rows is obs_env plus the supplementals, read never typed", {
  con <- new_measurements_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  build <- function(...) build_measurements_catalog(
    con, mfx_record(), measurement_type = mfx_measurement_type(),
    variable = mfx_variable(), category = mfx_categories(),
    underway_datasets = "ds_c", ...)
  # both supplementals supplied by the caller (from a promoted release's catalog.json)
  expect_identical(build(supplemental_rows = c(obs_ctd_full = 1000, obs_mets_full = 500))$counts$full_rows,
                   26L + 1500L)
  # one missing is NA, not a silent undercount
  expect_true(is.na(build(supplemental_rows = c(obs_ctd_full = 1000))$counts$full_rows))
  # a supplemental ON the connection is counted there
  DBI::dbExecute(con, "CREATE TABLE obs_ctd_full AS SELECT * FROM obs_env")
  expect_identical(build(supplemental_rows = c(obs_mets_full = 500))$counts$full_rows,
                   26L + 26L + 500L)
})

test_that("a unified key carries both series, and the totals are counted once", {
  m <- mm_of(fixture_measurements_record(), "temperature")
  expect_true(m$is_unified)
  expect_identical(m$slug, "temperature")
  expect_length(m$series, 2L)
  expect_identical(m$totals$n_values, 7L)
  expect_identical(m$totals$n_datasets, 2L)
  # distinct sample_keys across BOTH series: a1, a2, b1, b2 -> 4
  expect_identical(m$totals$n_samples, 4L)
  expect_identical(m$totals$year_min, 2000L)
  expect_identical(m$totals$year_max, 2011L)
  expect_identical(m$totals$depth_max_m, 2500)
  expect_true(m$climatology)
  # the biggest series first
  expect_identical(vapply(m$series, function(s) s$measurement_type, ""),
                   c("temp", "temp_ave"))
})

test_that("a single-series key is a page like any other", {
  m <- mm_of(fixture_measurements_record(), "btl_temp")
  expect_false(m$is_unified)
  expect_length(m$series, 1L)
  expect_identical(m$totals$n_values, 2L)
  expect_identical(m$series[[1]]$source_column, "btl_t")
})

test_that("a series' per-year, per-month, per-depth and per-flag counts sum to it", {
  s <- mm_ser(mm_of(fixture_measurements_record(), "temperature"), "ds_a")
  expect_identical(s$n_values, 4L)
  expect_identical(s$n_samples, 2L)
  expect_identical(s$n_roots, 2L)
  expect_identical(s$n_cells, 2L)
  expect_identical(s$n_cruises, 2L)
  expect_identical(unlist(s$years), c("2000" = 2L, "2001" = 2L))
  expect_identical(sum(unlist(s$years)), 4L)
  expect_identical(as.integer(s$months), c(2L, 0L, 0L, 2L, rep(0L, 8)))
  expect_identical(unlist(s$depth_bands),
                   c("0-10" = 1L, "10-50" = 1L, "50-100" = 0L, "100-200" = 1L,
                     "200-500" = 0L, "500-1000" = 1L, "1000-2000" = 0L, "2000+" = 0L))
  expect_identical(unlist(s$qual), c("6" = 1L, "8" = 1L, "none" = 2L))
  expect_identical(s$qual_ok_n, 3L)
  expect_identical(s$qual_column, "t_qual")
})

test_that("observed{} stays inside the declared bounds and out_of_bounds{} holds the rest", {
  rec <- fixture_measurements_record()
  s <- mm_ser(mm_of(rec, "temperature"), "ds_a")
  # the 99 is outside -2..40: it is counted, bracketed, and excluded from the quantiles
  expect_identical(s$n_values, 4L)                       # the raw count is untouched
  expect_identical(s$observed$min, 5)
  expect_identical(s$observed$max, 15)
  expect_identical(s$observed$p50, 10)
  expect_identical(s$out_of_bounds$n, 1L)
  expect_identical(s$out_of_bounds$min, 99)
  expect_identical(s$out_of_bounds$max, 99)
  expect_true("sentinel_suspected" %in% as.character(s$flags))
  # a series with no declared bound has no out_of_bounds block at all
  expect_null(mm_ser(mm_of(rec, "temperature"), "ds_b")$out_of_bounds)
  expect_null(mm_ser(mm_of(rec, "alk"), "ds_b")$out_of_bounds)
})

test_that("the no-bound sentinel heuristic needs both halves", {
  mt <- mfx_measurement_type(); mt_i <- stats::setNames(seq_len(nrow(mt)), mt$measurement_type)
  f <- function(type, mn, mx, p95, ob_n = 0L)
    calcofi4db:::.mm_series_flags(type, mt, mt_i, mn, mx, p95, ob_n)
  # `count_x` declares no bound. A large maximum consistent with the distribution
  # is not a sentinel: PAR really does read 14,187 beside a 95th of 9,000
  expect_false("sentinel_suspected" %in% f("count_x", 0, 14187, 9000))
  # far outside its own distribution AND >= 99: the METS 9,895 degC shape
  expect_true("sentinel_suspected" %in% f("count_x", 0, 9895, 20.42))
  # large ratio but below 99 is not a fill sentinel
  expect_false("sentinel_suspected" %in% f("count_x", 0, 98, 0.5))
  # with a bound declared, only out_of_bounds decides — the clipped observed cannot
  expect_false("sentinel_suspected" %in% f("temp", 5, 15, 14.5, 0L))
  expect_true("sentinel_suspected"  %in% f("temp", 5, 15, 14.5, 1L))
})

test_that("rows with no year are absent from years{} but counted in n_values", {
  s <- mm_ser(mm_of(fixture_measurements_record(), "count_x"), "ds_c")
  expect_identical(s$n_values, 3L)
  expect_identical(sum(unlist(s$years)), 2L)
  expect_identical(sum(s$months), 2L)
})

test_that("the flag vocabulary is exactly what the registry and the values say", {
  rec <- fixture_measurements_record()
  flags_of <- function(key, dk) as.character(mm_ser(mm_of(rec, key), dk)$flags)
  # the bottle series declares -2..40 and reads 99: a bound exceeded, not no_bound
  expect_identical(flags_of("temperature", "ds_a"), "sentinel_suspected")
  # the CTD mean: derived from a sensor pair, no bound, and no flag at this grain
  expect_identical(flags_of("temperature", "ds_b"),
                   c("sensor_mean", "no_bound", "no_flag_at_grain"))
  # the alkalinity series read ~2,200 umol/kg with no bound declared: large, but
  # consistent with their own distribution, so no sentinel is suspected
  expect_identical(flags_of("alk_rep1", "ds_a"),
                   c("replicate", "no_bound", "no_flag_at_grain"))
  expect_identical(flags_of("r_alk", "ds_a"),
                   c("reported_pre_qc", "no_bound", "no_flag_at_grain"))
  # the flags are always in the vocabulary's own order
  expect_identical(flags_of("alk_rep1", "ds_a"),
                   intersect(measurement_series_flags(), flags_of("alk_rep1", "ds_a")))
  # no NERC concept says exactly this
  expect_true("no_p01" %in% unlist(mm_ser(mm_of(rec, "count_x"), "ds_c")$flags))
  expect_true(is.na(mm_of(rec, "count_x")$nerc_p01))
  # a declared flag column is not a flag
  expect_false("no_flag_at_grain" %in% unlist(mm_ser(mm_of(rec, "alk"), "ds_b")$flags))
  all_flags <- unlist(lapply(rec$measurements, function(m)
    unlist(lapply(m$series, function(s) s$flags))))
  expect_true(all(all_flags %in% measurement_series_flags()))
  expect_length(measurement_series_flags(), 7L)
})

test_that("the declared bounds are the registry's, named by the type that declares them", {
  m <- mm_of(fixture_measurements_record(), "temperature")
  expect_identical(m$bounds$valid_min, -2)
  expect_identical(m$bounds$valid_max, 40)
  expect_identical(as.character(m$bounds$declared_by), "temp")
  expect_true(is.na(mm_of(fixture_measurements_record(), "alk")$bounds$valid_max))
})

test_that("related[] states why two keys sharing a P01 are kept apart", {
  rec <- fixture_measurements_record()
  expect_identical(mm_related(mm_of(rec, "temperature")),
                   c(btl_temp = "same_bottles", sst = "underway_vs_cast",
                     temp_s1 = "sensor_vs_mean", temp_s2 = "sensor_vs_mean"))
  # an underway intake beside a cast's own bottle table is underway, not same_bottles
  expect_identical(mm_related(mm_of(rec, "sst"))[["btl_temp"]], "underway_vs_cast")
  expect_identical(mm_related(mm_of(rec, "alk")),
                   c(alk_dic = "same_casts", alk_rep1 = "replicate_vs_mean",
                     r_alk = "pre_qc_twin"))
  expect_identical(mm_related(mm_of(rec, "alk_rep1"))[["r_alk"]], "replicate_vs_mean")
  # a raw sensor beside the mean of its pair, in the same dataset
  expect_identical(mm_related(mm_of(rec, "temp_s1"))[["temperature"]], "sensor_vs_mean")
  # another dataset's own casts of the same quantity, with no sharper marker
  expect_identical(mm_related(mm_of(rec, "alk_dic"))[["alk"]], "same_casts")
  # the two sensors of one instrument: same dataset, neither of them the mean
  expect_identical(mm_related(mm_of(rec, "temp_s1"))[["temp_s2"]], "paired_sensors")
  expect_identical(mm_related(mm_of(rec, "temp_s2"))[["temp_s1"]], "paired_sensors")
  # the relation is symmetric: every P01-sharing pair appears from both sides
  n_rel <- vapply(rec$measurements, function(m) length(m$related), 0L)
  p01   <- vapply(rec$measurements, function(m)
    if (is.null(m$nerc_p01) || is.na(m$nerc_p01)) NA_character_ else m$nerc_p01, "")
  grp   <- table(p01[!is.na(p01)])
  expect_identical(sum(n_rel), sum(as.integer(grp) * (as.integer(grp) - 1L)))
  # a key with no P01 is related to nothing
  expect_length(mm_of(rec, "count_x")$related, 0L)
  whys <- unlist(lapply(rec$measurements, function(m) vapply(m$related, function(r) r$why, "")))
  expect_true(all(whys %in% measurement_related_reasons()))
  expect_length(measurement_related_reasons(), 7L)
})

test_that("a non-canonical type that never reaches obs_env is listed under its dataset", {
  rec <- fixture_measurements_record()
  fro <- mm_ds(rec, "ds_b")$full_resolution_only
  expect_length(fro, 1L)
  expect_identical(fro[[1]]$measurement_type, "temp_1")
  expect_identical(fro[[1]]$table, "obs_ctd_full")
  expect_false("temp_1" %in% vapply(rec$measurements, function(m) m$key, ""))
  # ds_a's `pres` is non-canonical and never reaches obs_env either, but its source
  # table is not a supplemental: it is simply not released, so it is listed nowhere
  expect_length(mm_ds(rec, "ds_a")$full_resolution_only, 0L)
  expect_false("pres" %in% vapply(rec$measurements, function(m) m$key, ""))
  # temp_ave, btl_temp, temp_s1 and alk reach obs_env; temp_1 does not
  expect_identical(mm_ds(rec, "ds_b")$n_series, 5L)
  expect_identical(mm_ds(rec, "ds_a")$color, "#4dabf7")
})

test_that("the label comes from variable.csv, and its absence is a flag, never an invention", {
  with_reg <- fixture_measurements_record()
  expect_identical(mm_of(with_reg, "temperature")$label, "Temperature")
  expect_length(mm_of(with_reg, "temperature")$flags, 0L)
  # a key with no row falls back to the canonical series' registry description
  expect_identical(mm_of(with_reg, "alk")$label, "Total alkalinity")
  expect_identical(as.character(mm_of(with_reg, "alk")$flags), "no_label")

  without <- fixture_measurements_record(variable = NULL)
  expect_identical(mm_of(without, "temperature")$label, "Water temperature (QC'd)")
  expect_identical(as.character(mm_of(without, "temperature")$flags), "no_label")
  expect_true(all(vapply(without$measurements,
                         function(m) identical(as.character(m$flags), "no_label"), TRUE)))
})

test_that("measurements[] is ordered by category, then by size, then by key", {
  rec <- fixture_measurements_record()
  ord <- vapply(rec$measurements, function(m) m$category$order, 0L)
  expect_identical(ord, sort(ord))
  phys <- Filter(function(m) m$category$name == "Physical Oceanography", rec$measurements)
  n <- vapply(phys, function(m) m$totals$n_values, 0L)
  expect_identical(n, sort(n, decreasing = TRUE))
})

test_that("the record validates against measurements.schema.json", {
  rec <- fixture_measurements_record()
  expect_true(validate_measurements_catalog(rec))
  d <- withr::local_tempdir()
  p <- write_measurements_catalog(rec, d)
  expect_identical(basename(p), "measurements.json")
  expect_true(validate_measurements_catalog(p))
  # empty objects/arrays survive the round trip as {} and [], never null
  j <- jsonlite::fromJSON(p, simplifyVector = FALSE)
  cx <- j$measurements[[which(vapply(j$measurements, function(m) m$key, "") == "count_x")]]
  expect_identical(cx$related, list())
  expect_length(cx$series[[1]]$months, 12L)
})

test_that("check_measurements_catalog passes on the fixture and catches a double count", {
  con <- new_measurements_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  rec <- build_measurements_catalog(
    con, mfx_record(), measurement_type = mfx_measurement_type(),
    variable = mfx_variable(), category = mfx_categories(), underway_datasets = "ds_c")
  d <- check_measurements_catalog(rec, con, mfx_record(), mfx_measurement_type())
  expect_true(all(d$ok))
  expect_setequal(d$check, names(measurements_catalog_checks()))
  expect_silent(assert_measurements_catalog(d, quiet = TRUE))

  # a series counted twice: the arithmetic gate is what catches it
  bad <- rec
  bad$measurements[[1]]$series[[1]]$n_values <-
    bad$measurements[[1]]$series[[1]]$n_values + 1L
  db <- check_measurements_catalog(bad, con, mfx_record(), mfx_measurement_type())
  expect_false(db$ok[db$check == "series_total"])
  expect_error(assert_measurements_catalog(db), "series_total")
})

test_that("an unregistered measurement_type stops the build", {
  con <- new_measurements_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  mt <- mfx_measurement_type()
  expect_error(
    build_measurements_catalog(con, mfx_record(),
                               measurement_type = mt[mt$measurement_type != "sst", ],
                               variable = mfx_variable(), category = mfx_categories()),
    "absent from the registry")
})
