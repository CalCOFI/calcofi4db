# measurements.json 1.1 — the face registries and the per-band anomaly
# (plan 2026-09-11 "Measurement faces …", § D6/D8, Appendix A; WS-MF3).
#
# A second, smaller synthetic release than test-catalog_measurements.R's, built
# so every number below is arithmetic a reader can redo by hand. One key,
# `temperature`, UNIFIED over two series (ds_a `temp` and ds_b `temp_ave`), at
# one station in one calendar month, against a climatology of three cells:
#
#   band       depth_bin  clim_mean   what is in it
#   0-10       0          10 (both)   1990: cruise c1 holds ds_a 12, 14 and ds_b 16
#                                            -> cruise mean anomaly (2+4+6)/3 = 4
#                                           cruise c2 holds ds_a 11 -> 1
#                                            -> the YEAR is (4+1)/2 = 2.5 over 2 cruises
#                                     2000: cruise c3 alone, ds_a 15 -> 5, ONE cruise
#                                     2010: cruises c4 (13 -> 3) and c5 (15 -> 5) -> 4
#                                     and ds_a 39 on c1, FLAGGED: in bounds, excluded
#   10-50      20         8           1990: c1, ds_a 9 -> 1 · 2000: c3, ds_a 12 -> 4
#                                     (one cruise each: no trend, no extremes, no ymax)
#   500-1000   600        none        1990: c1, ds_a 5 -> a band with values and no
#                                     normal: `deeper[]`, never a silent drop
#
# So: the trend is fitted over 1990 and 2010 (the years with >= 2 cruises inside
# 1984-2021) — slope 1.5/20 per year, 0.75 per decade — the 2000 one-cruise year
# stays in the series and steers nothing, and ymax is 4 for BOTH bands.

mfz_measurement_type <- function() data.frame(
  measurement_type = c("temp", "temp_ave"),
  description  = c("Water temperature (QC'd)", "Average temperature"),
  units        = c("degC", "degC"),
  valid_min    = c(-2, NA),
  valid_max    = c(40, NA),
  derivation   = c(NA, "Mean of the two temperature sensors."),
  is_canonical = c(TRUE, FALSE),
  `_source_column`   = c("t_degc", "temp_ave"),
  `_qual_column`     = c("t_qual", NA),
  `_source_table`    = c("bottle", "ctd_raw"),
  `_source_datasets` = c("ds_a", "ds_b"),
  category = c("Physical Oceanography", "Physical Oceanography"),
  variable = c("temperature", "temperature"),
  nerc_p01 = rep("http://vocab.nerc.ac.uk/collection/P01/current/TEMPPR01/", 2),
  units_nerc_p06 = rep(NA_character_, 2),
  check.names = FALSE, stringsAsFactors = FALSE)

mfz_record <- function() list(
  schema_version = "1.1",
  release = list(version = "v2026.01.01", release_date = "2026-01-01"),
  datasets = list(
    list(dataset_key = "ds_a", dataset_name_short = "Alpha Bottles", color = "#4dabf7",
         category = list(name = "Physical Oceanography", realm = "env",
                         icon = "cat-physical", order = 1L)),
    list(dataset_key = "ds_b", dataset_name_short = "Beta Casts", color = "#3bc9db",
         category = list(name = "Physical Oceanography", realm = "env",
                         icon = "cat-physical", order = 1L))))

mfz_categories <- function() data.frame(
  category = "Physical Oceanography", order = 1L, realm = "env", icon = "cat-physical",
  stringsAsFactors = FALSE)

# one obs_env row
mfz_row <- function(dataset_key, measurement_type, year, cruise_key, depth, value,
                    qual_ok = TRUE, measurement_qual = NA_character_)
  data.frame(dataset_key = dataset_key, measurement_type = measurement_type,
             year = as.integer(year), cruise_key = cruise_key, depth_min_m = depth,
             value = value, qual_ok = qual_ok, measurement_qual = measurement_qual,
             stringsAsFactors = FALSE)

new_faces_fixture <- function() {
  testthat::skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  o <- rbind(
    # band 0-10, depth_bin 0
    mfz_row("ds_a", "temp",     1990, "c1", 0, 12), mfz_row("ds_a", "temp", 1990, "c1", 0, 14),
    mfz_row("ds_b", "temp_ave", 1990, "c1", 0, 16),
    mfz_row("ds_a", "temp",     1990, "c2", 0, 11),
    mfz_row("ds_a", "temp",     2000, "c3", 0, 15),
    mfz_row("ds_a", "temp",     2010, "c4", 0, 13), mfz_row("ds_a", "temp", 2010, "c5", 0, 15),
    # a FLAGGED extreme, inside the declared -2..40: it must not reach observed{},
    # the anomaly or any mean, and must be counted in n_flagged
    mfz_row("ds_a", "temp",     1990, "c1", 0, 39, qual_ok = FALSE, measurement_qual = "8"),
    # band 10-50, depth_bin 20
    mfz_row("ds_a", "temp",     1990, "c1", 20, 9), mfz_row("ds_a", "temp", 2000, "c3", 20, 12),
    # band 500-1000, depth_bin 600: no climatology cell reaches this deep
    mfz_row("ds_a", "temp",     1990, "c1", 600, 5))
  o$obs_id     <- seq_len(nrow(o))
  o$sample_key <- paste0("s", o$obs_id)
  o$root_id    <- o$obs_id
  o$grid_key   <- "g1"
  o$site_key   <- "090.0 060.0"
  o$depth_bin  <- as.integer(floor(o$depth_min_m / 10) * 10)
  o$depth_max_m <- o$depth_min_m
  o$datetime   <- as.POSIXct(sprintf("%04d-01-15 12:00:00", o$year), tz = "UTC")
  clim <- data.frame(
    dataset_key = c("ds_a", "ds_b", "ds_a"),
    site_key    = "090.0 060.0",
    grid_key    = "g1",
    month       = 1L,
    depth_bin   = c(0L, 0L, 20L),
    measurement_type = c("temp", "temp_ave", "temp"),
    clim_mean   = c(10, 10, 8), clim_sd = c(1, 1, 1),
    clim_n      = 30L, n_cruises = 10L,
    clim_yr_min = 1993L, clim_yr_max = 2013L, stringsAsFactors = FALSE)
  DBI::dbWriteTable(con, "obs_env", o)
  DBI::dbWriteTable(con, "climatology", clim)
  con
}

mfz_registries <- function()
  testthat::test_path("fixtures", "measurement_faces")

faces_record <- function(registries = mfz_registries(), ...) {
  con <- new_faces_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  build_measurements_catalog(
    con, mfz_record(), measurement_type = mfz_measurement_type(),
    variable = NULL, category = mfz_categories(), registries = registries, ...)
}

mfz_of <- function(rec, key)
  rec$measurements[[which(vapply(rec$measurements, function(m) m$key, "") == key)]]
mfz_band <- function(m, band)
  m$anomaly$bands[[which(vapply(m$anomaly$bands, function(b) b$band, "") == band)]]

# ---------------------------------------------------------------------------------

test_that("the anomaly is one yearly series per band, merged over a unified key's series", {
  m <- mfz_of(faces_record(), "temperature")
  expect_true(m$is_unified)
  expect_identical(as.integer(m$anomaly$baseline), c(1993L, 2013L))
  expect_identical(vapply(m$anomaly$bands, function(b) b$band, ""), c("0-10", "10-50"))

  b <- mfz_band(m, "0-10")
  # [year, anom, n_cruises, n_values] — 1990 merges ds_a's two values and ds_b's
  # one into cruise c1 BEFORE the year's mean: (4 + 1) / 2, not (2+4+6+1)/4
  expect_equal(b$series[[1]], c(1990, 2.5, 2, 4), ignore_attr = TRUE)
  expect_equal(b$series[[2]], c(2000, 5,   1, 1), ignore_attr = TRUE)
  expect_equal(b$series[[3]], c(2010, 4,   2, 2), ignore_attr = TRUE)
  expect_identical(b$n_values, 7L)
  expect_identical(b$n_years, 3L)

  b2 <- mfz_band(m, "10-50")
  expect_equal(b2$series[[1]], c(1990, 1, 1, 1), ignore_attr = TRUE)
  expect_equal(b2$series[[2]], c(2000, 4, 1, 1), ignore_attr = TRUE)
})

test_that("a one-cruise year is in the series and steers nothing", {
  m <- mfz_of(faces_record(), "temperature")
  b <- mfz_band(m, "0-10")
  # the trend is fitted over 1990 and 2010 only: (4 - 2.5) / 20 years = 0.75 / decade
  expect_equal(b$trend$per_decade, 0.75)
  expect_equal(b$trend$intercept, round(2.5 - 0.075 * 1990, 4))
  expect_identical(b$trend$from, 1990L)
  expect_identical(b$trend$to, 2010L)
  expect_identical(b$trend$n_years, 2L)
  # the extremes are the >= 2-cruise years: the 2000 value of 5 is the largest
  # number in the series and is NOT the high
  expect_equal(b$ext$hi, c(2010, 4),   ignore_attr = TRUE)
  expect_equal(b$ext$lo, c(1990, 2.5), ignore_attr = TRUE)
  # a band whose every year holds one cruise gets no trend and no extremes
  b2 <- mfz_band(m, "10-50")
  expect_null(b2$trend)
  expect_null(b2$ext)
  # ymax is shared by BOTH bands and set by the >= 2-cruise years, so the 5 and
  # the 4 of the thin band lie outside it: the page clips them and says so
  expect_equal(m$anomaly$ymax, 4)
  expect_identical(m$anomaly$min_cruises, 2L)
  # the head's sparkline is the band with the most values
  expect_identical(m$anomaly$spark_band, "0-10")
})

test_that("a band with values and no normal is named in deeper[], never dropped", {
  m <- mfz_of(faces_record(), "temperature")
  expect_length(m$anomaly$deeper, 1L)
  expect_identical(m$anomaly$deeper[[1]]$band, "500-1000")
  expect_identical(m$anomaly$deeper[[1]]$n_obs, 1L)
  expect_false("500-1000" %in% vapply(m$anomaly$bands, function(b) b$band, ""))
})

test_that("a provider's flag outranks the bound: observed{} excludes it, n_flagged counts it", {
  m <- mfz_of(faces_record(), "temperature")
  s <- m$series[[which(vapply(m$series, function(x) x$measurement_type, "") == "temp")]]
  # 39 is INSIDE the declared -2..40 and is the series' largest value; it is
  # flagged, so it is not the observed maximum and not in out_of_bounds either
  expect_identical(s$n_values, 10L)
  expect_identical(s$qual_ok_n, 9L)
  expect_identical(s$n_flagged, 1L)
  expect_identical(s$observed$max, 15)
  expect_identical(s$observed$min, 5)
  expect_identical(s$out_of_bounds$n, 0L)
  # and it is in no yearly mean: 1990 at 0-10 m stays 2.5
  expect_equal(mfz_band(m, "0-10")$series[[1]][[2]], 2.5)
  # the key's own heads-up arithmetic, which the landing page reads instead of
  # recomputing (CalCOFI.github.io #22)
  expect_identical(m$totals$n_flagged, m$totals$n_values - m$totals$qual_ok_n)
  expect_identical(m$totals$n_flagged, 1L)
})

test_that("the five registries are carried per key, and a computed mark is recomputed", {
  rec <- faces_record()
  m <- mfz_of(rec, "temperature")
  expect_identical(m$face$kind, "scale")
  expect_true(is.na(m$face$face_of))
  # chem has no temperature row: the field is absent, never an empty invention
  expect_null(m$chem)
  # one method entry per SERIES the registry carries a row for, in series order
  expect_length(m$method, 2L)
  expect_identical(vapply(m$method, function(x) x$measurement_type, ""), c("temp", "temp_ave"))
  expect_identical(m$method[[1]]$instrument, "Reversing thermometer")
  expect_identical(as.character(m$method[[1]]$steps),
                   c("Trip the frame", "Read both thermometers"))
  expect_identical(as.character(m$method[[1]]$bibkeys), "carpenter1965")
  expect_length(m$method[[2]]$steps, 0L)
  # why is ranked, and rank 1 leads however the file is ordered
  expect_identical(vapply(m$why, function(x) x$rank, 0L), c(1L, 2L))
  expect_identical(m$why[[1]]$kind, "authored")
  expect_identical(as.character(m$why[[1]]$bibkeys), c("bograd2008", "rudnick2017"))
  expect_identical(m$why[[2]]$goos_doc, "17473")
})

test_that("a computed scale mark is recomputed from its own how, never read as a number", {
  skip_if_not_installed("gsw")
  m <- mfz_of(faces_record(), "temperature")
  kinds <- vapply(m$scale, function(x) x$kind, "")
  labs <- vapply(m$scale, function(x) x$label, "")
  cm <- m$scale[[which(kinds == "computed" & grepl("^the freezing point of seawater", labs))]]
  # the registry types -99, a number nobody should trust; the build recomputes
  # gsw.t_freezing(35, 0, 1) and the value is the freezing point, near -1.91 degC
  expect_true(cm$computed_at_build)
  expect_false(isTRUE(all.equal(cm$value, -99)))
  expect_equal(cm$value, round(gsw::gsw_t_freezing(35, 0, 1), 6))
  # a mark this package cannot recompute passes through, and SAYS it did
  expect_false(vapply(m$scale, function(x) isTRUE(x$computed_at_build), TRUE)[kinds == "familiar"])
})

test_that("a computed mark this package cannot evaluate passes through and says so", {
  m <- mfz_of(faces_record(), "temperature")
  labs <- vapply(m$scale, function(x) x$label, "")
  pc <- m$scale[[grep("^pCO2", labs)]]
  # PyCO2SYS has no R equivalent installed (seacarb is not a dependency): the
  # registry's own value stands, and the record says it was NOT recomputed here —
  # the one thing a reader must not have to guess
  expect_identical(pc$kind, "computed")
  expect_false(pc$computed_at_build)
  expect_equal(pc$value, 368)
  expect_match(pc$how, "^PyCO2SYS")
})

test_that("absent registries and an absent climatology write exactly the 1.0 record", {
  bare <- faces_record(registries = NULL, anomaly = FALSE)
  m <- mfz_of(bare, "temperature")
  for (f in c("face", "chem", "method", "scale", "why", "anomaly")) expect_null(m[[f]])
  expect_true(validate_measurements_catalog(bare))
  # and a metadata/ directory holding NONE of the five files is the same thing —
  # which is what the release does until WS-MF1's registries are authored
  empty <- mfz_of(faces_record(registries = withr::local_tempdir(), anomaly = FALSE), "temperature")
  for (f in c("face", "chem", "method", "scale", "why", "anomaly")) expect_null(empty[[f]])
  # and the record built WITH them validates too
  expect_true(validate_measurements_catalog(faces_record()))
})

test_that("the 1.1 record round-trips through the file and the schema", {
  rec <- faces_record()
  expect_identical(rec$schema_version, "1.1")
  d <- withr::local_tempdir()
  p <- write_measurements_catalog(rec, d)
  expect_true(validate_measurements_catalog(p))
  j <- jsonlite::fromJSON(p, simplifyVector = FALSE)
  m <- j$measurements[[which(vapply(j$measurements, function(x) x$key, "") == "temperature")]]
  # the series rows are JSON arrays of four numbers, not objects
  expect_length(m$anomaly$bands[[1]]$series[[1]], 4L)
  expect_identical(m$anomaly$baseline[[1]], 1993L)
  expect_identical(m$face$kind, "scale")
})

test_that("the registries read the same from a directory and from data frames", {
  from_dir <- faces_record()
  dfs <- lapply(stats::setNames(names(calcofi4db:::CC_MEASUREMENT_REGISTRY_FILES),
                                names(calcofi4db:::CC_MEASUREMENT_REGISTRY_FILES)),
                function(nm) as.data.frame(readr::read_csv(
                  file.path(mfz_registries(), calcofi4db:::CC_MEASUREMENT_REGISTRY_FILES[[nm]]),
                  na = "", show_col_types = FALSE, progress = FALSE)))
  expect_identical(mfz_of(faces_record(registries = dfs), "temperature")$why,
                   mfz_of(from_dir, "temperature")$why)
  # an unknown registry name is a stop, not a silent skip
  expect_error(faces_record(registries = list(nonsense = data.frame(key = "x"))),
               "unknown registries")
})

test_that("the check suite still passes on a 1.1 record", {
  con <- new_faces_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  rec <- build_measurements_catalog(
    con, mfz_record(), measurement_type = mfz_measurement_type(),
    category = mfz_categories(), registries = mfz_registries())
  d <- check_measurements_catalog(rec, con, mfz_record(), mfz_measurement_type())
  expect_true(all(d$ok))
})
