# Fixtures for the netCDF planner. Each builds a minimal core `sample`/`obs` so a
# single rule is asserted in isolation — the hierarchy walk, the shape decision,
# and the failure modes each get their own case.

new_nc_fixture <- function(sample_rows, obs_rows = NULL, attr_rows = NULL,
                           smea_rows = NULL) {
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbWriteTable(con, "sample", sample_rows, overwrite = TRUE)
  if (is.null(obs_rows)) {
    obs_rows <- data.frame(
      dataset_key = character(), sample_key = character(),
      measurement_type = character(), depth_min_m = numeric())
  }
  DBI::dbWriteTable(con, "obs", obs_rows, overwrite = TRUE)
  if (!is.null(attr_rows)) DBI::dbWriteTable(con, "obs_attribute", attr_rows, overwrite = TRUE)
  if (!is.null(smea_rows)) DBI::dbWriteTable(con, "sample_measurement", smea_rows, overwrite = TRUE)
  con
}

# site -> tow -> net, the ichthyo shape
nested_samples <- function(ds = "swfsc_ichthyo") data.frame(
  dataset_key       = ds,
  sample_key        = c("S1", "S2", "T1", "T2", "N1", "N2", "N3"),
  sample_type       = c("site", "site", "tow", "tow", "net", "net", "net"),
  parent_sample_key = c(NA, NA, "S1", "S2", "T1", "T1", "T2"),
  stringsAsFactors  = FALSE)

test_that("discover_sample_levels recovers nesting order from the adjacency list", {
  con <- new_nc_fixture(nested_samples())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  lv <- discover_sample_levels(con, "swfsc_ichthyo")

  expect_equal(lv$sample_type, c("site", "tow", "net"))     # topological order
  expect_equal(lv$depth, c(0L, 1L, 2L))
  expect_equal(lv$parent_sample_type, c(NA, "site", "tow"))
  expect_equal(lv$n, c(2L, 2L, 3L))
  expect_equal(sum(lv$n_orphan), 0L)
})

test_that("discover_sample_levels returns a zero-row tibble for an unknown dataset", {
  con <- new_nc_fixture(nested_samples())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  expect_equal(nrow(discover_sample_levels(con, "does_not_exist")), 0L)
})

test_that("an unresolved parent is counted as an orphan, not dropped", {
  s <- nested_samples()
  s$parent_sample_key[s$sample_key == "N3"] <- "T_MISSING"
  con <- new_nc_fixture(s)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  lv <- discover_sample_levels(con, "swfsc_ichthyo")
  expect_equal(lv$n_orphan[lv$sample_type == "net"], 1L)
  # the level still reports all 3 rows: an orphan must not silently shrink a level
  expect_equal(lv$n[lv$sample_type == "net"], 3L)
  # and the majority vote still finds the real parent
  expect_equal(lv$parent_sample_type[lv$sample_type == "net"], "tow")
})

test_that("a mislabelled row cannot invent a level (parent is a majority vote)", {
  s <- nested_samples()
  # one 'net' hangs off a site instead of a tow; 2 of 3 still say tow
  s$parent_sample_key[s$sample_key == "N3"] <- "S1"
  con <- new_nc_fixture(s)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  lv <- discover_sample_levels(con, "swfsc_ichthyo")
  expect_equal(lv$parent_sample_type[lv$sample_type == "net"], "tow")
})

test_that("a self-referential level is not treated as its own parent", {
  s <- data.frame(
    dataset_key       = "x",
    sample_key        = c("A1", "A2"),
    sample_type       = c("cast", "cast"),
    parent_sample_key = c(NA, "A1"),       # within-level chain
    stringsAsFactors  = FALSE)
  con <- new_nc_fixture(s)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  lv <- discover_sample_levels(con, "x")
  expect_equal(nrow(lv), 1L)
  expect_true(is.na(lv$parent_sample_type))
  expect_equal(lv$depth, 0L)              # must not recurse forever
})

test_that("one level plus a depth axis plans as a CF profile", {
  s <- data.frame(
    dataset_key = "calcofi_ctd-cast", sample_key = c("C1", "C2"),
    sample_type = "cast", parent_sample_key = NA_character_,
    stringsAsFactors = FALSE)
  o <- data.frame(
    dataset_key = "calcofi_ctd-cast", sample_key = c("C1", "C1", "C2"),
    measurement_type = c("temperature_ave", "btl_ammonium", "temperature_ave"),
    depth_min_m = c(10, 229, 10), stringsAsFactors = FALSE)
  con <- new_nc_fixture(s, o)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  p <- plan_dataset_netcdf(con, "calcofi_ctd-cast")
  expect_equal(p$shape, "profile")
  expect_equal(p$feature_type, "profile")
  expect_true(p$has_depth_axis)
  # the union across rows, not whatever the first row happened to carry
  expect_equal(p$measurement_types, c("btl_ammonium", "temperature_ave"))
})

test_that("multiple levels plan as netCDF-4 groups, with attribute groups split by type", {
  o <- data.frame(
    dataset_key = "swfsc_ichthyo", sample_key = c("N1", "N2"),
    measurement_type = "abundance", depth_min_m = c(NA_real_, NA_real_),
    stringsAsFactors = FALSE)
  a <- data.frame(
    dataset_key = "swfsc_ichthyo", sample_key = c("N1", "N1"),
    measurement_type = c("body_length", "stage"), stringsAsFactors = FALSE)
  m <- data.frame(
    dataset_key = "swfsc_ichthyo", sample_key = "N1",
    measurement_type = "volume_sampled", stringsAsFactors = FALSE)
  con <- new_nc_fixture(nested_samples(), o, a, m)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  p <- plan_dataset_netcdf(con, "swfsc_ichthyo")
  expect_equal(p$shape, "groups")
  expect_true(is.na(p$feature_type))
  # body_length is mm and stage is an ordinal code: one variable cannot hold both,
  # so each attribute type must become its own group
  expect_equal(p$attribute_types, c("body_length", "stage"))
  expect_equal(p$effort_types, "volume_sampled")
})

test_that("a single level with NO depth axis is not mistaken for a profile", {
  s <- data.frame(
    dataset_key = "calcofi_phytoplankton", sample_key = "R1",
    sample_type = "region_pool", parent_sample_key = NA_character_,
    stringsAsFactors = FALSE)
  o <- data.frame(
    dataset_key = "calcofi_phytoplankton", sample_key = "R1",
    measurement_type = "abundance", depth_min_m = NA_real_,
    stringsAsFactors = FALSE)
  con <- new_nc_fixture(s, o)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  p <- plan_dataset_netcdf(con, "calcofi_phytoplankton")
  expect_false(p$has_depth_axis)
  expect_equal(p$shape, "groups")   # no depth axis -> not a CF profile
})

test_that("summarise_netcdf_plan renders one row per dataset", {
  con <- new_nc_fixture(nested_samples())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  s <- summarise_netcdf_plan(plan_dataset_netcdf(con, "swfsc_ichthyo"))
  expect_equal(nrow(s), 1L)
  expect_equal(s$levels, "site -> tow -> net")
  expect_equal(s$n_levels, 3L)
})

# ---- writers -----------------------------------------------------------------
# Every writer test round-trips through a real file: a variable that was DEFINED
# but never correctly written reads back as fill, and only re-opening catches it.

nc_tmp <- function() {
  f <- tempfile(fileext = ".nc")
  withr::defer(unlink(f), envir = parent.frame())
  f
}

# two profiles, 3 depth levels: C1 has 2, C2 has 1
wide_fx <- function() data.frame(
  profile_id = c("C1", "C1", "C2"),
  cruise_key = c("2018-04-33RR", "2018-04-33RR", "2018-04-33RR"),
  time       = c(1.5e9, 1.5e9, 1.6e9),
  latitude   = c(33, 33, 34),
  longitude  = c(-119, -119, -120),
  depth      = c(10, 20, 10),
  temperature_ave = c(15.5, 14.0, 16.5),
  stringsAsFactors = FALSE)

PROF_COLS <- c("profile_id", "cruise_key", "time", "latitude", "longitude")

test_that("measurement_var_meta falls back on blank cells instead of emitting them", {
  mt <- data.frame(
    measurement_type = c("temperature_ave", "mystery"),
    units            = c("degree_C", NA),
    description      = c("average temperature", ""),
    standard_name    = c("sea_water_temperature", NA),
    is_canonical     = c(TRUE, FALSE),
    stringsAsFactors = FALSE)
  vm <- measurement_var_meta(mt)

  expect_equal(vm$temperature_ave$units, "degree_C")
  expect_equal(vm$temperature_ave$standard_name, "sea_water_temperature")
  expect_true(vm$temperature_ave$canonical)
  # a blank unit must be "" (not "NA"), and a blank description falls back to the
  # type name — long_name = "NA" is worse than a plain name
  expect_equal(vm$mystery$units, "")
  expect_equal(vm$mystery$long_name, "mystery")
  expect_true(is.na(vm$mystery$standard_name))
})

test_that("measurement_var_meta tolerates a registry with only the type column", {
  vm <- measurement_var_meta(data.frame(measurement_type = "abundance",
                                        stringsAsFactors = FALSE))
  expect_equal(vm$abundance$units, "")
  expect_equal(vm$abundance$long_name, "abundance")
  expect_false(vm$abundance$canonical)
})

test_that("a CF profile round-trips with a consistent ragged array", {
  skip_if_not_installed("ncdf4")
  w <- wide_fx(); f <- nc_tmp()
  vm <- measurement_var_meta(data.frame(
    measurement_type = "temperature_ave", units = "degree_C",
    description = "average temperature", standard_name = "sea_water_temperature",
    stringsAsFactors = FALSE))

  d <- nc_profile_def(2, 3, w[PROF_COLS], "temperature_ave", vm)
  nc <- ncdf4::nc_create(f, unname(d$vars), force_v4 = TRUE)
  n <- nc_profile_write(nc, d$vars, w, PROF_COLS, "temperature_ave")
  nc_profile_atts(nc, "temperature_ave", vm, PROF_COLS)
  ncdf4::nc_close(nc)

  expect_equal(n, list(n_profile = 2L, n_obs = 3L))
  o <- ncdf4::nc_open(f); on.exit(ncdf4::nc_close(o))
  expect_equal(as.integer(ncdf4::ncvar_get(o, "rowSize")), c(2L, 1L))
  expect_equal(sum(ncdf4::ncvar_get(o, "rowSize")), o$dim$obs$len)
  expect_equal(as.vector(ncdf4::ncvar_get(o, "profile_id")), c("C1", "C2"))
  expect_equal(as.vector(ncdf4::ncvar_get(o, "depth")), c(10, 20, 10))
  expect_equal(as.vector(ncdf4::ncvar_get(o, "temperature_ave")), c(15.5, 14.0, 16.5))
  # profile-level values come from each profile's FIRST row, stored once
  expect_equal(as.vector(ncdf4::ncvar_get(o, "latitude")), c(33, 34))
  expect_equal(length(ncdf4::ncvar_get(o, "latitude")), 2L)
  # the DSG attributes are what make it a profile dataset, not a table
  expect_equal(ncdf4::ncatt_get(o, "profile_id", "cf_role")$value, "profile_id")
  expect_equal(ncdf4::ncatt_get(o, "rowSize", "sample_dimension")$value, "obs")
  expect_equal(ncdf4::ncatt_get(o, "depth", "positive")$value, "down")
  expect_equal(ncdf4::ncatt_get(o, "temperature_ave", "units")$value, "degree_C")
  expect_equal(ncdf4::ncatt_get(o, "temperature_ave", "standard_name")$value,
               "sea_water_temperature")
  expect_equal(ncdf4::ncatt_get(o, "temperature_ave", "coordinates")$value,
               "time latitude longitude depth")
})

test_that("a chunked profile write equals the single-shot write", {
  skip_if_not_installed("ncdf4")
  # this is the obs_ctd_full path: the same file assembled one partition at a
  # time. An off-by-one in the offsets leaves fill values that read as real
  # missing data, so assert the two paths agree value-for-value.
  w <- wide_fx()
  build <- function(chunks) {
    f <- tempfile(fileext = ".nc")
    d <- nc_profile_def(2, 3, w[PROF_COLS], "temperature_ave")
    nc <- ncdf4::nc_create(f, unname(d$vars), force_v4 = TRUE)
    op <- 1L; oo <- 1L
    for (ch in chunks) {
      n <- nc_profile_write(nc, d$vars, ch, PROF_COLS, "temperature_ave",
                            start_profile = op, start_obs = oo)
      op <- op + n$n_profile; oo <- oo + n$n_obs
    }
    ncdf4::nc_close(nc)
    o <- ncdf4::nc_open(f); on.exit({ ncdf4::nc_close(o); unlink(f) })
    list(rs  = as.integer(ncdf4::ncvar_get(o, "rowSize")),
         id  = as.vector(ncdf4::ncvar_get(o, "profile_id")),
         dp  = as.vector(ncdf4::ncvar_get(o, "depth")),
         tv  = as.vector(ncdf4::ncvar_get(o, "temperature_ave")),
         lat = as.vector(ncdf4::ncvar_get(o, "latitude")))
  }
  one <- build(list(w))
  two <- build(list(w[1:2, ], w[3, , drop = FALSE]))
  expect_equal(two, one)
  expect_equal(two$rs, c(2L, 1L))
})

test_that("non-contiguous profile rows are refused, not silently mis-indexed", {
  skip_if_not_installed("ncdf4")
  w <- wide_fx()[c(1, 3, 2), ]     # C1, C2, C1 — interleaved
  f <- nc_tmp()
  d <- nc_profile_def(2, 3, w[PROF_COLS], "temperature_ave")
  nc <- ncdf4::nc_create(f, unname(d$vars), force_v4 = TRUE)
  on.exit(ncdf4::nc_close(nc))

  expect_error(
    nc_profile_write(nc, d$vars, w, PROF_COLS, "temperature_ave"),
    "not contiguous")
})

test_that("a profile write refuses to truncate an over-long identifier", {
  skip_if_not_installed("ncdf4")
  w <- wide_fx(); w$profile_id <- paste0(w$profile_id, strrep("x", 70))
  f <- nc_tmp()
  d <- nc_profile_def(2, 3, w[PROF_COLS], "temperature_ave")
  nc <- ncdf4::nc_create(f, unname(d$vars), force_v4 = TRUE)
  on.exit(ncdf4::nc_close(nc))

  expect_error(
    nc_profile_write(nc, d$vars, w, PROF_COLS, "temperature_ave"),
    "exceed strlen")
})

test_that("a missing wide column is named rather than written as fill", {
  skip_if_not_installed("ncdf4")
  w <- wide_fx(); f <- nc_tmp()
  d <- nc_profile_def(2, 3, w[PROF_COLS], "temperature_ave")
  nc <- ncdf4::nc_create(f, unname(d$vars), force_v4 = TRUE)
  on.exit(ncdf4::nc_close(nc))

  expect_error(
    nc_profile_write(nc, d$vars, w[setdiff(names(w), "temperature_ave")],
                     PROF_COLS, "temperature_ave"),
    "missing column\\(s\\): temperature_ave")
})

test_that("valid_min/valid_max are emitted only when the registry carries them", {
  skip_if_not_installed("ncdf4")
  w <- wide_fx(); f <- nc_tmp()
  vm <- measurement_var_meta(data.frame(
    measurement_type = "temperature_ave", units = "degree_C",
    valid_min = 0, valid_max = 40, stringsAsFactors = FALSE))
  d <- nc_profile_def(2, 3, w[PROF_COLS], "temperature_ave", vm)
  nc <- ncdf4::nc_create(f, unname(d$vars), force_v4 = TRUE)
  nc_profile_write(nc, d$vars, w, PROF_COLS, "temperature_ave")
  nc_profile_atts(nc, "temperature_ave", vm, PROF_COLS)
  ncdf4::nc_close(nc)

  o <- ncdf4::nc_open(f); on.exit(ncdf4::nc_close(o))
  expect_equal(ncdf4::ncatt_get(o, "temperature_ave", "valid_min")$value, 0)
  expect_equal(ncdf4::ncatt_get(o, "temperature_ave", "valid_max")$value, 40)

  # and absent when unset — an invented range licenses a reader to drop real data
  f2 <- nc_tmp()
  vm2 <- measurement_var_meta(data.frame(measurement_type = "temperature_ave",
                                         stringsAsFactors = FALSE))
  d2 <- nc_profile_def(2, 3, w[PROF_COLS], "temperature_ave", vm2)
  nc2 <- ncdf4::nc_create(f2, unname(d2$vars), force_v4 = TRUE)
  nc_profile_write(nc2, d2$vars, w, PROF_COLS, "temperature_ave")
  nc_profile_atts(nc2, "temperature_ave", vm2, PROF_COLS)
  ncdf4::nc_close(nc2)
  o2 <- ncdf4::nc_open(f2); on.exit(ncdf4::nc_close(o2), add = TRUE)
  expect_false(ncdf4::ncatt_get(o2, "temperature_ave", "valid_min")$hasatt)
})

test_that("nc_level_vars types each column and only links a parent when given one", {
  skip_if_not_installed("ncdf4")
  df <- data.frame(key = "T1", n_int = 3L, val = 1.5, stringsAsFactors = FALSE)
  d  <- ncdf4::ncdim_def("tow_n",  "", seq_len(1), create_dimvar = FALSE)
  dp <- ncdf4::ncdim_def("site_n", "", seq_len(1), create_dimvar = FALSE)

  root <- nc_level_vars("tow", df, d)
  expect_null(root[["__parent_index"]])
  expect_equal(root$key$prec, "char")
  expect_equal(root$n_int$prec, "integer")
  expect_equal(root$val$prec, "double")
  expect_equal(root$key$name, "tow/key")          # slash => real netCDF-4 group

  child <- nc_level_vars("tow", df, d, dp, 1L)
  expect_equal(child[["__parent_index"]]$name, "tow/parent_index")
  # a parent_index without a parent dimension is not a link, so none is defined
  expect_null(nc_level_vars("tow", df, d, NULL, 1L)[["__parent_index"]])
})

test_that("a nested level round-trips with its parent link documented", {
  skip_if_not_installed("ncdf4")
  f <- nc_tmp()
  site <- data.frame(site_id = c("S1", "S2"), stringsAsFactors = FALSE)
  tow  <- data.frame(tow_id = c("T1", "T2", "T3"), volume_sampled = c(10, 20, 30),
                     stringsAsFactors = FALSE)
  parent_ix <- c(1L, 1L, 2L)
  vm <- measurement_var_meta(data.frame(
    measurement_type = "volume_sampled", units = "m3",
    description = "volume filtered", stringsAsFactors = FALSE))

  d_s <- ncdf4::ncdim_def("site_n", "", seq_len(2), create_dimvar = FALSE)
  d_t <- ncdf4::ncdim_def("tow_n",  "", seq_len(3), create_dimvar = FALSE)
  v_s <- nc_level_vars("site", site, d_s, var_meta = vm)
  v_t <- nc_level_vars("tow",  tow,  d_t, d_s, parent_ix, vm)
  nc  <- ncdf4::nc_create(f, c(unname(v_s), unname(v_t)), force_v4 = TRUE)
  nc_level_put(nc, "site", site, v_s, var_meta = vm)
  nc_level_put(nc, "tow",  tow,  v_t, parent_ix, vm, "site")
  ncdf4::nc_close(nc)

  o <- ncdf4::nc_open(f); on.exit(ncdf4::nc_close(o))
  expect_setequal(unique(sub("/.*$", "", names(o$var))), c("site", "tow"))
  expect_equal(as.vector(ncdf4::ncvar_get(o, "tow/volume_sampled")), c(10, 20, 30))
  expect_equal(as.integer(ncdf4::ncvar_get(o, "tow/parent_index")), parent_ix)
  # effort is stored ONCE per tow — 3 values, not one per child row
  expect_equal(length(ncdf4::ncvar_get(o, "tow/volume_sampled")), 3L)
  expect_equal(ncdf4::ncatt_get(o, "tow/volume_sampled", "units")$value, "m3")
  expect_equal(ncdf4::ncatt_get(o, "tow/volume_sampled", "long_name")$value,
               "volume filtered")
  expect_equal(ncdf4::ncatt_get(o, "tow/parent_index", "instance_dimension")$value,
               "site")
  expect_match(ncdf4::ncatt_get(o, "tow/parent_index", "comment")$value,
               "double-count")
})

test_that("an NA string is written as empty, not as the literal 'NA'", {
  skip_if_not_installed("ncdf4")
  f <- nc_tmp()
  df <- data.frame(k = c("A", NA), stringsAsFactors = FALSE)
  d  <- ncdf4::ncdim_def("n", "", seq_len(2), create_dimvar = FALSE)
  v  <- nc_level_vars("lvl", df, d)
  nc <- ncdf4::nc_create(f, unname(v), force_v4 = TRUE)
  nc_level_put(nc, "lvl", df, v)
  ncdf4::nc_close(nc)

  o <- ncdf4::nc_open(f); on.exit(ncdf4::nc_close(o))
  expect_equal(as.vector(ncdf4::ncvar_get(o, "lvl/k")), c("A", ""))
})

test_that("nc_global_atts derives its text from dataset_meta", {
  dm <- list(dataset_name = "CalCOFI METS (Underway TSG/Meteorology)",
             description = "Shipboard underway thermosalinograph\n(TSG) data.",
             citation_main = "CalCOFI. Underway (METS) Data.",
             coverage_temporal = "2004-01 to 2022-11")
  a <- nc_global_atts("calcofi_mets", dm, "v2026.07.30", "profile",
                      workflow_url = "https://calcofi.io/w.html")

  expect_match(a$title, "^CalCOFI METS")
  # folded YAML keeps newlines; a multi-line attribute renders as a broken blob
  expect_false(grepl("\n", a$summary))
  expect_equal(a$featureType, "profile")
  expect_equal(a$cdm_data_type, "Profile")
  expect_equal(a$db_release, "v2026.07.30")
  expect_equal(a$citation, "CalCOFI. Underway (METS) Data.")
  expect_equal(a$time_coverage, "2004-01 to 2022-11")
  expect_equal(a$references, "https://calcofi.io/w.html")
  expect_equal(a$license, "CC-BY 4.0")
  expect_match(a$cf_scope, "Fully CF")
})

test_that("nc_global_atts falls back to the dataset_key and marks the nested shape", {
  a <- nc_global_atts("swfsc_ichthyo", list(), "v2026.07.30", "groups")
  expect_match(a$title, "^swfsc_ichthyo")
  expect_null(a$featureType)          # no CF feature type covers the nesting
  expect_match(a$cf_scope, "netCDF-4 groups")
  expect_null(a$citation)             # absent, not "NA"
})

test_that("date_created is the release date so a rebuild is byte-identical", {
  # a wall-clock date_created puts a fresh timestamp in every build, so no
  # rebuild ever matches an earlier release's sha256 and the publisher's
  # "bytes written once" check silently degrades to "always re-upload"
  a <- nc_global_atts("x", list(), "v2026.07.30", "profile")
  b <- nc_global_atts("x", list(), "v2026.07.30", "profile")
  expect_equal(a$date_created, "2026-07-30T00:00:00Z")
  expect_identical(a, b)

  # an unparseable release omits the attribute rather than inventing a date
  expect_null(nc_global_atts("x", list(), "working", "profile")$date_created)
})

test_that("nc_global_atts lets a per-file override win", {
  dm <- list(dataset_name = "Derived name", description = "derived summary")
  a <- nc_global_atts("x", modifyList(dm, list(title = "Authored title")),
                      "v2026.07.30", "profile",
                      cf_scope = "custom scope",
                      extra = list(comment = "hand-written"))
  expect_equal(a$title, "Authored title")
  expect_equal(a$cf_scope, "custom scope")
  expect_equal(a$comment, "hand-written")
})
