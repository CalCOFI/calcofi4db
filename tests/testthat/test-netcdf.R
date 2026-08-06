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

test_that("a single level with NO depth axis is a point collection, not a profile", {
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
  # CF's point feature needs only time and position, so a missing depth axis rules
  # out a profile without ruling out a CF claim altogether
  expect_equal(p$shape, "point")
  expect_equal(p$feature_type, "point")
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
  expect_match(a$cf_scope, "Fully CF")
})

test_that("an undeclared license omits the attribute rather than asserting one", {
  # regression: `license` used to default to "CC-BY 4.0", so every ingest that
  # never declared one — 14 of 16 — shipped netCDFs claiming terms nobody had
  # confirmed. Same rule as valid_min/valid_max: absent, not invented.
  a <- nc_global_atts("calcofi_mets", list(), "v2026.07.30", "profile")
  expect_null(a$license)

  # an empty string is a gap too, not a license
  b <- nc_global_atts("calcofi_mets", list(license = ""), "v2026.07.30", "profile")
  expect_null(b$license)

  # ...and a declared one is passed through verbatim
  d <- nc_global_atts("calcofi_dic", list(license = "CC BY 4.0"),
                      "v2026.07.30", "profile")
  expect_equal(d$license, "CC BY 4.0")
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

# ---- shape rule: four outcomes, not two --------------------------------------
# Every CalCOFI dataset carries a depth on its observations, but only ctd-cast
# has MANY depths per event. Deciding on "one level + a depth axis" alone stamped
# featureType=profile on tows, transects, underway tracks and region pools.

one_level <- function(ds, sample_type = "tow", n = 2) data.frame(
  dataset_key = ds, sample_key = paste0("K", seq_len(n)),
  sample_type = sample_type, parent_sample_key = NA_character_,
  stringsAsFactors = FALSE)

test_that("many depths per instance is a profile; one depth is not", {
  s <- one_level("calcofi_ctd-cast", "cast")
  o <- data.frame(
    dataset_key = "calcofi_ctd-cast",
    sample_key = c("K1", "K1", "K1", "K2", "K2"),
    measurement_type = "temperature_ave",
    depth_min_m = c(0, 10, 20, 0, 10), stringsAsFactors = FALSE)
  con <- new_nc_fixture(s, o); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  p <- plan_dataset_netcdf(con, "calcofi_ctd-cast")
  expect_equal(p$shape, "profile")
  expect_equal(p$feature_type, "profile")
  expect_gt(p$depths_per_instance, 1)
})

test_that("a single-depth net tow is a point collection, not a profile", {
  s <- one_level("cce-lter_zooscan", "tow")
  o <- data.frame(
    dataset_key = "cce-lter_zooscan", sample_key = c("K1", "K2"),
    measurement_type = "abundance", depth_min_m = c(210, 210),
    stringsAsFactors = FALSE)
  con <- new_nc_fixture(s, o); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  p <- plan_dataset_netcdf(con, "cce-lter_zooscan")
  expect_equal(p$shape, "point")
  expect_equal(p$feature_type, "point")
  expect_equal(p$depths_per_instance, 1)
})

test_that("an underway series is a trajectory, not a point collection", {
  # a moving platform is not inferable from row counts: underway data looks
  # exactly like scattered points until you know the ship was under way
  s <- one_level("calcofi_mets", "underway")
  o <- data.frame(
    dataset_key = "calcofi_mets", sample_key = c("K1", "K2"),
    measurement_type = "temperature", depth_min_m = c(3, 3),
    stringsAsFactors = FALSE)
  con <- new_nc_fixture(s, o); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  p <- plan_dataset_netcdf(con, "calcofi_mets")
  expect_equal(p$shape, "trajectory")
  expect_equal(p$feature_type, "trajectory")
})

test_that("a depth-less single level is a point collection; no level at all is not", {
  s <- one_level("cce-lter_euphausiids", "tow")
  o <- data.frame(
    dataset_key = "cce-lter_euphausiids", sample_key = c("K1", "K2"),
    measurement_type = "abundance", depth_min_m = NA_real_,
    stringsAsFactors = FALSE)
  con <- new_nc_fixture(s, o); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  p <- plan_dataset_netcdf(con, "cce-lter_euphausiids")
  expect_false(p$has_depth_axis)
  expect_equal(p$shape, "point")

  # a dataset with no sample rows has nothing to anchor a feature type to
  p0 <- plan_dataset_netcdf(con, "not_a_dataset")
  expect_equal(p0$shape, "groups")
  expect_true(is.na(p0$feature_type))
})

test_that("summarise_netcdf_plan reports the feature type and depth density", {
  s <- one_level("calcofi_mets", "underway")
  o <- data.frame(dataset_key = "calcofi_mets", sample_key = "K1",
                  measurement_type = "temperature", depth_min_m = 3,
                  stringsAsFactors = FALSE)
  con <- new_nc_fixture(s, o); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  r <- summarise_netcdf_plan(plan_dataset_netcdf(con, "calcofi_mets"))
  expect_equal(r$feature_type, "trajectory")
  expect_equal(r$depths_per_instance, 1)
})

# ---- trajectory and point writing --------------------------------------------

test_that("a trajectory puts its coordinates on the observation dimension", {
  skip_if_not_installed("ncdf4")
  # the structural difference from a profile: position varies along the track, so
  # time/lat/lon are per-observation, not per-instance
  w <- data.frame(
    cruise_key = c("C1", "C1", "C2"),
    time       = c(1.5e9, 1.5e9 + 60, 1.6e9),
    latitude   = c(33.0, 33.1, 34.0),
    longitude  = c(-119.0, -119.1, -120.0),
    depth      = c(3, 3, 3),
    temperature = c(15.5, 15.6, 16.5),
    stringsAsFactors = FALSE)
  OBS <- c("time", "latitude", "longitude", "depth")
  f <- nc_tmp()

  d <- nc_profile_def(2, 3, w["cruise_key"], "temperature", obs_cols = OBS)
  nc <- ncdf4::nc_create(f, unname(d$vars), force_v4 = TRUE)
  n <- nc_profile_write(nc, d$vars, w, "cruise_key", "temperature",
                        profile_id_col = "cruise_key", obs_cols = OBS)
  nc_profile_atts(nc, "temperature", profile_vars = "cruise_key",
                  profile_id_var = "cruise_key", feature_type = "trajectory",
                  obs_cols = OBS)
  ncdf4::nc_close(nc)

  expect_equal(n, list(n_profile = 2L, n_obs = 3L))
  o <- ncdf4::nc_open(f); on.exit(ncdf4::nc_close(o))
  expect_equal(as.integer(ncdf4::ncvar_get(o, "rowSize")), c(2L, 1L))
  # 3 positions, one per observation — NOT 2 collapsed onto the instances
  expect_equal(length(ncdf4::ncvar_get(o, "latitude")), 3L)
  expect_equal(as.vector(ncdf4::ncvar_get(o, "latitude")), c(33.0, 33.1, 34.0))
  expect_equal(ncdf4::ncatt_get(o, "cruise_key", "cf_role")$value, "trajectory_id")
  expect_equal(ncdf4::ncatt_get(o, "temperature", "coordinates")$value,
               "time latitude longitude depth")
})

test_that("declaring a column on both dimensions is refused", {
  skip_if_not_installed("ncdf4")
  expect_error(
    nc_profile_def(1, 1, data.frame(latitude = 33), "x",
                   obs_cols = c("latitude", "depth")),
    "BOTH the instance and observation dimension")
})

test_that("a point collection writes flat at the root with no ragged array", {
  skip_if_not_installed("ncdf4")
  w <- data.frame(
    sample_key = c("T1", "T2"), time = c(1.5e9, 1.6e9),
    latitude = c(33, 34), longitude = c(-119, -120), depth = c(210, 210),
    abundance = c(5, 9), stringsAsFactors = FALSE)
  f <- nc_tmp()
  vm <- measurement_var_meta(data.frame(
    measurement_type = "abundance", units = "count/m3",
    description = "specimen abundance", stringsAsFactors = FALSE))

  d_obs <- ncdf4::ncdim_def("obs", "", seq_len(nrow(w)), create_dimvar = FALSE)
  v  <- nc_level_vars("", w, d_obs, var_meta = vm)     # "" => file root
  nc <- ncdf4::nc_create(f, unname(v), force_v4 = TRUE)
  nc_level_put(nc, "", w, v, var_meta = vm)
  nc_profile_atts(nc, "abundance", vm, profile_vars = character(),
                  profile_id_var = NULL, feature_type = "point",
                  obs_cols = c("time", "latitude", "longitude", "depth"))
  ncdf4::nc_close(nc)

  o <- ncdf4::nc_open(f); on.exit(ncdf4::nc_close(o))
  # root, not a group: no slash anywhere in the variable names
  expect_false(any(grepl("/", names(o$var))))
  expect_equal(as.vector(ncdf4::ncvar_get(o, "abundance")), c(5, 9))
  expect_equal(as.vector(ncdf4::ncvar_get(o, "sample_key")), c("T1", "T2"))
  expect_equal(ncdf4::ncatt_get(o, "abundance", "units")$value, "count/m3")
  expect_equal(ncdf4::ncatt_get(o, "depth", "positive")$value, "down")
  # a point collection has no instances, so no rowSize and no cf_role
  expect_false("rowSize" %in% names(o$var))
  expect_false(ncdf4::ncatt_get(o, "sample_key", "cf_role")$hasatt)
})

test_that("a cross-dataset parent is an external link, not a level or a crash", {
  # REGRESSION (real release data): calcofi_dic parents 6 of its bottles onto
  # calcofi_bottle CASTS. sample_key is globally unique, so the parent join
  # resolved to a sample_type ('cast') that calcofi_dic does not have, and the
  # depth walk indexed a name that was not there — "subscript out of bounds",
  # mid-loop over 15 datasets.
  s <- rbind(
    data.frame(dataset_key = "calcofi_bottle", sample_key = "B:cast:1",
               sample_type = "cast", parent_sample_key = NA_character_,
               stringsAsFactors = FALSE),
    data.frame(dataset_key = "calcofi_dic",
               sample_key = c("D:bottle:1", "D:bottle:2"),
               sample_type = "bottle",
               parent_sample_key = c("B:cast:1", NA_character_),
               stringsAsFactors = FALSE))
  con <- new_nc_fixture(s); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  lv <- discover_sample_levels(con, "calcofi_dic")
  expect_equal(nrow(lv), 1L)
  expect_equal(lv$sample_type, "bottle")
  # a root OF THIS FILE: the parent's rows belong to another dataset
  expect_true(is.na(lv$parent_sample_type))
  expect_equal(lv$depth, 0L)
  # the bridge is reported, not silently dropped, and it is not an orphan
  expect_equal(lv$n_external_parent, 1L)
  expect_equal(lv$n_orphan, 0L)
})

test_that("an unresolved parent and an external parent are counted separately", {
  s <- rbind(
    data.frame(dataset_key = "other", sample_key = "O:cast:1",
               sample_type = "cast", parent_sample_key = NA_character_,
               stringsAsFactors = FALSE),
    data.frame(dataset_key = "ds", sample_key = c("K1", "K2", "K3"),
               sample_type = "bottle",
               parent_sample_key = c("O:cast:1", "GONE", NA_character_),
               stringsAsFactors = FALSE))
  con <- new_nc_fixture(s); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  lv <- discover_sample_levels(con, "ds")
  expect_equal(lv$n_external_parent, 1L)
  expect_equal(lv$n_orphan, 1L)
  expect_equal(lv$n, 3L)             # every row still accounted for
})

# ---- widening ----------------------------------------------------------------

wide_con <- function() {
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbWriteTable(con, "obs", data.frame(
    dataset_key = "ds", sample_key = c("K1", "K1", "K1", "K1", "K2"),
    depth_min_m = c(210, 210, 210, 210, 210),
    taxon_key   = c("worms:1", "worms:1", "worms:2", "worms:2", "worms:1"),
    life_stage  = NA_character_,
    latitude    = c(33, 33, 33, 33, 34),
    measurement_type  = c("abundance", "biomass", "abundance", "biomass", "abundance"),
    measurement_value = c(5, 0.5, 9, 0.9, 2),
    stringsAsFactors = FALSE), overwrite = TRUE)
  con
}

test_that("widening keeps one row per taxon, not per sample", {
  # REGRESSION: grouping by sample_key alone collapsed 34,109 zooscan occurrences
  # over 23 taxa into 1,483 rows — 96% of the data gone, with MAX() silently
  # picking one taxon's value and the output still well-formed.
  con <- wide_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  w <- DBI::dbGetQuery(con, obs_wide_sql("ds", c("abundance", "biomass"),
                                         carry = "latitude"))

  expect_equal(nrow(w), 3L)                       # K1/taxon1, K1/taxon2, K2/taxon1
  expect_equal(w$abundance, c(5, 9, 2))
  expect_equal(w$biomass, c(0.5, 0.9, NA))
  expect_equal(w$latitude, c(33, 33, 34))

  # and the event-grain version is exactly the collapse being guarded against
  ev <- DBI::dbGetQuery(con, obs_wide_sql("ds", c("abundance", "biomass"),
                                          grain = "sample_key"))
  expect_equal(nrow(ev), 2L)
})

test_that("the collapse count exposes duplicate rows at the grain", {
  con <- wide_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  w <- DBI::dbGetQuery(con, obs_wide_sql("ds", c("abundance", "biomass"),
                                         count_col = "n_long"))
  # 2 long rows (abundance + biomass) per occurrence, 1 for the singleton
  expect_equal(w$n_long, c(2L, 2L, 1L))
  # more long rows than types would mean MAX() discarded a value
  expect_true(all(w$n_long <= 2L))
})

test_that("a measurement type that cannot be a netCDF variable name is refused", {
  expect_error(obs_wide_sql("ds", "bad name"), "not usable as netCDF variable")
  expect_error(obs_wide_sql("ds", "o'brien"),  "not usable as netCDF variable")
  expect_error(obs_wide_sql("ds", "depth"),    "reserved or grain column")
  expect_error(obs_wide_sql("ds", "sample_key"), "reserved or grain column")
})

test_that("widening with no measurement types still returns the grain", {
  # sio_pic-zooplankton is a tow registry: samples but no observations
  con <- wide_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  w <- DBI::dbGetQuery(con, obs_wide_sql("ds", character(), carry = "latitude"))
  expect_equal(nrow(w), 3L)
  expect_setequal(names(w),
                  c("sample_key", "depth_min_m", "taxon_key", "life_stage", "latitude"))
})
