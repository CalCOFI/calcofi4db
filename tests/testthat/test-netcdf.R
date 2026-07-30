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
