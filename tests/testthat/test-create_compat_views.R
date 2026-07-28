# the per-dataset event tables are rebuildable from the core: the adjacency list
# (sample_type + parent_sample_key) carries the hierarchy, the namespaced
# sample_key carries the source id, and sample_measurement carries the effort
# columns in long form. These tests pin the round-trip exactly.

test_that("net rebuilds from the core with its hierarchy and effort intact", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  build_sample_reference(con, datasets = "swfsc_ichthyo")
  append_sample_measurement(con, ich_sample_measurement_sql)

  # capture the source before the VIEW shadows the table
  src <- DBI::dbGetQuery(con, "
    SELECT net_uuid, tow_uuid, standard_haul_factor, volume_sampled,
           prop_sorted, smallplankton, totalplankton
    FROM net ORDER BY net_uuid")

  made <- create_compat_views(con, "swfsc_ichthyo")
  expect_true(all(c("site", "tow", "net") %in% made))

  got <- DBI::dbGetQuery(con, "
    SELECT net_uuid, tow_uuid, standard_haul_factor, volume_sampled,
           prop_sorted, smallplankton, totalplankton
    FROM net ORDER BY net_uuid")

  expect_equal(nrow(got), 3L)
  expect_equal(got$net_uuid, src$net_uuid)
  expect_equal(got$tow_uuid, src$tow_uuid)          # hierarchy from parent_sample_key
  expect_equal(got$volume_sampled, src$volume_sampled)
  expect_equal(got$standard_haul_factor, src$standard_haul_factor)
  expect_equal(got$smallplankton, src$smallplankton)
  expect_equal(got$totalplankton, src$totalplankton)
})

test_that("tow rebuilds with its parent site and gear code", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  build_sample_reference(con, datasets = "swfsc_ichthyo")
  src <- DBI::dbGetQuery(con,
    "SELECT tow_uuid, site_uuid, tow_type_key, datetime_start_utc FROM tow")

  create_compat_views(con, "swfsc_ichthyo")
  got <- DBI::dbGetQuery(con,
    "SELECT tow_uuid, site_uuid, tow_type_key, datetime_start_utc FROM tow")

  expect_equal(got$tow_uuid, src$tow_uuid)
  expect_equal(got$site_uuid, src$site_uuid)        # site<-tow containment survives
  expect_equal(got$tow_type_key, src$tow_type_key)  # gear code from sample.tow_type
  expect_equal(got$datetime_start_utc, src$datetime_start_utc)
})

test_that("the three-level site -> tow -> net chain still joins after rebuild", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  build_sample_reference(con, datasets = "swfsc_ichthyo")
  append_sample_measurement(con, ich_sample_measurement_sql)
  create_compat_views(con, "swfsc_ichthyo")

  # the join that consumers actually write, run entirely against VIEWs
  j <- DBI::dbGetQuery(con, "
    SELECT s.site_uuid, s.cruise_key, COUNT(DISTINCT t.tow_uuid) n_tows,
           COUNT(DISTINCT n.net_uuid) n_nets, SUM(n.volume_sampled) vol
    FROM net n JOIN tow t USING (tow_uuid) JOIN site s USING (site_uuid)
    GROUP BY 1, 2")
  expect_equal(nrow(j), 1L)
  expect_equal(j$n_tows, 1L)
  expect_equal(j$n_nets, 3L)
  expect_equal(j$vol, 330)                          # 100 + 110 + 120
  expect_equal(j$cruise_key, "2020-01-NODC")
})

test_that("a measurement triple rebuilds from obs", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE zoodb_sample AS
    SELECT 1::INTEGER sample_id, 'st1-ln1'::VARCHAR grid_key, '090.0 060.0'::VARCHAR site_key,
           '1998-02-33JD'::VARCHAR cruise_key,
           32.8::DOUBLE latitude, -117.9::DOUBLE longitude,
           TIMESTAMP '1998-02-14 21:00:00' datetime_start_utc,
           0::DOUBLE min_depth_m, 210::DOUBLE max_depth_m")
  DBI::dbExecute(con, "CREATE TABLE zoodb_measurement AS
    SELECT 1::INTEGER measurement_id, 1::INTEGER sample_id, 7::INTEGER taxon_id,
           'zooplankton_abundance'::VARCHAR measurement_type, 12.5::DOUBLE measurement_value
    UNION ALL SELECT 2, 1, 7, 'zooplankton_biomass_carbon', 0.8")

  emit_core_tables(con, "cce-lter_zoodb", taxa = FALSE)
  create_compat_views(con, "cce-lter_zoodb")

  got <- DBI::dbGetQuery(con,
    "SELECT sample_id, measurement_type, measurement_value FROM zoodb_measurement
     ORDER BY measurement_type")
  expect_equal(nrow(got), 2L)
  expect_equal(got$sample_id, c("1", "1"))   # recovered from the namespaced key
  expect_equal(got$measurement_type,
               c("zooplankton_abundance", "zooplankton_biomass_carbon"))
  expect_equal(got$measurement_value, c(12.5, 0.8))
})

test_that("create_compat_views() is a no-op for a dataset with no spec", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))
  expect_equal(create_compat_views(con, "pic_zooplankton"), character())
})

# promoted core columns -------------------------------------------------------
# site_key and order_occ are event-level and cross-dataset (site_key appears on
# 13 of 18 source event tables), so they live on `sample` rather than being lost
# to consolidation. bottom_depth_m is an event property -> sample_measurement.

test_that("sample carries site_key and order_occ, inherited down the hierarchy", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  build_sample_reference(con, datasets = "swfsc_ichthyo")
  s <- DBI::dbGetQuery(con,
    "SELECT sample_type, site_key, order_occ FROM sample ORDER BY sample_type")

  # site carries them from the source; tow and net inherit from their site, the
  # same way grid_key/cruise_key already do
  expect_equal(sort(unique(s$site_key)), "090.0 060.0")
  expect_equal(sort(unique(s$order_occ)), 3L)
  expect_setequal(s$sample_type, c("site", "tow", "net", "net", "net"))
})

test_that("site rebuilds with site_key and order_occ after promotion", {
  con <- new_ichthyo_fixture()
  on.exit(close_duckdb(con))

  build_sample_reference(con, datasets = "swfsc_ichthyo")
  src <- DBI::dbGetQuery(con,
    "SELECT site_uuid, site_key, order_occ, cruise_key FROM site")

  create_compat_views(con, "swfsc_ichthyo")
  got <- DBI::dbGetQuery(con,
    "SELECT site_uuid, site_key, order_occ, cruise_key FROM site")

  expect_equal(got$site_uuid,  src$site_uuid)
  expect_equal(got$site_key,   src$site_key)    # was lost before promotion
  expect_equal(got$order_occ,  as.integer(src$order_occ))
  expect_equal(got$cruise_key, src$cruise_key)
})

test_that("bottom_depth_m becomes a sample_measurement on the cast", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE casts AS
    SELECT 5::INTEGER cast_id, 'st1-ln1'::VARCHAR grid_key, '090.0 060.0'::VARCHAR site_key,
           '2018-04-33RR'::VARCHAR cruise_key, 2::SMALLINT order_occ,
           33.0::DOUBLE latitude, -119.0::DOUBLE longitude,
           TIMESTAMP '2018-04-05 12:00:00' datetime_start_utc, 1350.0::DOUBLE bottom_depth_m")
  DBI::dbExecute(con, "CREATE TABLE cast_condition AS
    SELECT 1::INTEGER cast_condition_id, 5.0::DOUBLE cast_id,
           'wind_speed'::VARCHAR condition_type, 12.0::DOUBLE condition_value")

  build_sample_reference(con, datasets = "calcofi_bottle")
  append_sample_measurement(con, calcofi4db:::.sample_measurement_arm_sql("calcofi_bottle"))

  sm <- DBI::dbGetQuery(con,
    "SELECT sample_key, measurement_type, measurement_value FROM sample_measurement
     ORDER BY measurement_type")
  expect_equal(sm$measurement_type, c("bottom_depth", "wind_speed"))
  expect_equal(sm$measurement_value, c(1350, 12))
  # both attach to the cast event, not the bottle
  expect_true(all(sm$sample_key == "calcofi_bottle:cast:5"))

  # and cast_condition must NOT gain a phantom bottom_depth row on rebuild
  create_compat_views(con, "calcofi_bottle")
  cc <- DBI::dbGetQuery(con, "SELECT condition_type FROM cast_condition")
  expect_equal(cc$condition_type, "wind_speed")
})
