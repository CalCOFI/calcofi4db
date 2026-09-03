# browser-shaped release objects (plan D4): built over a tiny synthetic core, asserted row by row
ex_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  load_duckdb_extension(con, "spatial")
  con
}
# the two calcofi4r fragments, pinned here as text so this package never depends on calcofi4r
QUAL_OK <- "COALESCE(NOT ((o.dataset_key = 'calcofi_bottle' AND regexp_replace(o.measurement_qual, '\\.0+$', '') IN ('8', '9'))), TRUE)"
DENSITY <- paste(readLines(test_path("fixtures", "density_sql.txt")), collapse = "\n")

ex_fixture <- function(con) {
  # a site (root) with one tow and one net; a bottle cast (root) with two bottles; a CUFES underway root
  DBI::dbExecute(con, "CREATE TABLE sample AS SELECT * FROM (VALUES
    ('ich:site:1', 'site',   NULL,          'ich:site:1', 'swfsc_ichthyo',  'st90-ln90', NULL, '2019-04-33UD', 1, 32.9, -117.3, TIMESTAMP '2019-04-02 22:00', NULL, NULL, NULL, NULL, 800.0),
    ('ich:tow:1',  'tow',    'ich:site:1',  'ich:site:1', 'swfsc_ichthyo',  'st90-ln90', NULL, '2019-04-33UD', 1, 32.9, -117.3, TIMESTAMP '2019-04-02 22:10', 0.0, 210.0, 'CB', NULL, 800.0),
    ('ich:net:1',  'net',    'ich:tow:1',   'ich:site:1', 'swfsc_ichthyo',  'st90-ln90', NULL, '2019-04-33UD', 1, 32.9, -117.3, TIMESTAMP '2019-04-02 22:10', 0.0, 210.0, 'CB', NULL, 800.0),
    ('btl:cast:1', 'cast',   NULL,          'btl:cast:1', 'calcofi_bottle', 'st90-ln90', NULL, '2019-04-33UD', 1, 32.9, -117.3, TIMESTAMP '2019-04-02 23:00', 0.0, 500.0, NULL, NULL, 800.0),
    ('btl:b:1',    'bottle', 'btl:cast:1',  'btl:cast:1', 'calcofi_bottle', 'st90-ln90', NULL, '2019-04-33UD', 1, 32.9, -117.3, TIMESTAMP '2019-04-02 23:00', 10.0, 10.0, NULL, NULL, 800.0),
    ('btl:b:2',    'bottle', 'btl:cast:1',  'btl:cast:1', 'calcofi_bottle', 'st90-ln90', NULL, '2019-04-33UD', 1, 32.9, -117.3, TIMESTAMP '2019-04-02 23:00', 250.0, 250.0, NULL, NULL, 800.0),
    ('cuf:u:1',    'underway', NULL,        'cuf:u:1',    'swfsc_cufes',    NULL,        NULL, '2019-04-33UD', NULL, 33.5, -118.0, TIMESTAMP '2019-04-03 01:00', 0.0, 0.0, NULL, NULL, NULL),
    ('nan:1',      'site',   NULL,          'nan:1',      'swfsc_cufes',    NULL,        NULL, NULL, NULL, 'NaN'::DOUBLE, -118.0, TIMESTAMP '2019-04-03 02:00', 0.0, 0.0, NULL, NULL, NULL)
    ) t(sample_key, sample_type, parent_sample_key, root_sample_key, dataset_key, grid_key, site_key, cruise_key, order_occ,
        latitude, longitude, datetime, depth_min_m, depth_max_m, tow_type, data_stage, seafloor_depth_m)")
  DBI::dbExecute(con, "CREATE TABLE sample_measurement AS SELECT * FROM (VALUES
    (1, 'ich:net:1', 'swfsc_ichthyo', 'std_haul_factor', 2.0, NULL), (2, 'ich:net:1', 'swfsc_ichthyo', 'prop_sorted', 0.5, NULL),
    (3, 'ich:net:1', 'swfsc_ichthyo', 'volume_sampled', 100.0, NULL)
    ) t(sample_measurement_id, sample_key, dataset_key, measurement_type, measurement_value, measurement_qual)")
  DBI::dbExecute(con, "CREATE TABLE measurement_type AS SELECT * FROM (VALUES
    ('abundance', 'count'), ('temperature', 'degC'), ('sardine_eggs', 'count')) t(measurement_type, units)")
  # hex_id at res 10 for (32.9, -117.3) computed once with h3; hard-coded so the test needs no extension
  # column order is the released obs's (OBS_VIEW_COLUMNS): the view tests compare positionally
  DBI::dbExecute(con, "CREATE TABLE obs AS SELECT * FROM (VALUES
    (1, 'bio', 'swfsc_ichthyo',  'ich:net:1', 'st90-ln90', '2019-04-33UD', 32.9, -117.3, TIMESTAMP '2019-04-02 22:10', NULL::DOUBLE, NULL::DOUBLE, 'worms:217452', 'larva', 'abundance', 10.0, NULL, NULL::DOUBLE, 623333527607443455::UBIGINT),
    (2, 'env', 'calcofi_bottle', 'btl:b:1',   'st90-ln90', '2019-04-33UD', 32.9, -117.3, TIMESTAMP '2019-04-02 23:00', 10.0, 10.0, NULL, NULL, 'temperature', 15.5, '6', NULL, 623333527607443455::UBIGINT),
    (3, 'env', 'calcofi_bottle', 'btl:b:2',   'st90-ln90', '2019-04-33UD', 32.9, -117.3, TIMESTAMP '2019-04-02 23:00', 250.0, 250.0, NULL, NULL, 'temperature', 8.1, '8', NULL, 623333527607443455::UBIGINT),
    (4, 'bio', 'swfsc_cufes',    'cuf:u:1',   NULL,        '2019-04-33UD', 33.5, -118.0, TIMESTAMP '2019-04-03 01:00', 0.0, 0.0, 'worms:217452', 'egg', 'sardine_eggs', 3.0, NULL, NULL, NULL)
    ) t(obs_id, realm, dataset_key, sample_key, grid_key, cruise_key, latitude, longitude, datetime, depth_min_m, depth_max_m, taxon_key, life_stage,
        measurement_type, measurement_value, measurement_qual, measurement_prec, hex_id)")
  DBI::dbExecute(con, "CREATE TABLE spatial AS SELECT spatial_key, id, layer, name, ST_GeomFromText(wkt) AS geom FROM (VALUES
    ('mpa:1', 1, 'Marine Protected Areas', 'Near site',   'POLYGON((-117.4 32.8, -117.2 32.8, -117.2 33.0, -117.4 33.0, -117.4 32.8))'),
    ('mpa:2', 2, 'Marine Protected Areas', 'Far away',    'POLYGON((-120 35, -119 35, -119 36, -120 36, -120 35))'),
    ('eez:1', 1, '200NM EEZ',              'Everything',  'POLYGON((-125 30, -115 30, -115 36, -125 36, -125 30))'),
    ('lim:1', 1, '12NM Territorial Sea',   'A boundary',  'LINESTRING(-125 30, -115 36)'),
    ('port:1', 1, 'CA Ports',              'A port',      'POINT(-117.3 32.9)')
    ) t(spatial_key, id, layer, name, wkt)")
}

test_that("h3_parent_sql() agrees with the h3 extension", {
  con <- ex_con()
  ok <- tryCatch({ load_duckdb_extension(con, "h3", from = "community"); TRUE }, error = function(e) FALSE)
  skip_if_not(ok, "h3 community extension not available")
  DBI::dbExecute(con, "CREATE TABLE c AS SELECT h3_latlng_to_cell(lat, lng, 10)::UBIGINT AS h
    FROM (VALUES (32.9, -117.3), (34.4, -120.5), (30.0, -125.0), (-10.0, 100.0)) t(lat, lng)")
  d <- dplyr::bind_rows(lapply(0:9, function(r) DBI::dbGetQuery(con, glue::glue(
    "SELECT {r} AS r, count(*) AS n, count(*) FILTER (WHERE {h3_parent_sql('h', r)} = h3_cell_to_parent(h, {r})::UBIGINT) AS n_ok FROM c"))))
  expect_equal(d$n_ok, d$n)
  expect_equal(nrow(d), 10)
})

test_that("h3_parent_sql() is the documented bit arithmetic", {
  expect_equal(h3_parent_sql("hex7", 5), "(((hex7 & ~(15::UBIGINT << 52)) | (5::UBIGINT << 52)) | ((1::UBIGINT << 30) - 1))")
  expect_error(h3_parent_sql("h", 16))
})

test_that("build_sample_root() numbers roots densely and deterministically", {
  con <- ex_con(); ex_fixture(con)
  expect_equal(build_sample_root(con), 4)
  r <- DBI::dbGetQuery(con, "SELECT root_id, root_sample_key, tow_type, seafloor_depth_m FROM sample_root ORDER BY root_id")
  expect_equal(r$root_id, 1:4)
  expect_equal(r$root_sample_key, sort(c("ich:site:1", "btl:cast:1", "cuf:u:1", "nan:1")))
  expect_equal(r$seafloor_depth_m[r$root_sample_key == "ich:site:1"], 800)
  # same input, same ids
  build_sample_root(con)
  expect_equal(DBI::dbGetQuery(con, "SELECT root_id FROM sample_root WHERE root_sample_key = 'ich:site:1'")$root_id,
               r$root_id[r$root_sample_key == "ich:site:1"])
})

test_that("build_obs_slim() carries root_id, the net's span and effort, qual_ok, hex7 and the densities", {
  con <- ex_con(); ex_fixture(con); build_sample_root(con)
  expect_equal(build_obs_slim(con, "bio", QUAL_OK, DENSITY), 2)
  expect_equal(build_obs_slim(con, "env", QUAL_OK, DENSITY), 2)
  b <- DBI::dbGetQuery(con, "SELECT * FROM obs_bio ORDER BY obs_id")
  e <- DBI::dbGetQuery(con, "SELECT * FROM obs_env ORDER BY obs_id")
  expect_identical(names(b), names(e))   # one schema for both realms
  # the ichthyo larva: depth from the net (obs had none), effort from its own net, C-B is areal + volumetric
  expect_equal(b$depth_min_m[1], 0); expect_equal(b$depth_max_m[1], 210); expect_equal(b$depth_bin[1], 0)
  expect_equal(b$tow_type[1], "CB"); expect_equal(b$std_haul_factor[1], 2); expect_equal(b$prop_sorted[1], 0.5)
  expect_equal(b$density_per_10m2[1], 40); expect_equal(b$density_per_1000m3[1], 200); expect_equal(b$effort_class[1], "count_with_effort")
  expect_equal(b$year[1], 2019L); expect_equal(b$quarter[1], 2L); expect_true(b$qual_ok[1])
  expect_equal(b$root_id[1], DBI::dbGetQuery(con, "SELECT root_id FROM sample_root WHERE root_sample_key = 'ich:site:1'")$root_id)
  # CUFES: no effort in the release -> raw count, no densities, no grid, hex7 NULL because hex_id was NULL
  expect_equal(b$effort_class[2], "raw_count_no_effort"); expect_true(is.na(b$density_per_10m2[2])); expect_true(is.na(b$hex7[2]))
  # hex7 is the res-7 parent of the res-10 cell: resolution field reads 7, the three finer digits are 7
  h <- DBI::dbGetQuery(con, "SELECT (hex7 >> 52) & 15 AS res, hex7 & 511 AS low9 FROM obs_bio WHERE obs_id = 1")
  expect_equal(h$res, 7); expect_equal(h$low9, 511)
  # env: bottle flag 8 is not ok, 6 is; densities NULL, effort_class other_unit; depth_bin from its own depth
  expect_equal(e$qual_ok, c(TRUE, FALSE)); expect_equal(e$depth_bin, c(10L, 250L))
  expect_true(all(is.na(e$density_per_10m2))); expect_equal(unique(e$effort_class), "other_unit")
  expect_equal(e$units, c("degC", "degC")); expect_equal(e$value, c(15.5, 8.1))
  # D-S1 (3.31.0): the pair is a strict superset of obs under a name mapping — the observation's
  # own sample_key (the net / bottle, not just the root), measurement_prec and the res-10 hex_id
  expect_equal(b$sample_key, c("ich:net:1", "cuf:u:1")); expect_equal(e$sample_key, c("btl:b:1", "btl:b:2"))
  expect_true(all(is.na(b$measurement_prec))); expect_true("measurement_prec" %in% names(e))
  expect_equal(b$hex_id[1], DBI::dbGetQuery(con, "SELECT hex_id FROM obs WHERE obs_id = 1")$hex_id)
  expect_true(is.na(b$hex_id[2]))
  expect_true(all(c("sample_key", "measurement_prec", "hex_id", "hex7") %in% names(e)))
})

test_that("obs_view_sql() reconstructs obs from the pair: 18 columns in order, tokens or tables", {
  con <- ex_con(); ex_fixture(con); build_sample_root(con)
  build_obs_slim(con, "bio", QUAL_OK, DENSITY); build_obs_slim(con, "env", QUAL_OK, DENSITY)
  # the stored form carries tokens; every resolver substitutes its own reader
  sql <- obs_view_sql()
  expect_equal(release_view_tables(sql), c("obs_bio", "obs_env"))
  expect_match(sql, "'bio' AS realm"); expect_match(sql, "'env' AS realm"); expect_match(sql, "value AS measurement_value")
  expect_equal(substitute_view_tables(sql), obs_view_sql('"obs_bio"', '"obs_env"'))
  expect_equal(substitute_view_tables(sql, function(t) paste0("read_parquet('", t, ".parquet')")),
               obs_view_sql("read_parquet('obs_bio.parquet')", "read_parquet('obs_env.parquet')"))
  expect_false(grepl("{{", substitute_view_tables(sql), fixed = TRUE))
  v <- DBI::dbGetQuery(con, paste("SELECT * FROM (", substitute_view_tables(sql), ") ORDER BY obs_id"))
  expect_identical(names(v), OBS_VIEW_COLUMNS)
  expect_identical(names(v), DBI::dbListFields(con, "obs"))
  expect_equal(v$obs_id, 1:4); expect_equal(v$realm, c("bio", "env", "env", "bio"))
  expect_equal(v$measurement_value, c(10, 15.5, 8.1, 3))
  # row for row the view IS obs, except the documented depth fallback: the ichthyo larva had no depth
  # in obs and carries its net's 0-210 m span through the pair
  diff_a <- DBI::dbGetQuery(con, paste("SELECT * FROM (", substitute_view_tables(sql), ") EXCEPT SELECT * FROM obs"))
  diff_b <- DBI::dbGetQuery(con, paste("SELECT * FROM obs EXCEPT SELECT * FROM (", substitute_view_tables(sql), ")"))
  expect_equal(nrow(diff_a), 1); expect_equal(nrow(diff_b), 1)
  expect_equal(diff_a$obs_id, 1); expect_equal(diff_a$depth_min_m, 0); expect_equal(diff_a$depth_max_m, 210)
  expect_true(is.na(diff_b$depth_min_m))
})

test_that("check_obs_pair_parity() passes on the pair and names the group that breaks it", {
  con <- ex_con(); ex_fixture(con); build_sample_root(con)
  build_obs_slim(con, "bio", QUAL_OK, DENSITY); build_obs_slim(con, "env", QUAL_OK, DENSITY)
  p <- check_obs_pair_parity(con)
  expect_equal(nrow(p), 3); expect_true(all(p$ok))
  expect_equal(p$realm, c("bio", "bio", "env")); expect_equal(p$dataset_key, c("swfsc_cufes", "swfsc_ichthyo", "calcofi_bottle"))
  expect_equal(p$n_obs, c(1, 1, 2)); expect_equal(p$n_pair, p$n_obs); expect_equal(p$n_id_pair, p$n_obs)
  expect_equal(p$n_depth_filled, c(0, 1, 0)); expect_equal(p$n_depth_changed, c(0, 0, 0))
  # a lost row
  DBI::dbExecute(con, "CREATE TABLE obs_env_short AS SELECT * FROM obs_env WHERE obs_id <> 3")
  expect_error(check_obs_pair_parity(con, env = "obs_env_short"), "env/calcofi_bottle \\(obs 2, pair 1")
  # a changed value (same rows, same ids)
  DBI::dbExecute(con, "CREATE TABLE obs_env_val AS SELECT * REPLACE (value + 1 AS value) FROM obs_env")
  expect_error(check_obs_pair_parity(con, env = "obs_env_val"), "values differ")
  # a non-NULL depth rewritten
  DBI::dbExecute(con, "CREATE TABLE obs_env_dep AS SELECT * REPLACE (depth_min_m + 5 AS depth_min_m) FROM obs_env")
  expect_error(check_obs_pair_parity(con, env = "obs_env_dep"), "2 depths changed")
  # a group on one side only
  DBI::dbExecute(con, "CREATE TABLE obs_bio_ds AS SELECT * REPLACE ('x_y' AS dataset_key) FROM obs_bio")
  expect_error(check_obs_pair_parity(con, bio = "obs_bio_ds"), "absent")
})

test_that("build_sample_spatial() is exact per root sample, chunked per layer, and refuses duplicates", {
  con <- ex_con(); ex_fixture(con); build_sample_root(con)
  s <- build_sample_spatial(con)
  expect_equal(s$layer, c("12NM Territorial Sea", "200NM EEZ", "CA Ports", "Marine Protected Areas"))
  expect_equal(s$n_polys, c(0L, 1L, 0L, 2L))   # lines and points never hold a sample
  s <- s[s$n_polys > 0, ]
  # the NaN-latitude root is excluded from the points; the EEZ holds the other three; the site and the
  # cast (same position) are in the near MPA, the underway root is not
  expect_equal(s$n_memberships, c(3L, 2L))
  m <- DBI::dbGetQuery(con, "SELECT root_sample_key, spatial_key, spatial_name FROM sample_spatial WHERE layer = 'Marine Protected Areas' ORDER BY 1")
  expect_equal(m$root_sample_key, c("btl:cast:1", "ich:site:1")); expect_equal(unique(m$spatial_key), "mpa:1"); expect_equal(unique(m$spatial_name), "Near site")
  expect_true(all(c("root_id", "root_sample_key") %in% DBI::dbListFields(con, "sample_spatial")))
  # a single layer on request
  s1 <- build_sample_spatial(con, layers = "Marine Protected Areas", tbl = "ss1")
  expect_equal(nrow(s1), 1); expect_equal(s1$n_memberships, 2L)
})

test_that("build_coverage() is the cube behind the first paint, and deterministic", {
  con <- ex_con(); ex_fixture(con); build_sample_root(con)
  cv <- build_coverage(con, "v2026.09.01")
  expect_equal(cv$version, "v2026.09.01")
  expect_equal(cv$datasets$dataset_key, c("calcofi_bottle", "swfsc_cufes", "swfsc_ichthyo"))
  expect_equal(cv$datasets$n_obs, c(2L, 1L, 1L)); expect_equal(cv$datasets$n_roots, c(1L, 1L, 1L))
  expect_equal(length(cv$stations), 1); expect_equal(cv$stations[[1]]$grid_key, "st90-ln90")
  expect_equal(cv$stations[[1]]$datasets$dataset_key, c("calcofi_bottle", "swfsc_ichthyo"))
  expect_equal(cv$years$year, c(2019L, 2019L, 2019L))
  expect_null(cv$station_months)
  cs <- build_coverage_stations(con, "v2026.09.01")
  expect_equal(length(cs$stations), 1); st <- cs$stations[[1]]
  expect_equal(st$grid_key, "st90-ln90"); expect_equal(sapply(st$datasets, `[[`, "dataset_key"), c("calcofi_bottle", "swfsc_ichthyo"))
  expect_equal(st$datasets[[1]]$years, list(c(2019, 2)))    # two bottles in 2019
  expect_equal(st$datasets[[1]]$months[4], 2L); expect_equal(sum(st$datasets[[1]]$months), 2L)
  expect_equal(st$datasets[[2]]$n_obs, 1L)
  expect_equal(cv$variables$measurement_type, c("temperature", "sardine_eggs", "abundance"))
  expect_equal(cv$variables$depth_max_m[cv$variables$measurement_type == "temperature"], 250)
  j1 <- jsonlite::toJSON(cv, auto_unbox = TRUE, digits = NA); j2 <- jsonlite::toJSON(build_coverage(con, "v2026.09.01"), auto_unbox = TRUE, digits = NA)
  expect_identical(j1, j2)
  expect_false(grepl("generated", j1))
})

test_that("build_coverage() carries taxa[] and the registry's category / variable (explorer UI plan D14)", {
  con <- ex_con(); ex_fixture(con); build_sample_root(con)
  # a taxon reference + the two new registry columns; the test fixture's measurement_type has neither by default
  DBI::dbExecute(con, "CREATE TABLE taxon AS SELECT * FROM (VALUES ('worms:217452', 'Sardinops sagax', 'Pacific sardine', 'Species', 'Teleostei')) t(taxon_key, scientific_name, common_name, rank, class)")
  DBI::dbExecute(con, "ALTER TABLE measurement_type ADD COLUMN category VARCHAR"); DBI::dbExecute(con, "ALTER TABLE measurement_type ADD COLUMN variable VARCHAR")
  DBI::dbExecute(con, "UPDATE measurement_type SET category = 'Physical Oceanography', variable = 'temperature' WHERE measurement_type = 'temperature'")
  cv <- build_coverage(con, "v2026.09.01")
  # variables[] gains the two fields; a type without them is NA, never an error
  v <- cv$variables
  expect_equal(v$category[v$measurement_type == "temperature"], "Physical Oceanography")
  expect_equal(v$variable[v$measurement_type == "temperature"], "temperature")
  expect_true(is.na(v$category[v$measurement_type == "abundance"]))
  # taxa[]: one row for the sardine, its two datasets nested with n_obs each, both life stages, names from the reference
  expect_equal(length(cv$taxa), 1); tx <- cv$taxa[[1]]
  expect_equal(tx$taxon_key, "worms:217452"); expect_equal(tx$common_name, "Pacific sardine"); expect_equal(tx$class, "Teleostei"); expect_equal(tx$rank, "Species")
  expect_equal(tx$n_obs, 2L); expect_equal(tx$year_min, 2019L); expect_equal(tx$year_max, 2019L)
  expect_equal(unlist(tx$life_stages), c("egg", "larva"))
  expect_equal(tx$datasets$dataset_key, c("swfsc_cufes", "swfsc_ichthyo")); expect_equal(tx$datasets$n_obs, c(1L, 1L))
  # deterministic, and it serializes (life_stages as an array, datasets as rows)
  j1 <- jsonlite::toJSON(cv, auto_unbox = TRUE, digits = NA); j2 <- jsonlite::toJSON(build_coverage(con, "v2026.09.01"), auto_unbox = TRUE, digits = NA)
  expect_identical(j1, j2); expect_true(grepl('"life_stages":\\["egg","larva"\\]', j1))
  # an obs without taxon_key / a connection without taxon: taxa is empty, nothing errors
  DBI::dbExecute(con, "DROP TABLE taxon")
  expect_equal(build_coverage(con, "v2026.09.01")$taxa[[1]]$common_name, NA_character_)
  DBI::dbExecute(con, "ALTER TABLE obs DROP COLUMN taxon_key")
  expect_equal(length(build_coverage(con, "v2026.09.01")$taxa), 0)
})

test_that("build_spatial_layers joins the registry with the release spatial table (D23)", {
  con <- ex_con(); ex_fixture(con)
  DBI::dbExecute(con, "CREATE TABLE sample_spatial AS SELECT * FROM (VALUES
    (1, 'ich:site:1', 'Marine Protected Areas', 'mpa:1', 'Near site'),
    (2, 'btl:cast:1', 'Marine Protected Areas', 'mpa:1', 'Near site'),
    (3, 'ich:site:1', 'National Marine Sanctuaries', 'nms:1', 'Channel Islands')
    ) t(root_id, root_sample_key, layer, spatial_key, spatial_name)")
  csv <- withr::local_tempfile(fileext = ".csv")
  writeLines(c(
    "dataset_id,dataset_group,layer,group,geom_type,filter_expr,line_color,fill_color,line_width,fill_opacity,default_visible,name_field,description,attribution",
    "ca_mpas,ca_marine_protected_areas,Marine Protected Areas,Protected Areas,polygon,,#388e3c,#a5d6a7,1,0.2,FALSE,fullname,MPAs,CDFW",
    "eez,noaa_maritime_boundaries,200NM EEZ,Maritime Zones,line,\"[\"\"==\"\", [\"\"get\"\", \"\"eez\"\"], 1]\",#1565c0,,2,0,TRUE,region,EEZ,NOAA"), csv)
  x <- build_spatial_layers(con, csv, "vTEST", "https://example.org/_spatial/", built = "2026-08-24")
  expect_identical(x$version, "vTEST")
  expect_length(x$layers, 2)
  mpa <- x$layers[[1]]; eez <- x$layers[[2]]
  expect_identical(mpa$id, "ca_mpas")
  expect_gte(mpa$n_features, 1)                       # the fixture has MPA polygons
  expect_true(is.list(mpa$names) && length(mpa$names) >= 1)
  expect_identical(mpa$n_memberships, 2L)             # two distinct roots inside mpa:1
  expect_null(mpa$filter)
  expect_length(mpa$bbox, 4)
  # the filter expression rides through VERBATIM as parsed JSON
  expect_identical(eez$filter, list("==", list("get", "eez"), 1L))
  expect_true(eez$default_visible); expect_false(mpa$default_visible)
  # a registry layer absent from `spatial` warns and carries zero features
  expect_identical(suppressWarnings(eez$n_features >= 0), TRUE)
  # names_max: above the cap the by-name palette is off (NULL), not truncated
  y <- suppressWarnings(build_spatial_layers(con, csv, "vTEST", "u/", names_max = 0))
  expect_null(y$layers[[1]]$names)
})
