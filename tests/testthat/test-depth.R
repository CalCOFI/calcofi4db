# depth plausibility: an absolute ceiling that fails, and a seafloor comparison
# that reports. v2026.08.14 shipped a CTD cast with scans at 14,671 m over a
# 101 m seafloor — the pressure VALUE was deleted by its bound, the depth
# COORDINATE derived from it was not, because nothing bounded a coordinate.

dp_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  con
}

# a 10x10-cell raster, 0.01 deg cells, -100 m everywhere except a -1000 m cell at
# the centre and a 3x3 block of land (+50 m) in the NE corner (so a position in
# its middle has NO water cell in its neighbourhood); lon -120.05..-119.95,
# lat 32.95..33.05
dp_tif <- function() {
  skip_if_not_installed("terra")
  r <- terra::rast(nrows = 10, ncols = 10, xmin = -120.05, xmax = -119.95,
                   ymin = 32.95, ymax = 33.05, crs = "EPSG:4326")
  terra::values(r) <- -100
  r[5, 5] <- -1000
  r[1:3, 8:10] <- 50
  f <- tempfile(fileext = ".tif")
  terra::writeRaster(r, f, overwrite = TRUE)
  f
}

dp_fixture <- function(con) {
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key        = c("a:cast:1", "a:bottle:1", "a:cast:2", "a:cast:3", "b:tow:1", "c:cast:9"),
    parent_sample_key = c(NA, "a:cast:1", NA, NA, NA, NA),
    root_sample_key   = c("a:cast:1", "a:cast:1", "a:cast:2", "a:cast:3", "b:tow:1", "c:cast:9"),
    dataset_key       = c("a", "a", "a", "a", "b", "c"),
    sample_type       = c("cast", "bottle", "cast", "cast", "tow", "cast"),
    cruise_key        = "2000-01-XX",
    longitude = c(-120.03, -120.03, -120.005, -119.965, -120.03, -110),  # last: off raster
    latitude  = c(33.03, 33.03, 33.005, 33.035, 33.03, 33),
    depth_min_m = c(NA, 105, NA, NA, 0, NA),
    depth_max_m = c(NA, 105, NA, NA, 200, NA),
    stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "obs", data.frame(
    sample_key  = c("a:bottle:1", "a:cast:2", "a:cast:2", "a:cast:3", "c:cast:9"),
    dataset_key = c("a", "a", "a", "a", "c"),
    depth_min_m = c(105, 900, 14671, 20, 7000),
    depth_max_m = c(105, 900, 14671, 20, 7000),
    stringsAsFactors = FALSE))
}

test_that("check_depth_bounds() flags the ceiling, negatives and NaN per dataset", {
  con <- dp_con(); dp_fixture(con)
  DBI::dbExecute(con, "INSERT INTO obs VALUES ('b:tow:1', 'b', -5, 'NaN'::DOUBLE)")
  d <- check_depth_bounds(con)
  expect_true(all(c("table", "dataset_key", "depth_col", "n_above", "status") %in% names(d)))
  a <- d[d$table == "obs" & d$dataset_key == "a" & d$depth_col == "depth_min_m", ]
  expect_equal(a$n_above, 1)                 # 14671
  expect_equal(a$v_max, 14671)
  expect_identical(a$status, "out_of_range")
  cc <- d[d$table == "obs" & d$dataset_key == "c" & d$depth_col == "depth_min_m", ]
  expect_equal(cc$n_above, 1)                # 7000 > 6500
  b_min <- d[d$table == "obs" & d$dataset_key == "b" & d$depth_col == "depth_min_m", ]
  expect_equal(b_min$n_below, 1)
  b_max <- d[d$table == "obs" & d$dataset_key == "b" & d$depth_col == "depth_max_m", ]
  expect_equal(b_max$n_nan, 1)
  s <- d[d$table == "sample" & d$dataset_key == "a", ]
  expect_true(all(s$status == "ok"))
  # worst first
  expect_identical(d$status[1], "out_of_range")
  expect_equal(CC_DEPTH_MAX_M, 6500)
})

test_that("sample_seafloor() extracts depth and the deepest 3x3 cell, land -> 0, off-raster -> NA", {
  con <- dp_con(); dp_fixture(con)
  sf <- sample_seafloor(con, dp_tif())
  expect_setequal(names(sf), c("sample_key", "seafloor_depth_m", "seafloor_max3x3_m"))
  r <- function(k) sf[sf$sample_key == k, ]
  expect_equal(r("a:cast:1")$seafloor_depth_m, 100)
  expect_equal(r("a:cast:1")$seafloor_max3x3_m, 100)
  # next to the 1000 m cell: neighbourhood max sees it, the cell itself does not
  expect_equal(r("a:cast:2")$seafloor_max3x3_m, 1000)
  expect_gt(r("a:cast:2")$seafloor_depth_m, 100)   # bilinear leans toward the hole
  expect_equal(r("a:cast:3")$seafloor_depth_m, 0)  # land clamped, not negative
  expect_equal(r("a:cast:3")$seafloor_max3x3_m, 0)  # every neighbour is land too
  expect_true(is.na(r("c:cast:9")$seafloor_depth_m))
})

test_that("check_depth_vs_seafloor() reports the deepest attributed depth over the 3x3 max + tolerance", {
  con <- dp_con(); dp_fixture(con)
  v <- check_depth_vs_seafloor(con, dp_tif(), tolerance_m = 10)
  # a:cast:1 -> 105 m via its bottle over a 100 m floor: within tolerance
  expect_false("a:cast:1" %in% v$sample_key)
  # a:cast:2 -> 14671 via obs over 1000 m neighbourhood: 13671 m over
  expect_equal(v$sample_key[1], "a:cast:2")
  expect_equal(v$excess_m[1], 13671)
  # a:cast:3 -> 20 m on land (floor 0): on_land, and a finding
  expect_true(v$on_land[v$sample_key == "a:cast:3"])
  # b:tow:1 -> 200 m over 100: finding; c:cast:9 off raster -> unknown, not a finding
  expect_true("b:tow:1" %in% v$sample_key)
  expect_false("c:cast:9" %in% v$sample_key)
  s <- attr(v, "summary")
  expect_equal(s$n_over[s$dataset_key == "a"], 2)
  expect_equal(s$n_unknown[s$dataset_key == "c"], 1)
  expect_equal(s$max_excess_m[s$dataset_key == "a"], 13671)
  # a wider tolerance swallows the tow and the 20 m on-land sample, not the cast
  v2 <- check_depth_vs_seafloor(con, dp_tif(), tolerance_m = 150)
  expect_equal(v2$sample_key, "a:cast:2")
})

test_that("add_sample_seafloor() appends seafloor_depth_m without disturbing other columns", {
  con <- dp_con(); dp_fixture(con)
  before <- DBI::dbListFields(con, "sample")
  add_sample_seafloor(con, dp_tif())
  after <- DBI::dbListFields(con, "sample")
  expect_equal(after, c(before, "seafloor_depth_m"))
  got <- DBI::dbGetQuery(con, "SELECT sample_key, seafloor_depth_m FROM sample ORDER BY 1")
  expect_equal(nrow(got), 6)
  expect_equal(got$seafloor_depth_m[got$sample_key == "a:cast:1"], 100)
  expect_true(is.na(got$seafloor_depth_m[got$sample_key == "c:cast:9"]))
  # idempotent: a second call replaces, not duplicates, the column
  add_sample_seafloor(con, dp_tif())
  expect_equal(DBI::dbListFields(con, "sample"), after)
})

test_that("sample_seafloor() stamps each sample once when positions differ past the 15th digit", {
  # regression: unique() kept two positions 1e-16 apart distinct, merge() compared
  # them as 15-digit strings and matched both to every sample -> 4,855 samples
  # doubled in v2026.08.25
  con <- dp_con()
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = c("a:cast:1", "a:bottle:1", "a:bottle:2"),
    longitude  = c(-120.03, -120.03 + 1e-16 * 128, -120.03),   # differs in the 16th digit only
    latitude   = c(33.03, 33.03, 33.03),
    stringsAsFactors = FALSE))
  sf <- sample_seafloor(con, dp_tif())
  expect_equal(nrow(sf), 3)
  expect_false(anyDuplicated(sf$sample_key) > 0)
  expect_equal(sf$seafloor_depth_m, rep(100, 3))
  add_sample_seafloor(con, dp_tif())
  expect_equal(DBI::dbGetQuery(con, "SELECT count(*) n FROM sample")$n, 3)
})

test_that("check_core_pk_unique() passes on unique keys and fails the release on a duplicate", {
  con <- dp_con(); dp_fixture(con)
  DBI::dbWriteTable(con, "cruise", data.frame(cruise_key = c("2000-01-XX", "2000-02-XX")))
  res <- check_core_pk_unique(con, c("sample", "cruise", "taxon"))   # taxon absent: skipped
  expect_setequal(res$table, c("sample", "cruise"))
  expect_true(all(res$n_dup == 0))
  DBI::dbExecute(con, "INSERT INTO sample SELECT * FROM sample WHERE sample_key = 'a:cast:1'")
  expect_error(check_core_pk_unique(con, c("sample", "cruise")), "sample\\(sample_key\\): 1 duplicate")
})

test_that("check_seafloor_nulls classifies every NULL by cause and gates only inside-tile ones", {
  con <- get_duckdb_con(":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "CREATE TABLE sample AS SELECT * FROM (VALUES
    ('a', 'ds1', -120.0,   33.0, NULL),          -- inside the tile, NULL  -> the gate
    ('b', 'ds1',   NULL,   33.0, NULL),          -- no coordinates
    ('c', 'ds2', -120.0,    NULL, NULL),         -- no coordinates
    ('d', 'ds2', 'NaN'::DOUBLE, 33.0, NULL),     -- NaN coordinate
    ('e', 'ds2',  -85.0,   33.0, NULL),          -- east of the source tile
    ('f', 'ds2', -120.0,   33.0, 1234.0)         -- sampled fine: not listed
  ) t(sample_key, dataset_key, longitude, latitude, seafloor_depth_m)")
  x <- check_seafloor_nulls(con, source_bbox = c(-180, 0, -90, 90))
  expect_setequal(x$cause, c("inside_tile_null", "no_coordinates", "nan_coordinate", "outside_source_tile"))
  expect_equal(x$n[x$cause == "no_coordinates"], 2)
  expect_equal(x$n[x$cause == "inside_tile_null"], 1)
  expect_identical(attr(x, "n_inside_null"), 1)
  expect_match(x$datasets[x$cause == "inside_tile_null"], "ds1")
})

test_that("sample_seafloor lets a /vsicurl/ path past the existence guard", {
  con <- get_duckdb_con(":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "CREATE TABLE sample (sample_key TEXT, longitude DOUBLE, latitude DOUBLE)")
  # a plain missing local path errors in OUR guard...
  expect_error(sample_seafloor(con, "/no/such/file.tif"), "GEBCO tif not found")
  # ...but a /vsicurl/ (or https://) source must reach terra: whatever an unreachable
  # URL raises, it is not the guard's message
  msg <- tryCatch(suppressWarnings(sample_seafloor(
    con, "/vsicurl/https://storage.googleapis.com/definitely-not-a-bucket-xyz/nope.tif")),
    error = function(e) conditionMessage(e))
  expect_false(grepl("GEBCO tif not found", paste(msg, collapse = " ")))
})
