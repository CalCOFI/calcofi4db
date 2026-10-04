# the rebuilt CalCOFI grid (CalCOFI/workflows#130): how a position is keyed (assign_grid_key()),
# what the released `grid` is (build_grid_reference()), and the crosswalk from the previous grid
# (grid_crosswalk(), build_grid_crosswalk(), check_grid_crosswalk()). One fixture per rule.

# assign_grid_key: the rule, on two unit squares ----

test_that("assign_grid_key: a point on a shared edge takes the key that sorts first, every time", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  withr::defer(close_duckdb(con))
  load_duckdb_extension(con, "spatial")
  two_square_grid(con)
  #                     interior of each   the shared edge   a shared vertex
  k <- key_in_duckdb(con, c(0.5, 1.5,        1, 1,             1), c(0.5, 0.5, 0.5, 0.25, 1))
  expect_identical(k, c("st20-ln90", "st100-ln90", "st100-ln90", "st100-ln90", "st100-ln90"))
  # regression: the rule used to be `LIMIT 1` with no order, which returned whichever cell the
  # join met first. Reverse the table: the answer must not move
  DBI::dbExecute(con, "CREATE OR REPLACE TABLE grid AS SELECT * FROM grid ORDER BY grid_key DESC")
  expect_identical(key_in_duckdb(con, c(1, 1), c(0.5, 0.25)), c("st100-ln90", "st100-ln90"))
})

test_that("assign_grid_key: a point outside every cell, or with no geometry, gets NULL", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  withr::defer(close_duckdb(con))
  load_duckdb_extension(con, "spatial")
  two_square_grid(con)
  DBI::dbExecute(con, "CREATE TABLE ev AS
    SELECT 1 AS id, ST_Point(5, 5) AS geom UNION ALL
    SELECT 2, NULL UNION ALL
    SELECT 3, ST_Point(0.5, 0.5)")
  st <- suppressMessages(assign_grid_key(con, "ev"))
  expect_identical(DBI::dbGetQuery(con, "SELECT grid_key FROM ev ORDER BY id")$grid_key,
                   c(NA, NA, "st20-ln90"))
  expect_equal(st$n[st$status == "in_grid"], 1)
  expect_equal(st$n[st$status == "not_in_grid"], 2)
  # a second call overwrites, it does not append a column or keep a stale key
  DBI::dbExecute(con, "UPDATE ev SET geom = ST_Point(1.5, 0.5) WHERE id = 3")
  suppressMessages(assign_grid_key(con, "ev"))
  expect_identical(DBI::dbGetQuery(con, "SELECT grid_key FROM ev WHERE id = 3")$grid_key, "st100-ln90")
})

test_that("assign_grid_key: DuckDB and R agree on interiors, edges and vertices of two squares", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf")
  con <- get_duckdb_con(":memory:")
  withr::defer(close_duckdb(con))
  load_duckdb_extension(con, "spatial")
  two_square_grid(con)
  g <- sf::st_sf(grid_key = c("st20-ln90", "st100-ln90"),
                 geom = sf::st_sfc(ll_rect(0, 1, 0, 1), ll_rect(1, 2, 0, 1)))
  x <- c(seq(-0.25, 2.25, by = 0.125), rep(1, 9), 0, 1, 2)
  y <- c(rep(0.5, 21), seq(0, 1, by = 0.125), 0, 0, 1)
  expect_identical(key_in_duckdb(con, x, y), key_in_r(x, y, g))
})

# the released grid: build_grid_reference() on the rebuilt cells ----

test_that("build_grid_reference: the rebuilt grid is one cell per key, with the columns consumers read", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid()
  con <- get_duckdb_con(":memory:")
  withr::defer(close_duckdb(con))
  n <- build_grid_reference(con, cc_grid = g$cc_grid, cc_grid_ctrs = g$cc_grid_ctrs)
  expect_identical(as.integer(n), nrow(g$cc_grid))
  expect_identical(
    DBI::dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'grid' ORDER BY ordinal_position")$column_name,
    c("grid_key", "station", "line", "shore", "pattern", "spacing", "zone", "area_km2", "geom", "geom_ctr"))
  d <- DBI::dbGetQuery(con, "
    SELECT grid_key, station, line, shore, pattern, spacing, zone, area_km2,
           ST_GeometryType(geom)::VARCHAR AS gtype, ST_IsValid(geom) AS valid,
           ST_Intersects(geom_ctr, geom) AS site_in_cell
    FROM grid")
  expect_false(anyDuplicated(d$grid_key) > 0)
  expect_setequal(d$grid_key, g$cc_grid$grid_key)                 # the key the package states is the key derived
  # a station cell is one polygon, but for the one the build reports (21 km2 of Tomales Bay that
  # no station cell reaches by water); a kept cell is the previous cell, in as many pieces as it was
  src <- g$cc_grid$sta_source[match(d$grid_key, g$cc_grid$grid_key)]
  expect_identical(d$grid_key[src == "official" & d$gtype != "POLYGON"], "st53-ln60")
  expect_identical(sum(src == "previous" & d$gtype == "MULTIPOLYGON"), 13L)
  expect_true(all(d$valid))
  expect_true(all(d$site_in_cell))                                # every site (station) lies in its own cell
  expect_identical(d$zone, paste0(d$shore, "-", d$pattern))
  expect_setequal(unique(d$shore), c("nearshore", "offshore"))
  expect_setequal(unique(d$pattern), c("standard", "extended", "historical"))
  expect_setequal(unique(d$spacing), c(5L, 10L, 20L))
  expect_true(all(d$area_km2 > 0))
  expect_true(all(grepl("_hist$", d$grid_key) == (d$pattern == "historical")))
  # decimal stations and lines survive into the key and the columns
  r <- d[d$grid_key == "st26.4-ln93.4", ]
  expect_identical(nrow(r), 1L)
  expect_equal(c(r$station, r$line), c(26.4, 93.4))
  expect_identical(c(r$shore, r$pattern, r$zone), c("nearshore", "standard", "nearshore-standard"))
})

test_that("build_grid_reference: stops when the key it derives is not the key cc_grid carries", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid()
  con <- get_duckdb_con(":memory:")
  withr::defer(close_duckdb(con))
  bad <- g$cc_grid; bad$grid_key[1] <- "st999-ln999"
  expect_error(build_grid_reference(con, cc_grid = bad, cc_grid_ctrs = g$cc_grid_ctrs), "differs from cc_grid\\$grid_key")
  dup <- rbind(g$cc_grid[1, ], g$cc_grid)
  expect_error(build_grid_reference(con, cc_grid = dup, cc_grid_ctrs = g$cc_grid_ctrs), "duplicated grid_key")
})

test_that("build_grid_reference: the previous grid still builds, with the keys releases through v2026.10.01 carry", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid()
  con <- get_duckdb_con(":memory:")
  withr::defer(close_duckdb(con))
  v1  <- g$cc_grid_v1
  ctr <- sf::st_sf(sf::st_drop_geometry(v1)[c("sta_key", "sta_pattern")],
                   geom = sf::st_as_sfc(sf::st_as_sf(sf::st_drop_geometry(v1), coords = c("lon_ctr", "lat_ctr"), crs = 4326)))
  n <- build_grid_reference(con, cc_grid = v1, cc_grid_ctrs = ctr)
  expect_identical(as.integer(n), 218L)
  expect_setequal(DBI::dbGetQuery(con, "SELECT grid_key FROM grid")$grid_key, v1$grid_key)
})

# the rebuilt grid's rules, one named position each ----

rebuilt_con <- function(g, env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(close_duckdb(con), envir = env)
  build_grid_reference(con, cc_grid = g$cc_grid, cc_grid_ctrs = g$cc_grid_ctrs)
  con
}

test_that("rebuilt grid: an official station keys to its own cell", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid(); con <- rebuilt_con(g)
  # 90.0 37.0 and the SCCOOS station 93.4 26.4, at their listed positions
  expect_identical(key_in_duckdb(con, c(-118.38708, -117.27357), c(33.18462, 32.94905)),
                   c("st37-ln90", "st26.4-ln93.4"))
  # and every site of the grid, in DuckDB as in R
  xy <- sf::st_coordinates(g$cc_grid_ctrs)
  expect_identical(key_in_duckdb(con, xy[, 1], xy[, 2]), g$cc_grid_ctrs$grid_key)
  expect_identical(key_in_r(xy[, 1], xy[, 2], g$cc_grid), g$cc_grid_ctrs$grid_key)
  # under the previous grid 90.0 37.0 sat in the lattice cell labelled 35: same family of names,
  # another polygon, which is why nothing maps between the grids by name
  expect_identical(key_in_r(-118.38708, 33.18462, g$cc_grid_v1), "st35-ln90")
})

test_that("rebuilt grid: a historical position within 20 nmi of the official pattern takes the nearest station's cell", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid(); con <- rebuilt_con(g)
  p <- from_calcofi(90, 32)                      # 90.0 32.0, occupied 1959-84, not on the official list
  expect_identical(key_in_duckdb(con, p[1], p[2]), "st30-ln90")
  expect_false("st32-ln90" %in% g$cc_grid$grid_key)
})

test_that("rebuilt grid: a historical position beyond 20 nmi keeps its historical cell and key", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid(); con <- rebuilt_con(g)
  p <- from_calcofi(110, 100)                    # line 110 off Baja California
  expect_identical(key_in_duckdb(con, p[1], p[2]), "st100-ln110_hist")
  expect_identical(key_in_r(p[1], p[2], g$cc_grid_v1), "st100-ln110_hist")
  # all 112 kept cells carry a key the previous grid had
  kept <- g$cc_grid$grid_key[g$cc_grid$sta_source == "previous"]
  expect_length(kept, 112)
  expect_true(all(kept %in% g$cc_grid_v1$grid_key))
})

test_that("rebuilt grid: a point in a former water pocket keys to the cell it shares water with", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid(); con <- rebuilt_con(g)
  # the mouth of San Diego Bay: the nearest station is the SCCOOS station 93.4 26.4, but Point
  # Loma cuts that water off from it, so the pocket joined 93.3 28.0's cell
  lon <- -117.2067; lat <- 32.66724
  sites <- sf::st_transform(g$cc_grid_ctrs, 3310)
  here  <- sf::st_transform(sf::st_sfc(sf::st_point(c(lon, lat)), crs = 4326), 3310)
  expect_identical(sites$grid_key[sf::st_nearest_feature(here, sites)], "st26.4-ln93.4")
  expect_identical(key_in_duckdb(con, lon, lat), "st28-ln93.3")
  # and the cell it joined is still one polygon
  expect_true(sf::st_geometry_type(g$cc_grid[g$cc_grid$grid_key == "st28-ln93.3", ]) == "POLYGON")
})

test_that("rebuilt grid: the station cells stop where the previous grid stopped; beyond, a position keeps its kept cell", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid(); con <- rebuilt_con(g)
  # line 93.3 is the southern edge of the official pattern, and the previous grid ended its cells
  # 20 nmi beyond it (line 95.0; a line unit is 12 nmi). 15 nmi south of 93.3 60 is still that
  # station's cell; 25 nmi south is the kept line-100 cell, not an official one
  p15 <- from_calcofi(93.3 + 15 / 12, 60); p25 <- from_calcofi(93.3 + 25 / 12, 60)
  expect_identical(key_in_duckdb(con, c(p15[1], p25[1]), c(p15[2], p25[2])), c("st60-ln93.3", "st60-ln100_hist"))
  expect_identical(key_in_r(p25[1], p25[2], g$cc_grid_v1), "st60-ln100_hist")     # as before
  # regression (2026-10-04): a free Voronoi cell of 93.3 60 reached 40 nmi, to line 96.65, so
  # historical line 96.7 sat on a cell edge and 18,795 of its sample rows left their line-100 cells
  ln967 <- do.call(rbind, lapply(seq(30, 120, by = 10), function(s) from_calcofi(96.7, s)))
  k <- key_in_duckdb(con, ln967[, 1], ln967[, 2])
  expect_true(all(grepl("-ln100_hist$", k)))
  expect_identical(k, key_in_r(ln967[, 1], ln967[, 2], g$cc_grid_v1))
  # every kept cell holds the positions it held: its previous site keys to it, now as then
  v1   <- g$cc_grid_v1[g$cc_grid_v1$grid_key %in% g$cc_grid$grid_key[g$cc_grid$sta_source == "previous"], ]
  was  <- key_in_r(v1$lon_ctr, v1$lat_ctr, g$cc_grid_v1)
  here <- !is.na(was) & was == v1$grid_key          # the centre of an odd-shaped cell can lie outside it
  expect_gt(sum(here), 100)
  expect_identical(key_in_duckdb(con, v1$lon_ctr[here], v1$lat_ctr[here]), v1$grid_key[here])
})

test_that("rebuilt grid: a point on land, or outside the hull, has no cell", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid(); con <- rebuilt_con(g)
  #                          Los Angeles   Santa Cruz Island   Honolulu   mid-Atlantic
  k <- key_in_duckdb(con, c(-118.25,      -119.75,            -157.8,    -40),
                          c(34.05,         34.02,              21.3,      30))
  expect_identical(k, rep(NA_character_, 4))
  # the coastline is pulled back 300 m: a ship at its berth in San Diego Bay still has a cell
  expect_identical(key_in_duckdb(con, -117.2368, 32.70737), "st28-ln93.3")
})

test_that("rebuilt grid: DuckDB and R give the same key on every vertex of every cell", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid(); con <- rebuilt_con(g)
  # a vertex is on an edge two cells share, or a junction of three: the hardest positions there are
  v <- unique(sf::st_coordinates(sf::st_cast(sf::st_geometry(g$cc_grid), "MULTIPOLYGON"))[, 1:2])
  r <- key_in_r(v[, 1], v[, 2], g$cc_grid)
  expect_identical(key_in_duckdb(con, v[, 1], v[, 2]), r)
  expect_false(anyNA(r))
  n_cells <- lengths(sf::st_intersects(
    sf::st_as_sf(as.data.frame(v), coords = 1:2), sf::st_set_crs(sf::st_geometry(g$cc_grid), NA)))
  expect_gt(sum(n_cells >= 2), 5000)             # thousands of them really are shared
})

# grid_crosswalk: shares, on rectangles ----

# previous: P1 lon -120..-119, P2 -119..-118; current: G1 -120..-119.5, G2 -119.5..-118.5,
# G3 -118.5..-118 (all lat 33..34). An equal-area projection halves a lon/lat rectangle cut
# along a meridian exactly, so every share is one half or one
xw_fixture <- function() list(
  prev = sf::st_sf(grid_key = c("P1", "P2"),
                   geom = sf::st_sfc(ll_rect(-120, -119, 33, 34), ll_rect(-119, -118, 33, 34), crs = 4326)),
  grid = sf::st_sf(grid_key = c("G1", "G2", "G3"),
                   geom = sf::st_sfc(ll_rect(-120, -119.5, 33, 34), ll_rect(-119.5, -118.5, 33, 34),
                                     ll_rect(-118.5, -118, 33, 34), crs = 4326)))

test_that("grid_crosswalk: one row per overlapping pair, with the share of each cell", {
  skip_if_not_installed("sf")
  f  <- xw_fixture()
  xw <- grid_crosswalk(f$prev, f$grid)
  expect_named(xw, c("prev_grid_key", "grid_key", "overlap_km2", "prev_frac", "grid_frac"))
  expect_identical(paste(xw$prev_grid_key, xw$grid_key), c("P1 G1", "P1 G2", "P2 G2", "P2 G3"))
  expect_equal(xw$prev_frac, c(0.5, 0.5, 0.5, 0.5), tolerance = 1e-7)
  expect_equal(xw$grid_frac, c(1, 0.5, 0.5, 1),     tolerance = 1e-7)
  # cells that only touch along an edge (P1 and G3 do not even touch; P2 and a cell east of it
  # share the meridian -118) are not a pair
  east <- rbind(f$grid, sf::st_sf(grid_key = "G4", geom = sf::st_sfc(ll_rect(-118, -117, 33, 34), crs = 4326)))
  expect_false("G4" %in% grid_crosswalk(f$prev, east)$grid_key)
})

test_that("grid_crosswalk: the shares of a covered cell sum to one, on both sides", {
  skip_if_not_installed("sf")
  f  <- xw_fixture()
  xw <- grid_crosswalk(f$prev, f$grid)
  expect_equal(as.numeric(tapply(xw$prev_frac, xw$prev_grid_key, sum)), c(1, 1), tolerance = 1e-7)
  expect_equal(as.numeric(tapply(xw$grid_frac, xw$grid_key, sum)), c(1, 1, 1), tolerance = 1e-7)
  # overlap areas add up to the area of the common footprint (about 111 km x 186 km)
  expect_equal(sum(xw$overlap_km2),
               as.numeric(sf::st_area(sf::st_transform(sf::st_union(sf::st_segmentize(
                 sf::st_set_crs(sf::st_geometry(f$prev), NA), 0.05)) |> sf::st_set_crs(4326), 3310))) / 1e6,
               tolerance = 1e-6)
})

test_that("grid_crosswalk: an uncovered part is left out of the shares, never rescaled away", {
  skip_if_not_installed("sf")
  f  <- xw_fixture()
  xw <- grid_crosswalk(f$prev, f$grid[1:2, ])          # nothing covers the east half of P2
  expect_equal(sum(xw$prev_frac[xw$prev_grid_key == "P2"]), 0.5, tolerance = 1e-7)
  expect_equal(sum(xw$prev_frac[xw$prev_grid_key == "P1"]), 1,   tolerance = 1e-7)
})

test_that("grid_crosswalk: refuses a duplicated or missing key", {
  skip_if_not_installed("sf")
  f <- xw_fixture()
  expect_error(grid_crosswalk(rbind(f$prev, f$prev[1, ]), f$grid), "duplicated key in the previous grid")
  expect_error(grid_crosswalk(f$prev, rbind(f$grid, f$grid[1, ])), "duplicated key in the current grid")
  bad <- f$grid; bad$grid_key[1] <- NA
  expect_error(grid_crosswalk(f$prev, bad))
})

# build_grid_crosswalk + check_grid_crosswalk: the release table and its gate ----

xw_con <- function(f, env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(close_duckdb(con), envir = env)
  load_duckdb_extension(con, "spatial")
  DBI::dbWriteTable(con, "grid", data.frame(
    grid_key = f$grid$grid_key, wkt = sf::st_as_text(sf::st_geometry(f$grid))))
  DBI::dbExecute(con, "ALTER TABLE grid ADD COLUMN geom GEOMETRY")
  DBI::dbExecute(con, "UPDATE grid SET geom = ST_GeomFromText(wkt)")
  DBI::dbExecute(con, "ALTER TABLE grid DROP COLUMN wkt")
  con
}

test_that("build_grid_crosswalk: writes the table from the connection's grid, keyed on the pair", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf")
  f <- xw_fixture(); con <- xw_con(f)
  xw <- build_grid_crosswalk(con, grid_prev = f$prev)
  d  <- DBI::dbGetQuery(con, "SELECT * FROM grid_crosswalk ORDER BY prev_grid_key, grid_key")
  expect_equal(d, as.data.frame(xw))
  expect_identical(nrow(d), 4L)
  expect_identical(core_relationships("grid_crosswalk")$primary_keys$grid_crosswalk, c("prev_grid_key", "grid_key"))
  expect_identical(release_sort_keys()$grid_crosswalk, list(partition_by = NULL, order_by = c("prev_grid_key", "grid_key")))
  # the declared key is a key, and every grid_key resolves in grid
  expect_equal(DBI::dbGetQuery(con, "SELECT count(*) - count(DISTINCT (prev_grid_key, grid_key)) AS n FROM grid_crosswalk")$n, 0)
  expect_equal(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM grid_crosswalk WHERE grid_key NOT IN (SELECT grid_key FROM grid)")$n, 0)
})

test_that("check_grid_crosswalk: every cell of both grids is accounted for", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf")
  f <- xw_fixture(); con <- xw_con(f)
  build_grid_crosswalk(con, grid_prev = f$prev)
  r <- check_grid_crosswalk(con, grid_prev = f$prev)
  expect_identical(paste(r$side, r$grid_key), c("prev P1", "prev P2", "grid G1", "grid G2", "grid G3"))
  expect_true(all(r$status == "ok"))
  expect_equal(r$frac, rep(1, 5), tolerance = 1e-7)
  expect_identical(r$main_key, c("G1", "G2", "P1", "P1", "P2"))    # ties go to the key that sorts first
})

test_that("check_grid_crosswalk: a part-covered cell is reported, a mostly uncovered or missing one fails", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf")
  f <- xw_fixture(); f$grid <- f$grid[1:2, ]; con <- xw_con(f)      # the east half of P2 has no successor
  build_grid_crosswalk(con, grid_prev = f$prev)
  r <- check_grid_crosswalk(con, grid_prev = f$prev, min_cover = 0.4)
  expect_identical(r$status[r$side == "prev"], c("ok", "partial"))
  # the default floor is 0.95: half a cell without a successor is a hole in the new grid
  expect_error(check_grid_crosswalk(con, grid_prev = f$prev), "1 cell\\(s\\) not accounted for.*prev P2 \\(under min_cover\\)")
  far <- rbind(f$prev, sf::st_sf(grid_key = "P9", geom = sf::st_sfc(ll_rect(-130, -129, 33, 34), crs = 4326)))
  build_grid_crosswalk(con, grid_prev = far)
  expect_error(check_grid_crosswalk(con, grid_prev = far, min_cover = 0.4), "prev P9 \\(no overlap\\)")
  expect_identical(check_grid_crosswalk(con, grid_prev = far, min_cover = 0.4, halt = FALSE)$status[3], "no overlap")
})

test_that("check_grid_crosswalk: overlapping previous cells are reported on the current cell, overlapping current cells fail", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf")
  f <- xw_fixture(); con <- xw_con(f)
  # previous cells that overlap each other (P1 reaches 0.25 degrees into P2): the current cell G2
  # under the overlap is covered more than once. A fact about the previous grid, not a failure
  lap <- f$prev; sf::st_geometry(lap)[1] <- sf::st_sfc(ll_rect(-120, -118.75, 33, 34), crs = 4326)
  build_grid_crosswalk(con, grid_prev = lap)
  r <- check_grid_crosswalk(con, grid_prev = lap)
  expect_identical(r$status[r$side == "grid"], c("ok", "overlapped", "ok"))
  expect_equal(r$frac[r$side == "grid"][2], 1.25, tolerance = 1e-6)
  # current cells that overlap each other put more than the whole of a previous cell somewhere
  DBI::dbExecute(con, "UPDATE grid SET geom = ST_GeomFromText('POLYGON((-120 33, -119.25 33, -119.25 34, -120 34, -120 33))') WHERE grid_key = 'G1'")
  build_grid_crosswalk(con, grid_prev = f$prev)
  expect_error(check_grid_crosswalk(con, grid_prev = f$prev), "prev P1 \\(over one\\)")
})

test_that("check_grid_crosswalk: a repeated pair, a share over one and a key of neither grid fail", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf")
  f <- xw_fixture(); con <- xw_con(f)
  build_grid_crosswalk(con, grid_prev = f$prev)
  DBI::dbExecute(con, "INSERT INTO grid_crosswalk SELECT * FROM grid_crosswalk WHERE prev_grid_key = 'P1' AND grid_key = 'G1'")
  expect_error(check_grid_crosswalk(con, grid_prev = f$prev), "1 repeated pair")
  build_grid_crosswalk(con, grid_prev = f$prev)
  DBI::dbExecute(con, "INSERT INTO grid_crosswalk VALUES ('P1', 'G9', 1.0, 0.1, 0.1)")
  expect_error(check_grid_crosswalk(con, grid_prev = f$prev), "1 key\\(s\\) of neither grid")
})

test_that("grid_crosswalk: the real grids: every previous key is covered, and no share exceeds one", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid(); con <- rebuilt_con(g)
  xw <- build_grid_crosswalk(con, grid_prev = g$cc_grid_v1)
  r  <- check_grid_crosswalk(con, grid_prev = g$cc_grid_v1)        # halts on a failure
  expect_setequal(r$grid_key[r$side == "prev"], g$cc_grid_v1$grid_key)   # all 218 previous keys
  expect_setequal(r$grid_key[r$side == "grid"], g$cc_grid$grid_key)      # all 225 current keys
  expect_true(all(r$n >= 1))
  p <- r[r$side == "prev", ]
  expect_true(all(p$frac <= 1 + 1e-6))                 # the current cells do not overlap
  # the shares of a previous cell sum to one, less the part of it that is land under the finer
  # coastline: measured 2026-10-04, 180 of 218 sum to one within 1e-6 and the smallest sum is
  # 0.9776 (st45-ln66.7). Ratchets: neither may get worse
  expect_gte(min(p$frac), 0.977)
  expect_gte(sum(abs(p$frac - 1) <= 1e-6), 180)
  expect_setequal(unique(p$status), c("ok", "partial"))
  # a current cell is covered once, except where previous cells overlap each other (seven cells
  # along the line 93.3 / line 100 boundary, by at most 1e-4 of the cell)
  q <- r[r$side == "grid", ]
  expect_gte(min(q$frac), 0.982)
  expect_lte(max(q$frac), 1 + 2e-4)
  expect_true(all(grepl("-ln93\\.3$|-ln100_hist$", q$grid_key[q$status == "overlapped"])))
  # a kept cell is the previous cell: its row is the identity, with the whole of the previous
  # cell but the part that is land under the finer coastline (82 of the 112 exactly; least 0.9828)
  kept <- g$cc_grid$grid_key[g$cc_grid$sta_source == "previous"]
  id   <- xw[xw$prev_grid_key == xw$grid_key & xw$grid_key %in% kept, ]
  expect_identical(nrow(id), 112L)
  expect_gte(min(id$prev_frac), 0.98)
  expect_gte(sum(abs(id$prev_frac - 1) <= 1e-6), 82)
  # what else a kept previous cell overlaps is only where the previous cells overlapped each
  # other: under 1 km2 in all
  oth <- xw[xw$prev_grid_key %in% kept & xw$prev_grid_key != xw$grid_key, ]
  expect_lt(sum(oth$overlap_km2), 1)
  expect_lt(max(oth$prev_frac), 1e-4)
  # the name is not the cell: st30-ln90 keeps its name and about half of its previous water;
  # the rest went to four new inshore cells
  s <- xw[xw$prev_grid_key == "st30-ln90", ]
  expect_equal(s$prev_frac[s$grid_key == "st30-ln90"], 0.52, tolerance = 0.02)
  expect_true(all(c("st30.1-ln88.5", "st28-ln90", "st26.4-ln91.7", "st27.7-ln90") %in% s$grid_key))
})

# check_grid_key_assignment: a key must name the cell the position is in ----

keyed_samples <- function(con) {
  two_square_grid(con)
  DBI::dbExecute(con, "CREATE OR REPLACE TABLE sample AS SELECT * FROM (VALUES
    ('a:1', 'ds_a', 'st20-ln90',  0.5, 0.5),    -- right
    ('a:2', 'ds_a', 'st100-ln90', 1.5, 0.5),    -- right
    ('a:3', 'ds_a', 'st100-ln90', 1.0, 0.5),    -- on the shared edge: the key that sorts first
    ('a:4', 'ds_a', NULL,         5.0, 5.0),    -- outside, unkeyed: right
    ('b:1', 'ds_b', NULL,         0.5, 0.5),    -- in a cell, unkeyed: reported
    ('b:2', 'ds_b', 'st20-ln90',  NULL, NULL)   -- keyed, no position: reported
  ) t(sample_key, dataset_key, grid_key, longitude, latitude)")
  invisible(con)
}

test_that("check_grid_key_assignment: right keys pass; unkeyed-in-cell and keyed-without-position are reported", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  withr::defer(close_duckdb(con))
  load_duckdb_extension(con, "spatial")
  keyed_samples(con)
  r <- check_grid_key_assignment(con)
  expect_identical(r$dataset_key, c("ds_a", "ds_b"))
  expect_equal(r$n,                   c(4, 2))
  expect_equal(r$n_position,          c(4, 1))
  expect_equal(r$n_keyed,             c(3, 1))
  expect_equal(r$n_same,              c(4, 0))
  expect_equal(r$n_wrong,             c(0, 0))
  expect_equal(r$n_unkeyed_in_cell,   c(0, 1))
  expect_equal(r$n_keyed_no_position, c(0, 1))
})

test_that("check_grid_key_assignment: a key that names another cell, or a cell for a position in none, fails", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  withr::defer(close_duckdb(con))
  load_duckdb_extension(con, "spatial")
  keyed_samples(con)
  # the stale-ingest case: a real key of this grid, on a position that is in the other cell
  DBI::dbExecute(con, "UPDATE sample SET grid_key = 'st20-ln90' WHERE sample_key = 'a:2'")
  expect_error(check_grid_key_assignment(con), "1 sample row\\(s\\) carry a grid_key that is not the cell.*ds_a \\(1\\)")
  expect_equal(check_grid_key_assignment(con, halt = FALSE)$n_wrong, c(1, 0))
  # the edge tie is part of the rule: the other cell's key on the shared edge is wrong too
  DBI::dbExecute(con, "UPDATE sample SET grid_key = 'st100-ln90' WHERE sample_key = 'a:2'")
  DBI::dbExecute(con, "UPDATE sample SET grid_key = 'st20-ln90'  WHERE sample_key = 'a:3'")
  expect_equal(check_grid_key_assignment(con, halt = FALSE)$n_wrong, c(1, 0))
  # and a key on a position outside every cell
  DBI::dbExecute(con, "UPDATE sample SET grid_key = 'st100-ln90' WHERE sample_key = 'a:3'")
  DBI::dbExecute(con, "UPDATE sample SET grid_key = 'st20-ln90'  WHERE sample_key = 'a:4'")
  expect_equal(check_grid_key_assignment(con, halt = FALSE)$n_wrong, c(1, 0))
})

test_that("check_grid_key_assignment: keys assigned by assign_grid_key() on the rebuilt grid all pass", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid(); con <- rebuilt_con(g)
  xy <- rbind(sf::st_coordinates(g$cc_grid_ctrs),
              unique(sf::st_coordinates(sf::st_cast(sf::st_geometry(g$cc_grid), "MULTIPOLYGON"))[, 1:2])[1:2000, ],
              c(-118.25, 34.05))
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = paste0("t:", seq_len(nrow(xy))), dataset_key = "t", longitude = xy[, 1], latitude = xy[, 2]))
  suppressMessages(add_point_geom(con, "sample", lon_col = "longitude", lat_col = "latitude"))
  suppressMessages(assign_grid_key(con, "sample"))
  r <- check_grid_key_assignment(con)
  expect_equal(c(r$n_wrong, r$n_unkeyed_in_cell), c(0, 0))
  expect_equal(r$n_same, r$n)
})

test_that("check_grid_key_assignment: rows staged against the previous grid fail, where an FK check sees under two thirds of them", {
  skip_if_not_installed("duckdb"); skip_if_not_installed("sf"); skip_if_not_installed("units")
  g <- rebuilt_grid(); con <- rebuilt_con(g)
  # one sample at each of the 225 sites, keyed against the PREVIOUS grid: the stale-ingest case
  xy <- sf::st_coordinates(g$cc_grid_ctrs)
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = paste0("t:", seq_len(nrow(xy))), dataset_key = "t", longitude = xy[, 1], latitude = xy[, 2]))
  suppressMessages(add_point_geom(con, "sample", lon_col = "longitude", lat_col = "latitude"))
  v1  <- g$cc_grid_v1
  ctr <- sf::st_sf(sf::st_drop_geometry(v1)[c("sta_key", "sta_pattern")],
                   geom = sf::st_as_sfc(sf::st_as_sf(sf::st_drop_geometry(v1), coords = c("lon_ctr", "lat_ctr"), crs = 4326)))
  build_grid_reference(con, grid_tbl = "grid_v1", cc_grid = v1, cc_grid_ctrs = ctr)
  suppressMessages(assign_grid_key(con, "sample", grid_table = "grid_v1"))
  expect_error(check_grid_key_assignment(con), "29 sample row\\(s\\) carry a grid_key that is not the cell")
  stale <- check_grid_key_assignment(con, halt = FALSE)
  expect_equal(stale$n_wrong, 29)        # the 29 stations whose cell is new (measured 2026-10-04)
  # 12 of those 29 stale keys are still keys of the new grid (st30-ln90 for 90.0 28.0, ...), so
  # the foreign-key check on sample.grid_key sees only the other 17
  fk_orphans <- DBI::dbGetQuery(con, "
    SELECT count(*) AS n FROM sample WHERE grid_key IS NOT NULL AND grid_key NOT IN (SELECT grid_key FROM grid)")$n
  expect_equal(fk_orphans, 17)
})
