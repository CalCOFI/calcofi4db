# the grid under test. The rebuilt CalCOFI grid (CalCOFI/workflows#130) ships in calcofi4r as
# cc_grid / cc_grid_ctrs with the previous grid as cc_grid_v1. Before that calcofi4r is installed
# (a worktree run beside a checkout of it) point CALCOFI4R_DIR at the checkout and its data/*.rda
# are read directly; with neither, the tests that need the real cells skip.
rebuilt_grid <- function() {
  need <- c("cc_grid", "cc_grid_ctrs", "cc_grid_v1")
  dir  <- Sys.getenv("CALCOFI4R_DIR")
  if (nzchar(dir)) {
    f <- file.path(dir, "data", paste0(need, ".rda"))
    if (!all(file.exists(f))) testthat::skip("CALCOFI4R_DIR has no rebuilt grid datasets")
    e <- new.env()
    for (x in f) load(x, envir = e)
    return(mget(need, envir = e))
  }
  testthat::skip_if_not_installed("calcofi4r")
  have <- utils::data(package = "calcofi4r")$results[, "Item"]
  if (!all(need %in% have)) testthat::skip("the installed calcofi4r predates the rebuilt grid (no cc_grid_v1)")
  stats::setNames(lapply(need, function(n) getExportedValue("calcofi4r", n)), need)
}

# two unit squares sharing the edge x = 1, as a DuckDB `grid` table. "st100-ln90" sorts before
# "st20-ln90" in byte order, and is the second row, so neither insertion order nor a numeric
# reading of the key gives the expected answer by accident
two_square_grid <- function(con) {
  DBI::dbExecute(con, "CREATE OR REPLACE TABLE grid AS
    SELECT 'st20-ln90'::VARCHAR  AS grid_key, ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS geom
    UNION ALL
    SELECT 'st100-ln90'::VARCHAR AS grid_key, ST_GeomFromText('POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))') AS geom")
  invisible(con)
}

# key a data frame of lon/lat with assign_grid_key(), in row order
key_in_duckdb <- function(con, lon, lat, grid_table = "grid") {
  DBI::dbWriteTable(con, "pts", data.frame(id = seq_along(lon), longitude = lon, latitude = lat), overwrite = TRUE)
  suppressMessages(add_point_geom(con, "pts", lon_col = "longitude", lat_col = "latitude"))
  suppressMessages(assign_grid_key(con, "pts", grid_table = grid_table))
  DBI::dbGetQuery(con, "SELECT grid_key FROM pts ORDER BY id")$grid_key
}

# the same rule in R (calcofi4r::cc_grid_key()), written out so these tests do not need the
# calcofi4r that exports it: planar lon/lat intersects, byte-order minimum on a tie
key_in_r <- function(lon, lat, grid) {
  pts   <- sf::st_as_sf(data.frame(x = lon, y = lat), coords = c("x", "y"))
  cells <- sf::st_set_crs(sf::st_geometry(grid), NA)
  vapply(sf::st_intersects(pts, cells), function(i)
    if (!length(i)) NA_character_ else sort(grid$grid_key[i], method = "radix")[1], "")
}

# a lon/lat rectangle
ll_rect <- function(x0, x1, y0, y1)
  sf::st_polygon(list(rbind(c(x0, y0), c(x1, y0), c(x1, y1), c(x0, y1), c(x0, y0))))
