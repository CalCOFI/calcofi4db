# the grid crosswalk: how the cells of the previous CalCOFI grid map onto the cells of the grid a
# release ships (CalCOFI/workflows#130). grid_crosswalk() is the pure overlap of two polygon sets,
# build_grid_crosswalk() materializes it in the release connection as `grid_crosswalk`, and
# check_grid_crosswalk() is its gate. The grid itself is build_grid_reference() (R/model.R) and a
# position is keyed to a cell by assign_grid_key() (R/spatial.R).

# add vertices along each lon/lat edge so that no edge is longer than `deg` degrees. The chord is
# the planar lon/lat one, never a great circle: that is the edge DuckDB's ST_Intersects tests in
# assign_grid_key(), so an area measured after projecting is the area of the cell positions key to
.densify_lonlat <- function(x, deg = 0.05) {
  crs <- sf::st_crs(x)
  sf::st_set_crs(sf::st_segmentize(sf::st_set_crs(x, NA), deg), crs)
}

#' Area crosswalk between two grids
#'
#' Every pair of a previous and a current grid cell that overlap, with the area of the overlap and
#' its share of each cell. This is the only way to carry a cell-keyed value from one grid to the
#' other: when the grid was rebuilt from the official station positions (CalCOFI/workflows#130),
#' 84 of the 113 station cells kept a key whose polygon changed, so a key matched by name can be
#' a different piece of ocean. (The 112 previous cells kept as they were are identity rows.)
#'
#' Areas are planar in an equal-area projection (`crs_m`, default California Albers), after the
#' lon/lat edges of both grids are densified so the projection keeps them where positions are
#' keyed. The shares are of each cell's **whole** area: `prev_frac` sums to less than one for a
#' previous cell part of which no current cell covers (land under a finer coastline), and
#' `grid_frac` for a current cell part of which no previous cell covered. Nothing is rescaled to
#' hide that; [check_grid_crosswalk()] reports it.
#'
#' @param prev,grid `sf` polygons in EPSG:4326, the previous and the current cells, each with the
#'   key column
#' @param key name of the key column in both (default `"grid_key"`)
#' @param crs_m equal-area CRS for the areas (default 3310)
#' @param min_km2 an overlap smaller than this is a numerical sliver and is dropped (default
#'   1e-4 km2, 100 square metres: cell vertices are rounded to 1e-9 degrees, which along a 200 km
#'   edge shared by an unchanged cell and its neighbour is some 20 square metres)
#' @return a tibble: `prev_grid_key`, `grid_key`, `overlap_km2` (rounded to the square metre),
#'   `prev_frac` (the overlap as a fraction of the previous cell) and `grid_frac` (as a fraction of
#'   the current cell), both rounded to 9 decimals; ordered by `prev_grid_key`, `grid_key`
#' @export
#' @concept grid
#' @examples
#' \dontrun{
#' xw <- grid_crosswalk(calcofi4r::cc_grid_v1, calcofi4r::cc_grid)
#' # where the previous cell st30-ln90 went, largest share first
#' xw[xw$prev_grid_key == "st30-ln90", ] |> dplyr::arrange(dplyr::desc(prev_frac))
#' }
grid_crosswalk <- function(prev, grid, key = "grid_key", crs_m = 3310, min_km2 = 1e-4) {
  for (pkg in c("sf", "tibble"))
    if (!requireNamespace(pkg, quietly = TRUE))
      stop("grid_crosswalk() requires the '", pkg, "' package.", call. = FALSE)
  stopifnot(
    inherits(prev, "sf"), inherits(grid, "sf"),
    key %in% names(prev), key %in% names(grid),
    !anyNA(prev[[key]]), !anyNA(grid[[key]]),
    "duplicated key in the previous grid" = !anyDuplicated(prev[[key]]),
    "duplicated key in the current grid"  = !anyDuplicated(grid[[key]]))
  to_m <- function(x) sf::st_make_valid(sf::st_transform(.densify_lonlat(sf::st_geometry(x)), crs_m))
  p <- sf::st_sf(prev_grid_key = as.character(prev[[key]]), geom = to_m(prev))
  g <- sf::st_sf(grid_key      = as.character(grid[[key]]), geom = to_m(grid))
  p$prev_km2 <- as.numeric(sf::st_area(p)) / 1e6
  g$grid_km2 <- as.numeric(sf::st_area(g)) / 1e6
  x <- suppressWarnings(sf::st_intersection(p, g))
  x$overlap_km2 <- as.numeric(sf::st_area(x)) / 1e6
  x <- sf::st_drop_geometry(x)
  x <- x[x$overlap_km2 >= min_km2, , drop = FALSE]
  x$prev_frac   <- round(x$overlap_km2 / x$prev_km2, 9)
  x$grid_frac   <- round(x$overlap_km2 / x$grid_km2, 9)
  x$overlap_km2 <- round(x$overlap_km2, 6)
  x <- x[order(x$prev_grid_key, x$grid_key, method = "radix"),
         c("prev_grid_key", "grid_key", "overlap_km2", "prev_frac", "grid_frac")]
  rownames(x) <- NULL
  tibble::as_tibble(x)
}

#' Build the `grid_crosswalk` release table
#'
#' Materializes [grid_crosswalk()] between the previous grid and the `grid` table of the
#' connection (the cells this release ships, as [build_grid_reference()] or the ichthyo ingest
#' wrote them), so the published crosswalk describes the published polygons. One row per
#' overlapping pair, primary key (`prev_grid_key`, `grid_key`); `grid_key` is a foreign key to
#' `grid`, `prev_grid_key` names a cell of the grid releases through v2026.10.01 carried. A cell
#' the rebuilt grid kept as it was (the 112 beyond the official pattern) is an identity row,
#' with a `prev_frac` of one less the part of it that is land under the finer coastline.
#'
#' @param con a DuckDB connection holding `grid_tbl` with `grid_key` and a `geom` GEOMETRY
#' @param grid_prev the previous grid: `sf` polygons in EPSG:4326 with `grid_key` (default
#'   `calcofi4r::cc_grid_v1`)
#' @param grid_tbl the current grid table (default `"grid"`)
#' @param tbl the table to write (default `"grid_crosswalk"`)
#' @param crs_m,min_km2 passed to [grid_crosswalk()]
#' @return (invisibly) the crosswalk as written, a tibble
#' @export
#' @concept grid
#' @examples
#' \dontrun{
#' con <- get_duckdb_con(":memory:")
#' build_grid_reference(con)
#' xw <- build_grid_crosswalk(con)
#' check_grid_crosswalk(con)
#' }
build_grid_crosswalk <- function(con, grid_prev = calcofi4r::cc_grid_v1, grid_tbl = "grid",
                                 tbl = "grid_crosswalk", crs_m = 3310, min_km2 = 1e-4) {
  if (!requireNamespace("sf", quietly = TRUE))
    stop("build_grid_crosswalk() requires the 'sf' package.", call. = FALSE)
  .load_spatial(con)
  stopifnot(grid_tbl %in% DBI::dbListTables(con))
  # ST_AsWKB of a CRS-tagged GEOMETRY is plain WKB: the tag (EPSG:4326 or OGC:CRS84, both WGS 84
  # lon/lat) is not needed to measure an overlap
  d <- DBI::dbGetQuery(con, glue::glue(
    "SELECT grid_key, ST_AsWKB(geom) AS wkb FROM {grid_tbl} ORDER BY grid_key"))
  grid <- sf::st_sf(grid_key = d$grid_key,
                    geom = sf::st_as_sfc(structure(as.list(d$wkb), class = "WKB"), crs = 4326))
  xw <- grid_crosswalk(grid_prev, grid, crs_m = crs_m, min_km2 = min_km2)
  DBI::dbWriteTable(con, tbl, as.data.frame(xw), overwrite = TRUE)
  invisible(xw)
}

#' Gate the grid crosswalk
#'
#' What must hold for `grid_crosswalk` to be the map between the two grids: every previous cell
#' appears, every current cell appears, no pair repeats, every key is a cell of its grid, and no
#' previous cell's shares sum to more than one (they would only if two current cells overlapped,
#' and the current grid is a partition).
#'
#' Three things are reported rather than failed, because they are facts about the two grids and
#' not errors in the map. A cell whose shares sum to **less** than one, down to `min_cover`, is
#' `"partial"`: the remainder is area the other grid does not cover (the previous cells were
#' clipped by a coarser coastline, so part of one can be land now). Below `min_cover` it fails,
#' since a cell most of which has no counterpart is a hole. A **current** cell whose shares sum
#' to more than one is `"overlapped"`: previous cells overlap each other there (the previous grid
#' was assembled in `+proj=calcofi` and glued by hand along line 93.3, and is not an exact
#' partition in longitude/latitude; seven current cells either side of that boundary are covered
#' twice over at most 1e-4 of their area).
#'
#' @param con a DuckDB connection holding `tbl` and `grid_tbl`
#' @param grid_prev the previous grid (default `calcofi4r::cc_grid_v1`); only its keys are read
#' @param tbl,grid_tbl the crosswalk and the current grid tables
#' @param min_cover the smallest summed share a cell may have (default 0.95; measured on the
#'   rebuilt grid: 0.978 for a previous cell, 0.982 for a current one)
#' @param tol tolerance on a sum differing from one (default 1e-6)
#' @param halt stop on a failure (default `TRUE`); `FALSE` returns the report regardless
#' @return a data frame, one row per cell of either grid: `side` (`"prev"` or `"grid"`),
#'   `grid_key`, `n` (cells of the other grid it overlaps), `frac` (summed share), `main_key`
#'   (the other grid's cell holding its largest share), `main_frac` and `status` (`"ok"`,
#'   `"partial"`, `"overlapped"`, or a failure: `"no overlap"`, `"under min_cover"`, `"over one"`)
#' @export
#' @concept grid
check_grid_crosswalk <- function(con, grid_prev = calcofi4r::cc_grid_v1, tbl = "grid_crosswalk",
                                 grid_tbl = "grid", min_cover = 0.95, tol = 1e-6, halt = TRUE) {
  stopifnot(all(c(tbl, grid_tbl) %in% DBI::dbListTables(con)), "grid_key" %in% names(grid_prev))
  xw   <- DBI::dbGetQuery(con, glue::glue("SELECT * FROM {tbl}"))
  keys <- list(prev = as.character(grid_prev$grid_key),
               grid = DBI::dbGetQuery(con, glue::glue("SELECT grid_key FROM {grid_tbl}"))$grid_key)
  n_dup    <- sum(duplicated(xw[c("prev_grid_key", "grid_key")]))
  n_orphan <- sum(!xw$grid_key %in% keys$grid) + sum(!xw$prev_grid_key %in% keys$prev)
  one <- function(side) {
    k  <- xw[[if (side == "prev") "prev_grid_key" else "grid_key"]]
    ot <- xw[[if (side == "prev") "grid_key" else "prev_grid_key"]]
    f  <- xw[[paste0(side, "_frac")]]
    do.call(rbind, lapply(sort(keys[[side]], method = "radix"), function(kk) {
      i <- which(k == kk)
      j <- if (length(i)) i[order(-f[i], ot[i], method = "radix")[1]] else NA_integer_
      data.frame(side = side, grid_key = kk, n = length(i), frac = sum(f[i]),
                 main_key  = if (length(i)) ot[j] else NA_character_,
                 main_frac = if (length(i)) f[j] else NA_real_)
    }))
  }
  rpt <- rbind(one("prev"), one("grid"))
  rpt$status <- ifelse(
    rpt$n == 0, "no overlap", ifelse(
      rpt$frac > 1 + tol, ifelse(rpt$side == "prev", "over one", "overlapped"), ifelse(
        rpt$frac < min_cover, "under min_cover", ifelse(
          rpt$frac < 1 - tol, "partial", "ok"))))
  bad <- rpt[!rpt$status %in% c("ok", "partial", "overlapped"), , drop = FALSE]
  if (halt && (nrow(bad) || n_dup || n_orphan))
    stop(glue::glue(
      "grid_crosswalk fails: {n_dup} repeated pair(s), {n_orphan} key(s) of neither grid, ",
      "{nrow(bad)} cell(s) not accounted for",
      "{if (nrow(bad)) paste0(': ', paste(utils::head(paste0(bad$side, ' ', bad$grid_key, ' (', bad$status, ')'), 8), collapse = ', ')) else ''}"),
      call. = FALSE)
  attr(rpt, "n_dup")    <- n_dup
  attr(rpt, "n_orphan") <- n_orphan
  rpt
}

#' Check that every keyed sample sits in the cell its key names
#'
#' Recomputes the cell of every `sample` position against the grid of the connection, by the rule
#' of [assign_grid_key()], and compares it with the `grid_key` the row carries. This is the gate
#' that a grid change needs: when the cells were rebuilt (CalCOFI/workflows#130), 84 of the 218
#' previous keys survived as **names** over different polygons, so an ingest staged against the
#' previous grid ships keys that pass every foreign-key check and name the wrong piece of ocean.
#' It also means a row must be keyed by its own position: one that inherits its key from a parent
#' event at another position can sit in a neighbouring cell, and fails.
#'
#' A row is `wrong` when it carries a key and its position falls in another cell or in none; that
#' fails. A row with a position inside a cell and no key (`n_unkeyed_in_cell`) is reported, not
#' failed: a region-pooled dataset is ungridded by design, and the owning ingest decides.
#'
#' @param con a DuckDB connection holding `sample_tbl` (`dataset_key`, `grid_key`, `longitude`,
#'   `latitude`) and `grid_tbl` (`grid_key`, `geom`)
#' @param sample_tbl,grid_tbl table names (defaults `"sample"`, `"grid"`)
#' @param halt stop when any row is `wrong` (default `TRUE`)
#' @return a data frame, one row per `dataset_key`: `n` (rows), `n_position` (with a finite
#'   position), `n_keyed`, `n_same` (key equals the recomputed cell, both NULL included),
#'   `n_wrong`, `n_unkeyed_in_cell` and `n_keyed_no_position`
#' @export
#' @concept grid
check_grid_key_assignment <- function(con, sample_tbl = "sample", grid_tbl = "grid", halt = TRUE) {
  .load_spatial(con)
  stopifnot(all(c(sample_tbl, grid_tbl) %in% DBI::dbListTables(con)))
  finite <- "longitude IS NOT NULL AND latitude IS NOT NULL AND isfinite(longitude) AND isfinite(latitude)"
  # the grid's geometry through WKB, so a CRS tag on either side cannot refuse the predicate
  rpt <- DBI::dbGetQuery(con, glue::glue("
    WITH g AS (SELECT grid_key, ST_GeomFromWKB(ST_AsWKB(geom)) AS geom FROM {grid_tbl}),
    pos AS (SELECT DISTINCT longitude, latitude FROM {sample_tbl} WHERE {finite}),
    cell AS (
      SELECT p.longitude, p.latitude, min(g.grid_key) AS cell_key
      FROM pos p LEFT JOIN g ON ST_Intersects(ST_Point(p.longitude, p.latitude), g.geom)
      GROUP BY p.longitude, p.latitude)
    SELECT s.dataset_key,
           count(*)                                                              AS n,
           count(*) FILTER (WHERE {finite})                                      AS n_position,
           count(s.grid_key)                                                     AS n_keyed,
           count(*) FILTER (WHERE s.grid_key IS NOT DISTINCT FROM c.cell_key)    AS n_same,
           count(*) FILTER (WHERE {finite} AND s.grid_key IS NOT NULL
                                  AND s.grid_key IS DISTINCT FROM c.cell_key)    AS n_wrong,
           count(*) FILTER (WHERE s.grid_key IS NULL AND c.cell_key IS NOT NULL) AS n_unkeyed_in_cell,
           count(*) FILTER (WHERE s.grid_key IS NOT NULL AND NOT ({finite}))     AS n_keyed_no_position
    FROM {sample_tbl} s LEFT JOIN cell c USING (longitude, latitude)
    GROUP BY s.dataset_key ORDER BY s.dataset_key"))
  num <- setdiff(names(rpt), "dataset_key")
  rpt[num] <- lapply(rpt[num], as.numeric)
  if (halt && sum(rpt$n_wrong) > 0) {
    bad <- rpt[rpt$n_wrong > 0, , drop = FALSE]
    stop(glue::glue(
      "{format(sum(rpt$n_wrong), big.mark = ',')} sample row(s) carry a grid_key that is not the cell ",
      "their position falls in: {paste0(bad$dataset_key, ' (', format(bad$n_wrong, big.mark = ',', trim = TRUE), ')', collapse = ', ')}. ",
      "Was the ingest staged against another grid? Re-stage it against this release's `grid`."),
      call. = FALSE)
  }
  rpt
}
