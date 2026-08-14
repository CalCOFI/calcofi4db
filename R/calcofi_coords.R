# CalCOFI line/station <-> geographic coordinates ------------------------------

#' Convert CalCOFI line/station to longitude/latitude
#'
#' The CalCOFI station plan is a coordinate system in its own right, and PROJ
#' ships it as `+proj=calcofi` — so this is a projection, not a lookup against
#' `grid`. That distinction matters: a lookup only resolves stations that exist
#' in the grid table, while the transform resolves **any** line/station pair,
#' including the historical inshore stations and the Gulf of California and Baja
#' lines that the modern pattern dropped.
#'
#' Use it to recover a position for a row that records where it was in CalCOFI
#' terms but carries no lon/lat. Once a position exists, `hex_id` and `grid_key`
#' follow from it in the usual way (`.hex_expr()`, `assign_grid_key()`), so a
#' recovered row becomes a full participant in spatial rollups rather than an
#' ungridded remainder.
#'
#' @param line,station numeric vectors of CalCOFI line and station, recycled to a
#'   common length. `NA` in either yields `NA` in both outputs.
#' @return a data.frame with `longitude` and `latitude` (WGS 84), one row per input
#' @export
#' @concept spatial
#' @examples
#' \dontrun{
#' cc_calcofi_to_lonlat(90, 60)   # -119.96, 32.42 — off San Diego
#' }
cc_calcofi_to_lonlat <- function(line, station) {
  n <- max(length(line), length(station))
  line <- rep_len(line, n); station <- rep_len(station, n)
  out <- data.frame(longitude = rep(NA_real_, n), latitude = rep(NA_real_, n))
  ok <- !is.na(line) & !is.na(station)
  if (!any(ok)) return(out)

  pts <- sf::st_as_sf(
    data.frame(lin = as.numeric(line[ok]), pos = as.numeric(station[ok])),
    coords = c("lin", "pos"), crs = sf::st_crs("+proj=calcofi"))
  xy <- sf::st_coordinates(sf::st_transform(pts, 4326))
  out$longitude[ok] <- xy[, 1]
  out$latitude[ok]  <- xy[, 2]
  out
}

#' Region polygons from a station-membership list
#'
#' Some datasets pool their samples across a named set of CalCOFI stations before
#' measuring — the counting happened at the microscope, so there is no per-station
#' observation and no `grid_key` to hang one on. All the source gives is *which
#' stations went into which region*. This turns that membership list into one
#' polygon per region.
#'
#' The naive reading — a convex hull over each region's member stations — fails on
#' real membership lists in three ways, all of them silent:
#'
#' * **A region whose stations are collinear has no hull.** Four stations on one
#'   CalCOFI line give a zero-width slab, not a region.
#' * **Regions interleave, so their hulls overlap.** A hull claims everything
#'   between its members, including the parts another region owns.
#' * **The hulls do not tile.** Space between regions belongs to nobody, so a
#'   point-in-polygon lookup returns nothing for a third of the sampled domain.
#'
#' So the partition is built the other way round: every station claims the area
#' nearest to it (a Voronoi tessellation), the cells are clipped to the convex
#' hull of *all* the stations, and then dissolved by region. The result tiles the
#' pooled domain exactly — no overlaps, no gaps — and each region comes out as one
#' connected piece even when its own members are not adjacent, which a union of
#' member cells cannot do.
#'
#' The outer boundary is the hull of the stations themselves, deliberately: the
#' pooling says nothing about water beyond the outermost station occupied, and
#' padding it outward would be inventing extent. Land is **not** erased — the
#' geometry describes where the sampling was, and subtracting a coastline would
#' bind the released polygons to one coastline vintage. Erase at render time if a
#' map needs it.
#'
#' Positions come from [cc_calcofi_to_lonlat()] rather than a `grid` lookup, so a
#' historical inshore station outside the modern pattern places exactly like any
#' other instead of dropping out of its region.
#'
#' @param x data.frame of station membership, one row per (region, station).
#' @param group,line,station column names in `x` holding the region label and the
#'   CalCOFI line and station. Defaults `"region"`, `"line"`, `"station"`.
#' @return an `sf` with one row per region: the `group` column, `n_stations`,
#'   `longitude`/`latitude` of a representative point **guaranteed to fall inside
#'   the region's own polygon** (regions are concave — one wrapping another puts a
#'   centroid outside it), and `geom`, a `POLYGON` in EPSG:4326.
#' @export
#' @concept spatial
#' @examples
#' \dontrun{
#' # the four Venrick phytoplankton pooling regions
#' cc_station_regions(data.frame(
#'   region  = c("SE", "SE",   "Offshore", "Offshore"),
#'   line    = c(93.3, 93.3,   93.3,       93.3),
#'   station = c(30,   40,     70,         80)))
#' }
cc_station_regions <- function(x, group = "region", line = "line",
                               station = "station") {
  if (!requireNamespace("sf", quietly = TRUE))
    stop("cc_station_regions() needs the 'sf' package", call. = FALSE)
  stopifnot(is.data.frame(x), all(c(group, line, station) %in% names(x)))

  d <- data.frame(
    grp = as.character(x[[group]]),
    lin = as.numeric(x[[line]]),
    pos = as.numeric(x[[station]]))
  d <- d[stats::complete.cases(d), , drop = FALSE]
  if (!nrow(d)) stop("no complete (region, line, station) rows", call. = FALSE)

  # A station in two regions makes the partition ill-defined — every point it
  # owns would belong to both. That is a membership error in the source, not
  # something to average away, so fail rather than silently pick one.
  dup <- d[duplicated(d[c("lin", "pos")]) | duplicated(d[c("lin", "pos")], fromLast = TRUE), ]
  dup <- unique(dup)
  if (nrow(dup) > nrow(unique(dup[c("lin", "pos")])))
    stop("station(s) declared in more than one region: ",
         paste(unique(paste0(dup$lin, "/", dup$pos)), collapse = ", "), call. = FALSE)
  d <- unique(d)

  ll <- cc_calcofi_to_lonlat(d$lin, d$pos)
  d$longitude <- ll$longitude; d$latitude <- ll$latitude
  if (anyNA(d$longitude))
    stop("cc_calcofi_to_lonlat() could not place ", sum(is.na(d$longitude)),
         " station(s)", call. = FALSE)

  # Voronoi on lon/lat degrees would stretch cells with latitude, so tessellate
  # in an equal-area conic fitted to the data rather than to a fixed region.
  rl <- range(d$latitude); rn <- range(d$longitude)
  crs_work <- sprintf(
    "+proj=aea +lat_1=%f +lat_2=%f +lat_0=%f +lon_0=%f +datum=WGS84 +units=m",
    rl[1] + diff(rl) / 6, rl[2] - diff(rl) / 6, mean(rl), mean(rn))

  pts    <- sf::st_transform(
    sf::st_as_sf(d, coords = c("longitude", "latitude"), crs = 4326, remove = FALSE),
    sf::st_crs(crs_work))
  domain <- sf::st_convex_hull(sf::st_union(pts))

  if (nrow(pts) < 3) {
    # too few stations for a hull: fall back to the cells themselves, bounded by
    # a buffer, so a tiny membership list still yields a polygon
    domain <- sf::st_buffer(sf::st_union(pts), 20000)
  }
  env <- sf::st_as_sfc(sf::st_bbox(sf::st_buffer(domain, 5e5)))
  vor <- sf::st_collection_extract(
    sf::st_voronoi(sf::st_union(pts), envelope = env), "POLYGON")

  # st_voronoi returns cells in its own order, so map each generator to its cell
  # rather than assuming the two line up
  idx <- vapply(sf::st_intersects(pts, vor), function(i) i[1], integer(1))
  stopifnot(!anyNA(idx), !anyDuplicated(idx))
  # geometry-only intersection: st_intersection() on an `sf` warns that it is
  # assuming attributes are spatially constant, which is noise here — the only
  # attribute is the region label, and clipping a cell does not change it
  cells <- sf::st_intersection(sf::st_geometry(vor)[idx], domain)
  # st_intersection() DROPS non-intersecting geometries rather than returning
  # empties, so a short result would shift every region label by one silently.
  # Each cell contains its own generator and the domain is the hull of all of
  # them, so this cannot happen — assert it rather than rely on it.
  stopifnot(length(cells) == nrow(pts))

  out <- lapply(split(seq_along(cells), pts$grp), function(i)
    sf::st_make_valid(sf::st_union(cells[i])))
  grps <- names(out)
  geom <- sf::st_sfc(lapply(out, function(g) g[[1]]), crs = sf::st_crs(crs_work))

  n_parts <- vapply(geom, function(g)
    if (inherits(g, "MULTIPOLYGON")) length(g) else 1L, integer(1))
  if (any(n_parts > 1))
    warning("region(s) not contiguous: ",
            paste(grps[n_parts > 1], collapse = ", "), call. = FALSE)

  # point_on_surface, not centroid: a region that wraps around another is
  # concave, and its centroid can land in the neighbour
  ctr <- sf::st_coordinates(sf::st_transform(
    suppressWarnings(sf::st_point_on_surface(geom)), 4326))

  res <- sf::st_sf(
    grp        = grps,
    n_stations = as.integer(table(d$grp)[grps]),
    longitude  = ctr[, 1],
    latitude   = ctr[, 2],
    geom       = sf::st_transform(geom, 4326))
  names(res)[names(res) == "grp"] <- group
  res[order(res[[group]]), ]
}

#' Convert longitude/latitude to CalCOFI line/station
#'
#' The inverse of [cc_calcofi_to_lonlat()]. Returns the CONTINUOUS line/station
#' position, not the nearest standard station — 90.7 is a real answer, not a
#' rounding error, and rounding it would silently move a sample onto a station it
#' was not taken at. Round deliberately at the call site if a station label is
#' what you want.
#'
#' @param lon,lat numeric vectors (WGS 84), recycled to a common length
#' @return a data.frame with `line` and `station`, one row per input
#' @export
#' @concept spatial
cc_lonlat_to_calcofi <- function(lon, lat) {
  n <- max(length(lon), length(lat))
  lon <- rep_len(lon, n); lat <- rep_len(lat, n)
  out <- data.frame(line = rep(NA_real_, n), station = rep(NA_real_, n))
  ok <- !is.na(lon) & !is.na(lat) & is.finite(lon) & is.finite(lat)
  if (!any(ok)) return(out)

  pts <- sf::st_as_sf(
    data.frame(x = as.numeric(lon[ok]), y = as.numeric(lat[ok])),
    coords = c("x", "y"), crs = 4326)
  xy <- sf::st_coordinates(sf::st_transform(pts, sf::st_crs("+proj=calcofi")))
  out$line[ok]    <- xy[, 1]
  out$station[ok] <- xy[, 2]
  out
}
