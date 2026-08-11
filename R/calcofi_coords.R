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
