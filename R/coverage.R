# coverage.R — measure a dataset's real extent from the assembled core, rather
# than trusting the extent an ingest asserted about itself in YAML.

#' Format a Bounding Box as a Human-Readable Extent
#'
#' Renders a decimal-degree bounding box the way a data catalog writes one:
#' unsigned magnitudes carrying a hemisphere suffix, in geographic order
#' (south to north, west to east).
#'
#' @details
#' Geographic order is preserved rather than numeric order, so a western
#' longitude span reads `"126.5–117.3°W"` — west edge first — instead of the
#' signed `"-126.5 to -117.3"`. When a span crosses the equator or the prime
#' meridian the two ends carry their own suffix (`"3.2°S–12.7°N"`), because a
#' single trailing hemisphere would silently mislabel half the range.
#'
#' @param lat_min,lat_max,lon_min,lon_max Bounds in decimal degrees. Any
#'   non-finite value (`NA`, `NaN`, `±Inf`) yields `NA_character_` — a partial
#'   box is not a box.
#' @param digits Decimal places to show.
#'
#' @return Length-1 character, e.g. `"29.8–37.8°N, 126.5–117.3°W"`, or `NA`.
#' @export
#' @concept release
#' @examples
#' format_bbox(29.8, 37.8, -126.5, -117.3)
format_bbox <- function(lat_min, lat_max, lon_min, lon_max, digits = 1) {
  v <- suppressWarnings(as.numeric(c(lat_min, lat_max, lon_min, lon_max)))
  if (length(v) != 4L || any(!is.finite(v))) return(NA_character_)
  paste0(.hemi_range(v[1], v[2], "S", "N", digits), ", ",
         .hemi_range(v[3], v[4], "W", "E", digits))
}

.hemi_range <- function(lo, hi, neg, pos, digits) {
  h <- function(x) if (x < 0) neg else pos
  f <- function(x) formatC(abs(x), format = "f", digits = digits)
  if (identical(h(lo), h(hi)))
    sprintf("%s–%s°%s", f(lo), f(hi), h(lo))
  else
    sprintf("%s°%s–%s°%s", f(lo), h(lo), f(hi), h(hi))
}

#' Measure Observed Temporal and Spatial Coverage per Dataset
#'
#' Derives each dataset's real extent from the assembled core (`sample` +
#' `obs`) instead of the `coverage_temporal` / `coverage_spatial` strings an
#' ingest asserts in its `calcofi.dataset_meta` YAML.
#'
#' @details
#' **Why measure rather than assert.** A hand-written extent cannot help going
#' stale — it is authored once and the data grows underneath it. Checked
#' against release `v2026.08.06`, the asserted temporal string was wrong for 7
#' of 15 datasets: `cce-lter_zoodb` claimed coverage through 2021-05 when its
#' data ends 2015-04, `calcofi_phyllosoma` stopped a year short of its own
#' rows, and three datasets said `"present"` while in fact stalling in 2019,
#' 2022 and 2023.
#'
#' **`NaN` is not `NULL`.** A `NaN` coordinate survives `IS NOT NULL`, and
#' `min()`/`max()` propagate it, so a single poisoned row would blow a
#' dataset's whole bounding box out to `NaN` while every nullity check passed.
#' The coordinate filter is `isfinite()`, which rejects `NaN` and `±Inf` alike.
#' See the same trap in [append_sample()], which normalizes these at write time.
#'
#' **Absent beats invented.** A dataset with no usable datetimes gets `NA` for
#' the temporal half, not a guess — `calcofi_phytoplankton` is region-pooled and
#' carries coordinates but no `datetime`, so it legitimately measures spatially
#' and not temporally. Callers fall back to a declared static value there.
#'
#' @param con DuckDB connection holding the assembled core.
#' @param tables Tables to measure, in order. Each contributes whichever of
#'   `datetime` / `latitude` / `longitude` it actually has; a table absent from
#'   the connection is skipped rather than erroring.
#' @param digits Decimal places for the formatted bbox label.
#'
#' @return Tibble, one row per `dataset_key`, sorted by key:
#'   `time_min`/`time_max` (`"YYYY-MM"`), `lat_min`/`lat_max`/`lon_min`/
#'   `lon_max` (numeric), and the display labels
#'   `coverage_temporal_observed` / `coverage_spatial_observed`.
#'
#' @export
#' @concept release
#' @importFrom DBI dbListTables dbGetQuery
#' @importFrom glue glue
#' @importFrom tibble tibble
observed_coverage <- function(con,
                              tables = c("sample", "obs"),
                              digits = 1) {
  present <- intersect(tables, DBI::dbListTables(con))
  cols_of <- function(tbl) DBI::dbGetQuery(con, glue::glue(
    "SELECT column_name FROM information_schema.columns
     WHERE table_name = '{tbl}'"))$column_name

  time_parts <- character()
  geo_parts  <- character()
  for (tbl in present) {
    cols <- cols_of(tbl)
    if (!"dataset_key" %in% cols) next
    if ("datetime" %in% cols)
      time_parts <- c(time_parts, glue::glue(
        'SELECT dataset_key, datetime FROM "{tbl}" WHERE datetime IS NOT NULL'))
    if (all(c("latitude", "longitude") %in% cols))
      geo_parts <- c(geo_parts, glue::glue(
        'SELECT dataset_key, latitude, longitude FROM "{tbl}"
         WHERE isfinite(latitude) AND isfinite(longitude)'))
  }

  d_time <- if (length(time_parts)) DBI::dbGetQuery(con, glue::glue("
    SELECT dataset_key,
           strftime(min(datetime), '%Y-%m') AS time_min,
           strftime(max(datetime), '%Y-%m') AS time_max
    FROM ({paste(time_parts, collapse = ' UNION ALL ')})
    GROUP BY 1")) else
      data.frame(dataset_key = character(), time_min = character(),
                 time_max = character(), stringsAsFactors = FALSE)

  d_geo <- if (length(geo_parts)) DBI::dbGetQuery(con, glue::glue("
    SELECT dataset_key,
           min(latitude)  AS lat_min, max(latitude)  AS lat_max,
           min(longitude) AS lon_min, max(longitude) AS lon_max
    FROM ({paste(geo_parts, collapse = ' UNION ALL ')})
    GROUP BY 1")) else
      data.frame(dataset_key = character(), lat_min = numeric(),
                 lat_max = numeric(), lon_min = numeric(),
                 lon_max = numeric(), stringsAsFactors = FALSE)

  keys <- sort(union(d_time$dataset_key, d_geo$dataset_key))
  pick <- function(d, k, col) {
    i <- match(k, d$dataset_key)
    if (is.na(i)) if (is.numeric(d[[col]])) NA_real_ else NA_character_ else d[[col]][i]
  }
  out <- lapply(keys, function(k) {
    lat_min <- pick(d_geo, k, "lat_min"); lat_max <- pick(d_geo, k, "lat_max")
    lon_min <- pick(d_geo, k, "lon_min"); lon_max <- pick(d_geo, k, "lon_max")
    t_min   <- pick(d_time, k, "time_min"); t_max <- pick(d_time, k, "time_max")
    tibble::tibble(
      dataset_key = k,
      time_min = t_min, time_max = t_max,
      lat_min = lat_min, lat_max = lat_max,
      lon_min = lon_min, lon_max = lon_max,
      coverage_temporal_observed = if (is.na(t_min) || is.na(t_max))
        NA_character_ else paste(t_min, "to", t_max),
      coverage_spatial_observed  =
        format_bbox(lat_min, lat_max, lon_min, lon_max, digits = digits))
  })
  if (!length(out)) out <- list(tibble::tibble(
    dataset_key = character(), time_min = character(), time_max = character(),
    lat_min = numeric(), lat_max = numeric(),
    lon_min = numeric(), lon_max = numeric(),
    coverage_temporal_observed = character(),
    coverage_spatial_observed = character()))
  do.call(rbind, out)
}
