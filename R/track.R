# sample_track + COGO -----------------------------------------------------------
#
# A sample whose event has extent along the sea surface (an effort segment of a
# line-transect survey, the observer's line of sight to a sighting, later a net
# tow's start and end) carries one row in `sample_track` beside its `sample` row:
# the beginning, middle and end of the path in x, y and t, its length and a
# LINESTRING. `sample` keeps one point per event (the midpoint for a segment, the
# animal for a sighting), so every consumer that reads `sample` is unchanged.

# mean Earth radius (m), the same sphere as calcofi4r::cc_distance_km_sql()
CC_EARTH_RADIUS_M <- 6371008.8

.ensure_sample_track_schema <- function(con, tbl = "sample_track") {
  DBI::dbExecute(con, glue::glue(
    "CREATE TABLE IF NOT EXISTS {tbl} (
       sample_key     VARCHAR,
       dataset_key    VARCHAR,
       track_type     VARCHAR,
       datetime_beg   TIMESTAMP,
       datetime_mid   TIMESTAMP,
       datetime_end   TIMESTAMP,
       longitude_beg  DOUBLE,
       latitude_beg   DOUBLE,
       longitude_mid  DOUBLE,
       latitude_mid   DOUBLE,
       longitude_end  DOUBLE,
       latitude_end   DOUBLE,
       length_km      DOUBLE,
       geom           GEOMETRY)"))
  invisible(tbl)
}

#' Append path rows into the core `sample_track` table
#'
#' `sample_track` holds one row per sample whose event is a path on the sea
#' surface rather than a point: where it began and ended in x, y and t, its
#' middle, its length and a `LINESTRING` from beginning to end. The sample keeps
#' its single point in `sample` (the caller decides which: a segment's midpoint,
#' a sighting's animal position); `sample_track.sample_key` is a foreign key to
#' it and the table's primary key, so a sample has at most one path.
#'
#' `select_sql` is bound **positionally** and must yield 10 columns:
#' `sample_key`, `dataset_key`, `track_type`, `datetime_beg`, `datetime_end`,
#' `longitude_beg`, `latitude_beg`, `longitude_end`, `latitude_end`, `length_km`.
#' The middle is computed here, never supplied: `datetime_mid` is halfway in time
#' and `longitude_mid` / `latitude_mid` the great-circle midpoint. A NULL
#' `length_km` is filled with the great-circle distance; a supplied one (the
#' provider's own) is kept. `track_type` names what the path is
#' (`effort_segment`, `sight_line`, …). Times may be NULL (a sight line is
#' instantaneous: pass the same time for both ends).
#'
#' Non-finite coordinates (`NaN`, `Inf`) become NULL, as in [append_sample()],
#' and a row with any NULL end coordinate gets no `geom`. The line is minted
#' with `ST_SetCRS(…, 'EPSG:4326')` as [append_sample()] mints its point; the
#' release tags every geometry column `EPSG:4326`.
#'
#' @inheritParams append_obs
#' @param track_tbl target table (default `"sample_track"`)
#' @return (invisibly) the total row count of `track_tbl` after the append
#' @export
#' @concept model
append_sample_track <- function(con, select_sql, track_tbl = "sample_track") {
  .load_spatial(con)
  .ensure_sample_track_schema(con, track_tbl)

  src_cols <- c("sample_key", "dataset_key", "track_type", "datetime_beg", "datetime_end",
                "longitude_beg", "latitude_beg", "longitude_end", "latitude_end", "length_km")
  n_col <- nrow(DBI::dbGetQuery(con, glue::glue("DESCRIBE ({select_sql})")))
  if (n_col != length(src_cols))
    stop("append_sample_track(): `select_sql` must yield 10 columns (",
         paste(src_cols, collapse = ", "), "); got ", n_col, ".", call. = FALSE)
  src_alias <- paste(src_cols, collapse = ", ")

  fin <- function(x) glue::glue("CASE WHEN isnan({x}) OR isinf({x}) THEN NULL ELSE {x} END")
  # great-circle midpoint (spherical): B = (Bx, By), the mean of the two unit
  # vectors projected on the beginning's meridian
  mid_sql <- "
    bx AS (SELECT *,
             cos(radians(latitude_end)) * cos(radians(longitude_end - longitude_beg)) AS bx_,
             cos(radians(latitude_end)) * sin(radians(longitude_end - longitude_beg)) AS by_
           FROM fin)"
  km_sql <- calcofi4r::cc_distance_km_sql("longitude_beg", "latitude_beg", "longitude_end", "latitude_end")

  DBI::dbExecute(con, glue::glue(
    "INSERT INTO {track_tbl}
       (sample_key, dataset_key, track_type, datetime_beg, datetime_mid, datetime_end,
        longitude_beg, latitude_beg, longitude_mid, latitude_mid, longitude_end, latitude_end,
        length_km, geom)
     WITH src AS (SELECT * FROM ( {select_sql} ) AS s({src_alias})),
          fin AS (
            SELECT sample_key, dataset_key, track_type,
                   CAST(datetime_beg AS TIMESTAMP) AS datetime_beg,
                   CAST(datetime_end AS TIMESTAMP) AS datetime_end,
                   {fin('CAST(longitude_beg AS DOUBLE)')} AS longitude_beg,
                   {fin('CAST(latitude_beg  AS DOUBLE)')} AS latitude_beg,
                   {fin('CAST(longitude_end AS DOUBLE)')} AS longitude_end,
                   {fin('CAST(latitude_end  AS DOUBLE)')} AS latitude_end,
                   CAST(length_km AS DOUBLE) AS length_km
            FROM src),
          {mid_sql}
     SELECT sample_key, dataset_key, track_type, datetime_beg,
            datetime_beg + (datetime_end - datetime_beg) / 2 AS datetime_mid,
            datetime_end, longitude_beg, latitude_beg,
            longitude_beg + degrees(atan2(by_, cos(radians(latitude_beg)) + bx_)) AS longitude_mid,
            degrees(atan2(sin(radians(latitude_beg)) + sin(radians(latitude_end)),
                          sqrt(pow(cos(radians(latitude_beg)) + bx_, 2) + pow(by_, 2)))) AS latitude_mid,
            longitude_end, latitude_end,
            COALESCE(length_km, {km_sql}) AS length_km,
            CASE WHEN longitude_beg IS NULL OR latitude_beg IS NULL
                   OR longitude_end IS NULL OR latitude_end IS NULL THEN NULL
                 ELSE ST_SetCRS(ST_MakeLine(ST_Point(longitude_beg, latitude_beg),
                                            ST_Point(longitude_end, latitude_end)), 'EPSG:4326')
                 END AS geom
     FROM bx"))
  invisible(DBI::dbGetQuery(
    con, glue::glue("SELECT COUNT(*) AS n FROM {track_tbl}"))$n)
}

#' True bearing from the ship's heading and a sighting's angle off the bow
#'
#' Line-transect surveys record the angle between the ship's heading and the line
#' of sight (0 = dead ahead, 90 = abeam, 180 = astern) and the side it was on.
#' The true bearing is the heading plus the angle to starboard, minus it to port.
#'
#' @param heading ship's true heading, degrees (0-360)
#' @param angle angle off the bow, degrees (0-180)
#' @param side `"port"` or `"starboard"` (case-insensitive; `"P"`, `"S"`,
#'   `"Star"` accepted); anything else gives `NA`
#' @return true bearing in degrees, `[0, 360)`
#' @export
#' @concept spatial
#' @examples
#' cogo_true_bearing(272, 25, "Port")  # 247
#' cogo_true_bearing(350, 30, "Star")  # 20
cogo_true_bearing <- function(heading, angle, side) {
  s <- tolower(trimws(as.character(side)))
  sgn <- ifelse(s %in% c("p", "port"), -1,
         ifelse(s %in% c("s", "star", "stbd", "starboard"), 1, NA_real_))
  (heading + sgn * angle) %% 360
}

#' Destination point from a position, a true bearing and a distance
#'
#' The coordinate-geometry (COGO) step that turns an observer's position, a
#' bearing and a range into the position of what was seen. Spherical (mean
#' radius 6,371,008.8 m, the sphere of [calcofi4r::cc_distance_km_sql()]); at the
#' ranges of a visual survey (< 20 km) the error against the ellipsoid is under
#' 0.5%.
#'
#' @param lon,lat origin, decimal degrees
#' @param bearing true bearing, degrees clockwise from north
#' @param distance_m distance along the surface, metres
#' @return a data frame with `longitude`, `latitude` (decimal degrees,
#'   longitude in `[-180, 180)`); `NA` wherever an input is `NA`
#' @export
#' @concept spatial
#' @examples
#' cogo_destination(-120, 34, 90, 1000)
cogo_destination <- function(lon, lat, bearing, distance_m) {
  d  <- distance_m / CC_EARTH_RADIUS_M
  p1 <- lat * pi / 180
  l1 <- lon * pi / 180
  b  <- bearing * pi / 180
  p2 <- asin(sin(p1) * cos(d) + cos(p1) * sin(d) * cos(b))
  l2 <- l1 + atan2(sin(b) * sin(d) * cos(p1), cos(d) - sin(p1) * sin(p2))
  data.frame(
    longitude = ((l2 * 180 / pi + 540) %% 360) - 180,
    latitude  = p2 * 180 / pi)
}
