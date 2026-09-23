# ctd derived hydrographic products: values computed from a CTD profile, not measured ----------
#
# Rasmus Swalethorp asked (CTD-on-EDI meeting 2026-09-16; email 2026-09-22) for derived
# products shown beside the measured series in ctd-transects and the Explorer: spice, mixed-layer
# depth, depth of the chlorophyll-a maximum, integrated chlorophyll-a, averaged sigma-theta and
# relative geostrophic velocity (CalCOFI/workflows#98, #99-#103). Each rule lives here once, so the
# ingest that stages `calcofi_ctd-derived`, a notebook and a test cannot disagree.
#
# Two rules every function below follows:
# - a value its provider flags 8 (questionable) or 9 (bad) never enters a derived value; the flag
#   is dropped first, with the same normalization `combine_sensor_pair()` uses ("8.0" == "8");
# - inputs are the full-resolution 1 m bins (the cast files / `obs_ctd_full`), never the thinned
#   `ctd_thin` series in `obs`: an MLD or a chl max read off a 10 m grid is biased toward the grid.

#' Averaged sigma-theta from a CTD sensor pair
#'
#' `sigma_theta_1` / `sigma_theta_2` (the files' `SigThetaTS1` / `SigThetaTS2`) combined by the same
#' flag rule as every other CTD pair — see [combine_sensor_pair()]: a sensor flagged 8/9 is dropped,
#' 1/2 select a sensor, otherwise the mean (Rasmus Swalethorp, 2026-09-22: "average unless one is
#' flagged"; CalCOFI/workflows#99).
#'
#' @param s1,s2 numeric: sigma-theta from sensor pair 1 and 2 (kg m^-3).
#' @param q1,q2 their quality codes (`NA` = good).
#' @return numeric vector, the averaged sigma-theta.
#' @export
#' @concept ctd_derived
#' @examples
#' ctd_sigma_theta_ave(c(25.1, 25.1), c(25.3, 25.3), q1 = c(NA, "9"))
#' # 25.2 25.3
ctd_sigma_theta_ave <- function(s1, s2, q1 = NA, q2 = NA) {
  stopifnot(is.numeric(s1) || all(is.na(s1)), is.numeric(s2) || all(is.na(s2)))
  combine_sensor_pair(s1, s2, q1, q2)
}

#' Spice (spiciness at 0 dbar, TEOS-10) per CTD sample
#'
#' Rasmus Swalethorp's recipe (email 2026-09-22; CalCOFI/workflows#100), with the Gibbs SeaWater
#' toolbox: absolute salinity from practical salinity, conservative temperature from in-situ
#' temperature, then `gsw_spiciness0()` (McDougall & Krzysik 2015, *J. Mar. Res.* 73, 141-152).
#' Positive is "spicy" (warm, salty), negative "minty" (cool, fresh). Pass the sensor-pair
#' averaged, corrected series (`temperature_ave`, `salinity_ave_corr`), not a single sensor; a
#' sample whose temperature or salinity is flagged 8/9 is `NA`.
#'
#' @param temperature in-situ temperature (deg C, ITS-90).
#' @param salinity practical salinity (PSS-78).
#' @param pressure sea pressure (dbar); depth in metres is an acceptable stand-in in the upper
#'   500 m (the difference is < 1 %, well inside spice's sensitivity).
#' @param longitude,latitude decimal degrees (recycled).
#' @param q_temperature,q_salinity quality codes (`NA` = good).
#' @return numeric vector, spiciness (kg m^-3).
#' @export
#' @concept ctd_derived
#' @importFrom gsw gsw_SA_from_SP gsw_CT_from_t gsw_spiciness0
#' @examples
#' ctd_spice(15, 33.5, 10, -120, 33)
ctd_spice <- function(temperature, salinity, pressure, longitude, latitude,
                      q_temperature = NA, q_salinity = NA) {
  n <- max(length(temperature), length(salinity), length(pressure))
  stopifnot(n >= 1, is.numeric(longitude), is.numeric(latitude))
  t  <- .drop_flagged(rep_len(as.numeric(temperature), n), q_temperature)
  sp <- .drop_flagged(rep_len(as.numeric(salinity), n), q_salinity)
  p  <- rep_len(as.numeric(pressure), n)
  out <- rep(NA_real_, n)
  ok <- !is.na(t) & !is.na(sp) & !is.na(p)
  if (!any(ok)) return(out)
  lon <- rep_len(longitude, n)[ok]; lat <- rep_len(latitude, n)[ok]
  sa <- gsw::gsw_SA_from_SP(sp[ok], p[ok], lon, lat)
  ct <- gsw::gsw_CT_from_t(sa, t[ok], p[ok])
  out[ok] <- gsw::gsw_spiciness0(sa, ct)
  out
}

#' Mixed-layer depth of one CTD cast (threshold criterion)
#'
#' The first depth below `ref_depth` where the profile departs from its value at `ref_depth` by
#' `threshold`, linearly interpolated between the two bracketing samples (CalCOFI/workflows#101).
#' The criterion is an argument because the choice is Rasmus Swalethorp's to confirm; the defaults
#' are de Boyer Montegut et al. (2004, *JGR* 109, C12003): reference 10 m, sigma-theta increase of
#' 0.03 kg m^-3. The classic alternative is 0.125 kg m^-3 (Levitus 1982; Monterey & Levitus 1997);
#' the temperature criterion (|Delta T| >= 0.2 deg C) needs no salinity, so it is the only one a
#' `preliminary_without_bottle` cast can have.
#'
#' The value at `ref_depth` is interpolated from the samples either side of it (it is `NA`, status
#' `no_reference`, when the cast does not bracket it); a cast that never crosses the threshold has
#' `mld_m = NA` and status `mixed_to_bottom` (its deepest sample is reported, so a consumer can say
#' "deeper than"). Flagged samples (8/9) are dropped first.
#'
#' @param depth depth (m), positive down; one cast.
#' @param value sigma-theta (kg m^-3) or temperature (deg C), per `criterion`.
#' @param qual quality codes of `value` (`NA` = good).
#' @param criterion `"sigma_theta"` (value must increase by `threshold`) or `"temperature"`
#'   (absolute departure of `threshold`).
#' @param threshold the departure that ends the mixed layer; default 0.03 for sigma-theta, 0.2
#'   for temperature.
#' @param ref_depth reference depth (m), default 10.
#' @return one-row tibble: `mld_m`, `ref_value`, `depth_max_m` (deepest good sample), `status`
#'   (`ok`, `mixed_to_bottom`, `no_reference`, `no_data`), `criterion`, `threshold`, `ref_depth`.
#' @export
#' @concept ctd_derived
#' @importFrom tibble tibble
#' @importFrom stats approx aggregate
#' @examples
#' z <- 0:100
#' ctd_mld(z, ifelse(z < 40, 25, 25.5))           # step at 40 m
#' ctd_mld(z, ifelse(z < 40, 25, 25.5), threshold = 0.125)
ctd_mld <- function(depth, value, qual = NA, criterion = c("sigma_theta", "temperature"),
                    threshold = NULL, ref_depth = 10) {
  criterion <- match.arg(criterion)
  if (is.null(threshold)) threshold <- if (criterion == "sigma_theta") 0.03 else 0.2
  stopifnot(is.numeric(depth), length(depth) == length(value),
            is.numeric(threshold), length(threshold) == 1, threshold > 0,
            is.numeric(ref_depth), length(ref_depth) == 1, ref_depth >= 0)
  prof <- .clean_profile(depth, value, qual)
  res <- function(mld = NA_real_, ref = NA_real_, status) tibble::tibble(
    mld_m = as.numeric(mld), ref_value = as.numeric(ref),
    depth_max_m = if (nrow(prof)) max(prof$depth) else NA_real_, status = status,
    criterion = criterion, threshold = threshold, ref_depth = ref_depth)
  if (nrow(prof) < 2) return(res(status = "no_data"))
  if (ref_depth < min(prof$depth) || ref_depth > max(prof$depth)) return(res(status = "no_reference"))
  ref <- stats::approx(prof$depth, prof$value, xout = ref_depth, ties = mean)$y
  dev <- if (criterion == "sigma_theta") prof$value - ref else abs(prof$value - ref)
  below <- prof$depth > ref_depth
  hit <- which(below & dev >= threshold)
  if (!length(hit)) return(res(ref = ref, status = "mixed_to_bottom"))
  i <- hit[1]
  # bracket: the sample above the first crossing (or the reference itself when the crossing is
  # the first sample below it)
  z0 <- if (i > 1 && prof$depth[i - 1] >= ref_depth) prof$depth[i - 1] else ref_depth
  d0 <- if (i > 1 && prof$depth[i - 1] >= ref_depth) dev[i - 1] else 0
  z1 <- prof$depth[i]; d1 <- dev[i]
  mld <- if (d1 == d0) z1 else z0 + (threshold - d0) * (z1 - z0) / (d1 - d0)
  res(mld = mld, ref = ref, status = "ok")
}

#' Depth of the chlorophyll-a maximum of one CTD cast
#'
#' The depth of the maximum of a running median of the profile (default 5 m window), so a single
#' spiking bin cannot be "the max" (CalCOFI/workflows#102). Use the bottle-fitted sensor estimate
#' `est_chlorophyll_a_sta_corr` (Rasmus Swalethorp, 2026-09-09). The window is in samples of the
#' 1 m bins (`window_m` bins, forced odd); on irregular spacing it is a window of that many samples.
#' Flagged samples are dropped first. Where the median flattens the peak into a plateau, the
#' plateau depth with the highest raw value wins (then the shallowest).
#'
#' @param depth depth (m), one cast.
#' @param chl chlorophyll-a (mg m^-3).
#' @param qual quality codes of `chl` (`NA` = good).
#' @param window_m running-median window (1 m bins), default 5.
#' @return one-row tibble: `chl_max_depth_m`, `chl_max_value` (the smoothed value there), `n`.
#' @export
#' @concept ctd_derived
#' @importFrom stats runmed
#' @examples
#' z <- 0:150
#' ctd_chl_max(z, 0.2 + 2 * exp(-((z - 40) / 10)^2))
ctd_chl_max <- function(depth, chl, qual = NA, window_m = 5) {
  stopifnot(is.numeric(depth), length(depth) == length(chl),
            is.numeric(window_m), length(window_m) == 1, window_m >= 1)
  prof <- .clean_profile(depth, chl, qual)
  if (!nrow(prof)) return(tibble::tibble(chl_max_depth_m = NA_real_, chl_max_value = NA_real_, n = 0L))
  k <- as.integer(window_m); if (k %% 2 == 0) k <- k + 1L
  sm <- if (nrow(prof) >= k && k > 1) as.numeric(stats::runmed(prof$value, k, endrule = "median")) else prof$value
  # a running median flattens a peak into a plateau: among the depths tied at the smoothed
  # maximum, take the one whose raw value is highest (then the shallowest)
  top <- which(sm == max(sm))
  i <- top[which.max(prof$value[top])]
  tibble::tibble(chl_max_depth_m = prof$depth[i], chl_max_value = sm[i], n = nrow(prof))
}

#' Depth-integrate one CTD cast's profile
#'
#' Trapezoidal integral of `value` from the surface to `z_max` (default 200 m) or to the deepest
#' good sample if shallower, which is recorded (CalCOFI/workflows#102; Rasmus Swalethorp,
#' 2026-09-16: integrated chlorophyll "is just summing up all the 1 m bins" — on 1 m bins the
#' trapezoid and the sum differ only by half the two end bins). The shallowest good sample is
#' carried up to the surface when it is no deeper than `z_top_max` (default 5 m); a cast that
#' starts deeper is `NA` (status `no_surface`), so a missing surface is never silently skipped.
#' Flagged samples are dropped first. For chlorophyll-a in mg m^-3 the result is mg m^-2.
#'
#' @param depth depth (m), one cast.
#' @param value the quantity per unit volume.
#' @param qual quality codes of `value` (`NA` = good).
#' @param z_max integration floor (m), default 200.
#' @param z_top_max deepest acceptable first sample (m), default 5.
#' @return one-row tibble: `integrated`, `depth_reached_m`, `status` (`ok`, `shallow`, `no_surface`,
#'   `no_data`), `z_max`.
#' @export
#' @concept ctd_derived
#' @examples
#' ctd_integrate(0:300, rep(1, 301))   # 200 over 0-200 m
#' @importFrom utils head tail
ctd_integrate <- function(depth, value, qual = NA, z_max = 200, z_top_max = 5) {
  stopifnot(is.numeric(depth), length(depth) == length(value),
            is.numeric(z_max), length(z_max) == 1, z_max > 0,
            is.numeric(z_top_max), length(z_top_max) == 1, z_top_max >= 0)
  prof <- .clean_profile(depth, value, qual)
  res <- function(x = NA_real_, reached = NA_real_, status) tibble::tibble(
    integrated = as.numeric(x), depth_reached_m = as.numeric(reached), status = status, z_max = z_max)
  if (!nrow(prof)) return(res(status = "no_data"))
  if (min(prof$depth) > z_top_max) return(res(status = "no_surface"))
  z <- prof$depth; v <- prof$value
  if (z[1] > 0) { z <- c(0, z); v <- c(v[1], v) }
  bottom <- min(z_max, max(z))
  if (max(z) > bottom) {
    vb <- stats::approx(z, v, xout = bottom, ties = mean)$y
    keep <- z < bottom
    z <- c(z[keep], bottom); v <- c(v[keep], vb)
  }
  x <- if (length(z) < 2) 0 else sum(diff(z) * (utils::head(v, -1) + utils::tail(v, -1)) / 2)
  res(x, bottom, if (bottom < z_max) "shallow" else "ok")
}

#' Relative geostrophic velocity between adjacent CTD stations
#'
#' Rasmus Swalethorp's recipe (email 2026-09-22; CalCOFI/workflows#103): per station, absolute
#' salinity and conservative temperature on a `dp` dbar grid to `p_ref`, dynamic height anomaly
#' relative to `p_ref` (`gsw_geo_strf_dyn_height()`), then for each adjacent pair
#' `v = (dh2 - dh1) / (f * dx)` with `f` the Coriolis parameter at the pair's mean latitude and `dx`
#' the distance between the two stations. Positive is 90 degrees to the left of the direction of
#' increasing station order — for stations ordered nearshore to offshore on a CalCOFI line
#' (which runs west-south-west) that is equatorward.
#'
#' The flow is relative to `p_ref` (there is no level of known motion), so it has **no anomaly**.
#' A station shallower than `p_ref` has its deepest density carried down (`approx(rule = 2)`, as in
#' the recipe) and the pair is marked `shallow = TRUE`. Flagged temperature or salinity samples are
#' dropped first; a station with fewer than 2 good samples is skipped.
#'
#' @param casts data frame, one row per sample, ordered or orderable by `station_order`: columns
#'   `station` (id), `latitude`, `longitude`, `pressure` (dbar), `temperature` (deg C),
#'   `salinity` (PSS-78); optional `q_temperature`, `q_salinity`, `station_order` (numeric, default
#'   the order of first appearance) and `dist_km` (along-line distance; default the great-circle
#'   distance between the pair).
#' @param p_ref reference pressure (dbar), default 500.
#' @param dp grid spacing (dbar), default 1.
#' @param min_dx_km minimum station spacing (km), default 10. A station closer than this to the
#'   last station kept is skipped: the geostrophic shear is `1 / dx`, so the extra inshore stations
#'   a few km apart (the SCCOOS 90.27.7 beside 90.28, 93.26.4 beside 93.26.7) turn a small density
#'   difference into metres per second of spurious flow.
#' @return tibble, one row per (station pair x pressure): `station_1`, `station_2`,
#'   `dist_mid_km` (from the first station), `dx_km`, `pressure`, `velocity_m_s`, `shallow`.
#' @export
#' @concept ctd_derived
#' @importFrom gsw gsw_geo_strf_dyn_height
#' @importFrom dplyr bind_rows
ctd_geostrophic <- function(casts, p_ref = 500, dp = 1, min_dx_km = 10) {
  need <- c("station", "latitude", "longitude", "pressure", "temperature", "salinity")
  stopifnot(is.data.frame(casts), all(need %in% names(casts)),
            is.numeric(p_ref), length(p_ref) == 1, p_ref > 0, is.numeric(dp), dp > 0,
            is.numeric(min_dx_km), length(min_dx_km) == 1, min_dx_km >= 0)
  if (!"q_temperature" %in% names(casts)) casts$q_temperature <- NA
  if (!"q_salinity" %in% names(casts)) casts$q_salinity <- NA
  if (!"station_order" %in% names(casts)) casts$station_order <- match(casts$station, unique(casts$station))
  empty <- tibble::tibble(station_1 = casts$station[0], station_2 = casts$station[0],
                          dist_mid_km = numeric(), dx_km = numeric(), pressure = numeric(),
                          velocity_m_s = numeric(), shallow = logical())
  p_grid <- seq(dp, p_ref, by = dp)
  st <- unique(casts[order(casts$station_order), c("station", "station_order")])$station
  prof <- lapply(st, function(s) {
    d <- casts[casts$station == s, , drop = FALSE]
    t  <- .drop_flagged(as.numeric(d$temperature), d$q_temperature)
    sp <- .drop_flagged(as.numeric(d$salinity), d$q_salinity)
    ok <- !is.na(t) & !is.na(sp) & !is.na(d$pressure)
    if (sum(ok) < 2) return(NULL)
    lat <- d$latitude[ok][1]; lon <- d$longitude[ok][1]
    sa <- gsw::gsw_SA_from_SP(sp[ok], d$pressure[ok], lon, lat)
    ct <- gsw::gsw_CT_from_t(sa, t[ok], d$pressure[ok])
    o <- order(d$pressure[ok]); p <- d$pressure[ok][o]; sa <- sa[o]; ct <- ct[o]
    keep <- !duplicated(p)
    sa_g <- stats::approx(p[keep], sa[keep], xout = p_grid, rule = 2)$y
    ct_g <- stats::approx(p[keep], ct[keep], xout = p_grid, rule = 2)$y
    list(station = s, lat = lat, lon = lon,
         dist_km = if ("dist_km" %in% names(d)) d$dist_km[1] else NA_real_,
         shallow = max(p) < p_ref,
         dh = as.numeric(gsw::gsw_geo_strf_dyn_height(sa_g, ct_g, p_grid, p_ref = p_ref)))
  })
  prof <- Filter(Negate(is.null), prof)
  if (length(prof) < 2) return(empty)
  spacing <- function(a, b) if (!is.na(a$dist_km) && !is.na(b$dist_km)) b$dist_km - a$dist_km
                            else .haversine_km(a$lat, a$lon, b$lat, b$lon)
  kept <- prof[1]
  for (b in prof[-1]) if (spacing(kept[[length(kept)]], b) >= min_dx_km) kept[[length(kept) + 1]] <- b
  prof <- kept
  if (length(prof) < 2) return(empty)
  dist0 <- 0
  out <- vector("list", length(prof) - 1)
  for (i in seq_len(length(prof) - 1)) {
    a <- prof[[i]]; b <- prof[[i + 1]]
    dx <- spacing(a, b)
    if (!is.finite(dx) || dx <= 0) next
    f <- 2 * 7.292115e-5 * sin(mean(c(a$lat, b$lat)) * pi / 180)
    out[[i]] <- tibble::tibble(
      station_1 = a$station, station_2 = b$station,
      dist_mid_km = dist0 + dx / 2, dx_km = dx, pressure = p_grid,
      velocity_m_s = (b$dh - a$dh) / (f * dx * 1000),
      shallow = a$shallow || b$shallow)
    dist0 <- dist0 + dx
  }
  out <- dplyr::bind_rows(Filter(Negate(is.null), out))
  if (!nrow(out)) empty else out
}

# helpers ---------------------------------------------------------------------------------------

# a flagged (8/9) value becomes NA; quality codes normalized as combine_sensor_pair() does
.drop_flagged <- function(x, q) {
  q <- rep_len(.norm_qual(q), length(x))
  x[q %in% c("8", "9")] <- NA_real_
  x
}

# one cast's profile: flagged and missing dropped, sorted by depth, duplicate depths averaged
.clean_profile <- function(depth, value, qual) {
  v <- .drop_flagged(as.numeric(value), qual)
  ok <- !is.na(depth) & !is.na(v) & is.finite(v)
  if (!any(ok)) return(data.frame(depth = numeric(), value = numeric()))
  d <- stats::aggregate(v[ok], by = list(depth = as.numeric(depth[ok])), FUN = mean)
  names(d) <- c("depth", "value")
  d[order(d$depth), , drop = FALSE]
}

.haversine_km <- function(lat1, lon1, lat2, lon2) {
  p1 <- lat1 * pi / 180; p2 <- lat2 * pi / 180
  dphi <- (lat2 - lat1) * pi / 180; dl <- (lon2 - lon1) * pi / 180
  a <- sin(dphi / 2)^2 + cos(p1) * cos(p2) * sin(dl / 2)^2
  2 * 6371 * atan2(sqrt(a), sqrt(1 - a))
}
