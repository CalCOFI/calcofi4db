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
#' The default is the provider's definition, the CalCOFI legacy one (Rasmus Swalethorp, 2026-09-23,
#' questions.csv Q01, adopted 2026-10-01): "MLD is the depth at which sigma-theta is 0.02 kg/m3
#' greater [than at] a reference depth of 10 m". The criterion stays an argument: the
#' alternatives published beside it are the classic 0.125 kg m^-3 (Levitus 1982; Monterey &
#' Levitus 1997) and the temperature criterion (|Delta T| >= 0.2 deg C, de Boyer Montegut et al.
#' 2004), which needs no salinity, so it is the only one a `preliminary_without_bottle` cast can
#' have. The 0.03 kg m^-3 default of calcofi4db 4.16.0-4.17.x (de Boyer Montegut et al. 2004) is
#' superseded and only reachable as `threshold = 0.03`.
#'
#' The provider's answer is silent on the edge cases, so these are unchanged from 4.16.0: the
#' value at `ref_depth` is interpolated from the samples either side of it (it is `NA`, status
#' `no_reference`, when the cast does not bracket it, e.g. a cast starting below 10 m); a cast
#' that never crosses the threshold has `mld_m = NA` and status `mixed_to_bottom` (its deepest
#' sample is reported, so a consumer can say "deeper than"). Flagged (8/9), missing and
#' non-finite samples are dropped first, so a `NaN` bin is a gap the interpolation spans.
#'
#' @param depth depth (m), positive down; one cast.
#' @param value sigma-theta (kg m^-3) or temperature (deg C), per `criterion`.
#' @param qual quality codes of `value` (`NA` = good).
#' @param criterion `"sigma_theta"` (value must increase by `threshold`) or `"temperature"`
#'   (absolute departure of `threshold`).
#' @param threshold the departure that ends the mixed layer; default 0.02 for sigma-theta (the
#'   provider's definition), 0.2 for temperature.
#' @param ref_depth reference depth (m), default 10.
#' @return one-row tibble: `mld_m`, `ref_value`, `depth_max_m` (deepest good sample), `status`
#'   (`ok`, `mixed_to_bottom`, `no_reference`, `no_data`), `criterion`, `threshold`, `ref_depth`.
#' @export
#' @concept ctd_derived
#' @importFrom tibble tibble
#' @importFrom stats approx aggregate
#' @examples
#' z <- 0:100
#' ctd_mld(z, ifelse(z < 40, 25, 25.5))           # step at 40 m, the provider's 0.02
#' ctd_mld(z, ifelse(z < 40, 25, 25.5), threshold = 0.125)
ctd_mld <- function(depth, value, qual = NA, criterion = c("sigma_theta", "temperature"),
                    threshold = NULL, ref_depth = 10) {
  criterion <- match.arg(criterion)
  if (is.null(threshold)) threshold <- if (criterion == "sigma_theta") 0.02 else 0.2
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

#' Depth of the chlorophyll-a maximum (DCM) of one CTD cast
#'
#' The provider's definition (Rasmus Swalethorp, 2026-09-23, questions.csv Q02, adopted
#' 2026-10-01): "To avoid a potential narrow spike being adopted as the max I suggest we do a 3 m
#' running mean (I was considering 5m but sometimes the layers can be pretty narrow), and call the
#' depth of the highest value within that running mean the DCM" (CalCOFI/workflows#102). Use the
#' bottle-fitted sensor estimate `est_chlorophyll_a_sta_corr` (Rasmus Swalethorp, 2026-09-09).
#' This replaces the 5-sample running median of calcofi4db 4.16.0-4.17.x.
#'
#' The window is in metres of depth, not in samples: each good sample's smoothed value is the mean
#' of the good samples within `window_m / 2` of it (on the 1 m bins, the bin and its two
#' neighbours), so an uneven grid or a gap is averaged over what the water column actually has.
#' The answer is silent on the ends of the profile and on ties, so: the window is truncated at the
#' top and bottom (the shallowest bin averages itself and the bin below), and among depths tied at
#' the smoothed maximum the one whose raw value is highest wins, then the shallowest (the 4.16.0
#' tie rule; a flat profile's DCM is its shallowest bin). Flagged (8/9), missing and non-finite
#' samples are dropped first.
#'
#' @param depth depth (m), one cast.
#' @param chl chlorophyll-a (mg m^-3).
#' @param qual quality codes of `chl` (`NA` = good).
#' @param window_m running-mean window (m), default 3; `0` takes the raw maximum.
#' @return one-row tibble: `chl_max_depth_m`, `chl_max_value` (the running mean there), `n`.
#' @export
#' @concept ctd_derived
#' @examples
#' z <- 0:150
#' ctd_chl_max(z, 0.2 + 2 * exp(-((z - 40) / 10)^2))
ctd_chl_max <- function(depth, chl, qual = NA, window_m = 3) {
  stopifnot(is.numeric(depth), length(depth) == length(chl),
            is.numeric(window_m), length(window_m) == 1, window_m >= 0)
  prof <- .clean_profile(depth, chl, qual)
  if (!nrow(prof)) return(tibble::tibble(chl_max_depth_m = NA_real_, chl_max_value = NA_real_, n = 0L))
  sm <- .running_mean_m(prof$depth, prof$value, window_m)
  # among the depths tied at the smoothed maximum, take the one whose raw value is highest
  # (then the shallowest)
  top <- which(sm == max(sm))
  i <- top[which.max(prof$value[top])]
  tibble::tibble(chl_max_depth_m = prof$depth[i], chl_max_value = sm[i], n = nrow(prof))
}

#' Depth-integrate one CTD cast's profile
#'
#' The provider's definition for the CTD (Rasmus Swalethorp, 2026-09-23, questions.csv Q02,
#' adopted 2026-10-01): "For the CTD data here we should just sum up all the 1m bins within the top
#' 200m or as deep as the station was on shallower stations. For the bottle data we will need to
#' integrate between points. I believe trapezoidal integration was used in the past"
#' (CalCOFI/workflows#102). So `method = "sum"` (the default) sums the `bin_m` bins from the
#' surface to `z_max` (default 200 m), or to the deepest good sample if shallower, which is
#' recorded; `method = "trapezoid"` is the trapezoidal integral between points, for bottle data
#' (and the CTD rule of calcofi4db 4.16.0-4.17.x). For chlorophyll-a in mg m^-3 the result is
#' mg m^-2.
#'
#' The sum runs over the bins centred at `bin_m, 2 * bin_m, ..., bottom` (200 bins of 1 m in the
#' top 200 m). The answer is silent on missing bins, so the 4.16.0 behaviour is kept for both
#' methods: the shallowest good sample is carried up to the surface when it is no deeper than
#' `z_top_max` (default 5 m), a cast that starts deeper is `NA` (status `no_surface`, so a missing
#' surface is never silently skipped), and a bin missing inside the profile (a flagged or `NaN`
#' bin) is filled by linear interpolation between its neighbours rather than counted as zero.
#' Flagged (8/9), missing and non-finite samples are dropped first.
#'
#' @param depth depth (m), one cast.
#' @param value the quantity per unit volume.
#' @param qual quality codes of `value` (`NA` = good).
#' @param z_max integration floor (m), default 200.
#' @param z_top_max deepest acceptable first sample (m), default 5.
#' @param method `"sum"` (the provider's CTD rule: the sum of the `bin_m` bins) or `"trapezoid"`
#'   (between points, for bottle data).
#' @param bin_m the bin thickness `"sum"` adds up (m), default 1.
#' @return one-row tibble: `integrated`, `depth_reached_m`, `status` (`ok`, `shallow`, `no_surface`,
#'   `no_data`), `z_max`, `method`.
#' @export
#' @concept ctd_derived
#' @examples
#' ctd_integrate(0:300, rep(1, 301))                       # 200 over 0-200 m
#' ctd_integrate(c(0, 100, 300), c(1, 1, 3), method = "trapezoid")
#' @importFrom utils head tail
ctd_integrate <- function(depth, value, qual = NA, z_max = 200, z_top_max = 5,
                          method = c("sum", "trapezoid"), bin_m = 1) {
  method <- match.arg(method)
  stopifnot(is.numeric(depth), length(depth) == length(value),
            is.numeric(z_max), length(z_max) == 1, z_max > 0,
            is.numeric(z_top_max), length(z_top_max) == 1, z_top_max >= 0,
            is.numeric(bin_m), length(bin_m) == 1, bin_m > 0)
  prof <- .clean_profile(depth, value, qual)
  res <- function(x = NA_real_, reached = NA_real_, status) tibble::tibble(
    integrated = as.numeric(x), depth_reached_m = as.numeric(reached), status = status,
    z_max = z_max, method = method)
  if (!nrow(prof)) return(res(status = "no_data"))
  if (min(prof$depth) > z_top_max) return(res(status = "no_surface"))
  z <- prof$depth; v <- prof$value
  bottom <- min(z_max, max(z))
  status <- if (bottom < z_max) "shallow" else "ok"
  if (method == "sum") {
    zb <- seq(bin_m, bottom + 1e-9, by = bin_m)
    if (!length(zb)) return(res(0, bottom, status))
    # rule = 2 carries the shallowest good sample up to the surface (checked <= z_top_max above)
    vb <- if (length(z) == 1) rep(v, length(zb)) else
      stats::approx(z, v, xout = zb, rule = 2, ties = mean)$y
    return(res(sum(vb) * bin_m, bottom, status))
  }
  if (z[1] > 0) { z <- c(0, z); v <- c(v[1], v) }
  if (max(z) > bottom) {
    vb <- stats::approx(z, v, xout = bottom, ties = mean)$y
    keep <- z < bottom
    z <- c(z[keep], bottom); v <- c(v[keep], vb)
  }
  x <- if (length(z) < 2) 0 else sum(diff(z) * (utils::head(v, -1) + utils::tail(v, -1)) / 2)
  res(x, bottom, status)
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

# a centred running mean over a window in metres of depth (not samples): each sample's value is
# the mean of the samples within half the window of it, so the window is truncated at the ends
# of the profile and an uneven grid or a gap averages what is there. `depth` sorted, no NA.
.running_mean_m <- function(depth, value, window_m) {
  if (window_m <= 0 || length(value) < 2) return(as.numeric(value))
  h  <- window_m / 2 + 1e-9
  # the window's first and last sample, by binary search: linear in the profile, not quadratic
  lo <- findInterval(depth - h, depth, left.open = TRUE) + 1L
  hi <- findInterval(depth + h, depth)
  vapply(seq_along(depth), function(i) mean(value[lo[i]:hi[i]]), numeric(1))
}

.haversine_km <- function(lat1, lon1, lat2, lon2) {
  p1 <- lat1 * pi / 180; p2 <- lat2 * pi / 180
  dphi <- (lat2 - lat1) * pi / 180; dl <- (lon2 - lon1) * pi / 180
  a <- sin(dphi / 2)^2 + cos(p1) * cos(p2) * sin(dl / 2)^2
  2 * 6371 * atan2(sqrt(a), sqrt(1 - a))
}
