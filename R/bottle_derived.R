# bottle derived hydrographic products: values computed from each bottle's own temperature and
# salinity, not measured ----------------------------------------------------------------------
#
# Rasmus Swalethorp (thread "calcofi derived data", 2026-10-19) asked for derived values to live in
# a dataset of their own, "back calculating spice and sigma theta to the beginning using the values
# in the bottle database"; Ben Best (2026-10-20): "Spiciness and TEOS-10 sigma theta go in first",
# from the bottle database's own temperature and salinity. The bottle database keeps its legacy
# DECODR sigma_theta; this is the TEOS-10 value beside it. The rule lives here once, so the ingest
# that stages `calcofi_bottle-derived`, a notebook and a test cannot disagree.

#' Spice and TEOS-10 sigma-theta per bottle
#'
#' For each bottle, from its own in-situ temperature and practical salinity at its depth: sea
#' pressure from depth and latitude (`gsw_p_from_z(-depth, latitude)`), absolute salinity
#' (`gsw_SA_from_SP`), conservative temperature (`gsw_CT_from_t`), then
#' * `spiciness0`, spiciness at 0 dbar (`gsw_spiciness0`; McDougall & Krzysik 2015), by
#'   [ctd_spice()], the same rule as the CTD series;
#' * `sigma_theta_teos10`, potential density anomaly referenced to 0 dbar (`gsw_sigma0`), the
#'   TEOS-10 sigma-theta.
#'
#' **Flags first.** A bottle whose temperature or salinity its provider flags 8 (suspect) or 9
#' (missing) gets neither value: a derived value carries no flag, so nothing downstream could undo
#' a flagged input. Code 6 ("data OK but taken from CTD") and an empty code are good. Codes are
#' normalized as [combine_sensor_pair()] does (`"8.0"` is `"8"`). A missing or non-finite input
#' gives `NA`, never an error.
#'
#' @param temperature in-situ temperature (deg C).
#' @param salinity practical salinity (PSS-78).
#' @param depth depth of the bottle (m, positive down).
#' @param longitude,latitude decimal degrees (recycled).
#' @param q_temperature,q_salinity quality codes of `temperature` and `salinity` (`NA` = good).
#' @return a tibble with one row per input: `pressure_dbar`, `spiciness0` (kg m^-3),
#'   `sigma_theta_teos10` (kg m^-3). `pressure_dbar` is `NA` where either value is.
#' @export
#' @concept bottle_derived
#' @importFrom gsw gsw_p_from_z gsw_SA_from_SP gsw_CT_from_t gsw_sigma0
#' @importFrom tibble tibble
#' @references McDougall, T. J. and O. A. Krzysik (2015), Spiciness, *J. Mar. Res.* 73, 141-152,
#'   doi:10.1357/002224015816665589. IOC, SCOR and IAPSO (2010), *The international thermodynamic
#'   equation of seawater - 2010 (TEOS-10)*, IOC Manuals and Guides 56.
#' @examples
#' bottle_spice_sigma0(c(15, 15, 15), c(33.5, 33.5, 33.5), 10, -120, 33,
#'                     q_salinity = c(NA, "6", "8"))
bottle_spice_sigma0 <- function(temperature, salinity, depth, longitude, latitude,
                                q_temperature = NA, q_salinity = NA) {
  n <- max(length(temperature), length(salinity), length(depth))
  stopifnot(n >= 1, is.numeric(longitude) || all(is.na(longitude)),
            is.numeric(latitude) || all(is.na(latitude)))
  t   <- .drop_flagged(rep_len(as.numeric(temperature), n), q_temperature)
  sp  <- .drop_flagged(rep_len(as.numeric(salinity), n), q_salinity)
  z   <- rep_len(as.numeric(depth), n)
  lon <- rep_len(as.numeric(longitude), n)
  lat <- rep_len(as.numeric(latitude), n)
  ok  <- is.finite(t) & is.finite(sp) & is.finite(z) & is.finite(lon) & is.finite(lat)

  p <- spice <- sig0 <- rep(NA_real_, n)
  if (any(ok)) {
    p[ok]     <- gsw::gsw_p_from_z(-z[ok], lat[ok])
    # spice by the CTD rule, so the two datasets share one definition; the flags were dropped above
    spice[ok] <- ctd_spice(t[ok], sp[ok], p[ok], lon[ok], lat[ok])
    sa        <- gsw::gsw_SA_from_SP(sp[ok], p[ok], lon[ok], lat[ok])
    sig0[ok]  <- gsw::gsw_sigma0(sa, gsw::gsw_CT_from_t(sa, t[ok], p[ok]))
  }
  tibble::tibble(pressure_dbar = p, spiciness0 = spice, sigma_theta_teos10 = sig0)
}
