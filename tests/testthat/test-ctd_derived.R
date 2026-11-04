# derived hydrographic products (CalCOFI/workflows#98-#103): one fixture per rule / branch

z <- 0:100
step <- ifelse(z < 40, 25, 25.5)                 # sigma-theta step at 40 m

# ctd_sigma_theta_ave ----

test_that("sigma-theta pair: mean, flagged sensor dropped, 1/2 select", {
  expect_equal(ctd_sigma_theta_ave(c(25.1, 25.1, 25.1, 25.1), c(25.3, 25.3, 25.3, 25.3),
                                   q1 = c(NA, "9", "1", "2")),
               c(25.2, 25.3, 25.1, 25.3))
})

# ctd_mld ----
# the provider's definition (Rasmus Swalethorp 2026-09-23, questions.csv Q01): sigma-theta
# 0.02 kg/m3 greater than at the 10 m reference

test_that("mld: the default is the provider's 0.02 kg/m3 from a 10 m reference", {
  r <- ctd_mld(z, step)
  expect_equal(r$threshold, 0.02)
  expect_identical(r$ref_depth, 10)
  expect_equal(r$mld_m, 39.04)                     # 39 + 0.02 / 0.5 between the 39 and 40 m bins
  expect_equal(r$ref_value, 25)
  expect_identical(r$status, "ok")
})

test_that("mld: the threshold crossed between two bins is interpolated linearly", {
  # 25 to 10 m, then +0.008 per m: dev 0.016 at 12 m, 0.024 at 13 m -> 0.02 at 12.5 m
  v <- ifelse(z <= 10, 25, 25 + 0.008 * (z - 10))
  expect_equal(ctd_mld(z, v)$mld_m, 12.5)
  # dev = 0.02 * (z - 10): 0.02 at 11 m, 0.04 at 12 m -> 0.03 at 11.5 m
  expect_equal(ctd_mld(z, 25 + 0.02 * z, threshold = 0.03)$mld_m, 11.5)
})

test_that("mld: the criterion stays an argument (0.125, Levitus; the superseded 0.03)", {
  expect_equal(ctd_mld(z, step, threshold = 0.125)$mld_m, 39.25)
  expect_equal(ctd_mld(z, step, threshold = 0.03)$mld_m, 39.06)
})

test_that("mld: a profile that never crosses is NA, mixed_to_bottom, with its depth", {
  r <- ctd_mld(z, ifelse(z < 50, 25, 25.019))     # 0.019 short of the 0.02 threshold
  expect_true(is.na(r$mld_m))
  expect_identical(r$status, "mixed_to_bottom")
  expect_equal(r$depth_max_m, 100)
})

test_that("mld: a flat profile is mixed to the bottom", {
  r <- ctd_mld(z, rep(25, length(z)))
  expect_true(is.na(r$mld_m))
  expect_identical(r$status, "mixed_to_bottom")
})

test_that("mld: a cast starting below the 10 m reference is NA, no_reference", {
  r <- ctd_mld(20:100, ifelse(20:100 < 40, 25, 25.5))
  expect_true(is.na(r$mld_m))
  expect_identical(r$status, "no_reference")
  expect_identical(ctd_mld(11:100, rep(25, 90))$status, "no_reference")
})

test_that("mld: a NaN bin is a gap the interpolation spans", {
  v <- step; v[z %in% c(10, 40)] <- NaN
  r <- ctd_mld(z, v)
  # reference interpolated from 9 and 11 m (25); first crossing at 41 m, bracketed by 39 m
  expect_equal(r$ref_value, 25)
  expect_equal(r$mld_m, 39.08)                     # 39 + 0.02 * 2 / 0.5
  expect_identical(r$status, "ok")
})

test_that("mld: a flagged spike is dropped before the threshold is tested", {
  spike <- step; spike[z == 20] <- 26
  expect_equal(ctd_mld(z, spike)$mld_m, 19.02)                       # unflagged: spike is the MLD
  q <- ifelse(z == 20, "9", NA)
  expect_equal(ctd_mld(z, spike, qual = q)$mld_m, 39.04)             # flagged 9: ignored
  expect_equal(ctd_mld(z, spike, qual = sub("9", "8.0", q))$mld_m, 39.04)  # "8.0" == 8
})

test_that("mld: temperature criterion uses the absolute departure (0.2 deg C)", {
  r <- ctd_mld(z, ifelse(z < 30, 15, 14), criterion = "temperature")
  expect_equal(r$threshold, 0.2)
  expect_equal(r$mld_m, 29.2)
})

test_that("mld: fewer than two good samples is no_data", {
  expect_identical(ctd_mld(c(5, 10), c(25, NA))$status, "no_data")
})

# ctd_chl_max ----
# the provider's definition (Rasmus Swalethorp 2026-09-23, questions.csv Q02): the depth of the
# highest value of a 3 m running mean

gauss <- 0.2 + 2 * exp(-((0:150 - 40) / 10)^2)

test_that("chl max: depth of the maximum of the 3 m running mean, and the mean there", {
  r <- ctd_chl_max(0:150, gauss)
  expect_equal(r$chl_max_depth_m, 40)
  expect_equal(r$chl_max_value, mean(gauss[40:42]))  # 39, 40, 41 m
  expect_equal(r$n, 151L)
})

test_that("chl max: the 3 m mean, not a 5 m median: a narrow spike below the peak's mean loses", {
  spk <- gauss; spk[101] <- 5                          # 100 m: (5 + 0.2 + 0.2) / 3 = 1.8 < 2.19
  expect_equal(ctd_chl_max(0:150, spk)$chl_max_depth_m, 40)
  expect_equal(ctd_chl_max(0:150, spk, window_m = 0)$chl_max_depth_m, 100)  # raw: it would win
  # a 3-bin layer survives the 3 m mean whole (why Rasmus chose 3 m over 5 m)
  lay <- rep(0.2, 151); lay[c(70, 71, 72)] <- 3        # 69, 70, 71 m
  r <- ctd_chl_max(0:150, lay)
  expect_equal(r$chl_max_depth_m, 70)
  expect_equal(r$chl_max_value, 3)
})

test_that("chl max: on an uneven grid the window is 3 m of depth, not 3 samples", {
  # 0.5 m samples to 2 m, then 1 m bins from 5 m
  d <- c(0, 0.5, 1, 1.5, 2, 5, 6, 7, 8, 9)
  v <- c(0, 0, 3, 0, 0, 0, 0, 4, 0, 0)
  # 1 m averages the FIVE samples within 1.5 m (0-2 m) -> 3 / 5, not 3 samples' 1; the ends
  # average what is in reach (0 m: 0-1.5 m; 2 m: 0.5-2 m); 6-8 m each hold the 4 -> 4 / 3
  expect_equal(.running_mean_m(d, v, 3),
               c(3 / 4, 3 / 5, 3 / 5, 3 / 5, 3 / 4, 0, 4 / 3, 4 / 3, 4 / 3, 0))
  r <- ctd_chl_max(d, v)
  expect_equal(r$chl_max_depth_m, 7)                   # tied 6-8 m: the highest raw value
  expect_equal(r$chl_max_value, 4 / 3)
})

test_that("chl max: the window is truncated at the ends of the profile", {
  r <- ctd_chl_max(1:5, c(4, 1, 0, 0, 0))
  expect_equal(r$chl_max_depth_m, 1)
  expect_equal(r$chl_max_value, 2.5)                   # (4 + 1) / 2: no bin above the first
})

test_that("chl max: a flat profile ties everywhere and takes the shallowest bin", {
  r <- ctd_chl_max(2:60, rep(0.5, 59))
  expect_equal(r$chl_max_depth_m, 2)
  expect_equal(r$chl_max_value, 0.5)
})

test_that("chl max: a NaN bin drops out of its neighbours' means", {
  v <- gauss; v[41] <- NaN                             # 40 m
  r <- ctd_chl_max(0:150, v)
  expect_equal(r$n, 150L)
  # 39 m averages 38 and 39 m; 41 m averages 41 and 42 m
  expect_equal(r$chl_max_value, max(mean(gauss[39:40]), mean(gauss[42:43])))
  expect_true(r$chl_max_depth_m %in% c(39, 41))
})

test_that("chl max: a flagged bin is dropped; ties go to the highest raw value, then shallowest", {
  big <- gauss; big[81:86] <- 9                        # 80-85 m
  expect_equal(ctd_chl_max(0:150, big)$chl_max_depth_m, 81)   # 81-84 m all average 9
  expect_equal(ctd_chl_max(0:150, big, qual = ifelse((0:150) %in% 80:85, "9", NA))$chl_max_depth_m, 40)
  expect_true(is.na(ctd_chl_max(1:3, rep(NA, 3))$chl_max_depth_m))
})

# ctd_integrate ----
# the provider's CTD rule (Rasmus Swalethorp 2026-09-23, questions.csv Q02): the sum of the 1 m
# bins in the top 200 m, or as deep as the station was; trapezoid between points for bottles

test_that("integrate: the default sums the 1 m bins to 200 m", {
  r <- ctd_integrate(0:300, rep(1, 301))
  expect_equal(r$integrated, 200)
  expect_identical(r$status, "ok")
  expect_identical(r$method, "sum")
  expect_equal(ctd_integrate(0:300, 0:300)$integrated, sum(1:200))  # 20100: bins 1..200 m
})

test_that("integrate: trapezoid between points stays available (bottle data)", {
  expect_equal(ctd_integrate(0:300, 0:300, method = "trapezoid")$integrated, 20000)
  expect_equal(ctd_integrate(c(0, 100, 300), c(1, 1, 3), method = "trapezoid")$integrated, 250)
})

test_that("integrate: the shallowest sample is carried to the surface when <= 5 m", {
  r <- ctd_integrate(2:300, rep(1, 299))
  expect_equal(r$integrated, 200)
  expect_identical(r$status, "ok")
  expect_equal(ctd_integrate(3:300, c(2, rep(1, 297)))$integrated, 2 * 3 + 197)  # 1-3 m read 2
})

test_that("integrate: a cast starting deeper than 5 m is NA, no_surface", {
  r <- ctd_integrate(10:300, rep(1, 291))
  expect_true(is.na(r$integrated))
  expect_identical(r$status, "no_surface")
})

test_that("integrate: a shallow cast sums to its bottom and says so", {
  r <- ctd_integrate(0:150, rep(1, 151))
  expect_equal(r$integrated, 150)
  expect_equal(r$depth_reached_m, 150)
  expect_identical(r$status, "shallow")
})

test_that("integrate: a missing, NaN or flagged bin is interpolated, never counted as zero", {
  # sparse points: 1 to 100 m, then 1 -> 2 at 200 m: 100 + 100 + sum(1:100) / 100
  expect_equal(ctd_integrate(c(0, 100, 300), c(1, 1, 3))$integrated, 250.5)
  v <- rep(1, 301); v[51] <- 1000
  expect_equal(ctd_integrate(0:300, v, qual = ifelse(0:300 == 50, "9", NA))$integrated, 200)
  v[51] <- NaN
  expect_equal(ctd_integrate(0:300, v)$integrated, 200)
})

# ctd_spice ----

test_that("spice: gsw recipe; spicy positive vs minty; flagged is NA", {
  s <- ctd_spice(c(16, 8), c(33.6, 34.2), c(10, 200), -120, 33)
  expect_equal(s, c(1.474704169095670, 0.336962149684789), tolerance = 1e-12)
  # warm/salty is spicier than cold/fresh at the same density neighbourhood
  expect_gt(ctd_spice(18, 34.0, 10, -120, 33), ctd_spice(10, 33.2, 10, -120, 33))
  expect_true(is.na(ctd_spice(16, 33.6, 10, -120, 33, q_salinity = "9")))
})

# profile_threshold_depth ----

test_that("threshold depth: the crossing is interpolated linearly between the bracketing samples", {
  r <- profile_threshold_depth(c(0, 10, 20, 30, 50), c(0.1, 0.2, 0.5, 1.5, 8), threshold = 1)
  expect_equal(r$depth_m, 25)
  expect_equal(r$status, "ok")
  expect_equal(c(r$value_above, r$value_below), c(0.5, 1.5))
  expect_equal(r$direction, "above"); expect_equal(r$threshold, 1)
  expect_equal(r$depth_max_m, 50); expect_equal(r$n, 5L)
  # the legacy script's tolerance: 0.9999 counts as reaching 1
  expect_equal(profile_threshold_depth(c(0, 10, 20), c(0.2, 0.9999, 3), threshold = 1)$depth_m, 10)
})

test_that("threshold depth: a first sample already past the threshold is at_surface only when shallow", {
  r <- profile_threshold_depth(c(5, 20, 40), c(2, 5, 9), threshold = 1)
  expect_equal(r$depth_m, 5); expect_equal(r$status, "at_surface")
  expect_equal(c(r$value_above, r$value_below), c(2, 2))
  # deeper than 20 m: nothing above it to bracket the crossing
  r <- profile_threshold_depth(c(25, 40, 60), c(2, 5, 9), threshold = 1)
  expect_true(is.na(r$depth_m)); expect_equal(r$status, "no_bracket")
  # shallow but only two samples (the script wants > 2 depths)
  r <- profile_threshold_depth(c(5, 40), c(2, 9), threshold = 1)
  expect_true(is.na(r$depth_m)); expect_equal(r$status, "no_bracket")
})

test_that("threshold depth: never reached, no data, flagged samples dropped first", {
  r <- profile_threshold_depth(c(0, 10, 20), c(0.1, 0.2, 0.3), threshold = 1)
  expect_true(is.na(r$depth_m)); expect_equal(r$status, "not_reached")
  expect_equal(profile_threshold_depth(10, 5, threshold = 1)$status, "no_data")
  expect_equal(profile_threshold_depth(c(0, 10), c(NA, 5), threshold = 1)$status, "no_data")
  # a flagged spike at 10 m would have put the crossing at 5 m; dropped, it is 25 m
  r <- profile_threshold_depth(c(0, 10, 20, 30), c(0.1, 9, 0.5, 1.5), qual = c(NA, "9", NA, NA),
                               threshold = 1)
  expect_equal(r$depth_m, 25); expect_equal(r$n, 3L)
})

test_that("threshold depth: direction below is the hypoxic rule (oxygen falling to the threshold)", {
  r <- profile_threshold_depth(c(0, 50, 100, 200), c(6, 4, 2, 1), threshold = 2.4, direction = "below")
  expect_equal(r$depth_m, 90); expect_equal(r$status, "ok")
  expect_equal(c(r$value_above, r$value_below), c(4, 2))
  # only the shallowest crossing: oxygen recovering below the minimum is not reported
  r <- profile_threshold_depth(c(0, 100, 200, 300), c(6, 2, 6, 2), threshold = 2.4, direction = "below")
  expect_equal(r$depth_m, 90)
})

# ctd_hypoxic_depth ----

test_that("hypoxic: one row per threshold, in order, named by the registry key", {
  z <- seq(0, 600, by = 10)
  r <- ctd_hypoxic_depth(z, 6 - z / 100)
  expect_equal(r$measurement_type, c("hypoxic_depth_24", "hypoxic_depth_14", "hypoxic_depth_05"))
  expect_equal(r$depth_m, c(360, 460, 550))
  expect_equal(r$direction, rep("below", 3)); expect_equal(r$status, rep("ok", 3))
  # a cast that never reaches the severe boundary (oxygen 1 mL/L at its bottom)
  r <- ctd_hypoxic_depth(seq(0, 500, by = 10), 6 - seq(0, 500, by = 10) / 100)
  expect_equal(r$status, c("ok", "ok", "not_reached"))
  expect_true(is.na(r$depth_m[3]))
})

# ctd_n2 ----

test_that("n2: a two-layer profile peaks at the interface; flagged samples are NA; aligned to input", {
  p <- 1:150
  t <- ifelse(p < 60, 16, 10)
  n2 <- ctd_n2(t, 33.5, p, -120, 33)
  expect_length(n2, 150)
  # undefined within half a window (5 dbar) of the top and bottom, finite elsewhere
  expect_true(all(is.na(n2[c(1:5, 146:150)])))
  expect_true(all(is.finite(n2[6:145])))
  expect_true(abs(p[which.max(n2)] - 60) <= 5)
  # far from the interface the water is uniform: N2 near zero
  expect_lt(max(n2[c(6:40, 100:145)]), max(n2, na.rm = TRUE) / 100)
  # a flagged temperature gives NA at that sample and takes no part elsewhere
  q <- rep(NA, 150); q[30] <- "9"
  n2q <- ctd_n2(t, 33.5, p, -120, 33, q_temperature = q)
  expect_true(is.na(n2q[30]))
  expect_equal(n2q[-30], ctd_n2(t[-30], 33.5, p[-30], -120, 33), tolerance = 1e-12)
  # positions may be scalar or per sample
  expect_equal(n2, ctd_n2(t, rep(33.5, 150), p, rep(-120, 150), rep(33, 150)))
})

test_that("n2: the running mean is a window in dbar, and smooth_m = 0 is the raw aligned N2", {
  p <- 1:100
  t <- ifelse(p < 50, 16, 10)
  raw <- ctd_n2(t, 33.5, p, -120, 33, smooth_m = 0)
  sm  <- ctd_n2(t, 33.5, p, -120, 33, smooth_m = 10)
  # the raw step is one sharp spike at the interface (two mid-points straddle it), at every sample
  expect_true(all(is.finite(raw)))
  expect_lt(sum(raw > max(raw) / 2), 4)
  # smoothed, the spike is spread over about the window and lowered, never raised
  expect_gte(sum(sm > max(sm, na.rm = TRUE) / 2, na.rm = TRUE), 5)
  expect_lt(max(sm, na.rm = TRUE), max(raw))
  # the window is the hand-made triangular mean over +/- 5 dbar; blank within 5 dbar of the ends
  w <- c(1:4, 5, 4:1) / 5
  expect_equal(sm[50], sum(w * raw[46:54]) / sum(w), tolerance = 1e-12)
  expect_equal(sm[6], sum(w * raw[2:10]) / sum(w), tolerance = 1e-12)
  expect_true(all(is.na(sm[c(1:5, 96:100)])))
  # the smoothed peak sits on the step (49.5 dbar, between bins 49 and 50), not at a plateau's edge
  expect_true(p[which.max(sm)] %in% 49:50)
  # fewer than two good samples: all NA
  expect_true(all(is.na(ctd_n2(c(16, NA), c(33.5, 33.5), c(1, 2), -120, 33))))
})

# ctd_pycnocline ----

test_that("pycnocline: the maximum of smoothed N2 inside 10-200 dbar; a surface gradient is ignored", {
  p <- 1:300
  # a sharper gradient at 5 dbar (outside the window) than the real pycnocline at 60
  t <- ifelse(p < 5, 26, ifelse(p < 60, 16, 10))
  n2 <- ctd_n2(t, 33.5, p, -120, 33)
  raw <- ctd_n2(t, 33.5, p, -120, 33, smooth_m = 0)
  expect_true(p[which.max(raw)] %in% 4:5)           # the raw maximum is the surface step
  expect_gt(max(n2[6:9]), max(n2[10:200]))          # smoothed, it still outranks the pycnocline
  r <- ctd_pycnocline(p, n2)
  expect_equal(r$status, "ok")
  expect_true(abs(r$pycnocline_depth_m - 60) <= 1)   # the step is at 59.5
  expect_equal(r$n2_max, max(n2[p >= 10 & p <= 200]))
  expect_equal(r$n, 191L)
  # a deeper gradient below 200 dbar is outside the window too
  t2 <- ifelse(p < 60, 16, ifelse(p < 250, 10, 4))
  r2 <- ctd_pycnocline(p, ctd_n2(t2, 33.5, p, -120, 33))
  expect_true(abs(r2$pycnocline_depth_m - 60) <= 1)
  # a tied run takes its centre
  expect_equal(ctd_pycnocline(1:100, c(rep(1e-5, 9), rep(1e-4, 11), rep(1e-5, 80)))$pycnocline_depth_m, 15)
})

test_that("pycnocline: the legacy conditions -- a cast must start by 15 m, reach 50 m, with 5 points", {
  r <- ctd_pycnocline(1:40, rep(1e-4, 40))
  expect_true(is.na(r$pycnocline_depth_m)); expect_equal(r$status, "too_shallow")
  r <- ctd_pycnocline(20:100, rep(1e-4, 81))
  expect_equal(r$status, "no_surface")
  r <- ctd_pycnocline(c(1, 5, 12, 150, 160, 300), rep(1e-4, 6))
  expect_equal(r$status, "too_few"); expect_equal(r$n, 3L)
  expect_equal(ctd_pycnocline(1:100, rep(NA_real_, 100))$status, "no_data")
  # NA inside the window do not count as points
  v <- rep(1e-4, 100); v[10:100] <- NA; v[c(20, 30, 40, 50, 60)] <- 2e-4
  expect_equal(ctd_pycnocline(1:100, v)$n, 5L)
  expect_equal(ctd_pycnocline(1:100, v)$pycnocline_depth_m, 40)   # five tied points: the centre
})

# ctd_geostrophic ----

mk_cast <- function(station, lat, lon, t_top, p_max = 500) {
  p <- seq(1, p_max, by = 1)
  data.frame(station = station, latitude = lat, longitude = lon, pressure = p,
             temperature = ifelse(p < 100, t_top, 8), salinity = 33.8)
}

test_that("geostrophic: zero at p_ref, sign and magnitude match (dh2 - dh1) / (f dx)", {
  casts <- rbind(mk_cast("a", 33, -120, 14), mk_cast("b", 33, -120.5, 16))   # b lighter aloft
  g <- ctd_geostrophic(casts)
  expect_equal(unique(g$station_1), "a")
  expect_equal(nrow(g), 500)
  expect_equal(g$velocity_m_s[g$pressure == 500], 0, tolerance = 1e-10)
  expect_gt(g$velocity_m_s[g$pressure == 1], 0)                    # higher dynamic height at b
  expect_false(any(g$shallow))
  # independent recomputation at the surface
  dh <- function(tt, lon) {
    sa <- gsw::gsw_SA_from_SP(rep(33.8, 500), 1:500, lon, 33)   # gsw recycles to its FIRST arg
    gsw::gsw_geo_strf_dyn_height(
      sa, gsw::gsw_CT_from_t(sa, ifelse(1:500 < 100, tt, 8), 1:500), 1:500, p_ref = 500)[1]
  }
  f <- 2 * 7.292115e-5 * sin(33 * pi / 180)
  dx <- g$dx_km[1] * 1000
  expect_equal(g$velocity_m_s[1], (dh(16, -120.5) - dh(14, -120)) / (f * dx), tolerance = 1e-8)
  expect_gt(abs(g$velocity_m_s[1]), 0.05); expect_lt(abs(g$velocity_m_s[1]), 1)   # O(0.1 m/s)
})

test_that("geostrophic: a station shallower than p_ref flags the pair; dist_km is honoured", {
  casts <- rbind(mk_cast("a", 33, -120, 14, p_max = 300), mk_cast("b", 33, -120.5, 16))
  casts$dist_km <- ifelse(casts$station == "a", 0, 40)
  g <- ctd_geostrophic(casts)
  expect_true(all(g$shallow))
  expect_equal(unique(g$dx_km), 40)
  expect_equal(unique(g$dist_mid_km), 20)
})

test_that("geostrophic: flagged samples drop; a station with < 2 good samples is skipped", {
  casts <- rbind(mk_cast("a", 33, -120, 14), mk_cast("b", 33, -120.5, 16), mk_cast("c", 33, -121, 15))
  casts$q_salinity <- ifelse(casts$station == "b", "9", NA)
  g <- ctd_geostrophic(casts)
  expect_equal(unique(paste(g$station_1, g$station_2)), "a c")
  expect_equal(nrow(ctd_geostrophic(mk_cast("a", 33, -120, 14))), 0)
})

test_that("geostrophic: a station closer than min_dx_km to the last kept one is skipped", {
  casts <- rbind(mk_cast("a", 33, -120, 14), mk_cast("a2", 33, -120.02, 15),   # ~1.9 km from a
                 mk_cast("b", 33, -120.5, 16))
  g <- ctd_geostrophic(casts)
  expect_equal(unique(paste(g$station_1, g$station_2)), "a b")
  expect_equal(unique(paste(ctd_geostrophic(casts, min_dx_km = 0)$station_1)), c("a", "a2"))
})
