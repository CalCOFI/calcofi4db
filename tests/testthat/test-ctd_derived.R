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
