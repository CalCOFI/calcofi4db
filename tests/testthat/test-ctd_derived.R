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

test_that("mld: step profile crosses by linear interpolation at the default 0.03", {
  r <- ctd_mld(z, step)
  expect_equal(r$mld_m, 39.06)
  expect_equal(r$ref_value, 25)
  expect_identical(r$status, "ok")
  expect_identical(r$ref_depth, 10)
})

test_that("mld: threshold is an argument (0.125, Levitus)", {
  expect_equal(ctd_mld(z, step, threshold = 0.125)$mld_m, 39.25)
})

test_that("mld: linear gradient crosses between samples", {
  # dev = 0.02 * (z - 10): 0.02 at 11 m, 0.04 at 12 m -> 0.03 at 11.5 m
  expect_equal(ctd_mld(z, 25 + 0.02 * z)$mld_m, 11.5)
})

test_that("mld: a profile that never crosses is NA, mixed_to_bottom, with its depth", {
  r <- ctd_mld(z, rep(25, length(z)))
  expect_true(is.na(r$mld_m))
  expect_identical(r$status, "mixed_to_bottom")
  expect_equal(r$depth_max_m, 100)
})

test_that("mld: a cast that does not bracket the reference depth is NA, no_reference", {
  r <- ctd_mld(20:100, ifelse(20:100 < 40, 25, 25.5))
  expect_true(is.na(r$mld_m))
  expect_identical(r$status, "no_reference")
})

test_that("mld: a flagged spike is dropped before the threshold is tested", {
  spike <- step; spike[z == 20] <- 26
  expect_equal(ctd_mld(z, spike)$mld_m, 19.03)                       # unflagged: spike is the MLD
  q <- ifelse(z == 20, "9", NA)
  expect_equal(ctd_mld(z, spike, qual = q)$mld_m, 39.06)             # flagged 9: ignored
  expect_equal(ctd_mld(z, spike, qual = sub("9", "8.0", q))$mld_m, 39.06)  # "8.0" == 8
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

gauss <- 0.2 + 2 * exp(-((0:150 - 40) / 10)^2)

test_that("chl max: depth of the (smoothed) maximum", {
  r <- ctd_chl_max(0:150, gauss)
  expect_equal(r$chl_max_depth_m, 40)
  expect_equal(r$n, 151L)
})

test_that("chl max: a single-bin spike does not survive the 5 m running median", {
  spk <- gauss; spk[101] <- 50                     # 100 m
  expect_equal(ctd_chl_max(0:150, spk)$chl_max_depth_m, 40)
  expect_equal(ctd_chl_max(0:150, spk, window_m = 1)$chl_max_depth_m, 100)  # no smoothing: it would
})

test_that("chl max: a flagged bin is dropped; no data is NA", {
  big <- gauss; big[81:86] <- 9                    # 80-85 m, a plateau a median keeps
  expect_equal(ctd_chl_max(0:150, big)$chl_max_depth_m, 80)
  expect_equal(ctd_chl_max(0:150, big, qual = ifelse((0:150) %in% 80:85, "9", NA))$chl_max_depth_m, 40)
  expect_true(is.na(ctd_chl_max(1:3, rep(NA, 3))$chl_max_depth_m))
})

# ctd_integrate ----

test_that("integrate: constant to 200 m, linear is exact under the trapezoid", {
  expect_equal(ctd_integrate(0:300, rep(1, 301))$integrated, 200)
  expect_equal(ctd_integrate(0:300, 0:300)$integrated, 20000)       # int_0^200 z dz
  expect_identical(ctd_integrate(0:300, rep(1, 301))$status, "ok")
})

test_that("integrate: the shallowest sample is carried to the surface when <= 5 m", {
  r <- ctd_integrate(2:300, rep(1, 299))
  expect_equal(r$integrated, 200)
  expect_identical(r$status, "ok")
})

test_that("integrate: a cast starting deeper than 5 m is NA, no_surface", {
  r <- ctd_integrate(10:300, rep(1, 291))
  expect_true(is.na(r$integrated))
  expect_identical(r$status, "no_surface")
})

test_that("integrate: a shallow cast integrates to its bottom and says so", {
  r <- ctd_integrate(0:150, rep(1, 151))
  expect_equal(r$integrated, 150)
  expect_equal(r$depth_reached_m, 150)
  expect_identical(r$status, "shallow")
})

test_that("integrate: floor between samples is interpolated; flagged bins drop out", {
  expect_equal(ctd_integrate(c(0, 100, 300), c(1, 1, 3))$integrated, 250)  # 100 + (1 + 2) / 2 * 100
  v <- rep(1, 301); v[51] <- 1000
  expect_equal(ctd_integrate(0:300, v, qual = ifelse(0:300 == 50, "9", NA))$integrated, 200)
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
