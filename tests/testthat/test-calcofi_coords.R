# `+proj=calcofi` is a projection, not a lookup against `grid` — so it resolves
# ANY line/station pair, including historical inshore stations and the Gulf of
# California / Baja lines the modern pattern dropped. These pin that, plus the
# round trip and the NA handling that a recovery path depends on.

test_that("a known station lands where it should", {
  r <- cc_calcofi_to_lonlat(90, 60)
  # line 90 station 60 is off San Diego
  expect_equal(round(r$longitude, 3), -119.959)
  expect_equal(round(r$latitude, 3), 32.418)
})

test_that("the transform round-trips exactly", {
  line <- c(76.7, 80, 90, 93.3)
  sta  <- c(51, 55, 60, 70)
  ll   <- cc_calcofi_to_lonlat(line, sta)
  back <- cc_lonlat_to_calcofi(ll$longitude, ll$latitude)
  expect_equal(back$line,    line, tolerance = 1e-6)
  expect_equal(back$station, sta,  tolerance = 1e-6)
})

test_that("NA in either component yields NA in both outputs, without dropping rows", {
  r <- cc_calcofi_to_lonlat(c(90, NA, 80), c(60, 60, NA))
  expect_equal(nrow(r), 3)                      # position is preserved
  expect_false(is.na(r$longitude[1]))
  expect_true(all(is.na(unlist(r[2, ]))))
  expect_true(all(is.na(unlist(r[3, ]))))

  b <- cc_lonlat_to_calcofi(c(-120, NA, NaN), c(33, 33, 33))
  expect_equal(nrow(b), 3)
  expect_false(is.na(b$line[1]))
  expect_true(is.na(b$line[2]))
  expect_true(is.na(b$line[3]))                 # NaN is not a position either
})

test_that("it resolves stations outside the modern grid, which a lookup could not", {
  # an historical inshore station and a far-offshore one: both are legitimate
  # CalCOFI coordinates even though the modern occupied pattern omits them
  r <- cc_calcofi_to_lonlat(c(60, 120), c(20, 120))
  expect_false(any(is.na(r$longitude)))
  expect_true(all(r$latitude > 20 & r$latitude < 50))
  expect_true(all(r$longitude > -140 & r$longitude < -110))
})

test_that("inputs are recycled to a common length", {
  r <- cc_calcofi_to_lonlat(90, c(50, 60, 70))
  expect_equal(nrow(r), 3)
  expect_equal(length(unique(round(r$latitude, 4))), 3)
})
