# tests for the CTD upload path (R/ctd_upload.R)
#
# Fixtures are written inline rather than committed as files: each one exists to
# pin ONE property of a real format, and writing it here says which.

write_lines_to <- function(x, ext, env = parent.frame()) {
  p <- withr::local_tempfile(fileext = paste0(".", ext), .local_envir = env)
  writeLines(x, p)
  p
}

# a minimal registry, shaped like metadata/measurement_type.csv
mt_fixture <- function() {
  tibble::tibble(
    measurement_type = c("temperature_1", "salinity_1", "pressure",
                         "oxygen_ml_l_1", "ph"),
    units          = c("degC", "PSU", "dbar", "ml/L", "pH"),
    valid_min      = c(-2, 0, 0, 0, 6),
    valid_max      = c(40, 45, 6000, 15, 9),
    `_source_column` = c("temp1", "salt1", "pressure", "ox1", "p_h"),
    `_qual_column`   = c("temp1q", "salt1q", "pr_q", "ox1q", "p_hq"))
}

sbe_fixture <- function() {
  tibble::tibble(
    sbe_name = c("PrDM", "T090C", "Sal00", "Sbeox0ML/L", "DepSM", "V0",
                 "FlECO-AFL", "Latitude", "Longitude", "Scan"),
    role = c("measurement", "measurement", "measurement", "measurement",
             "depth", "voltage", "unmapped", "position", "position",
             "identifier"),
    measurement_type = c("pressure", "temperature_1", "salinity_1",
                         "oxygen_ml_l_1", NA, NA, NA, NA, NA, NA),
    note = "")
}

test_that(".hex is refused with the reason, not parsed", {
  # the refusal IS the correct behaviour: .hex is A/D counts, and converting it
  # needs the .xmlcon calibration file. A "best effort" conversion would be
  # invented numbers presented as measurements.
  p <- write_lines_to("00A1B2", "hex")
  expect_error(read_ctd_upload(p), "\\.xmlcon")
  expect_error(read_ctd_upload(p), "raw instrument output")
})

test_that("an unsupported extension names what is expected", {
  p <- write_lines_to("x", "xlsx")
  expect_error(read_ctd_upload(p), "expected .csv, .cnv, .asc or .btl")
})

# SeaSave lays every field out right-aligned in a fixed-width column, header
# names included — which is exactly why adjacent names can touch.
sbe_row <- function(x, w = 11) paste0(formatC(x, width = w), collapse = "")

test_that("sbe_split_header recovers names that run together", {
  # THE REGRESSION THIS LOCKS DOWN: 179 of 200 CalCOFI .asc files have adjacent
  # header names touching. Splitting on whitespace yields the wrong column count
  # and silently mis-assigns every column after the collision.
  nm     <- c("PrDM", "T090C", "Sbeox0ML/L", "Sbeox0Mm/Kg")
  header <- sbe_row(nm)
  rows   <- c(sbe_row(c("521.962", "5.7043", "4.6094", "238.969")),
              sbe_row(c("445.199", "6.1073", "4.6102", "204.087")))
  expect_true(grepl("Sbeox0ML/LSbeox0Mm/Kg", header))   # they really do touch
  expect_equal(sbe_split_header(header, rows), nm)
  # and the whitespace split it replaces would have found 3 columns, not 4
  expect_equal(length(strsplit(trimws(header), "\\s+")[[1]]), 3L)
})

test_that("sbe_split_header refuses rather than guessing when ambiguous", {
  # a name with an internal space cannot be attributed to a column
  expect_error(
    sbe_split_header("  Pr DM      T090C", c("  521.962     5.7043")),
    "could not be split unambiguously")
})

test_that("read_sbe_cnv uses the header's own column names and bad_flag", {
  p <- write_lines_to(c(
    "* Sea-Bird SBE 9 Data File:",
    "** Ship: SALLY RIDE",
    "** Cruise: 2211SR",
    "** Station: 90.0 100.0",
    "** Cast: 019",
    "* NMEA Latitude = 31 05.10 N",
    "* NMEA Longitude = 122 39.67 W",
    "* NMEA UTC (Time) = Nov 09 2022  14:05:31",
    "# name 0 = prDM: Pressure, Digiquartz [db]",
    "# name 1 = t090C: Temperature [ITS-90, deg C]",
    "# name 2 = sal00: Salinity, Practical [PSU]",
    "# bad_flag = -9.990e-29",
    "*END*",
    "    1.937    17.1824    33.5377",
    "    5.000    17.1000 -9.990e-29"), "cnv")

  d <- read_sbe_cnv(p)
  expect_equal(names(d), c("prDM", "t090C", "sal00"))
  expect_equal(nrow(d), 2L)
  # bad_flag becomes NA rather than travelling as a reading
  expect_true(is.na(d$sal00[2]))

  h <- attr(d, "sbe_header")
  expect_equal(h$cruise, "2211SR")
  expect_equal(h$cast, "019")
  # NMEA degrees + decimal minutes, with the hemisphere applied
  expect_equal(h$latitude,  31 + 5.10 / 60,   tolerance = 1e-6)
  expect_equal(h$longitude, -(122 + 39.67 / 60), tolerance = 1e-6)
  expect_s3_class(h$datetime, "POSIXct")
})

test_that("read_sbe_btl keeps one statistic and re-joins the split date", {
  # `Date` is ONE header word over THREE data fields (`Nov 09 2022`), and each
  # bottle contributes several tagged rows. Both would corrupt a naive read.
  p <- write_lines_to(c(
    "* Sea-Bird SBE 9 Data File:",
    "** Cruise: 2211SR",
    "** Cast: 019",
    "    Bottle        Date       PrDM      T090C",
    "  Position        Time",
    "      1    Nov 09 2022    521.962     5.7043 (avg)",
    "              14:17:04      0.420     0.0008 (sdev)",
    "      2    Nov 09 2022    445.199     6.1073 (avg)",
    "              14:19:06      0.485     0.0022 (sdev)"), "btl")

  d <- read_sbe_btl(p)
  expect_equal(names(d), c("Bottle", "Date", "PrDM", "T090C"))
  expect_equal(nrow(d), 2L)              # avg rows only, not avg+sdev
  expect_equal(d$Bottle, c(1, 2))
  expect_equal(d$Date, c("Nov 09 2022", "Nov 09 2022"))
  expect_equal(d$PrDM, c(521.962, 445.199))
})

test_that("ctd_map_columns reports unmapped columns instead of dropping them", {
  # unmapped columns are the RESULT, not a failure: they are where a format
  # change announces itself
  m <- ctd_map_columns(
    c("PrDM", "T090C", "DepSM", "V0", "FlECO-AFL", "SomethingNew"),
    mt_fixture(), sbe_fixture(), format = "cnv")

  expect_equal(m$measurement_type[m$column == "T090C"], "temperature_1")
  expect_equal(m$role[m$column == "DepSM"], "depth")
  expect_equal(m$role[m$column == "V0"], "voltage")
  expect_equal(m$role[m$column == "SomethingNew"], "unmapped")
  expect_true(is.na(m$measurement_type[m$column == "FlECO-AFL"]))
})

test_that("a CalCOFI csv maps through the measurement registry", {
  m <- ctd_map_columns(c("temp1", "temp1q", "salt1", "nope"),
                       mt_fixture(), format = "csv")
  expect_equal(m$measurement_type[m$column == "temp1"], "temperature_1")
  expect_equal(m$role[m$column == "temp1q"], "quality")
  expect_equal(m$role[m$column == "nope"], "unmapped")
})

test_that("ctd_upload_to_core emits obs/sample and deletes the sentinels", {
  d <- tibble::tibble(
    DepSM = c(10, 20, 30), PrDM = c(10.1, 20.2, 30.3),
    T090C = c(15.2, 13.1, -99), `Sal00` = c(33.4, 33.5, NA))
  m <- ctd_map_columns(names(d), mt_fixture(), sbe_fixture(), format = "cnv")
  core <- ctd_upload_to_core(
    d, m, header = list(cruise = "2211SR", cast = "019", station = "90.0 100.0",
                        latitude = 31.1, longitude = -122.7))

  expect_equal(nrow(core$sample), 1L)
  expect_equal(core$sample$cruise_key, "2211SR")
  expect_equal(core$sample$depth_min_m, 10)

  # -99 and NA are gone; nothing else is
  expect_false(any(core$obs$measurement_value == -99))
  expect_equal(core$n_sentinel, 2L)
  expect_setequal(unique(core$obs$measurement_type),
                  c("pressure", "temperature_1", "salinity_1"))
  # obs carries the core column set the rules read
  expect_true(all(c("realm", "dataset_key", "sample_key", "cruise_key",
                    "depth_min_m", "measurement_type", "measurement_value",
                    "measurement_qual") %in% names(core$obs)))
})

test_that("ctd_upload_to_core refuses a file with nothing to check", {
  d <- tibble::tibble(DepSM = c(1, 2), V0 = c(0.1, 0.2))
  m <- ctd_map_columns(names(d), mt_fixture(), sbe_fixture(), format = "cnv")
  expect_error(ctd_upload_to_core(d, m), "no column mapped to a measurement type")

  d2 <- tibble::tibble(T090C = c(15, 16))
  m2 <- ctd_map_columns(names(d2), mt_fixture(), sbe_fixture(), format = "cnv")
  expect_error(ctd_upload_to_core(d2, m2), "no depth or pressure column")
})

test_that("quality codes stored as doubles match the vocabulary", {
  # "9.0" from a double->string cast must match "9". Stripped textually, NOT via
  # an integer cast, which would round an unexpected "9.5" into a different code.
  expect_equal(calcofi4db:::.clean_qual(c("9.0", "8", "", NA, "9.5")),
               c("9", "8", NA, NA, "9.5"))
})

test_that("an upload becomes a connection every rule can run against", {
  d <- tibble::tibble(DepSM = c(10, 20, 30), T090C = c(15.2, 13.1, 12.0),
                      Sal00 = c(33.4, 33.5, 33.6))
  m <- ctd_map_columns(names(d), mt_fixture(), sbe_fixture(), format = "cnv")
  core <- ctd_upload_to_core(d, m, header = list(cruise = "C1", cast = "001"))

  con <- qc_upload_con(core, withr::local_tempdir())  # no registries: staged empty
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))

  # the upload IS obs / sample / obs_ctd_full — the names every rule uses
  # 3 depths x the 2 columns that map to a measurement type (DepSM is the axis)
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM obs")$n, 6L)
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM sample")$n, 1L)
  # an uploaded cast is full resolution, so the profile rules apply to it
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM obs_ctd_full")$n, 6L)
})
