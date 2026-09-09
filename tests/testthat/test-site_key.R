# site_key: one canonical spelling of the station. v2026.09.06 shipped 28 CTD casts
# with the source's own Sta_ID forms; each is a fixture here so it cannot come back.

sk_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  con
}

# the 14 source forms measured on v2026.09.06 (calcofi_ctd-cast), plus the canonical
# and negative-station cases that must pass through unchanged
SK_CASES <- c(
  "93.3    26.4" = "093.3 026.4",
  "0093. 060.0"  = "093.0 060.0",
  "0093. 045.0"  = "093.0 045.0",
  "090. 090.0"   = "090.0 090.0",
  "090.0 27.76"  = "090.0 027.8",   # one decimal: the grid's resolution
  "88.50 030.1"  = "088.5 030.1",
  "76.7    80.0" = "076.7 080.0",
  "86.8    32.5" = "086.8 032.5",
  "83.3    42.0" = "083.3 042.0",
  "76.6    80.0" = "076.6 080.0",
  "93.4    26.4" = "093.4 026.4",
  "093.3 026.4"  = "093.3 026.4",   # canonical is a fixed point
  "011.7 -02.6"  = "011.7 -02.6",   # ichthyo: a negative station keeps its sign and width
  "-07.7 028.5"  = "-07.7 028.5",   # crab / PIC: a negative line
  "93.3,26.4"    = "093.3 026.4",   # cc_grid's sta_key separator
  "line 90"      = NA_character_)   # fewer than two numbers: no station

test_that("normalize_site_key(): every measured source form maps to printf('%05.1f %05.1f')", {
  expect_equal(unname(normalize_site_key(names(SK_CASES))), unname(SK_CASES))
  expect_equal(normalize_site_key(c(NA, NA)), c(NA_character_, NA_character_))
  expect_equal(normalize_site_key(character(0)), character(0))
})

test_that("site_key_sql(): the SQL twin agrees with the R form on every case", {
  con <- sk_con()
  DBI::dbWriteTable(con, "t", data.frame(raw = names(SK_CASES), stringsAsFactors = FALSE))
  got <- DBI::dbGetQuery(con, glue::glue("SELECT raw, {site_key_sql('raw')} AS canon FROM t"))$canon
  expect_equal(got, unname(SK_CASES))
  expect_true(is.na(DBI::dbGetQuery(con, glue::glue(
    "SELECT {site_key_sql('NULL::VARCHAR')} AS canon"))$canon))
})

test_that("check_site_key_format(): NULL is allowed, a source form fails and names the dataset", {
  con <- sk_con()
  DBI::dbWriteTable(con, "sample", data.frame(
    dataset_key = c("calcofi_bottle", "calcofi_bottle", "calcofi_mets", "swfsc_ichthyo"),
    site_key    = c("090.0 062.0", "093.3 026.4", NA, "011.7 -02.6"),
    stringsAsFactors = FALSE))
  res <- check_site_key_format(con)
  expect_equal(res$n_bad, c(0L, 0L, 0L))
  expect_equal(res$n_site[res$dataset_key == "calcofi_mets"], 0L)

  DBI::dbExecute(con, "INSERT INTO sample VALUES ('calcofi_ctd-cast', '93.3    26.4')")
  expect_error(check_site_key_format(con), "calcofi_ctd-cast: 1 row\\(s\\), e.g. '93.3    26.4'")
})

test_that("append_sample() rewrites site_key on the way in, and says so", {
  con <- sk_con()
  arm <- "SELECT * FROM (VALUES
    ('calcofi_ctd-cast:cast:1', 'cast', NULL, 'calcofi_ctd-cast:cast:1', 'calcofi_ctd-cast',
     'st25-ln93.3', '93.3    26.4', '2019-02-33P4', 1, 33.48, -117.77, TIMESTAMP '2019-02-06 23:48',
     NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR),
    ('calcofi_ctd-cast:cast:2', 'cast', NULL, 'calcofi_ctd-cast:cast:2', 'calcofi_ctd-cast',
     'st30-ln90', '090.0 028.0', '2019-02-33P4', 2, 33.3, -117.9, TIMESTAMP '2019-02-07 03:00',
     NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR),
    ('calcofi_mets:underway:1', 'underway', NULL, 'calcofi_mets:underway:1', 'calcofi_mets',
     NULL, NULL, '2019-02-33P4', NULL, 33.0, -118.0, TIMESTAMP '2019-02-07 04:00',
     NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR)
    ) t(sample_key, sample_type, parent_sample_key, root_sample_key, dataset_key, grid_key, site_key,
        cruise_key, order_occ, latitude, longitude, datetime, depth_min_m, depth_max_m, tow_type)"
  expect_message(append_sample(con, arm), "1 row\\(s\\) had a non-canonical site_key")
  got <- DBI::dbGetQuery(con, "SELECT sample_key, site_key FROM sample ORDER BY sample_key")
  expect_equal(got$site_key, c("093.3 026.4", "090.0 028.0", NA))
  expect_silent(check_site_key_format(con))
})
