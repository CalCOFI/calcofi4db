# Download bundles (publish_to-zip.qmd): one dataset's rows of every table it needs, as zips by
# format. A small synthetic release stands in for the real one: two datasets ("ds_a" with
# taxa and stations, "ds_b" a derived product whose observations sit on ds_a's bottles), the
# shared vocabularies, and a supplemental table. One test per rule.

bundle_fixture_con <- function() {
  con <- DBI::dbConnect(duckdb::duckdb())
  ex <- function(sql) DBI::dbExecute(con, sql)
  ex("CREATE TABLE src_obs AS SELECT * FROM (VALUES
      (3, 'ds_a', 'ds_a:bottle:2', 't1', 'temp', 12.5, TIMESTAMP '2020-01-02 03:04:05', 1001::UBIGINT),
      (1, 'ds_a', 'ds_a:bottle:1', 't1', 'count', 4.0, TIMESTAMP '2020-01-01 00:00:00', 1002::UBIGINT),
      (2, 'ds_a', 'ds_a:bottle:1', NULL, 'temp', 11.0, TIMESTAMP '2020-01-01 00:00:00', 1003::UBIGINT),
      (4, 'ds_b', 'ds_a:bottle:2', NULL, 'oxy', 5.5, TIMESTAMP '2020-01-02 03:04:05', 1004::UBIGINT),
      (5, 'ds_c', 'ds_c:tow:1', 't2', 'count', 9.0, TIMESTAMP '2021-01-01 00:00:00', 1005::UBIGINT))
    t(obs_id, dataset_key, sample_key, taxon_key, measurement_type, measurement_value, datetime, hex_id)")
  ex("CREATE TABLE src_sample AS SELECT * FROM (VALUES
      ('ds_a:cast:1',   'cast',   NULL,          'ds_a:cast:1', 'ds_a', '090.0 060.0', '2020-01-CR', 32.5, -120.1, 7::UBIGINT),
      ('ds_a:bottle:1', 'bottle', 'ds_a:cast:1', 'ds_a:cast:1', 'ds_a', '090.0 060.0', '2020-01-CR', 32.5, -120.1, 7::UBIGINT),
      ('ds_a:cast:2',   'cast',   NULL,          'ds_a:cast:2', 'ds_a', 'SIO pier',    '2020-01-CR', 32.9, -117.3, 8::UBIGINT),
      ('ds_a:bottle:2', 'bottle', 'ds_a:cast:2', 'ds_a:cast:2', 'ds_a', 'SIO pier',    '2020-01-CR', 32.9, -117.3, 8::UBIGINT),
      ('ds_c:tow:1',    'tow',    NULL,          'ds_c:tow:1',  'ds_c', NULL,          '2021-01-XX', 33.0, -118.0, 9::UBIGINT))
    t(sample_key, sample_type, parent_sample_key, root_sample_key, dataset_key, site_key, cruise_key, latitude, longitude, hex7)")
  ex("CREATE TABLE src_sample_measurement AS SELECT * FROM (VALUES
      (1, 'ds_a:cast:1', 'ds_a', 'volume', 3.2), (2, 'ds_c:tow:1', 'ds_c', 'volume', 1.0))
    t(sample_measurement_id, sample_key, dataset_key, measurement_type, measurement_value)")
  ex("CREATE TABLE src_taxon AS SELECT * FROM (VALUES
      ('t1', 'Engraulis mordax', 'northern anchovy'), ('t2', 'Sardinops sagax', 'Pacific sardine'))
    t(taxon_key, scientific_name, common_name)")
  ex("CREATE TABLE src_measurement_type AS SELECT * FROM (VALUES
      ('temp', 'degC', 'temperature'), ('count', 'count', 'specimens'), ('oxy', 'umol/kg', 'oxygen'),
      ('volume', 'm3', 'volume filtered'), ('unused', 'x', 'nobody'))
    t(measurement_type, units, description)")
  ex("CREATE TABLE src_cruise AS SELECT * FROM (VALUES
      ('2020-01-CR', 'CR'), ('2021-01-XX', 'XX')) t(cruise_key, ship_key)")
  ex("CREATE TABLE src_ship AS SELECT * FROM (VALUES ('CR', 'Crest'), ('XX', 'Other')) t(ship_key, ship_name)")
  ex("CREATE TABLE src_region AS SELECT * FROM (VALUES ('r1', 'a region')) t(region_key, description)")
  con
}

bundle_cols <- function(con) {
  tbs <- c("obs", "sample", "sample_measurement", "taxon", "measurement_type", "cruise", "ship", "region")
  stats::setNames(lapply(tbs, function(tb)
    DBI::dbGetQuery(con, sprintf("DESCRIBE src_%s", tb))$column_name), tbs)
}
bundle_from <- function() {
  tbs <- c("obs", "sample", "sample_measurement", "taxon", "measurement_type", "cruise", "ship", "region")
  stats::setNames(paste0("src_", tbs), tbs)
}
rec_a <- list(dataset_key = "ds_a", dataset_name = "Dataset A", tables = list("sample", "obs", "sample_measurement", "obs_full", "ship", "region"),
              provider = list(key = "prov", name = "Provider Lab", url = "https://prov.example/"),
              description_md = "A small test dataset.",
              attribution = list(citation_main = "Doe J (2024). Dataset A. https://doi.org/10.1/abc", doi = "10.1/abc",
                                 license = "CC-BY-4.0", license_name = "Creative Commons Attribution 4.0 International",
                                 license_url = "https://creativecommons.org/licenses/by/4.0/",
                                 contact = "mailto:doe@prov.example", pi_names = list("J Doe")))
rec_b <- list(dataset_key = "ds_b", dataset_name = "Derived B", tables = list("obs", "measurement_type"),
              provider = list(key = "prov", name = "Provider Lab"), attribution = list())
release_fx <- list(version = "v2026.11.14", release_date = "2026-11-14",
                   citation = "CalCOFI (2026). CalCOFI Integrated Database, release v2026.11.14 [Data set]. https://doi.org/10.5281/zenodo.1",
                   doi = "10.5281/zenodo.1", concept_doi = "10.5281/zenodo.0")

test_that("bundle_file_name() is recognisable and never doubles the calcofi_ prefix", {
  expect_equal(bundle_formats(), c("csv", "xlsx", "parquet"))
  expect_equal(bundle_file_name("swfsc_ichthyo", "v2026.11.14", "csv"), "calcofi_swfsc_ichthyo_v2026.11.14_csv.zip")
  expect_equal(bundle_file_name("calcofi_bottle", "v2026.11.14", "xlsx"), "calcofi_bottle_v2026.11.14_xlsx.zip")
  expect_equal(bundle_file_name("calcofi_bottle", "v2026.11.14", "parquet", ""), "calcofi_bottle_v2026.11.14_parquet")
  expect_error(bundle_file_name("x", "v1", "pdf"))
})

test_that("bundle_plan() adds the companions, cuts each table by its rule and orders obs, sample first", {
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_a, bundle_cols(con), supplemental = "obs_full", supplemental_rows = c(obs_full = 285533356))
  expect_equal(p$table[1:2], c("obs", "sample"))
  rule <- stats::setNames(p$rule, p$table)
  expect_equal(unname(rule["obs"]), "dataset_key")
  expect_equal(unname(rule["sample"]), "dataset_key_or_referenced")
  expect_equal(unname(rule["ship"]), "linked"); expect_equal(p$link[p$table == "ship"], "ship_key")
  expect_equal(p$link[p$table == "cruise"], "cruise_key")   # cruise carries ship_key too: cruise_key wins
  expect_equal(p$link[p$table == "taxon"], "taxon_key")
  expect_equal(unname(rule["region"]), "whole")
  expect_equal(p$source[p$table == "cruise"], "companion")
  expect_equal(p$source[p$table == "region"], "record")
  # a supplemental table is excluded with its size and why
  ex <- p[p$table == "obs_full", ]
  expect_false(ex$included); expect_equal(ex$rule, "excluded")
  expect_match(ex$reason, "285,533,356 rows"); expect_match(ex$reason, "partitioned by cruise")
  # a table the record names but the release lacks
  p2 <- bundle_plan(list(dataset_key = "x", tables = list("obs", "ghost")), bundle_cols(con))
  expect_equal(p2$rule[p2$table == "ghost"], "absent"); expect_false(p2$included[p2$table == "ghost"])
  expect_equal(tail(p2$table, 1), "ghost")
})

test_that("bundle_build_tables() keeps only the dataset's rows and drops geometry/H3 columns", {
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_a, bundle_cols(con), supplemental = "obs_full")
  b <- bundle_build_tables(con, p, "ds_a", bundle_from())
  obs <- DBI::dbGetQuery(con, "SELECT * FROM bundle_obs")
  expect_equal(obs$obs_id, c(1, 2, 3))                 # ds_a only, ORDER BY ALL
  expect_false("hex_id" %in% names(obs))
  expect_equal(b$dropped[b$table == "obs"], "hex_id")
  expect_false("hex7" %in% DBI::dbGetQuery(con, "DESCRIBE bundle_sample")$column_name)
  expect_equal(b$table[1:2], c("obs", "sample"))
  expect_equal(b$rows[b$table == "obs"], 3)
})

test_that("bundle_build_tables() adds site_key, line and station beside sample_key, parsed only from a line/station", {
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_a, bundle_cols(con))
  bundle_build_tables(con, p, "ds_a", bundle_from())
  obs <- DBI::dbGetQuery(con, "SELECT * FROM bundle_obs ORDER BY obs_id")
  cols <- names(obs)
  expect_equal(cols[match("sample_key", cols) + 1:3], c("site_key", "line", "station"))
  expect_equal(obs$line, c(90, 90, NA)); expect_equal(obs$station, c(60, 60, NA))
  expect_equal(obs$site_key[3], "SIO pier")
  smp <- DBI::dbGetQuery(con, "SELECT * FROM bundle_sample ORDER BY sample_key")
  expect_equal(names(smp)[match("site_key", names(smp)) + 1:2], c("line", "station"))
})

test_that("bundle_build_tables() adds scientific_name, common_name and units beside their keys", {
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_a, bundle_cols(con))
  b <- bundle_build_tables(con, p, "ds_a", bundle_from())
  obs <- DBI::dbGetQuery(con, "SELECT * FROM bundle_obs ORDER BY obs_id")
  expect_equal(obs$scientific_name, c("Engraulis mordax", NA, "Engraulis mordax"))
  expect_equal(obs$common_name[1], "northern anchovy")
  expect_equal(obs$units, c("count", "degC", "degC"))
  expect_equal(names(obs)[match("measurement_type", names(obs)) + 1], "units")
  # never added to the table it comes from
  expect_equal(sum(DBI::dbGetQuery(con, "DESCRIBE bundle_measurement_type")$column_name == "units"), 1)
  expect_false("scientific_name" %in% strsplit(b$added[b$table == "taxon"], ",")[[1]])
  sm <- DBI::dbGetQuery(con, "SELECT * FROM bundle_sample_measurement")
  expect_equal(sm$units, "m3"); expect_equal(sm$line, 90)
})

test_that("bundle_build_tables() cuts shared vocabularies to the rows the dataset reaches", {
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_a, bundle_cols(con))
  b <- bundle_build_tables(con, p, "ds_a", bundle_from())
  expect_equal(DBI::dbGetQuery(con, "SELECT taxon_key FROM bundle_taxon")$taxon_key, "t1")
  expect_setequal(DBI::dbGetQuery(con, "SELECT measurement_type FROM bundle_measurement_type")$measurement_type,
                  c("count", "temp", "volume"))
  expect_equal(DBI::dbGetQuery(con, "SELECT cruise_key FROM bundle_cruise")$cruise_key, "2020-01-CR")
  expect_equal(DBI::dbGetQuery(con, "SELECT ship_key FROM bundle_ship")$ship_key, "CR")   # through cruise
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM bundle_region")$n, 1)          # whole
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM bundle_sample")$n, 4)          # ds_a's own
  # intermediate tables are gone
  expect_false(any(grepl("^_bundle_base_", DBI::dbListTables(con))))
})

test_that("bundle_build_tables() brings the samples a derived dataset's rows sit on, and their parents", {
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_b, bundle_cols(con))
  b <- bundle_build_tables(con, p, "ds_b", bundle_from())
  expect_equal(DBI::dbGetQuery(con, "SELECT sample_key FROM bundle_sample")$sample_key,
               c("ds_a:bottle:2", "ds_a:cast:2"))
  expect_equal(DBI::dbGetQuery(con, "SELECT measurement_type FROM bundle_measurement_type")$measurement_type, "oxy")
  # a companion that reaches nothing is dropped: ds_b names no taxon
  expect_false("taxon" %in% b$table)
  expect_false("bundle_taxon" %in% DBI::dbListTables(con))
  expect_false("scientific_name" %in% DBI::dbGetQuery(con, "DESCRIBE bundle_obs")$column_name)
})

test_that("bundle_table_signatures() moves with this dataset's rows only", {
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_a, bundle_cols(con))
  b <- bundle_build_tables(con, p, "ds_a", bundle_from())
  s1 <- bundle_table_signatures(con, b$table)
  expect_true(all(startsWith(names(s1), "data:")))
  # another dataset's vocabulary row: unchanged
  DBI::dbExecute(con, "INSERT INTO src_taxon VALUES ('t9', 'Other fish', NULL)")
  bundle_build_tables(con, p, "ds_a", bundle_from())
  expect_identical(bundle_table_signatures(con, b$table), s1)
  # one of this dataset's values: changed, in that table only
  DBI::dbExecute(con, "UPDATE src_obs SET measurement_value = 13 WHERE obs_id = 3")
  bundle_build_tables(con, p, "ds_a", bundle_from())
  s3 <- bundle_table_signatures(con, b$table)
  expect_equal(names(s3)[s3 != s1], "data:obs")
})

test_that("bundle_format_decision() offers Excel only when every table fits, and says why not", {
  d <- bundle_format_decision(c(obs = 1000, sample = 10))
  expect_equal(d$format, c("csv", "xlsx", "parquet"))
  expect_equal(d$write, c(TRUE, TRUE, TRUE)); expect_equal(d$excel_openable, c(TRUE, TRUE, FALSE))
  # half a million rows fits Excel: a workbook is offered (ichthyo, cufes, mets)
  d <- bundle_format_decision(c(obs = 488123, sample = 212000))
  expect_true(d$write[d$format == "xlsx"]); expect_equal(d$reason[d$format == "xlsx"], "")
  expect_true(d$excel_openable[d$format == "csv"])
  d <- bundle_format_decision(c(obs = 19000000, sample = 19800))
  expect_false(d$write[d$format == "xlsx"]); expect_false(d$excel_openable[d$format == "csv"])
  expect_match(d$reason[d$format == "xlsx"], "largest table, obs, has 19,000,000 rows")
  expect_match(d$reason[d$format == "xlsx"], "more than an Excel sheet holds (1,048,576 rows including the header row)", fixed = TRUE)
  expect_match(d$note[d$format == "csv"], "obs \\(19,000,000 rows\\)")
  # a lower cap, when a caller passes one, is named in the reason
  expect_match(bundle_format_decision(c(obs = 300), xlsx_max_rows = 200)$reason[2], "at most 200 rows")
})

test_that("bundle_format_decision()'s Excel edge: 1,048,575 data rows fit, 1,048,576 do not (header row)", {
  expect_equal(BUNDLE_XLSX_MAX_ROWS, 1048575L); expect_equal(EXCEL_MAX_ROWS, 1048576L)
  fits <- bundle_format_decision(c(obs = 1048575, sample = 10))
  expect_true(fits$write[fits$format == "xlsx"]); expect_true(fits$excel_openable[fits$format == "xlsx"])
  expect_true(fits$excel_openable[fits$format == "csv"]); expect_equal(fits$note[fits$format == "csv"], "")
  over <- bundle_format_decision(c(obs = 1048576, sample = 10))
  expect_false(over$write[over$format == "xlsx"]); expect_false(over$excel_openable[over$format == "xlsx"])
  expect_false(over$excel_openable[over$format == "csv"])
  expect_match(over$reason[over$format == "xlsx"], "obs, has 1,048,576 rows")
})

test_that("bundle_license() carries the legal text only for a recognised licence id", {
  l <- bundle_license(rec_a)
  expect_equal(l$kind, "standard")
  expect_match(l$text, "Attribution 4.0 International"); expect_match(l$text, "Section 1")
  expect_false(grepl("provider's own statement", l$text))
  far <- modifyList(rec_a, list(attribution = list(license = "CC-BY-4.0", license_url = "https://x.example/dsa.pdf")))
  expect_match(bundle_license(far)$text, "provider's own statement of these terms: https://x.example/dsa.pdf")
  cc0 <- bundle_license(modifyList(rec_a, list(attribution = list(license = "CC0-1.0"))))
  expect_equal(cc0$kind, "standard"); expect_match(cc0$text, "CC0 1.0 Universal")
})

test_that("bundle_license() points at a custom licence's terms and never restates them", {
  l <- bundle_license(list(dataset_key = "k", dataset_name = "K",
                           attribution = list(license = "custom", license_url = "https://edi.example/terms")))
  expect_equal(l$kind, "custom")
  expect_match(l$text, "https://edi.example/terms"); expect_false(grepl("Creative Commons", l$text))
  expect_match(l$summary, "custom terms set by the provider")
})

test_that("bundle_license() says plainly when no licence is established, and whom to ask", {
  for (a in list(list(), list(license = "unknown"), list(license = NULL, contact = "mailto:pi@x.example"))) {
    l <- bundle_license(list(dataset_key = "k", dataset_name = "K", attribution = a))
    expect_equal(l$kind, "unstated")
    expect_match(l$text, "No licence has been established")
    expect_match(l$text, "data@calcofi.io")
    expect_false(grepl("Creative Commons|CC-BY|CC0", l$text))
  }
  l <- bundle_license(list(dataset_key = "k", attribution = list(contact = "mailto:pi@x.example")))
  expect_match(l$text, "pi@x.example or data@calcofi.io")
  expect_equal(bundle_license(list(dataset_key = "k", attribution = list(license = "US-PD")))$kind, "public-domain")
})

test_that("bundle_citation() cites the dataset and the release, and invents nothing", {
  c1 <- bundle_citation(rec_a, release_fx)
  expect_true(c1$has_provider_citation)
  expect_match(c1$md, "Doe J (2024). Dataset A.", fixed = TRUE)
  expect_match(c1$md, release_fx$citation, fixed = TRUE)
  expect_match(c1$md, "https://doi.org/10.5281/zenodo.1", fixed = TRUE)
  expect_match(c1$md, "https://doi.org/10.5281/zenodo.0", fixed = TRUE)
  expect_match(c1$md, CC_REGISTER_USE_URL, fixed = TRUE)
  expect_match(c1$bib, "@misc{ds_a,", fixed = TRUE)
  expect_match(c1$bib, "@misc{calcofi_release_v2026_11_14,", fixed = TRUE)
  expect_match(c1$bib, "doi          = {10.1/abc}", fixed = TRUE)
  c2 <- bundle_citation(rec_b, release_fx)
  expect_false(c2$has_provider_citation)
  expect_match(c2$md, "has not supplied a citation")
  expect_false(grepl("@misc{ds_b", c2$bib, fixed = TRUE))
  expect_match(c2$bib, "% ds_b: the provider has not supplied a citation", fixed = TRUE)
  expect_match(c2$bib, "@misc{calcofi_release_v2026_11_14,", fixed = TRUE)
})

test_that("bundle_columns() describes every column from the release metadata and this package", {
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_a, bundle_cols(con))
  b <- bundle_build_tables(con, p, "ds_a", bundle_from())
  meta <- list(`obs.latitude` = list(units = "decimal degrees", description_md = "Latitude."),
               `obs.measurement_value` = list(description_md = "The measured value."))
  DBI::dbExecute(con, "ALTER TABLE bundle_obs ADD COLUMN latitude DOUBLE")
  cl <- bundle_columns(con, b$table, meta)
  expect_equal(names(cl), c("table", "column", "type", "units", "description"))
  expect_equal(cl$units[cl$table == "obs" & cl$column == "latitude"], "decimal degrees")
  expect_equal(cl$units[cl$table == "obs" & cl$column == "measurement_value"], "see the units column")
  expect_match(cl$description[cl$table == "obs" & cl$column == "line"], "CalCOFI line number")
  expect_equal(nrow(cl[cl$table == "obs", ]), ncol(DBI::dbGetQuery(con, "SELECT * FROM bundle_obs LIMIT 0")))
})

test_that("bundle_readme() says what, who, when, which files, what is left out and where to subset", {
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_a, bundle_cols(con), supplemental = "obs_full", supplemental_rows = c(obs_full = 1e8))
  b <- bundle_build_tables(con, p, "ds_a", bundle_from())
  d <- bundle_format_decision(c(obs = 2e6, sample = 4))
  r <- bundle_readme(rec_a, release_fx, "csv", b, p, d, bundle_license(rec_a))
  for (s in c("# Dataset A", "built from CalCOFI integrated database release **v2026.11.14** (2026-11-14)",
              "identical in\nevery later release that lists this file; see https://calcofi.io/datasets/ds_a/", "Provider Lab", "J Doe",
              "https://calcofi.io/datasets/ds_a/", "https://calcofi.io/explore/", CC_REGISTER_USE_URL,
              CC_STAY_INFORMED_URL, "| `obs.csv` |", "| 3 |", "`obs_full`: full-resolution supplemental",
              "XLSX: Not offered as an Excel workbook", "`hex_id`", "`sample_key` → `sample`",
              "Creative Commons Attribution 4.0 International"))
    expect_match(r, s, fixed = TRUE)
  rx <- bundle_readme(rec_a, release_fx, "parquet", b, p, d, bundle_license(rec_a))
  expect_match(rx, "| `obs.parquet` |", fixed = TRUE)
  rn <- bundle_readme(rec_b, release_fx, "csv", b, p, d, bundle_license(rec_b), has_citation = FALSE)
  expect_match(rn, "the provider has not supplied a dataset citation", fixed = TRUE)
})

test_that("write_bundle() writes a zip per offered format, reproducibly", {
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_a, bundle_cols(con), supplemental = "obs_full")
  b <- bundle_build_tables(con, p, "ds_a", bundle_from())
  out <- withr::local_tempdir()
  w <- write_bundle(con, rec_a, release_fx, b, p, file.path(out, "a"))
  expect_equal(w$format, bundle_formats()); expect_true(all(w$written))
  expect_equal(w$file[1], "calcofi_ds_a_v2026.11.14_csv.zip")
  expect_true(all(file.exists(w$path))); expect_true(all(nchar(w$sha256) == 64))
  expect_false(any(dir.exists(file.path(out, "a", sub("\\.zip$", "", w$file)))))  # folders removed
  # members: one folder named like the zip; documents first, then the tables
  m <- zip::zip_list(w$path[1])$filename
  stem <- "calcofi_ds_a_v2026.11.14_csv/"
  expect_equal(m[1:4], paste0(stem, c("README.md", "CITATION.md", "CITATION.bib", "LICENSE.txt")))
  expect_equal(tail(m, 1), paste0(stem, "columns.csv"))
  expect_true(paste0(stem, "obs.csv") %in% m)
  expect_length(unique(zip::zip_list(w$path[1])$timestamp), 1)   # one fixed instant
  # the same tables written again, elsewhere and later, are the same bytes
  Sys.sleep(1.1)
  w2 <- write_bundle(con, rec_a, release_fx, b, p, file.path(out, "b"))
  expect_identical(w2$sha256, w$sha256)
})

test_that("write_bundle()'s CSV, workbook and Parquet hold the same rows", {
  skip_if_not_installed("readxl"); skip_if_not_installed("arrow")
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_a, bundle_cols(con))
  b <- bundle_build_tables(con, p, "ds_a", bundle_from())
  out <- withr::local_tempdir()
  w <- write_bundle(con, rec_a, release_fx, b, p, out)
  x <- withr::local_tempdir()
  for (z in w$path) zip::unzip(z, exdir = x)
  csv <- utils::read.csv(file.path(x, "calcofi_ds_a_v2026.11.14_csv", "obs.csv"), na.strings = "")
  ref <- DBI::dbGetQuery(con, "SELECT * FROM bundle_obs")
  expect_equal(nrow(csv), 3); expect_equal(csv$obs_id, ref$obs_id)
  expect_equal(csv$datetime[3], "2020-01-02 03:04:05")
  expect_equal(csv$measurement_value, ref$measurement_value)
  xl <- file.path(x, "calcofi_ds_a_v2026.11.14_xlsx", "calcofi_ds_a_v2026.11.14_xlsx.xlsx")
  sheets <- readxl::excel_sheets(xl)
  expect_equal(sheets[1], "README"); expect_equal(tail(sheets, 1), "columns")
  expect_equal(sheets[2:3], c("obs", "sample"))
  xo <- readxl::read_excel(xl, "obs")
  expect_equal(xo$measurement_value, ref$measurement_value)
  expect_equal(xo$scientific_name, ref$scientific_name)
  pq <- arrow::read_parquet(file.path(x, "calcofi_ds_a_v2026.11.14_parquet", "obs.parquet"))
  expect_equal(pq$obs_id, ref$obs_id); expect_equal(names(pq), names(ref))
  cols <- utils::read.csv(file.path(x, "calcofi_ds_a_v2026.11.14_csv", "columns.csv"))
  expect_true(all(c("obs", "sample", "taxon") %in% cols$table))
  lic <- readLines(file.path(x, "calcofi_ds_a_v2026.11.14_csv", "LICENSE.txt"))
  expect_match(lic[1], "Dataset A: CC-BY-4.0")
})

test_that("write_bundle() stops, with the size, when a zip is too large for a browser download", {
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_a, bundle_cols(con))
  b <- bundle_build_tables(con, p, "ds_a", bundle_from())
  expect_error(write_bundle(con, rec_a, release_fx, b, p, withr::local_tempdir(), max_bytes = 100),
               "calcofi_ds_a_v2026.11.14_csv.zip is .* GB, over")
})

test_that("bundle_manifest_entry() is the site's contract: a url per file, a reason per skipped format", {
  con <- bundle_fixture_con(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  p <- bundle_plan(rec_a, bundle_cols(con), supplemental = "obs_full")
  b <- bundle_build_tables(con, p, "ds_a", bundle_from())
  w <- write_bundle(con, rec_a, release_fx, b, p, withr::local_tempdir(), xlsx_max_rows = 2)
  fp <- publish_fingerprint(data = bundle_table_signatures(con, b$table))
  e <- bundle_manifest_entry(rec_a, "v2026.11.14", "2026-11-14", "v2026.11.20", fp, b, p, w,
                             license_kind = "standard", has_provider_citation = TRUE)
  expect_equal(e$dataset_key, "ds_a"); expect_equal(e$version, "v2026.11.14")
  expect_equal(e$checked_version, "v2026.11.20"); expect_equal(e$license, "CC-BY-4.0")
  expect_equal(vapply(e$bundles, `[[`, "", "format"), c("csv", "xlsx", "parquet"))
  csv <- e$bundles[[1]]
  h24 <- substr(fp$content_hash, 1, 24)
  expect_equal(csv$url, sprintf("https://storage.googleapis.com/calcofi-db/publish/zip/ds_a/%s/calcofi_ds_a_v2026.11.14_csv.zip", h24))
  expect_equal(csv$path, sprintf("publish/zip/ds_a/%s/calcofi_ds_a_v2026.11.14_csv.zip", h24))
  expect_equal(e$content_hash, fp$content_hash); expect_equal(e$data_hash, fp$data_hash)
  expect_equal(e$since, "v2026.11.14")
  expect_equal(csv$rows$obs, 3); expect_true(csv$excel_openable); expect_equal(csv$bytes, w$bytes[1])
  xl <- e$bundles[[2]]
  expect_false(xl$written); expect_null(xl$url); expect_match(xl$reason, "Not offered as an Excel workbook")
  expect_equal(e$excluded[[1]]$table, "obs_full")
  expect_equal(e$tables[[1]], list(table = "obs", rows = 3, role = "observations"))
  j <- jsonlite::toJSON(e, auto_unbox = TRUE, null = "null")
  expect_true(jsonlite::validate(j))
})

test_that("dataset_distributions() carries bundles: this release's entry, else the newest", {
  ent <- function(v, fmt = "csv") list(dataset_key = "ds_a", version = v, bundles = list(
    list(format = "csv", written = TRUE, file = sprintf("calcofi_ds_a_%s_csv.zip", v),
         url = sprintf("https://x.example/ds_a/calcofi_ds_a_%s_csv.zip", v), bytes = 10, sha256 = "ab",
         excel_openable = TRUE, rows = list(obs = 3)),
    list(format = "xlsx", written = FALSE, reason = "too big")))
  man <- list(datasets = list(ent("v2026.11.01"), ent("v2026.11.14"),
                              list(dataset_key = "ds_z", version = "v2026.11.14", bundles = list())))
  d <- dataset_distributions("ds_a", list(), list(), bundles = man, version = "v2026.11.01")
  b <- Filter(function(r) identical(r$kind, "bundle"), d)
  expect_length(b, 1)
  expect_equal(b[[1]]$release, "v2026.11.01"); expect_equal(b[[1]]$format, "csv")
  expect_equal(b[[1]]$id, "calcofi_ds_a_v2026.11.01_csv"); expect_equal(b[[1]]$media_type, "application/zip")
  expect_equal(b[[1]]$skipped, list(list(format = "xlsx", reason = "too big")))
  # no entry for the release being built (the publisher runs after it): the newest
  d <- dataset_distributions("ds_a", list(), list(), bundles = man, version = "v2026.12.01")
  expect_equal(Filter(function(r) identical(r$kind, "bundle"), d)[[1]]$release, "v2026.11.14")
  expect_length(Filter(function(r) identical(r$kind, "bundle"),
                       dataset_distributions("ds_q", list(), list(), bundles = man)), 0)
  expect_true("bundle" %in% distribution_kinds())
  sch <- jsonlite::fromJSON(system.file("schema", "datasets.schema.json", package = "calcofi4db"), simplifyVector = FALSE)
  expect_true("bundle" %in% unlist(sch$definitions$distribution$properties$kind$enum))
})

test_that("fetch_bundle_manifest() returns the parsed manifest, or NULL when it does not answer", {
  f <- function(url, ...) if (grepl("manifest.json", url, fixed = TRUE))
    list(status = 200L, content = '{"datasets":[{"dataset_key":"ds_a","version":"v1"}]}') else
    list(status = 404L, content = "")
  m <- fetch_bundle_manifest(fetch = f)
  expect_equal(m$datasets[[1]]$dataset_key, "ds_a")
  expect_null(fetch_bundle_manifest("https://x.example/none", fetch = f))
})

test_that("a curated distribution.csv row cannot claim kind bundle (record-only, derived)", {
  bad <- withr::local_tempfile(fileext = ".csv")
  writeLines(c("dataset_key,kind,portal,id,url,title,status,superseded_by,observed_utc,notes",
               "a_b,bundle,gcs,x,https://x.example/a.zip,t,current,,,"), bad)
  expect_error(read_distribution_registry(bad), "bundle")
})

test_that("STAC reads a bundle as application/zip data", {
  expect_equal(.stac_media_type("https://x.example/calcofi_dic_v2026.11.14_csv.zip"), "application/zip")
})
