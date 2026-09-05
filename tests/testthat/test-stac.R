# The static STAC catalog (plan 2026-09-05 § D-5.3, WS-M1). Everything is built
# from the same offline fixtures the record uses (fixtures/catalog/), so nothing
# here touches the network: build_stac() is a pure function of datasets.json +
# metadata.json + spatial_layers.json, and check_stac()'s structural half is what
# fails a broken catalog on a machine with no stac-validator.

sfx <- function(...) testthat::test_path("fixtures", "catalog", ...)
sfx_text <- function(f) paste(readLines(sfx(f), warn = FALSE, encoding = "UTF-8"), collapse = "\n")

stac_fixture_record <- function() build_dataset_catalog(
  sfx("metadata.json"), sfx("coverage.json"), sfx("catalog.json"),
  read_catalog_registries(sfx("metadata")),
  erddap = parse_erddap_all_datasets(sfx_text("allDatasets.csv")),
  netcdf = list(swfsc_ichthyo = jsonlite::fromJSON(sfx("netcdf_swfsc_ichthyo.manifests.json"), simplifyVector = FALSE)),
  since = c(swfsc_ichthyo = "v2026.05.19", calcofi_dic = "v2026.05.19"),
  source_accessed = c(swfsc_ichthyo = "2026-08-25", calcofi_dic = "2026-08-25"),
  spatial_layers = sfx("spatial_layers.json"), bathymetry = sfx("gebco_2025.json"))

# the fixture's calcofi_dic is `visibility: internal` on purpose (it is what the
# internal test asserts), so most tests build with both datasets made public
stac_build <- function(dir, public_all = TRUE, ...) {
  rec <- stac_fixture_record()
  if (public_all) for (i in seq_along(rec$datasets)) rec$datasets[[i]]$visibility <- "public"
  build_stac(rec, catalog = sfx("catalog.json"), spatial_layers = sfx("spatial_layers.json"),
             dir = dir, metadata = sfx("metadata.json"),
             base_url = "https://storage.googleapis.com/calcofi-db/stac", ...)
  rec
}
rd <- function(...) jsonlite::fromJSON(file.path(...), simplifyVector = FALSE)

# helpers ---------------------------------------------------------------------------

test_that("the STAC helpers are exact about media types, checksums and geometry", {
  expect_equal(.stac_media_type("https://x/obs.parquet"), "application/x-parquet")
  expect_equal(.stac_media_type("https://x/a.nc"), "application/x-netcdf")
  expect_equal(.stac_media_type("https://x/iso19115.xml"), "application/xml")
  expect_equal(.stac_media_type("https://x/layer.pmtiles"), "application/vnd.pmtiles")
  expect_equal(.stac_media_type("https://erddap.calcofi.io/erddap/tabledap/x.html"), "text/html")
  # a sha256 becomes a multihash: 0x12 (sha2-256) + 0x20 (32 bytes) + the digest
  expect_equal(.stac_multihash(strrep("ab", 32)), paste0("1220", strrep("ab", 32)))
  expect_null(.stac_multihash("not-a-digest"))
  expect_null(.stac_multihash(NULL))
  expect_equal(.stac_bbox(list(lon_min = -180, lat_min = 0, lon_max = -100, lat_max = 50)),
               c(-180, 0, -100, 50))
  expect_null(.stac_bbox(list(lon_min = -180, lat_min = NA)))
  g <- .stac_geometry(c(-180, 0, -100, 50))
  expect_equal(g$type, "Polygon")
  expect_length(g$coordinates[[1]], 5)                 # a closed ring
  expect_equal(g$coordinates[[1]][[1]], g$coordinates[[1]][[5]])
  expect_equal(.stac_interval(list(year_min = 1951, year_max = 2023)),
               list("1951-01-01T00:00:00Z", "2023-12-31T23:59:59Z"))
  expect_equal(.stac_interval(list()), list(NULL, NULL))
})

# the catalog -----------------------------------------------------------------------

test_that("build_stac() writes a root catalog, a collection + item per dataset and one per layer", {
  dir <- withr::local_tempdir()
  rec <- stac_build(dir)
  root <- rd(dir, "catalog.json")
  expect_equal(root$type, "Catalog")
  expect_equal(root$stac_version, "1.0.0")
  expect_equal(root$id, "calcofi")
  n_pub <- length(rec$datasets)                          # both made public by stac_build()
  n_lyr <- length(jsonlite::fromJSON(sfx("spatial_layers.json"), simplifyVector = FALSE)$layers)
  child <- Filter(function(l) identical(l$rel, "child"), root$links)
  expect_equal(length(child), n_pub + n_lyr)
  expect_true(all(vapply(child, function(l) startsWith(l$href, "https://storage.googleapis.com/calcofi-db/stac/"), logical(1))))
  expect_true(file.exists(file.path(dir, "collections", "swfsc_ichthyo", "collection.json")))
  expect_true(file.exists(file.path(dir, "collections", "swfsc_ichthyo", "items", "v2026.09.04.json")))
  # every layer became its own collection, prefixed so it can never collide with a dataset_key
  expect_true(file.exists(file.path(dir, "collections", "layer_noaa_maritime_ts", "collection.json")))
})

test_that("a collection carries extent, licence, providers, table:tables and sci:*", {
  dir <- withr::local_tempdir(); stac_build(dir)
  co <- rd(dir, "collections", "calcofi_dic", "collection.json")
  expect_equal(co$type, "Collection")
  expect_true("https://stac-extensions.github.io/table/v1.2.0/schema.json" %in% unlist(co$stac_extensions))
  expect_true(nzchar(co$license))                       # never absent: "other" when unlicensed
  expect_length(co$extent$spatial$bbox[[1]], 4)
  expect_length(co$extent$temporal$interval[[1]], 2)
  expect_match(co$extent$temporal$interval[[1]][[1]], "^\\d{4}-01-01T00:00:00Z$")
  expect_true(any(vapply(co$providers, function(p) "host" %in% unlist(p$roles), logical(1))))
  expect_true(any(vapply(co$providers, function(p) "producer" %in% unlist(p$roles), logical(1))))
  tbl <- vapply(co$`table:tables`, function(t) t$name, "")
  expect_true(all(c("sample", "obs") %in% tbl))
  expect_true(any(vapply(co$`table:tables`, function(t) !is.null(t$description), logical(1))))
  # the dataset page is the human link; the release DOI/citation ride the scientific extension
  expect_true(any(vapply(co$links, function(l) identical(l$rel, "about"), logical(1))))
  expect_equal(co$`sci:doi`, "10.25921/3w9f-jd72")
})

test_that("an item is one release: bbox polygon, a datetime that is present, and the assets", {
  dir <- withr::local_tempdir(); stac_build(dir)
  it <- rd(dir, "collections", "swfsc_ichthyo", "items", "v2026.09.04.json")
  expect_equal(it$type, "Feature")
  expect_equal(it$id, "v2026.09.04")
  expect_equal(it$collection, "swfsc_ichthyo")
  expect_equal(it$geometry$type, "Polygon")
  expect_length(it$bbox, 4)
  expect_true("datetime" %in% names(it$properties))     # required, null only beside start/end
  expect_null(it$properties$datetime)
  expect_equal(it$properties$start_datetime, "1951-01-01T00:00:00Z")
  expect_equal(it$properties$end_datetime, "2023-12-31T23:59:59Z")
  types <- vapply(it$assets, function(a) a$type, "")
  expect_true("application/x-parquet" %in% types)
  expect_true("application/x-netcdf" %in% types)
  expect_true("application/xml" %in% types)             # the ERDDAP ISO 19115 record
  expect_true("text/html" %in% types)                   # the ERDDAP pages
  expect_equal(unlist(it$assets$netcdf$roles), "data")
  expect_equal(unlist(it$assets$iso19115$roles), "metadata")
  # a parquet asset carries its bytes, its checksum and its table's columns
  a <- it$assets$obs_partition
  expect_equal(a$type, "application/x-parquet")
  expect_equal(unlist(a$roles), "data")
  expect_true(a$`file:size` > 0)
  expect_match(a$`file:checksum`, "^1220[0-9a-f]{64}$")
  # the fixture metadata.json carries obs.* columns (added for the EML attributeList tests), so
  # the obs partition asset carries them as table:columns
  tc <- a$`table:columns`
  expect_true(is.list(tc) && length(tc) > 0)
  expect_true("obs_id" %in% vapply(tc, function(cl) cl$name, ""))
})

test_that("table:columns reach a parquet asset from metadata.json, per table", {
  md <- list(
    tables = list(obs = list(description_md = "Observations, one per measured value")),
    columns = list(
      obs.obs_id = list(name_long = "Obs ID", description_md = "row id", data_type = "BIGINT"),
      obs.measurement_value = list(description_md = "the value", data_type = "DOUBLE"),
      sample.sample_key = list(data_type = "VARCHAR")))
  cols <- .stac_table_columns(md, "obs")
  expect_equal(vapply(cols, function(c) c$name, ""), c("obs_id", "measurement_value"))
  expect_equal(cols[[1]]$type, "BIGINT")
  expect_equal(cols[[1]]$description, "row id")
  expect_null(.stac_table_columns(md, "taxon"))          # a table metadata.json does not describe
  expect_null(.stac_table_columns(list(), "obs"))

  dir <- withr::local_tempdir()
  rec <- stac_fixture_record()
  for (i in seq_along(rec$datasets)) rec$datasets[[i]]$visibility <- "public"
  build_stac(rec, spatial_layers = NULL, dir = dir, metadata = md)
  it <- rd(dir, "collections", "swfsc_ichthyo", "items", "v2026.09.04.json")
  expect_equal(vapply(it$assets$obs_partition$`table:columns`, function(c) c$name, ""),
               c("obs_id", "measurement_value"))
  co <- rd(dir, "collections", "swfsc_ichthyo", "collection.json")
  expect_equal(Filter(function(t) identical(t$name, "obs"), co$`table:tables`)[[1]]$description,
               "Observations, one per measured value")
})

test_that("a superseded or retired distribution never becomes an asset", {
  dir <- withr::local_tempdir(); rec <- stac_build(dir)
  it <- rd(dir, "collections", "calcofi_dic", "items", "v2026.09.04.json")
  hrefs <- vapply(it$assets, function(a) a$href, "")
  legacy <- "https://erddap.calcofi.io/erddap/tabledap/calcofi_dic_old.html"
  # the record lists it (nothing is deleted from the registry) …
  expect_true(legacy %in% vapply(rec$datasets[[which(vapply(rec$datasets, function(d) d$dataset_key, "") == "calcofi_dic")]]$distributions,
                                 function(d) d$url %||% "", ""))
  # … and STAC does not publish it as an asset
  expect_false(legacy %in% unname(hrefs))
})

test_that("an internal dataset gets no collection", {
  dir <- withr::local_tempdir()
  stac_build(dir, public_all = FALSE)                   # calcofi_dic is internal in the fixture
  expect_false(dir.exists(file.path(dir, "collections", "calcofi_dic")))
  expect_true(dir.exists(file.path(dir, "collections", "swfsc_ichthyo")))
})

test_that("a layer collection carries its PMTiles asset and its own bbox", {
  dir <- withr::local_tempdir(); stac_build(dir)
  co <- rd(dir, "collections", "layer_noaa_maritime_ts", "collection.json")
  expect_equal(co$type, "Collection")
  expect_equal(co$assets$pmtiles$type, "application/vnd.pmtiles")
  expect_match(co$assets$pmtiles$href, "\\.pmtiles$")
  expect_length(co$extent$spatial$bbox[[1]], 4)
})

test_that("build_stac() stops without a release version and honours a staging base_url", {
  dir <- withr::local_tempdir()
  rec <- stac_fixture_record(); rec$release$version <- NULL
  expect_error(build_stac(rec, dir = dir), "no release version")
  rec2 <- stac_fixture_record()
  build_stac(rec2, dir = dir, metadata = sfx("metadata.json"),
             base_url = "https://storage.googleapis.com/calcofi-db/stac-staging")
  root <- rd(dir, "catalog.json")
  self <- Filter(function(l) identical(l$rel, "self"), root$links)[[1]]
  expect_equal(self$href, "https://storage.googleapis.com/calcofi-db/stac-staging/catalog.json")
})

# the check -------------------------------------------------------------------------

test_that("check_stac() passes a built catalog and names every document", {
  dir <- withr::local_tempdir(); stac_build(dir)
  d <- check_stac(dir)
  expect_s3_class(d, "tbl_df")
  expect_equal(nrow(d[d$level == "error", ]), 0)
  expect_true("catalog.json" %in% d$document)
  expect_true(any(grepl("^collections/swfsc_ichthyo/items/", d$document)))
  expect_silent(assert_stac(d, quiet = TRUE))
})

test_that("check_stac() catches a broken document, a dangling child link and a bad asset", {
  dir <- withr::local_tempdir(); stac_build(dir)
  # a child link with no document behind it
  p <- file.path(dir, "catalog.json")
  root <- rd(p)
  root$links <- c(root$links, list(list(rel = "child", href = "https://x/collections/nope/collection.json",
                                        type = "application/json")))
  jsonlite::write_json(root, p, auto_unbox = TRUE, pretty = TRUE)
  d <- check_stac(dir)
  expect_true("bad_link" %in% d$finding)
  expect_error(assert_stac(d, quiet = TRUE), "STAC catalog is invalid")

  dir2 <- withr::local_tempdir(); stac_build(dir2)
  ip <- file.path(dir2, "collections", "calcofi_dic", "items", "v2026.09.04.json")
  it <- rd(ip); it$properties$datetime <- NULL; it$properties <- it$properties[names(it$properties) != "datetime"]
  it$assets[[1]]$type <- NULL
  jsonlite::write_json(it, ip, auto_unbox = TRUE, pretty = TRUE)
  d2 <- check_stac(dir2)
  expect_true("missing_field" %in% d2$finding)
  expect_true("bad_asset" %in% d2$finding)

  dir3 <- withr::local_tempdir(); stac_build(dir3)
  writeLines("{not json", file.path(dir3, "collections", "calcofi_dic", "collection.json"))
  expect_true("invalid_json" %in% check_stac(dir3)$finding)
})

test_that("stac_findings() levels are what assert_stac() enforces", {
  f <- stac_findings()
  expect_equal(unname(f[c("missing_field", "bad_link", "bad_asset", "validator_error")]), rep("error", 4))
  expect_equal(unname(f[c("no_validator", "no_asset")]), c("warn", "warn"))
})
