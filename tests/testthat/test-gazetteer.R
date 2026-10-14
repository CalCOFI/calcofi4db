# the oceanmetrics gazetteer as regions: read_gazetteer_layer() (pinned, verified, no network here)
# and build_spatial_layers()'s gazetteer status pass-through and per-region status

# a tiny GeoParquet (WKB + `geo` metadata + a GeoParquet 1.1 bbox covering column), the shape of the
# gazetteer's places.parquet v1.0.0 (no status_upstream / status_history unless `extra` adds them)
gz_parquet <- function(path, extra = NULL) {
  sfc <- sf::st_sfc(
    sf::st_polygon(list(rbind(c(-119, 34), c(-118, 34), c(-118, 35), c(-119, 35), c(-119, 34)))),
    sf::st_polygon(list(rbind(c(-121, 33), c(-120, 33), c(-120, 34), c(-121, 34), c(-121, 33)))),
    crs = 4326)
  df <- data.frame(
    place_id = c("AOA:A", "AOA:B"), authority = "AOA", name = c("Area A", "Area B"),
    status = c("identified", "rescinded"), status_source = "https://example.org/notice",
    status_date = c("2025-09-19", "2025-07-30"), Acres = c(1000, 500))
  if (!is.null(extra)) df <- cbind(df, extra)
  bb <- t(vapply(sfc, function(g) as.numeric(sf::st_bbox(g)), numeric(4)))
  tb <- arrow::Table$create(
    df,
    bbox     = arrow::StructArray$create(data.frame(xmin = bb[, 1], ymin = bb[, 2], xmax = bb[, 3], ymax = bb[, 4])),
    geometry = arrow::Array$create(lapply(sf::st_as_binary(sfc), as.raw), type = arrow::binary()))
  tb$metadata$geo <- as.character(jsonlite::toJSON(list(
    version = "1.1.0", primary_column = "geometry",
    columns = list(geometry = list(
      encoding = "WKB", geometry_types = list("Polygon"),
      covering = list(bbox = list(xmin = list("bbox", "xmin"), ymin = list("bbox", "ymin"),
                                  xmax = list("bbox", "xmax"), ymax = list("bbox", "ymax")))))),
    auto_unbox = TRUE))
  arrow::write_parquet(tb, path)
  invisible(digest::digest(path, algo = "sha256", file = TRUE))
}

# a local gazetteer: <dir>/index/layers.json, <dir>/<slug>/places.parquet and versions.json
gz_local <- function(dir, slug = "aoa", new_fields = TRUE, extra = NULL, sha = NULL) {
  dir.create(file.path(dir, slug), recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(dir, "index"), showWarnings = FALSE)
  pq  <- file.path(dir, slug, "places.parquet")
  got <- gz_parquet(pq, extra)
  jsonlite::write_json(list(current_version = "1.0.0", versions = list(list(
    version = "1.0.0", created = "2026-10-08T14:58:54Z",
    assets = list(places.parquet = list(sha256 = got, href = paste0(slug, "/places.parquet")))))),
    file.path(dir, slug, "versions.json"), auto_unbox = TRUE)
  row <- list(slug = slug, version = "1.0.0", n = 2L,
              pmtiles = paste0("https://example.org/g/", slug, "/places.pmtiles"))
  if (new_fields) row <- c(row, list(parquet = pq, parquet_sha256 = sha %||% got))
  man <- file.path(dir, "index", "layers.json")
  jsonlite::write_json(list(schema = 1, base = paste0(dir, "/"), layers = list(row)), man, auto_unbox = TRUE)
  list(manifest = man, parquet = pq, sha256 = got)
}

test_that("read_gazetteer_layer() reads a pinned, verified layer as sf with the place columns first", {
  skip_if_not_installed("sf")
  d <- withr::local_tempdir(); cache <- withr::local_tempdir()
  g <- gz_local(d)
  x <- read_gazetteer_layer("aoa", manifest = g$manifest, cache_dir = cache)
  expect_s3_class(x, "sf")
  expect_identical(attr(x, "sf_column"), "geometry")
  expect_identical(sf::st_crs(x)$epsg, 4326L)
  expect_identical(names(x), c("place_id", "name", "status", "status_date", "status_source",
                               "status_upstream", "status_history", "authority", "Acres", "geometry"))
  expect_identical(x$place_id, c("AOA:A", "AOA:B"))
  expect_identical(x$status, c("identified", "rescinded"))
  expect_identical(x$status_date, c("2025-09-19", "2025-07-30"))
  # today's parquet predates these: NA, never an error
  expect_identical(x$status_upstream, c(NA_character_, NA_character_))
  expect_identical(x$status_history, c(NA_character_, NA_character_))
  expect_identical(x$Acres, c(1000, 500))                  # a native attribute
  expect_false("bbox" %in% names(x))                       # the GeoParquet covering column is dropped
  expect_equal(as.numeric(sf::st_bbox(x)), c(-121, 33, -118, 35))
  expect_identical(attr(x, "gazetteer"),
                   list(slug = "aoa", version = "1.0.0", sha256 = g$sha256, url = g$parquet))
  expect_true(file.exists(file.path(cache, "aoa", "1.0.0", "places.parquet")))
})

test_that("read_gazetteer_layer() keeps status_upstream and status_history when the parquet has them", {
  skip_if_not_installed("sf")
  d <- withr::local_tempdir()
  hist <- '[{"date":"2024-01-01","value":"active"},{"date":"2025-07-30","value":"rescinded"}]'
  g <- gz_local(d, extra = data.frame(status_upstream = c("ACTIVE", NA), status_history = c(NA, hist)))
  x <- read_gazetteer_layer("aoa", manifest = g$manifest, cache_dir = withr::local_tempdir())
  expect_identical(x$status_upstream, c("ACTIVE", NA))
  expect_identical(x$status_history, c(NA, hist))
  expect_identical(names(x)[1:7], c("place_id", "name", "status", "status_date", "status_source",
                                    "status_upstream", "status_history"))
})

test_that("read_gazetteer_layer() falls back to <base><slug>/places.parquet and versions.json", {
  skip_if_not_installed("sf")
  d <- withr::local_tempdir()
  g <- gz_local(d, new_fields = FALSE)                     # the manifest has no parquet / parquet_sha256
  x <- read_gazetteer_layer("aoa", manifest = g$manifest, cache_dir = withr::local_tempdir())
  expect_identical(nrow(x), 2L)
  expect_identical(attr(x, "gazetteer")$sha256, g$sha256)
  expect_identical(attr(x, "gazetteer")$url, paste0(d, "/aoa/places.parquet"))
  # a version the gazetteer never published is refused
  expect_error(read_gazetteer_layer("aoa", version = "9.9.9", manifest = g$manifest,
                                    cache_dir = withr::local_tempdir()), "no version 9.9.9")
})

test_that("read_gazetteer_layer() refuses a sha256 mismatch and caches nothing", {
  skip_if_not_installed("sf")
  d <- withr::local_tempdir(); cache <- withr::local_tempdir()
  g <- gz_local(d, sha = strrep("0", 64))
  expect_error(read_gazetteer_layer("aoa", manifest = g$manifest, cache_dir = cache), "sha256 mismatch")
  expect_false(file.exists(file.path(cache, "aoa", "1.0.0", "places.parquet")))
  expect_error(read_gazetteer_layer("nope", manifest = g$manifest, cache_dir = cache), "no layer 'nope'")
})

test_that("read_gazetteer_layer() reuses a verified cached copy and replaces a corrupt one", {
  skip_if_not_installed("sf")
  d <- withr::local_tempdir(); cache <- withr::local_tempdir()
  g <- gz_local(d)
  read_gazetteer_layer("aoa", manifest = g$manifest, cache_dir = cache)
  cached <- file.path(cache, "aoa", "1.0.0", "places.parquet")
  # the source gone: the verified copy still serves
  file.rename(g$parquet, paste0(g$parquet, ".bak"))
  expect_identical(nrow(read_gazetteer_layer("aoa", manifest = g$manifest, cache_dir = cache)), 2L)
  # a corrupted cached copy is fetched again and re-verified
  file.rename(paste0(g$parquet, ".bak"), g$parquet)
  writeLines("garbage", cached)
  expect_identical(nrow(read_gazetteer_layer("aoa", manifest = g$manifest, cache_dir = cache)), 2L)
  expect_identical(digest::digest(cached, algo = "sha256", file = TRUE), g$sha256)
})

# build_spatial_layers(): a gazetteer row ingested into `spatial` is a region with status ----

gz_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  load_duckdb_extension(con, "spatial")
  DBI::dbExecute(con, "CREATE TABLE spatial AS SELECT spatial_key, id, layer, name, ST_GeomFromText(wkt) AS geom FROM (VALUES
    ('mpa:1',  1, 'Marine Protected Areas', 'Near site',  'POLYGON((-117.4 32.8, -117.2 32.8, -117.2 33.0, -117.4 33.0, -117.4 32.8))'),
    ('mpa:2',  2, 'Marine Protected Areas', 'Far away',   'POLYGON((-120 35, -119 35, -119 36, -120 36, -120 35))'),
    ('wpa:1',  1, 'BOEM Wind Planning Areas', 'Oregon PSN 0567', 'POLYGON((-125 42, -124 42, -124 43, -125 43, -125 42))'),
    ('wpa:2',  2, 'BOEM Wind Planning Areas', 'Hawaii Oahu South', 'POLYGON((-158 20, -157 20, -157 21, -158 21, -158 20))'),
    ('wpa:3',  3, 'BOEM Wind Planning Areas', 'No status', 'POLYGON((-126 40, -125 40, -125 41, -126 41, -126 40))'),
    ('port:1', 1, 'CA Ports', 'A port', 'POINT(-117.3 32.9)')
    ) t(spatial_key, id, layer, name, wkt)")
  # memberships: wpa:1 (2 roots), wpa:3 (1 root, no status), mpa:1 (1 root); wpa:2 has none
  DBI::dbExecute(con, "CREATE TABLE sample_spatial AS SELECT * FROM (VALUES
    (1, 'r:1', 'BOEM Wind Planning Areas', 'wpa:1', 'Oregon PSN 0567'),
    (2, 'r:2', 'BOEM Wind Planning Areas', 'wpa:1', 'Oregon PSN 0567'),
    (3, 'r:3', 'BOEM Wind Planning Areas', 'wpa:3', 'No status'),
    (4, 'r:4', 'Marine Protected Areas',   'mpa:1', 'Near site')
    ) t(root_id, root_sample_key, layer, spatial_key, spatial_name)")
  # EAV: wpa:1 status + a typed date, wpa:2 status (no members), wpa:3 an empty status, mpa:1 status as text
  DBI::dbExecute(con, "CREATE TABLE spatial_attribute AS SELECT * FROM (VALUES
    ('wpa:1', 'BOEM Wind Planning Areas', 1, 'status',      NULL::DOUBLE, NULL::INTEGER, 'rescinded', NULL::DATE,        NULL::BOOLEAN),
    ('wpa:1', 'BOEM Wind Planning Areas', 1, 'status_date', NULL, NULL, NULL,        DATE '2025-07-30', NULL),
    ('wpa:1', 'BOEM Wind Planning Areas', 1, 'acres',       12.5, NULL, NULL,        NULL,              NULL),
    ('wpa:2', 'BOEM Wind Planning Areas', 2, 'status',      NULL, NULL, 'rescinded', NULL,              NULL),
    ('wpa:3', 'BOEM Wind Planning Areas', 3, 'status',      NULL, NULL, '',          NULL,              NULL),
    ('mpa:1', 'Marine Protected Areas',   1, 'status',      NULL, NULL, 'designated', NULL,             NULL),
    ('mpa:1', 'Marine Protected Areas',   1, 'status_date', NULL, NULL, '2012-01-01', NULL,             NULL)
    ) t(spatial_key, layer, id, fld, val_dbl, val_int, val_chr, val_date, val_lgl)")
  con
}

gz_registry <- function(path) {
  writeLines(c(
    "dataset_id,dataset_group,layer,group,geom_type,filter_expr,line_color,fill_color,line_width,fill_opacity,default_visible,name_field,description,attribution,source_url,source_layer,popup_fields",
    "ca_mpas,ca_marine_protected_areas,Marine Protected Areas,Protected Areas,polygon,,#388e3c,#a5d6a7,1,0.2,FALSE,fullname,MPAs,CDFW,,,",
    "wind_planning,boem_wind_planning_rescinded,BOEM Wind Planning Areas,Energy,polygon,,,,,,FALSE,name,wpa,BOEM,https://example.org/g/boem_wind_planning_rescinded/places.pmtiles,boem_wind_planning_rescinded,status",
    "aoa,noaa_aoa_socal,NOAA SoCal AOAs,Energy,polygon,,,,,,FALSE,name,aoa,NOAA,https://example.org/g/noaa_aoa_socal/places.pmtiles,noaa_aoa_socal,status",
    "ports,ca_ports,CA Ports,Ports,point,,,,,,FALSE,name,ports,CDFW,,,"), path)
  path
}

test_that("build_spatial_layers(): a gazetteer row with spatial rows counts from spatial and carries its status", {
  con <- gz_con()
  csv <- gz_registry(withr::local_tempfile(fileext = ".csv"))
  man <- withr::local_tempfile(fileext = ".json")
  st  <- list(value = "rescinded", date = "2025-07-30", source = "https://example.org/boem", note = "BOEM notice")
  jsonlite::write_json(list(schema = 1, layers = list(
    list(slug = "boem_wind_planning_rescinded", version = "1.1.0", n = 99L, bbox = c(-180, -90, 180, 90),
         pmtiles = "https://example.org/g/boem_wind_planning_rescinded/places.pmtiles",
         status = st, status_history = list(st), status_label = "rescinded 2025-07-30",
         created = "2026-10-08", updated = "2026-10-14T10:00:00Z", source_date = "2025-07-30"),
    # an older manifest row: no status fields, no version
    list(slug = "noaa_aoa_socal", n = 10L, bbox = c(-119.6, 33.96, -118.56, 34.29),
         pmtiles = "https://example.org/g/noaa_aoa_socal/places.pmtiles"))), man, auto_unbox = TRUE, digits = NA)
  x   <- build_spatial_layers(con, csv, "vTEST", "u/", gazetteer_manifest = man)
  ids <- vapply(x$layers, function(l) l$id, "")
  w   <- x$layers[[match("wind_planning", ids)]]; a <- x$layers[[match("aoa", ids)]]
  mpa <- x$layers[[match("ca_mpas", ids)]];       p <- x$layers[[match("ports", ids)]]

  # counted from `spatial` (3 features, their bbox and names), not from the manifest's n = 99
  expect_identical(w$n_features, 3L)
  expect_identical(w$bbox, c(-158, 20, -124, 43))
  expect_identical(w$names, list("Hawaii Oahu South", "No status", "Oregon PSN 0567"))
  expect_identical(w$n_memberships, 3L)
  # the layer status rides through from the manifest row
  expect_identical(w$status, st)
  expect_identical(w$status_history, list(st))
  expect_identical(w$status_label, "rescinded 2025-07-30")
  expect_identical(w$created, "2026-10-08"); expect_identical(w$updated, "2026-10-14T10:00:00Z")
  expect_identical(w$source_date, "2025-07-30"); expect_identical(w$gazetteer_version, "1.1.0")
  # region_status: only regions with members AND a non-empty status (not wpa:2, not wpa:3)
  expect_identical(w$region_status, list(list(name = "Oregon PSN 0567", status = "rescinded", status_date = "2025-07-30")))

  # a gazetteer row with no spatial rows: old behaviour, counts from the manifest; an older manifest
  # row lacking the status fields gives NULLs
  expect_identical(a$n_features, 10L); expect_identical(a$bbox, c(-119.6, 33.96, -118.56, 34.29))
  expect_null(a$names); expect_identical(a$n_memberships, 0L); expect_null(a$region_status)
  for (f in c("status", "status_history", "status_label", "created", "updated", "source_date", "gazetteer_version"))
    expect_null(a[[f]])

  # region_status for every layer, not only gazetteer rows; a date stored as text passes as is
  expect_identical(mpa$region_status, list(list(name = "Near site", status = "designated", status_date = "2012-01-01")))
  expect_null(mpa$status); expect_null(mpa$gazetteer_version)
  # a points layer: no memberships, no region status (unchanged)
  expect_identical(p$n_memberships, 0L); expect_null(p$region_status)
})

test_that("build_spatial_layers(): region_status is NULL without spatial_attribute or sample_spatial", {
  con <- gz_con()
  csv <- gz_registry(withr::local_tempfile(fileext = ".csv"))
  DBI::dbExecute(con, "DROP TABLE spatial_attribute")
  x <- build_spatial_layers(con, csv, "vTEST", "u/", gazetteer_manifest = NULL)
  expect_true(all(vapply(x$layers, function(l) is.null(l$region_status), TRUE)))
  ids <- vapply(x$layers, function(l) l$id, "")
  expect_identical(x$layers[[match("wind_planning", ids)]]$n_features, 3L)
  # a gazetteer row without spatial rows and without a manifest: zero features, no warning
  expect_identical(x$layers[[match("aoa", ids)]]$n_features, 0L)
  DBI::dbExecute(con, "DROP TABLE sample_spatial")
  y <- build_spatial_layers(con, csv, "vTEST", "u/", gazetteer_manifest = NULL)
  expect_true(all(vapply(y$layers, function(l) is.null(l$region_status), TRUE)))
  expect_identical(y$layers[[match("wind_planning", ids)]]$n_memberships, 0L)
})

test_that("build_spatial_layers(): a status_date only in val_chr, and a missing date, serialise cleanly", {
  con <- gz_con()
  csv <- gz_registry(withr::local_tempfile(fileext = ".csv"))
  DBI::dbExecute(con, "DELETE FROM spatial_attribute WHERE spatial_key = 'mpa:1' AND fld = 'status_date'")
  x   <- build_spatial_layers(con, csv, "vTEST", "u/", gazetteer_manifest = NULL)
  mpa <- x$layers[[1]]
  expect_identical(mpa$region_status, list(list(name = "Near site", status = "designated", status_date = NULL)))
  expect_identical(as.character(jsonlite::toJSON(mpa$region_status, auto_unbox = TRUE, null = "null")),
                   '[{"name":"Near site","status":"designated","status_date":null}]')
})
