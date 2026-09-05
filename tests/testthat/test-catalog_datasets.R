# The dataset catalog record (plan 2026-09-05, WS-R0): one generated record per
# dataset_key, joined from the release sidecars, the registries and the measured
# endpoints. Nothing here touches the network — the fixtures under
# fixtures/catalog/ are the live v2026.09.04 sidecars trimmed to two datasets
# (swfsc_ichthyo, calcofi_dic) + one holding, ERDDAP's allDatasets.csv and one
# netCDF manifests.json as they answered on 2026-09-05, and copies of the
# workflows registries. Two records are pinned as snapshots; every check rule has
# its own red test.

cfx <- function(...) testthat::test_path("fixtures", "catalog", ...)
cfx_text <- function(f) paste(readLines(cfx(f), warn = FALSE, encoding = "UTF-8"), collapse = "\n")

fixture_registries <- function() read_catalog_registries(cfx("metadata"))
fixture_erddap     <- function() parse_erddap_all_datasets(cfx_text("allDatasets.csv"))
fixture_netcdf     <- function() list(swfsc_ichthyo = jsonlite::fromJSON(cfx("netcdf_swfsc_ichthyo.manifests.json"), simplifyVector = FALSE))

fixture_record <- function(...) {
  build_dataset_catalog(
    cfx("metadata.json"), cfx("coverage.json"), cfx("catalog.json"), fixture_registries(),
    erddap = fixture_erddap(), netcdf = fixture_netcdf(),
    since = c(swfsc_ichthyo = "v2026.05.19", calcofi_dic = "v2026.05.19"),
    source_accessed = c(swfsc_ichthyo = "2026-08-25", calcofi_dic = "2026-08-25"),
    spatial_layers = cfx("spatial_layers.json"), bathymetry = cfx("gebco_2025.json"), ...)
}
rec_of <- function(record, key) record$datasets[[which(vapply(record$datasets, function(d) d$dataset_key, "") == key)]]
dist_of <- function(r, kind = NULL, format = NULL, portal = NULL) Filter(function(d)
  (is.null(kind) || identical(d$kind, kind)) && (is.null(format) || identical(d$format, format)) &&
  (is.null(portal) || identical(d$portal, portal)), r$distributions)
reg_of <- function(r, portal) r$registrations[[which(vapply(r$registrations, `[[`, "", "portal") == portal)]]

# a fetcher over saved responses (as test-citation.R's)
fake_fetch <- function(map, status_default = 404L) {
  function(url, accept = NULL, method = "GET", ...) {
    for (pat in names(map)) if (grepl(pat, url, fixed = TRUE))
      return(list(status = 200L, content = map[[pat]], url = url))
    list(status = status_default, content = "", url = url)
  }
}

# vocabularies and classification -----------------------------------------------------

test_that("the registry vocabularies are what the plan says", {
  expect_setequal(distribution_kinds(), c("download", "service", "mirror", "source", "archive", "page", "notebook"))
  expect_true(all(c("erddap-calcofi", "erddap-noaa", "edi", "ncei", "obis", "ipt", "caloos", "datazoo",
                    "ucsd-library", "zenodo", "ncbi", "calcofi.org", "other") %in% distribution_portals()))
  expect_equal(distribution_statuses(), c("current", "superseded", "retired", "external", "planned"))
  expect_equal(registration_statuses(), c("published", "planned", "n/a"))
  expect_equal(holding_statuses(), c("planned", "external", "archived"))
  expect_equal(visibility_values(), c("public", "internal"))
  expect_true(all(c("description", "citation_main", "license", "doi", "contact", "keywords_gcmd", "visibility") %in%
                    dataset_meta_descriptive_keys()))
  expect_equal(dataset_meta_structural_keys(), c("dataset_name", "dataset_name_short", "category", "color", "tables", "in_release"))
  expect_length(intersect(dataset_meta_descriptive_keys(), dataset_meta_structural_keys()), 0)
})

test_that("classify_portal() sorts URLs by host family", {
  expect_equal(classify_portal(c(
    "https://portal.edirepository.org/nis/mapbrowse?packageid=edi.109.4",
    "https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.nodc:0301029",
    "https://coastwatch.pfeg.noaa.gov/erddap/tabledap/erdCalCOFIcufes.html",
    "https://oceanview.pfeg.noaa.gov/erddap/tabledap/CAC_FI_SBAS_obs.html",
    "https://erddap.calcofi.io/erddap/tabledap/calcofi_casts.html",
    "https://oceaninformatics.ucsd.edu/zoodb/",
    "https://library.ucsd.edu/dc/object/bb9217084g",
    "https://obis.org/dataset/0e223f55-c826-4513-ae9a-b04cbf2e189c",
    "https://ipt-obis.gbif.us/resource?r=calcofi_ichthyo",
    "https://data.caloos.org/#module-metadata/1",
    "https://doi.org/10.5281/zenodo.22310858",
    "https://zenodo.org/records/22310858",
    "https://www.ncbi.nlm.nih.gov/bioproject/555783",
    "https://calcofi.org/data/marine-ecosystem-data/zooplankton/",
    "https://storage.googleapis.com/calcofi-db/x.parquet",
    "", NA)),
    c("edi", "ncei", "erddap-noaa", "erddap-noaa", "erddap-calcofi", "datazoo", "ucsd-library", "obis", "ipt",
      "caloos", "other", "zenodo", "ncbi", "calcofi.org", "gcs", NA, NA))
})

test_that("parse_registration() reads the dataset_status.csv publish_* cells", {
  expect_equal(parse_registration("done"), list(status = "published", issues = character()))
  expect_equal(parse_registration("n/a"), list(status = "n/a", issues = character()))
  expect_equal(parse_registration(""), list(status = "n/a", issues = character()))
  expect_equal(parse_registration(NA), list(status = "n/a", issues = character()))
  p <- parse_registration("#38;#39;#40 planned")
  expect_equal(p$status, "planned")
  expect_equal(p$issues, paste0("https://github.com/CalCOFI/workflows/issues/", c(38, 39, 40)))
  expect_equal(parse_registration("#42 planned")$issues, "https://github.com/CalCOFI/workflows/issues/42")
  expect_equal(parse_registration("planned")$status, "planned")
})

# registries --------------------------------------------------------------------------------

test_that("read_distribution_registry() validates shape, vocabularies and absolute URLs", {
  d <- read_distribution_registry(cfx("metadata", "distribution.csv"))
  expect_equal(names(d), c("dataset_key", "kind", "portal", "id", "url", "title", "status", "superseded_by", "observed_utc", "notes"))
  expect_true(all(d$kind %in% distribution_kinds())); expect_true(all(d$status %in% distribution_statuses()))
  bad <- withr::local_tempfile(fileext = ".csv")
  writeLines(c("dataset_key,kind,portal,id,url,title,status,superseded_by,observed_utc,notes",
               "a_b,mirror,erddap-noaa,x,https://x.example/,t,current,,,"), bad)
  expect_silent(read_distribution_registry(bad))
  writeLines(c("dataset_key,kind,portal,id,url,title,status,superseded_by,observed_utc,notes",
               "a_b,copy,erddap-noaa,x,https://x.example/,t,current,,,"), bad)
  expect_error(read_distribution_registry(bad), "unknown `kind`")
  writeLines(c("dataset_key,kind,portal,id,url,title,status,superseded_by,observed_utc,notes",
               "a_b,mirror,coastwatch,x,https://x.example/,t,current,,,"), bad)
  expect_error(read_distribution_registry(bad), "unknown `portal`")
  writeLines(c("dataset_key,kind,portal,id,url,title,status,superseded_by,observed_utc,notes",
               "a_b,mirror,erddap-noaa,x,https://x.example/,t,live,,,"), bad)
  expect_error(read_distribution_registry(bad), "unknown `status`")
  writeLines(c("dataset_key,kind,portal,id,url,title,status,superseded_by,observed_utc,notes",
               "a_b,mirror,erddap-noaa,x,coastwatch erddap,t,current,,,"), bad)
  expect_error(read_distribution_registry(bad), "not absolute URLs")
  writeLines(c("dataset_key,kind,portal,id,url,title,status,superseded_by,observed_utc,notes",
               "a_b,mirror,erddap-noaa,x,https://x.example/,t,current,,NA,"), bad)
  expect_error(read_distribution_registry(bad), "sentinel")
})

test_that("read_portal_registry() and read_dataset_status() read their registries", {
  p <- read_portal_registry(cfx("metadata", "portal.csv"))
  expect_true(all(c("portal", "harvests_from_us", "observe_method") %in% names(p)))
  expect_true(all(c("edi", "ncei", "obis", "erddap", "zenodo", "odis", "caloos") %in% p$portal))
  s <- read_dataset_status(cfx("metadata", "dataset_status.csv"))
  expect_true(all(c("publish_obis", "publish_erddap", "publish_edi", "publish_ncei", "publish_caloos", "dataset_key") %in% names(s)))
  expect_setequal(s$dataset_key, c("swfsc_ichthyo", "calcofi_dic"))
})

test_that("read_dataset_sidecar() defaults visibility, validates it, the status and the licence", {
  f <- withr::local_tempfile(fileext = ".yml")
  writeLines("citation_main: x", f)
  expect_equal(read_dataset_sidecar(f)$visibility, "public")
  writeLines("visibility: secret", f)
  expect_error(read_dataset_sidecar(f), "unknown visibility")
  writeLines("status: someday", f)
  expect_error(read_dataset_sidecar(f), "unknown status")
  writeLines("license: CC BY 4.0", f)
  expect_error(read_dataset_sidecar(f, licenses = c("CC-BY-4.0")), "not an active id")
  expect_equal(read_dataset_sidecar(f)$license, "CC BY 4.0")   # no registry given: not checked here
  expect_null(read_dataset_sidecar(file.path(tempdir(), "nope.yml")))
})

test_that("read_catalog_registries() reads everything once and refuses an unregistered holding", {
  reg <- fixture_registries()
  expect_setequal(names(reg), c("category", "provider", "license", "dataset_status", "distribution", "portal",
                                "measurement_type", "sidecars", "questions", "questions_open", "metadata_dir"))
  expect_setequal(names(reg$sidecars), c("swfsc_ichthyo", "calcofi_dic", "cce-lter_hplc-pigments"))
  expect_equal(reg$sidecars$`cce-lter_hplc-pigments`$status, "planned")
  q <- reg$questions("swfsc_ichthyo")
  expect_true(all(q$related_table == "dataset")); expect_true(all(q$status %in% c("open", "proposed")))
  expect_true(reg$questions_open("swfsc_ichthyo") >= nrow(q))
  expect_true(is.na(reg$questions_open("nobody_here")))
  # a holding with a category / provider outside the registries errors at read time
  dir <- withr::local_tempdir()
  file.copy(cfx("metadata"), dir, recursive = TRUE)
  md <- file.path(dir, "metadata")
  dir.create(file.path(md, "acme", "widgets"), recursive = TRUE)
  writeLines(c("dataset_name: Widgets", "category: Gadgets", "status: planned"), file.path(md, "acme", "widgets", "dataset_meta.yml"))
  expect_error(read_catalog_registries(md), "category `Gadgets`")
  writeLines(c("dataset_name: Widgets", "category: Zooplankton", "status: planned"), file.path(md, "acme", "widgets", "dataset_meta.yml"))
  expect_error(read_catalog_registries(md), "provider `acme`")
  # a sidecar whose provider/dataset disagree with its directory is refused
  writeLines(c("provider: swfsc", "dataset: cufes"), file.path(md, "acme", "widgets", "dataset_meta.yml"))
  expect_error(read_catalog_registries(md), "sits under acme/widgets")
})

# measured inputs -----------------------------------------------------------------------------

test_that("unescape_unicode() decodes ERDDAP's \\uXXXX", {
  expect_equal(unescape_unicode("A \\u2014 B \\u00e9"), "A \u2014 B \u00e9")
  expect_equal(unescape_unicode(c("plain", NA)), c("plain", NA))
})

test_that("parse_erddap_all_datasets() / fetch_erddap_datasets() drop the header rows", {
  e <- fixture_erddap()
  expect_false(any(grepl("\\\\u2014", e$title)))   # 37 titles arrived escaped from ERDDAP's CSV
  expect_equal(e$title[e$datasetID == "swfsc_ichthyo"], "SWFSC Ichthyoplankton \u2014 observations")
  expect_false("allDatasets" %in% e$datasetID); expect_false(any(!nzchar(e$datasetID)))
  expect_equal(nrow(e), 44)     # 37 current + 7 legacy ids, measured 2026-09-05
  f <- fake_fetch(list("allDatasets.csv" = cfx_text("allDatasets.csv")))
  expect_equal(fetch_erddap_datasets(fetch = f), e)
  expect_null(fetch_erddap_datasets(fetch = fake_fetch(list())))
})

test_that("fetch_netcdf_manifests() keeps only the datasets that answer", {
  f <- fake_fetch(list("netcdf/swfsc_ichthyo/manifests.json" = cfx_text("netcdf_swfsc_ichthyo.manifests.json")))
  nc <- fetch_netcdf_manifests(c("swfsc_ichthyo", "calcofi_dic"), fetch = f)
  expect_equal(names(nc), "swfsc_ichthyo")
  expect_equal(nc$swfsc_ichthyo$releases[[1]]$version, "v2026.09.04")
})

test_that("dataset_since_versions() walks the versions oldest first and keeps the first sighting", {
  f <- fake_fetch(list(
    "v2026.03.14/metadata.json" = cfx_text("versions/v2026.03.14.metadata.json"),
    "v2026.05.19/metadata.json" = cfx_text("versions/v2026.05.19.metadata.json"),
    "v2026.08.25/metadata.json" = cfx_text("versions/v2026.08.25.metadata.json"),
    "v2026.09.04/metadata.json" = cfx_text("versions/v2026.09.04.metadata.json")))
  s <- dataset_since_versions(jsonlite::fromJSON(cfx("versions.json"), simplifyVector = FALSE), fetch = f)
  expect_equal(s[["swfsc_ichthyo"]], "v2026.05.19")     # an array-shaped datasets block (schema 1.1)
  expect_equal(s[["calcofi_bottle"]], "v2026.05.19")
  expect_equal(s[["calcofi_dic"]], "v2026.08.25")       # first seen in the keyed block
  # `known` short-circuits nothing it already has
  s2 <- dataset_since_versions(jsonlite::fromJSON(cfx("versions.json"), simplifyVector = FALSE), fetch = f,
                               known = c(calcofi_dic = "v2026.04.01"))
  expect_equal(s2[["calcofi_dic"]], "v2026.04.01")
})

# the record -----------------------------------------------------------------------------------

test_that("build_dataset_catalog() pins the two fixture records (snapshot)", {
  rec <- fixture_record()
  expect_equal(rec$schema_version, "1.0")
  expect_equal(rec$release$version, "v2026.09.04"); expect_equal(rec$release$doi, "10.5281/zenodo.22310858")
  expect_equal(rec$counts, list(datasets = 2L, holdings = 1L, reference = 8L))
  expect_equal(vapply(rec$datasets, `[[`, "", "dataset_key"), c("calcofi_dic", "swfsc_ichthyo"))
  dir <- withr::local_tempdir()
  paths <- write_dataset_catalog(rec, dir)
  expect_equal(basename(paths), c("datasets.json", "calcofi_dic.json", "swfsc_ichthyo.json", "cce-lter_hplc-pigments.json"))
  expect_snapshot_file(paths[which(basename(paths) == "swfsc_ichthyo.json")], "swfsc_ichthyo.json")
  expect_snapshot_file(paths[which(basename(paths) == "calcofi_dic.json")], "calcofi_dic.json")
  # deterministic: a second build writes identical bytes
  dir2 <- withr::local_tempdir()
  write_dataset_catalog(fixture_record(), dir2)
  expect_identical(readLines(file.path(dir, "datasets.json")), readLines(file.path(dir2, "datasets.json")))
  # and it validates against the shipped JSON schema
  expect_true(validate_dataset_catalog(file.path(dir, "datasets.json")))
  expect_true(validate_dataset_catalog(rec))
})

test_that("the swfsc_ichthyo record joins the sidecars, the registries and the measured endpoints", {
  rec <- fixture_record(); r <- rec_of(rec, "swfsc_ichthyo")
  expect_equal(r$provider[c("key", "short")], list(key = "swfsc", short = "NOAA SWFSC")); expect_true(r$provider$registered)
  expect_equal(r$category$name, "Fish Eggs & Larvae"); expect_equal(r$category$realm, "bio"); expect_equal(r$category$icon, "cat-ichthyo"); expect_equal(r$category$order, 10L)
  expect_equal(r$visibility, "public")
  expect_equal(as.character(r$keywords)[1], "EARTH SCIENCE > BIOLOGICAL CLASSIFICATION > ANIMALS/VERTEBRATES > FISH")
  expect_equal(r$attribution$creators[[1]]$name, "Ed Weber"); expect_equal(r$attribution$contact, "data@calcofi.io")
  expect_equal(r$attribution$citation_main, "NOAA Fisheries SWFSC. CalCOFI Ichthyoplankton Database.")
  expect_null(r$attribution$license); expect_equal(r$attribution$source_accessed, "2026-08-25")
  expect_equal(length(r$attribution$pi_names), 0)         # null in the release -> an empty array, never ""
  expect_equal(r$links$page, "https://calcofi.io/datasets/swfsc_ichthyo/")
  expect_equal(r$links$workflow, "https://calcofi.io/workflows/ingest_swfsc_ichthyo.html")
  expect_null(r$links$data_source)
  # coverage
  expect_equal(r$coverage$realm, "bio"); expect_equal(r$coverage$year_min, 1951L); expect_equal(r$coverage$year_max, 2023L)
  expect_equal(r$coverage$n_obs, 482250); expect_equal(r$coverage$n_roots, 54197)
  expect_equal(r$coverage$n_stations, 40L)                # the fixture keeps 40 stations, every one sampled by ichthyo
  expect_equal(r$coverage$n_variables, 1L)
  expect_equal(r$coverage$variables[[1]][c("name", "units")], list(name = "abundance", units = "count"))
  expect_equal(r$coverage$n_taxa, 6L)
  expect_length(r$coverage$taxa, 6); expect_false(is.unsorted(rev(vapply(r$coverage$taxa, `[[`, 1, "n_obs"))))
  expect_match(r$coverage$taxa[[1]]$taxon_key, "^(worms|itis|calcofi_)")
  expect_equal(r$coverage$years$year[1], 2015L); expect_true(all(diff(r$coverage$years$year) == 1))
  expect_equal(r$coverage$bbox$lat_max, 54.385)
  expect_equal(r$coverage$contributes_to, list())         # a bio dataset never contributes through `abundance`
  expect_null(r$coverage$life_stages)                     # the v2026.09.04 coverage predates per-dataset life stages
  # objects: the obs partition + the whole tables attributed to the dataset
  expect_true("obs" %in% vapply(r$objects, `[[`, "", "table"))
  part <- Filter(function(o) o$scope == "partition", r$objects)
  expect_length(part, 1); expect_match(part[[1]]$path, "dataset_key=swfsc_ichthyo"); expect_false(part[[1]]$shared)
  expect_match(part[[1]]$url, "^https://storage.googleapis.com/calcofi-db/")
  expect_equal(part[[1]]$since, "v2026.09.04")
  whole <- Filter(function(o) o$scope == "table", r$objects)
  expect_true(all(c("sample", "obs_attribute", "cruise", "ship") %in% vapply(whole, `[[`, "", "table")))
  expect_true(Filter(function(o) o$table == "sample", whole)[[1]]$shared)      # sample is contributed to by many datasets
  expect_false(Filter(function(o) o$table == "cruise", whole)[[1]]$shared)
  expect_equal(r$since_version, "v2026.05.19")
  # distributions
  expect_equal(vapply(dist_of(r, "service", "erddap"), `[[`, "", "id"), c("swfsc_ichthyo_attribute", "swfsc_ichthyo", "swfsc_ichthyo_sample"))
  expect_equal(vapply(dist_of(r, "service", "erddap"), `[[`, "", "grain"), c("length/stage frequency", "observations", "sampling events"))
  expect_equal(dist_of(r, "service", "iso19115")[[1]]$url, "https://erddap.calcofi.io/erddap/metadata/iso19115/xml/swfsc_ichthyo_iso19115.xml")
  nc <- dist_of(r, "download", "netcdf")
  expect_length(nc, 1); expect_equal(nc[[1]]$url, "https://storage.calcofi.io/calcofi-files-public/netcdf/swfsc_ichthyo/v2026.09.04/swfsc_ichthyo.nc")
  expect_equal(nc[[1]]$sha256, "1f9ab1b0a92b485c96b510e5998ed84cdcd78dc0b1f5a87c1ca47c192c25a7c4"); expect_equal(nc[[1]]$bytes, 225262134)
  expect_equal(dist_of(r, "notebook")[[1]]$url, r$links$workflow)
  expect_equal(dist_of(r, "page")[[1]]$url, "https://calcofi.org/data/marine-ecosystem-data/fish-eggs-larvae/")
  expect_length(dist_of(r, "source"), 0)                  # no public source URL exists
  mir <- dist_of(r, "mirror", portal = "erddap-noaa")
  expect_length(mir, 8); expect_true("erdCalCOFItows" %in% vapply(mir, `[[`, "", "id"))
  expect_equal(dist_of(r, "mirror", portal = "edi")[[1]]$id, "edi.109.4")
  expect_equal(dist_of(r, "archive", portal = "obis")[[1]]$status, "current")
  expect_equal(dist_of(r, "archive", portal = "ipt")[[1]]$id, "calcofi_ichthyo")
  # registrations: erddap + obis measured, zenodo from the release DOI, the rest from the registry
  expect_equal(reg_of(r, "erddap")[c("status", "url")], list(status = "published", url = "https://erddap.calcofi.io/erddap/info/swfsc_ichthyo/index.html"))
  expect_equal(reg_of(r, "obis")$status, "published"); expect_equal(reg_of(r, "obis")$url, "https://obis.org/dataset/0e223f55-c826-4513-ae9a-b04cbf2e189c")
  expect_equal(reg_of(r, "edi")$status, "n/a"); expect_null(reg_of(r, "edi")[["issue"]])
  expect_equal(reg_of(r, "caloos")$status, "planned")
  expect_equal(reg_of(r, "zenodo")$url, "https://doi.org/10.5281/zenodo.22310858")
  # status
  expect_equal(r$status$stage, "published"); expect_equal(r$status$priority, "must-complete"); expect_null(r$status$gh_issue)
  expect_true(r$status$questions_open >= 3)
  expect_equal(vapply(r$status$questions_dataset, `[[`, "", "field"), c("citation_main", "license", "pi_names"))
})

test_that("the calcofi_dic record: a legacy id, an NCEI archive, env contributions, an internal visibility", {
  rec <- fixture_record(); r <- rec_of(rec, "calcofi_dic")
  expect_equal(r$visibility, "internal")                   # the fixture sidecar says so (Decision 25)
  expect_equal(r$attribution$doi, "10.25921/3w9f-jd72"); expect_equal(r$attribution$license, "CC-BY-4.0")
  expect_equal(r$attribution$license_url, "https://creativecommons.org/licenses/by/4.0/")   # from license.csv when the dataset has none
  expect_equal(as.character(r$attribution$pi_names), c("Todd Martz", "Aaron Mau"))
  expect_equal(r$coverage$realm, "env")
  expect_equal(r$coverage$contributes_to, list(list(category = "Physical Oceanography", variables = I(c("ctdtemp_its90", "salinity_pss78")))))
  expect_equal(r$coverage$n_variables, 4L); expect_equal(r$coverage$depth_max_m, 3500)
  v <- r$coverage$variables; expect_equal(vapply(v, `[[`, "", "name"), c("alkalinity", "ctdtemp_its90", "dic", "salinity_pss78"))
  expect_equal(v[[3]]$units, "umol/kg"); expect_equal(v[[2]]$category, "Physical Oceanography")
  expect_true(is.null(v[[3]]$uri) || grepl("^https?://vocab.nerc.ac.uk/", v[[3]]$uri))
  leg <- dist_of(r, "service", portal = "erddap-calcofi")
  expect_length(leg, 1); expect_equal(leg[[1]]$id, "calcofi_dic_old"); expect_true(leg[[1]]$legacy); expect_true(leg[[1]]$live)
  expect_equal(leg[[1]]$status, "superseded"); expect_equal(leg[[1]]$superseded_by, "calcofi_dic")
  # the NCEI landing page is the source (link_data_source) — listed once, as the source
  src <- dist_of(r, "source"); expect_length(src, 1); expect_equal(src[[1]]$portal, "ncei")
  expect_length(dist_of(r, "archive", portal = "ncei"), 0)
  # …but the curated archive row still drives the registration
  expect_equal(reg_of(r, "ncei")[c("status", "url")], list(status = "published",
    url = "https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.nodc:0301029"))
  expect_equal(reg_of(r, "erddap")$status, "published")
  expect_equal(reg_of(r, "obis")$status, "n/a")
  expect_equal(r$status$gh_issue, "https://github.com/CalCOFI/workflows/issues/25")
  expect_length(dist_of(r, "download", "netcdf"), 0)      # no manifest was supplied for dic
  expect_equal(length(r$keywords), 0)
})

test_that("holdings[] and reference[] come from the sidecars, the catalog, the layers and the bathymetry", {
  rec <- fixture_record()
  h <- rec$holdings[[1]]
  expect_equal(h$dataset_key, "cce-lter_hplc-pigments"); expect_equal(h$status$stage, "planned")
  expect_equal(h$status$gh_issue, "https://github.com/CalCOFI/workflows/issues/34")
  expect_equal(h$category$name, "Productivity & Pigments"); expect_equal(h$provider$short, "CCE-LTER")
  expect_equal(h$attribution$doi, "10.6073/pasta/5ab16c6d3a9805a174691104aed3bca8")
  expect_equal(h$links$page, "https://calcofi.io/datasets/cce-lter_hplc-pigments/")
  expect_equal(dist_of(h, "source")[[1]]$portal, "edi")
  kinds <- vapply(rec$reference, `[[`, "", "kind")
  expect_equal(as.integer(table(kinds)[c("table", "layer", "raster")]), c(5L, 2L, 1L))
  tab <- rec$reference[[1]]
  expect_equal(tab$key, "cruise"); expect_equal(tab$rows, 842); expect_match(tab$url, "cruise")
  ly <- Filter(function(x) x$kind == "layer", rec$reference)[[1]]
  expect_equal(ly$key, "noaa_maritime_ts"); expect_equal(ly$url, "https://storage.googleapis.com/calcofi-files-public/_spatial/noaa_maritime_boundaries.pmtiles")
  gb <- Filter(function(x) x$kind == "raster", rec$reference)[[1]]
  expect_equal(gb$key, "gebco_2025"); expect_match(gb$attribution, "GEBCO Compilation Group")
  expect_true(any(grepl("contours.pmtiles$", gb$objects)))
  expect_true(all(startsWith(as.character(gb$objects), "https://storage.googleapis.com/calcofi-db/bathymetry/")))
})

test_that("a release with no netCDF of its own lists the newest published one, marked with its release", {
  rec <- fixture_record(version = "v2026.09.05")      # newer than every manifests.json entry
  nc <- dist_of(rec_of(rec, "swfsc_ichthyo"), "download", "netcdf")
  expect_length(nc, 1)
  expect_equal(nc[[1]]$release, "v2026.09.04")
  expect_match(nc[[1]]$url, "v2026.09.04/swfsc_ichthyo.nc")
  expect_equal(rec$release$version, "v2026.09.05")
})

test_that("release.url follows the prefix the run writes to", {
  rec <- fixture_record(release_prefix = "ducklake-staging/releases")
  expect_equal(rec$release$url, "https://storage.googleapis.com/calcofi-db/ducklake-staging/releases/v2026.09.04/")
  expect_match(rec$release$catalog_url, "^https://storage.googleapis.com/calcofi-db/ducklake-staging/releases/")
  expect_equal(fixture_record()$release$url, "https://storage.googleapis.com/calcofi-db/ducklake/releases/v2026.09.04/")
})

test_that("build_dataset_catalog() works without the optional measured inputs", {
  rec <- build_dataset_catalog(cfx("metadata.json"), cfx("coverage.json"), cfx("catalog.json"), fixture_registries())
  r <- rec_of(rec, "swfsc_ichthyo")
  expect_length(dist_of(r, "service", "erddap"), 0); expect_length(dist_of(r, "download", "netcdf"), 0)
  expect_null(r$since_version); expect_null(r$attribution$source_accessed)
  expect_equal(reg_of(r, "erddap")$status, "published")   # the registry cell says done
  expect_true(length(dist_of(r, "download", "parquet")) > 0)
  expect_equal(rec$counts$reference, 5L)
  expect_true(validate_dataset_catalog(rec))
})

test_that("holdings_from_sidecars() / write_holdings_csv() generate the index", {
  reg <- fixture_registries()
  h <- holdings_from_sidecars(reg)
  expect_equal(names(h), c("key", "name", "category", "provider", "status", "link", "doi", "module", "lead_name",
                           "lead_email", "lead_affiliation", "priority_caloos", "gh_issue", "notes"))
  expect_equal(nrow(h), 1); expect_equal(h$key, "cce-lter_hplc-pigments"); expect_equal(h$lead_email, "rgoericke@ucsd.edu")
  expect_true(is.na(h$priority_caloos))
  f <- withr::local_tempfile(fileext = ".csv")
  write_holdings_csv(reg, f)
  l <- readLines(f)
  expect_match(l[1], "^# GENERATED"); expect_equal(l[2], paste(names(h), collapse = ","))
  expect_false(any(grepl(",NA,", l)))
})

# the check ---------------------------------------------------------------------------------------

test_that("check_dataset_catalog() is clean on the fixture and reports each rule exactly once", {
  rec <- fixture_record(); reg <- fixture_registries()
  d <- check_dataset_catalog(rec, reg, network = FALSE)
  expect_equal(names(d), c("dataset_key", "finding", "level", "detail", "url", "exempt", "question"))
  expect_equal(d$finding, c("ok", "ok", "ok"))            # two datasets + one holding
  expect_silent(assert_dataset_catalog(d))

  # one red test per rule, by breaking the record
  broken <- function(f) { r <- rec; r$datasets[[2]] <- f(r$datasets[[2]]); r }
  rule <- function(f, finding, level = "error") {
    d <- check_dataset_catalog(broken(f), reg, network = FALSE)
    d <- d[d$dataset_key == "swfsc_ichthyo", ]
    expect_equal(d$finding, finding, info = finding); expect_equal(d$level, level, info = finding)
    d
  }
  rule(function(r) { r$dataset_name <- NULL; r }, "missing_name")
  rule(function(r) { r$category$name <- "Gadgets"; r }, "unregistered_category")
  rule(function(r) { r$provider$key <- "acme"; r }, "unregistered_provider")
  rule(function(r) { r$description_md <- NULL; r }, "missing_description")
  rule(function(r) { r$coverage$bbox <- NULL; r }, "missing_bbox")
  rule(function(r) { r$coverage$bbox$lon_max <- NULL; r }, "missing_bbox")
  rule(function(r) { r$distributions <- Filter(function(x) x$kind != "download", r$distributions); r }, "no_download")
  rule(function(r) { r$visibility <- "hidden"; r }, "invalid_visibility")
  rule(function(r) { r$attribution$license <- "CC BY 4.0"; r }, "unregistered_license")
  # no_citation: exempt while a question covers citation_main (Q10 does, in the fixture)
  d <- rule(function(r) { r$attribution$citation_main <- NULL; r }, "no_citation")
  expect_true(d$exempt); expect_equal(d$question, "Q10")
  expect_message(assert_dataset_catalog(check_dataset_catalog(broken(function(r) { r$attribution$citation_main <- NULL; r }), reg, network = FALSE)),
                 "exempt while a question")
  # …and blocking when no question names the field
  d <- rule(function(r) { r$attribution$citation_main <- NULL
    r$status$questions_dataset <- Filter(function(q) identical(q$field, "license"), r$status$questions_dataset); r }, "no_citation")
  expect_false(d$exempt)
  expect_error(assert_dataset_catalog(d), "1 blocking finding")
  # a relative URL, or prose holding two URLs, is dead without any network
  rule(function(r) { r$distributions[[1]]$url <- "parquet/obs.parquet"; r }, "url_dead")
  d <- rule(function(r) { r$distributions[[1]]$url <- "https://a.example/x & https://b.example/y"; r }, "url_dead")
  expect_match(d$detail, "whitespace")
  # errors and exemptions are counted from the whole table
  expect_error(assert_dataset_catalog(check_dataset_catalog(broken(function(r) { r$dataset_name <- NULL; r$description_md <- NULL; r }), reg, network = FALSE)),
               "2 blocking finding")
})

test_that("the network half probes every URL once: 404/410/451 dead, 5xx/no-answer unreachable, retired skipped", {
  rec <- fixture_record(); reg <- fixture_registries()
  asked <- character()
  probe <- function(url) {
    asked <<- c(asked, url)
    if (grepl("erdCalCOFItows", url)) return(410L)
    if (grepl("swfsc_ichthyo_iso19115", url)) return(503L)
    if (grepl("calcofi_dic_old", url)) return(404L)      # superseded -> must NOT be probed
    if (grepl("ipt-obis", url)) return(NA_integer_)
    206L
  }
  d <- check_dataset_catalog(rec, reg, network = TRUE, probe = probe)
  expect_equal(length(asked), length(unique(asked)))    # one request per distinct URL
  expect_false(any(grepl("calcofi_dic_old", asked)))
  expect_equal(sum(d$finding == "url_dead"), 1); expect_equal(d$level[d$finding == "url_dead"], "error")
  expect_match(d$url[d$finding == "url_dead"], "erdCalCOFItows"); expect_match(d$detail[d$finding == "url_dead"], "HTTP 410")
  expect_equal(sum(d$finding == "url_unreachable"), 2); expect_true(all(d$level[d$finding == "url_unreachable"] == "warn"))
  expect_true(any(grepl("no answer", d$detail[d$finding == "url_unreachable"])))
  expect_true("ok" %in% d$finding[d$dataset_key == "calcofi_dic"])
  expect_error(expect_message(assert_dataset_catalog(d), "2 warning"), "1 blocking finding")
  # a probe that answers everywhere: all ok, and the detail says the URLs were answered
  d2 <- check_dataset_catalog(rec, reg, network = TRUE, probe = function(u) 200L)
  expect_true(all(d2$finding == "ok")); expect_match(d2$detail[1], "every listed URL answers")
})

test_that("check_dataset_catalog() reads a written datasets.json too, and flags an unregistered holding", {
  rec <- fixture_record(); reg <- fixture_registries()
  dir <- withr::local_tempdir(); p <- write_dataset_catalog(rec, dir)[1]
  d <- check_dataset_catalog(p, reg, network = FALSE)
  expect_true(all(d$finding == "ok"))
  rec$holdings[[1]]$category$name <- "Gadgets"; rec$holdings[[1]]$dataset_name <- NULL
  d <- check_dataset_catalog(rec, reg, network = FALSE)
  expect_equal(d$finding[d$dataset_key == "cce-lter_hplc-pigments"], c("missing_name", "unregistered_category"))
  # without registries the record's own `registered` flags decide
  rec2 <- fixture_record(); rec2$datasets[[1]]$provider$registered <- FALSE
  expect_equal(check_dataset_catalog(rec2, NULL, network = FALSE)$finding[1], "unregistered_provider")
})

test_that("validate_dataset_catalog() refuses a record that breaks the schema", {
  rec <- fixture_record()
  rec$datasets[[1]]$visibility <- "hidden"
  expect_error(validate_dataset_catalog(rec), "does not validate|schema")
  rec <- fixture_record(); rec$datasets[[1]]$distributions[[1]]$kind <- "copy"
  expect_error(validate_dataset_catalog(rec))
  rec <- fixture_record(); rec$release$version <- "2026.09.04"
  expect_error(validate_dataset_catalog(rec))
})

# the notebook / sidecar split (plan § D-9) ---------------------------------------------------------

test_that("merge_dataset_meta() unions the structural notebook block with the descriptive sidecar", {
  nb <- list(dataset_name = "X", dataset_name_short = "x", category = "Zooplankton", color = "#000")
  sc <- list(citation_main = "Someone (2020). X. https://doi.org/10.1/x", license = "CC-BY-4.0", visibility = "public",
             creators = list(list(name = "A")), provider = "p", dataset = "d", path = "/tmp/x.yml")
  m <- merge_dataset_meta(nb, sc)
  expect_equal(m$dataset_name, "X"); expect_equal(m$license, "CC-BY-4.0"); expect_equal(m$creators[[1]]$name, "A")
  expect_null(m$path); expect_null(m$provider)
  # the same descriptive key in both with the same value is tolerated, a different value is two truths
  expect_silent(merge_dataset_meta(c(nb, list(license = "CC-BY-4.0")), sc))
  expect_error(merge_dataset_meta(c(nb, list(license = "CC0-1.0")), sc, notebook = "ingest_p_d.qmd"), "different values")
  expect_error(merge_dataset_meta(c(nb, list(license = "CC-BY-4.0")), sc, strict = TRUE), "still in")
  # no sidecar: the notebook block passes through
  expect_equal(merge_dataset_meta(nb, NULL), nb)
})

test_that("read_calcofi_meta() merges the sidecar and check_dataset_meta_split() polices the notebook", {
  dir <- withr::local_tempdir()
  qmd <- file.path(dir, "ingest_acme_widgets.qmd")
  writeLines(c("---", "title: x", "calcofi:", "  provider: acme", "  dataset: widgets",
               "  dataset_meta:", "    dataset_name: Widgets", "    category: Zooplankton", "---", "", "# body"), qmd)
  cc <- read_calcofi_meta(qmd)
  expect_equal(cc$provider_dataset, "acme_widgets"); expect_null(cc$dataset_meta$citation_main); expect_null(cc$dataset_meta_sidecar)
  sd <- file.path(dir, "metadata", "acme", "widgets"); dir.create(sd, recursive = TRUE)
  writeLines(c("# source: https://example.org, checked 2026-09-05", "citation_main: Acme (2026). Widgets. https://example.org",
               "license: CC0-1.0", "pi_names: A. Person; B. Person"), file.path(sd, "dataset_meta.yml"))
  cc <- read_calcofi_meta(qmd)
  expect_equal(cc$dataset_meta$citation_main, "Acme (2026). Widgets. https://example.org")
  expect_equal(cc$dataset_meta$dataset_name, "Widgets"); expect_equal(cc$dataset_meta$visibility, "public")
  expect_equal(cc$dataset_meta_sidecar, file.path(sd, "dataset_meta.yml"))
  # the dataset table sees one block, as before the split
  df <- ingest_yaml_to_dataset_df(read_ingest_yaml(dir))
  expect_equal(df$citation_main, "Acme (2026). Widgets. https://example.org"); expect_equal(df$license, "CC0-1.0"); expect_equal(df$dataset_name, "Widgets")
  chk <- check_dataset_meta_split(dir)
  expect_equal(chk$has_sidecar, TRUE); expect_equal(chk$descriptive_in_notebook, "")
  # a descriptive key left in the notebook fails the split check (and a conflicting one fails the merge)
  writeLines(c("---", "title: x", "calcofi:", "  provider: acme", "  dataset: widgets",
               "  dataset_meta:", "    dataset_name: Widgets", "    category: Zooplankton", "    license: CC0-1.0", "---"), qmd)
  expect_error(check_dataset_meta_split(dir), "still in ingest_acme_widgets.qmd: license")
  writeLines(c("---", "title: x", "calcofi:", "  provider: acme", "  dataset: widgets",
               "  dataset_meta:", "    dataset_name: Widgets", "    license: CC-BY-4.0", "---"), qmd)
  expect_error(read_calcofi_meta(qmd), "different values: license")
})
