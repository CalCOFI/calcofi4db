# One EML 2.2 document per dataset (plan 2026-09-05, WS-E1 / § D-8, Decision 13):
# generated from the catalog record + the descriptive sidecar, validated against
# EML 2.2's XSDs — which ship with emld, so nothing here touches the network.
# The two fixture datasets are the same ones test-catalog_datasets.R builds
# (fixtures/catalog/, the live v2026.09.04 sidecars trimmed); every check_eml()
# finding has its own red test over a minimal synthetic record.

efx <- function(...) testthat::test_path("fixtures", "catalog", ...)

eml_fixture_registries <- function() read_catalog_registries(efx("metadata"))
eml_fixture_gear       <- function() read_gear_registry(efx("metadata", "gear.csv"))

eml_fixture_catalog <- function() {
  build_dataset_catalog(
    efx("metadata.json"), efx("coverage.json"), efx("catalog.json"), eml_fixture_registries(),
    since = c(swfsc_ichthyo = "v2026.05.19", calcofi_dic = "v2026.05.19"),
    source_accessed = c(swfsc_ichthyo = "2026-08-25", calcofi_dic = "2026-08-25"),
    spatial_layers = efx("spatial_layers.json"))
}

eml_fixture_docs <- function() {
  reg <- eml_fixture_registries()
  build_eml_catalog(eml_fixture_catalog(), sidecars = reg, meta = efx("metadata.json"),
                    coverage = efx("coverage.json"), gear = eml_fixture_gear())
}

# a minimal record that produces a clean document: every required element present
min_record <- function(...) {
  r <- list(
    dataset_key = "acme_widgets",
    provider = list(key = "acme", short = "ACME", name = "ACME Institution",
                    url = "https://acme.example/", registered = TRUE),
    dataset = "widgets", dataset_name = "ACME Widgets", dataset_name_short = "Widgets",
    category = list(name = "Zooplankton", realm = "bio", registered = TRUE),
    visibility = "public",
    description_md = paste("A synthetic fixture dataset used to exercise the EML builder, long",
                           "enough that the twenty word abstract rule is satisfied without any",
                           "placeholder text at all."),
    keywords = c("EARTH SCIENCE > OCEANS > MARINE BIOLOGY", "widgets"),
    attribution = list(citation_main = "ACME (2026). Widgets.", license = "CC-BY-4.0",
                       license_name = "Creative Commons Attribution 4.0 International",
                       license_url = "https://creativecommons.org/licenses/by/4.0/",
                       creators = list(list(name = "Ada Lovelace", organization = "ACME Institution",
                                            orcid = "0000-0002-1825-0097", email = "ada@acme.example",
                                            role = "principalInvestigator")),
                       pi_names = character(), contact = "ada@acme.example",
                       source_accessed = "2026-09-01"),
    links = list(page = "https://calcofi.io/datasets/acme_widgets/", data_source = NULL),
    coverage = list(realm = "bio", temporal = "1990 to 2000", year_min = 1990L, year_max = 2000L,
                    spatial = "30-40 N, 120-110 W",
                    bbox = list(lat_min = 30, lat_max = 40, lon_min = -120, lon_max = -110),
                    variables = c("abundance")),
    tables = c("obs"),
    objects = list(list(table = "obs", scope = "partition", path = "t/obs/h/data_0.parquet",
                        url = "https://storage.googleapis.com/calcofi-db/t/obs/h/data_0.parquet",
                        bytes = 1234, sha256 = "abc")),
    status = list(questions_dataset = list()))
  mod <- list(...)
  for (k in names(mod)) r[[k]] <- mod[[k]]
  r
}

min_meta <- function() list(
  tables  = list(obs = list(name_long = "Observation", description_md = "One measurement per row.")),
  columns = list(
    obs.obs_id           = list(name_long = "Observation Id", description_md = "Row id.", data_type = "BIGINT"),
    obs.depth_min_m      = list(name_long = "Depth Min", units = "m", description_md = "Shallowest depth.", data_type = "DOUBLE"),
    obs.datetime         = list(name_long = "Datetime", description_md = "Event time.", data_type = "TIMESTAMP"),
    obs.measurement_type = list(name_long = "Measurement Type", description_md = "The measured quantity.", data_type = "VARCHAR")))

min_release <- function() list(version = "v2026.09.05", release_date = "2026-09-05",
                               citation = "CalCOFI (2026). Integrated Database v2026.09.05.")

min_doc <- function(...) build_eml(min_record(...), meta = min_meta(), release = min_release())

# check one built document, without touching the schema (validate = FALSE)
findings_of <- function(doc, record = NULL)
  check_eml(doc, record = record, validate = FALSE)$finding

# helpers --------------------------------------------------------------------------------

test_that("markdown is rendered to paragraphs WITHOUT eating the underscores in identifiers", {
  # `sample_key` arriving as `samplekey` in an attributeDefinition is the bug this pins
  expect_equal(calcofi4db:::.md_paras("`sample_key` points at **parent_sample_key**"),
               "sample_key points at parent_sample_key")
  expect_equal(calcofi4db:::.md_paras("one\n\ntwo"), c("one", "two"))
  expect_equal(calcofi4db:::.md_paras("see [the docs](https://x.example/)"),
               "see the docs (https://x.example/)")
  expect_equal(calcofi4db:::.md_paras(NULL), character())
  expect_equal(calcofi4db:::.n_words("a b c"), 3L)
  expect_equal(calcofi4db:::.n_words(""), 0L)
})

test_that("a person name splits into given/sur either way round", {
  expect_equal(calcofi4db:::.split_person("Ed Weber"), list(givenName = "Ed", surName = "Weber"))
  expect_equal(calcofi4db:::.split_person("Weber, Ed"), list(givenName = "Ed", surName = "Weber"))
  expect_equal(calcofi4db:::.split_person("Cher"), list(surName = "Cher"))
  expect_null(calcofi4db:::.split_person(""))
})

test_that("the storage type picks the EML measurementScale branch", {
  expect_equal(calcofi4db:::.eml_scale_of("DOUBLE"), "ratio")
  expect_equal(calcofi4db:::.eml_scale_of("UBIGINT"), "ratio")
  expect_equal(calcofi4db:::.eml_scale_of("TIMESTAMP"), "dateTime")
  expect_equal(calcofi4db:::.eml_scale_of("DATE"), "dateTime")
  expect_equal(calcofi4db:::.eml_scale_of("VARCHAR"), "nominal")
  expect_equal(calcofi4db:::.eml_scale_of("GEOMETRY('EPSG:4326')"), "nominal")
  expect_equal(calcofi4db:::.eml_number_type("BIGINT"), "integer")
  expect_equal(calcofi4db:::.eml_number_type("UBIGINT"), "whole")
  expect_equal(calcofi4db:::.eml_number_type("DOUBLE"), "real")
})

test_that("a unit becomes a standardUnit only on an exact match", {
  # the same rule the NERC / DwC id columns follow: `count/10m2` is a standardized
  # density no EML standard unit states, so it travels as the release's own string
  expect_equal(unname(calcofi4db:::EML_STANDARD_UNITS[["m"]]), "meter")
  expect_equal(unname(calcofi4db:::EML_STANDARD_UNITS[["decimal degrees"]]), "degree")
  expect_true(is.na(unname(calcofi4db:::EML_STANDARD_UNITS["count/10m2"])))
  expect_true(is.na(unname(calcofi4db:::EML_STANDARD_UNITS["count/1000m3"])))
})

test_that("eml_contact_address() is the documented CalCOFI role address (Decision 23)", {
  expect_equal(eml_contact_address(), "data@calcofi.io")
})

# the gear registry -------------------------------------------------------------------------

test_that("read_gear_registry() reads the registry and dataset_gear() filters by dataset", {
  g <- eml_fixture_gear()
  expect_equal(names(g), c("tow_type", "gear_name", "dwc_samplingProtocol", "nerc_l22", "datasets", "note"))
  expect_true(all(c("CB", "MT", "OBLIQUE") %in% g$tow_type))
  ich <- dataset_gear(g, "swfsc_ichthyo")
  expect_true("CB" %in% ich$tow_type)
  expect_false("OBLIQUE" %in% ich$tow_type)   # that code belongs to cdfw_dungeness-crab
  expect_equal(dataset_gear(g, "cdfw_dungeness-crab")$tow_type, "OBLIQUE")
  expect_equal(nrow(dataset_gear(g, "nobody_here")), 0L)
  bad <- withr::local_tempfile(fileext = ".csv")
  writeLines(c("tow_type,gear_name", "CB,bongo"), bad)
  expect_error(read_gear_registry(bad), "missing column")
})

# the field mapping (plan § D-8) --------------------------------------------------------------

test_that("build_eml() maps the record field by field, as § D-8 says", {
  docs <- eml_fixture_docs()
  d <- docs[["swfsc_ichthyo"]]
  ds <- d$dataset
  expect_equal(d$packageId, "swfsc_ichthyo.v2026.09.04")
  expect_equal(d$system, "calcofi.io")
  expect_equal(ds$title, "SWFSC Ichthyoplankton")             # <- dataset_name
  expect_equal(ds$shortName, "Ichthyoplankton")               # <- dataset_name_short
  expect_equal(ds$language, "eng")
  expect_equal(ds$pubDate, "2026-09-04")                      # <- release_date
  # alternateIdentifier: the dataset page (ichthyo has no DOI, so none is invented)
  expect_equal(as.character(ds$alternateIdentifier), "https://calcofi.io/datasets/swfsc_ichthyo/")
  # creator <- the sidecar's creators[]; EML's <creator> takes no <role>
  expect_equal(ds$creator[[1]]$individualName, list(givenName = "Ed", surName = "Weber"))
  expect_equal(ds$creator[[1]]$organizationName, "NOAA Southwest Fisheries Science Center")
  expect_null(ds$creator[[1]]$role)
  # keywordSet: the GCMD terms under their thesaurus, the category + variables free
  expect_equal(ds$keywordSet[[1]]$keywordThesaurus, "GCMD Science Keywords")
  expect_true(all(grepl("^EARTH SCIENCE", as.character(ds$keywordSet[[1]]$keyword))))
  expect_null(ds$keywordSet[[2]]$keywordThesaurus)
  expect_true("Fish Eggs & Larvae" %in% as.character(ds$keywordSet[[2]]$keyword))
  # coverage: geographic from the measured bbox, temporal from the observed years
  expect_equal(ds$coverage$geographicCoverage$boundingCoordinates$northBoundingCoordinate,
               eml_fixture_catalog()$datasets[[2]]$coverage$bbox$lat_max)
  expect_equal(ds$coverage$temporalCoverage$rangeOfDates$beginDate$calendarDate, "1951")
  expect_equal(ds$coverage$temporalCoverage$rangeOfDates$endDate$calendarDate, "2023")
  # taxonomic coverage: coverage.json's taxa, WoRMS-keyed
  tx <- ds$coverage$taxonomicCoverage$taxonomicClassification
  expect_gt(length(tx), 0)
  expect_true(all(vapply(tx, function(t) nzchar(t$taxonRankValue), logical(1))))
  expect_equal(tx[[1]]$taxonId$provider, "https://www.marinespecies.org")
  # methods: gear.csv's dwc_samplingProtocol sentences for this dataset's tow_types
  paras <- as.character(ds$methods$methodStep[[1]]$description$para)
  expect_true(any(grepl("bongo net", paras)))
  expect_true(any(grepl("Manta net", paras)))
  # distribution: the canonical dataset page
  expect_equal(ds$distribution$online$url$url, "https://calcofi.io/datasets/swfsc_ichthyo/")
  # dataTable[]: the record's tables, attributeList from metadata.json's columns{}
  expect_gt(length(ds$dataTable), 0)
  ent <- vapply(ds$dataTable, function(e) e$entityName, "")
  expect_true(all(c("sample", "obs") %in% ent))
  smp <- ds$dataTable[[match("sample", ent)]]
  expect_equal(smp$physical$dataFormat$externallyDefinedFormat$formatName, "Apache Parquet")
  expect_match(smp$physical$distribution$online$url$url, "^https://")
  a <- smp$attributeList$attribute
  nms <- vapply(a, function(x) x$attributeName, "")
  expect_true("sample_key" %in% nms)
  # the identifier survives the markdown pass intact
  expect_match(a[[match("sample_key", nms)]]$attributeDefinition, "dataset_key:sample_type:id", fixed = TRUE)
  lat <- a[[match("latitude", nms)]]
  expect_equal(lat$measurementScale$ratio$unit$standardUnit, "degree")
  expect_equal(lat$measurementScale$ratio$numericDomain$numberType, "real")
  expect_equal(a[[match("datetime", nms)]]$measurementScale$dateTime$formatString, "YYYY-MM-DDThh:mm:ss")
  expect_equal(a[[match("tow_type", nms)]]$measurementScale$nominal$nonNumericDomain$textDomain$definition,
               a[[match("tow_type", nms)]]$attributeDefinition)
  # additionalMetadata: the release citation and the dataset's own
  am <- d$additionalMetadata$metadata$calcofi
  expect_equal(am$datasetKey, "swfsc_ichthyo")
  expect_equal(am$release, "v2026.09.04")
  expect_match(am$releaseCitation, "CalCOFI")
  expect_match(am$datasetCitation, "Ichthyoplankton")
  expect_equal(am$sourceAccessed, "2026-08-25")
})

test_that("build_eml() omits what the record cannot supply and never invents it", {
  d <- eml_fixture_docs()[["swfsc_ichthyo"]]
  # swfsc_ichthyo has no licence (questions.csv Q11 is open) and no DOI
  expect_null(d$dataset$intellectualRights)
  expect_null(d$dataset$licensed)
  expect_false(any(grepl("doi.org", as.character(d$dataset$alternateIdentifier))))
  # and no acknowledgement / funding, so no <project>
  expect_null(d$dataset$project)
})

test_that("the CalCOFI role address is the contact of last resort, and only that", {
  # calcofi_dic's fixture sidecar carries no contact
  d <- eml_fixture_docs()[["calcofi_dic"]]
  expect_equal(d$dataset$contact$electronicMailAddress, eml_contact_address())
  expect_equal(attr(d, "eml_notes")$contact_source, "role")
  # swfsc_ichthyo's sidecar states one, so the fallback is not used
  i <- eml_fixture_docs()[["swfsc_ichthyo"]]
  expect_equal(attr(i, "eml_notes")$contact_source, "dataset")
})

test_that("the creator falls back record-first: creators[] -> pi_names -> the provider org", {
  d <- min_doc()
  expect_equal(attr(d, "eml_notes")$creator_source, "creators")
  expect_equal(d$dataset$creator[[1]]$userId, list(directory = "https://orcid.org/", userId = "0000-0002-1825-0097"))
  r <- min_record()
  r$attribution$creators <- list(); r$attribution$pi_names <- c("Todd Martz", "Aaron Mau")
  d2 <- build_eml(r, meta = min_meta(), release = min_release())
  expect_equal(attr(d2, "eml_notes")$creator_source, "pi_names")
  expect_equal(d2$dataset$creator[[2]]$individualName, list(givenName = "Aaron", surName = "Mau"))
  expect_equal(d2$dataset$creator[[1]]$organizationName, "ACME Institution")
  r$attribution$pi_names <- character()
  d3 <- build_eml(r, meta = min_meta(), release = min_release())
  expect_equal(attr(d3, "eml_notes")$creator_source, "provider")
  expect_equal(d3$dataset$creator[[1]], list(organizationName = "ACME Institution"))
})

test_that("build_eml() reads the year span off the measured temporal string when coverage has none", {
  # calcofi_phytoplankton and sio_pic-zooplankton carry `temporal` but no year_min/max
  r <- min_record()
  r$coverage$year_min <- NULL; r$coverage$year_max <- NULL
  r$coverage$temporal <- "1939-05 to 2024-04"
  d <- build_eml(r, meta = min_meta(), release = min_release())
  expect_equal(d$dataset$coverage$temporalCoverage$rangeOfDates$beginDate$calendarDate, "1939")
  expect_equal(d$dataset$coverage$temporalCoverage$rangeOfDates$endDate$calendarDate, "2024")
  r$coverage$temporal <- NULL
  expect_null(build_eml(r, meta = min_meta(), release = min_release())$dataset$coverage$temporalCoverage)
})

# validation ---------------------------------------------------------------------------------

test_that("the fixture datasets produce EML 2.2 that validates, with nothing blocking", {
  skip_if_not_installed("EML")
  docs <- eml_fixture_docs()
  expect_setequal(names(docs), c("swfsc_ichthyo", "calcofi_dic"))
  dir <- withr::local_tempdir()
  paths <- write_eml_files(docs, dir)
  expect_true(all(file.exists(paths)))
  expect_equal(basename(paths[["swfsc_ichthyo"]]), "swfsc_ichthyo.xml")
  expect_true(all(vapply(paths, function(p) isTRUE(as.logical(EML::eml_validate(p))), logical(1))))
  chk <- check_eml_catalog(docs, paths, eml_fixture_catalog())
  expect_setequal(names(chk), c("dataset_key", "finding", "level", "detail", "exempt", "question"))
  expect_false("invalid_eml" %in% chk$finding)
  expect_equal(nrow(chk[chk$level == "error" & !chk$exempt, ]), 0L)
  # ichthyo's missing licence is exempt only because questions.csv Q11 names it
  lic <- chk[chk$finding == "no_license", ]
  expect_equal(lic$dataset_key, "swfsc_ichthyo")
  expect_true(lic$exempt)
  expect_equal(lic$question, "Q11")
  expect_silent(assert_eml(chk, quiet = TRUE))
})

test_that("a synthetic clean record validates and reports a single ok row", {
  skip_if_not_installed("EML")
  r <- min_record(); r$coverage$realm <- "env"
  sc <- list(methods_md = "Widgets were counted under a microscope by the ACME laboratory.")
  d <- build_eml(r, sidecar = sc, meta = min_meta(), release = min_release())
  dir <- withr::local_tempdir()
  p <- write_eml_files(d, dir)
  expect_equal(basename(unname(p)), "acme_widgets.xml")
  expect_true(isTRUE(as.logical(EML::eml_validate(unname(p)))))
  chk <- check_eml(d, path = unname(p), record = r)
  expect_equal(chk$finding, "ok")
  expect_equal(chk$level, "ok")
  expect_false(chk$exempt)
})

test_that("check_eml() reports invalid_eml when the written document fails the schema", {
  skip_if_not_installed("EML")
  f <- withr::local_tempfile(fileext = ".xml")
  # a <creator> carrying a <role> — EML 2.2 allows role on associatedParty and
  # project/personnel only. This exact document was written by the first draft of
  # build_eml() and the schema is what caught it.
  writeLines(c(
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<eml:eml xmlns:eml="https://eml.ecoinformatics.org/eml-2.2.0"',
    '  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
    '  packageId="acme_widgets.v1" system="calcofi.io"',
    '  xsi:schemaLocation="https://eml.ecoinformatics.org/eml-2.2.0 https://eml.ecoinformatics.org/eml-2.2.0/eml.xsd">',
    '  <dataset><title>t</title>',
    '    <creator><organizationName>ACME</organizationName><role>creator</role></creator>',
    '    <contact><organizationName>ACME</organizationName></contact>',
    '  </dataset></eml:eml>'), f)
  chk <- check_eml(f)
  expect_true("invalid_eml" %in% chk$finding)
  expect_equal(chk$level[chk$finding == "invalid_eml"], "error")
  expect_equal(chk$dataset_key[1], sub("\\.xml$", "", basename(f)))
})

# one red test per finding ---------------------------------------------------------------------

test_that("eml_findings() levels are what the gate assumes", {
  f <- eml_findings()
  expect_equal(unname(f[c("invalid_eml", "no_title", "no_abstract", "no_creator", "no_pub_date",
                          "no_license", "no_geographic_coverage", "no_temporal_coverage", "no_data_table")]),
               rep("error", 9))
  expect_equal(unname(f[c("short_abstract", "creator_from_provider", "creator_no_organization",
                          "contact_role_address", "no_keywords", "no_methods",
                          "no_taxonomic_coverage", "undocumented_attributes", "custom_units")]),
               rep("warn", 9))
})

test_that("no_title: a record with no dataset_name", {
  expect_true("no_title" %in% findings_of(min_doc(dataset_name = NULL)))
})

test_that("no_abstract / short_abstract: an empty and an under-20-word description", {
  expect_true("no_abstract" %in% findings_of(min_doc(description_md = NULL)))
  f <- findings_of(min_doc(description_md = "Ichthyoplankton collected by bongo tow."))
  expect_true("short_abstract" %in% f)
  expect_false("no_abstract" %in% f)
})

test_that("no_creator: no creators[], no pi_names and no registered provider", {
  r <- min_record()
  r$attribution$creators <- list(); r$attribution$pi_names <- character()
  r$provider <- list(key = "acme", registered = FALSE)
  d <- build_eml(r, meta = min_meta(), release = min_release())
  expect_true("no_creator" %in% findings_of(d))
  expect_equal(attr(d, "eml_notes")$creator_source, "none")
})

test_that("creator_from_provider: the record names no person, so the provider org stands in", {
  r <- min_record()
  r$attribution$creators <- list(); r$attribution$pi_names <- character()
  expect_true("creator_from_provider" %in% findings_of(build_eml(r, meta = min_meta(), release = min_release())))
})

test_that("creator_no_organization: a creator with a name and no organization", {
  r <- min_record()
  r$attribution$creators <- list(list(name = "Ada Lovelace"))
  expect_true("creator_no_organization" %in% findings_of(build_eml(r, meta = min_meta(), release = min_release())))
})

test_that("contact_role_address: no contact and no creator email", {
  r <- min_record()
  r$attribution$contact <- NULL
  r$attribution$creators <- list(list(name = "Ada Lovelace", organization = "ACME Institution"))
  d <- build_eml(r, meta = min_meta(), release = min_release())
  expect_true("contact_role_address" %in% findings_of(d))
  expect_equal(d$dataset$contact$electronicMailAddress, eml_contact_address())
})

test_that("no_pub_date: a release with no release_date", {
  expect_true("no_pub_date" %in% findings_of(build_eml(min_record(), meta = min_meta(),
                                                       release = list(version = "v2026.09.05"))))
})

test_that("no_license: no licence on the record — and a licence question exempts it", {
  r <- min_record()
  r$attribution$license <- NULL; r$attribution$license_name <- NULL; r$attribution$license_url <- NULL
  d <- build_eml(r, meta = min_meta(), release = min_release())
  chk <- check_eml(d, record = r, validate = FALSE)
  expect_true("no_license" %in% chk$finding)
  expect_false(chk$exempt[chk$finding == "no_license"])
  r$status$questions_dataset <- list(list(label = "Q11", field = "license", status = "open"))
  chk2 <- check_eml(d, record = r, validate = FALSE)
  expect_true(chk2$exempt[chk2$finding == "no_license"])
  expect_equal(chk2$question[chk2$finding == "no_license"], "Q11")
  # a question naming a different field does not exempt it
  r$status$questions_dataset <- list(list(label = "Q05", field = "doi", status = "open"))
  expect_false(check_eml(d, record = r, validate = FALSE)$exempt[1])
})

test_that("no_keywords: a record with no keywords, category or variables", {
  r <- min_record()
  r$keywords <- character(); r$category <- list(registered = FALSE); r$coverage$variables <- character()
  expect_true("no_keywords" %in% findings_of(build_eml(r, meta = min_meta(), release = min_release())))
})

test_that("no_geographic_coverage: an incomplete bbox", {
  r <- min_record(); r$coverage$bbox$lat_max <- NULL
  expect_true("no_geographic_coverage" %in% findings_of(build_eml(r, meta = min_meta(), release = min_release())))
})

test_that("no_temporal_coverage: no year span and no temporal string", {
  r <- min_record(); r$coverage$year_min <- NULL; r$coverage$year_max <- NULL; r$coverage$temporal <- NULL
  expect_true("no_temporal_coverage" %in% findings_of(build_eml(r, meta = min_meta(), release = min_release())))
})

test_that("no_taxonomic_coverage: a bio dataset whose coverage.json names no taxa", {
  r <- min_record()
  d <- build_eml(r, meta = min_meta(), release = min_release())   # no coverage.json passed
  expect_true("no_taxonomic_coverage" %in% findings_of(d, record = r))
  r$coverage$realm <- "env"
  expect_false("no_taxonomic_coverage" %in% findings_of(d, record = r))
})

test_that("no_methods: no methods_md, no quality_control_md and no gear for the dataset", {
  expect_true("no_methods" %in% findings_of(min_doc()))
  d <- build_eml(min_record(), sidecar = list(methods_md = "Widgets were counted under a microscope."),
                 meta = min_meta(), release = min_release())
  expect_false("no_methods" %in% findings_of(d))
  expect_equal(as.character(d$dataset$methods$methodStep[[1]]$description$para),
               "Widgets were counted under a microscope.")
  # <sampling> needs BOTH halves; one alone is omitted rather than half-filled
  d2 <- build_eml(min_record(), sidecar = list(methods_md = "m", study_extent = "e"),
                  meta = min_meta(), release = min_release())
  expect_null(d2$dataset$methods$sampling)
  d3 <- build_eml(min_record(), sidecar = list(methods_md = "m", study_extent = "e", sampling_description = "s"),
                  meta = min_meta(), release = min_release())
  expect_equal(as.character(d3$dataset$methods$sampling$studyExtent$description$para), "e")
  expect_equal(as.character(d3$dataset$methods$sampling$samplingDescription$para), "s")
})

test_that("no_data_table: tables with no documented columns", {
  d <- build_eml(min_record(), meta = list(tables = list(), columns = list()), release = min_release())
  expect_true("no_data_table" %in% findings_of(d))
  expect_null(d$dataset$dataTable)
})

test_that("undocumented_attributes: a column metadata.json does not describe", {
  m <- min_meta()
  m$columns$obs.hex_id <- list(name_long = "Hex Id", data_type = "UBIGINT")
  d <- build_eml(min_record(), meta = m, release = min_release())
  expect_true("undocumented_attributes" %in% findings_of(d))
  expect_equal(attr(d, "eml_notes")$undocumented_attributes, 1L)
  a <- d$dataset$dataTable[[1]]$attributeList$attribute
  hx <- a[[match("hex_id", vapply(a, function(x) x$attributeName, ""))]]
  expect_equal(hx$attributeDefinition, "Hex Id")   # the long name, never a placeholder sentence
})

test_that("custom_units: a unit no EML standard unit states exactly", {
  m <- min_meta()
  m$columns$obs.density <- list(name_long = "Density", units = "count/10m2",
                                description_md = "Standardized density.", data_type = "DOUBLE")
  d <- build_eml(min_record(), meta = m, release = min_release())
  chk <- check_eml(d, validate = FALSE)
  expect_true("custom_units" %in% chk$finding)
  expect_match(chk$detail[chk$finding == "custom_units"], "count/10m2", fixed = TRUE)
  a <- d$dataset$dataTable[[1]]$attributeList$attribute
  dn <- a[[match("density", vapply(a, function(x) x$attributeName, ""))]]
  expect_equal(dn$measurementScale$ratio$unit$customUnit, "count/10m2")
  expect_null(dn$measurementScale$ratio$unit$standardUnit)
})

# the gate ------------------------------------------------------------------------------------

test_that("assert_eml() stops on a blocking finding and passes an exempt one", {
  d <- data.frame(dataset_key = "a_b", finding = "no_license", level = "error",
                  detail = "no intellectualRights", exempt = FALSE, question = NA_character_,
                  stringsAsFactors = FALSE)
  expect_error(assert_eml(d, quiet = TRUE), "1 blocking finding")
  d$exempt <- TRUE; d$question <- "Q11"
  expect_silent(assert_eml(d, quiet = TRUE))
  w <- data.frame(dataset_key = "a_b", finding = "short_abstract", level = "warn",
                  detail = "12 words", exempt = FALSE, question = NA_character_,
                  stringsAsFactors = FALSE)
  expect_silent(assert_eml(w, quiet = TRUE))
  expect_message(assert_eml(w), "1 warning")
})

# ERDDAP globals ------------------------------------------------------------------------------

test_that("erddap_globals() renders the same record ERDDAP's globals must agree with", {
  g <- erddap_globals(min_record())
  expect_equal(g[["title"]], "ACME Widgets")
  expect_equal(g[["institution"]], "ACME Institution")
  expect_equal(g[["creator_name"]], "Ada Lovelace")
  expect_equal(g[["creator_email"]], "ada@acme.example")
  expect_equal(g[["creator_type"]], "person")
  expect_equal(g[["infoUrl"]], "https://calcofi.io/datasets/acme_widgets/")
  expect_equal(g[["id"]], "acme_widgets")
  expect_equal(g[["naming_authority"]], "calcofi.io")
  expect_match(g[["license"]], "Creative Commons Attribution 4.0")
  expect_equal(g[["keywords_vocabulary"]], "GCMD Science Keywords")
  # nothing empty is emitted, and the role address is the email of last resort
  expect_true(all(nzchar(g)))
  r <- min_record()
  r$attribution$contact <- NULL
  r$attribution$creators <- list(list(name = "Ada Lovelace", organization = "ACME Institution"))
  expect_equal(erddap_globals(r)[["creator_email"]], eml_contact_address())
})
