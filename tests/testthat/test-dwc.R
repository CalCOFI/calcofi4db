# R/dwc.R — the generic Darwin Core Archive over the core.
#
# Every assertion is against `new_core_fixture()` (helper-fixtures.R), a stated
# synthetic core: one cruise -> site -> tow -> two nets, two taxa, three
# obs_attribute bins, five sample_measurement rows. No network: `scientificNameID`
# comes from the fixture's own `taxon` table, never from WoRMS.

test_that("dwc_event() projects sample's adjacency list, with the cruise as root", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  reg <- core_fixture_registries()
  ev <- dwc_event(con, "test_ds", gear = reg$gear, measurement_type = reg$measurement_type)

  # 4 sample rows + 1 cruise event
  expect_equal(nrow(ev), 5L)
  expect_equal(sort(ev$eventType), sort(c("cruise", "site", "tow", "net", "net")))
  expect_equal(sum(duplicated(ev$eventID)), 0L)

  cr <- ev[ev$eventType == "cruise", ]
  expect_equal(cr$eventID, "2020-01-NODC")
  expect_true(is.na(cr$parentEventID))
  expect_equal(cr$eventDate, "2020-01-01/2020-01-09")

  # the root sample is re-parented onto its cruise; nothing else moves
  site <- ev[ev$eventID == "test_ds:site:1", ]
  expect_equal(site$parentEventID, "2020-01-NODC")
  expect_equal(ev$parentEventID[ev$eventID == "test_ds:tow:1"], "test_ds:site:1")
  expect_equal(ev$parentEventID[ev$eventID == "test_ds:net:1"], "test_ds:tow:1")

  net1 <- ev[ev$eventID == "test_ds:net:1", ]
  expect_equal(net1$eventDate, "2020-01-02T10:00:00Z")
  expect_equal(net1$decimalLatitude, 32)
  expect_equal(net1$decimalLongitude, -120)
  expect_equal(net1$geodeticDatum, "WGS84")
  expect_equal(net1$minimumDepthInMeters, 0)
  expect_equal(net1$maximumDepthInMeters, 210)
  expect_equal(net1$locationID, "090.0 060.0")
  expect_equal(net1$datasetID, "test_ds")
  # samplingProtocol is the gear registry's sentence for tow_type CB, verbatim
  expect_equal(net1$samplingProtocol, "Oblique tow with a 71-cm bongo net.")
  # sampleSizeValue is the `volume_sampled` sample_measurement, its unit the registry's
  expect_equal(net1$sampleSizeValue, 100)
  expect_equal(net1$sampleSizeUnit, "m3")
  # a site has no time of its own and no gear: those stay empty, never guessed
  expect_true(is.na(site$eventDate))
  expect_true(is.na(site$samplingProtocol))
})

test_that("dwc_event(cruises = FALSE) leaves the root parentless", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  ev <- dwc_event(con, "test_ds", cruises = FALSE)
  expect_equal(nrow(ev), 4L)
  expect_false("cruise" %in% ev$eventType)
  expect_true(is.na(ev$parentEventID[ev$eventID == "test_ds:site:1"]))
})

test_that("dwc_event() fills no vocabulary id without a registry", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  ev <- dwc_event(con, "test_ds")
  # an all-NA column is dropped rather than shipped as a blank field
  expect_false("samplingProtocol" %in% names(ev))
  expect_false("sampleSizeUnit" %in% names(ev))
})

test_that("dwc_occurrence() maps obs_bio to Occurrence with the registries", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  reg <- core_fixture_registries()
  oc <- dwc_occurrence(con, "test_ds", life_stage = reg$life_stage,
                       measurement_type = reg$measurement_type)
  expect_equal(nrow(oc), 3L)
  expect_equal(sum(duplicated(oc$occurrenceID)), 0L)

  a <- oc[oc$eventID == "test_ds:net:1" & oc$scientificName == "Engraulis mordax", ]
  expect_equal(a$scientificNameID, "urn:lsid:marinespecies.org:taxname:1")
  expect_equal(a$taxonID, "worms:1")
  expect_equal(a$taxonRank, "Species")
  expect_equal(a$kingdom, "Animalia")
  expect_equal(a$family, "Engraulidae")
  expect_equal(a$vernacularName, "northern anchovy")
  expect_equal(a$lifeStage, "larva")
  expect_equal(a$basisOfRecord, "HumanObservation")
  expect_equal(a$occurrenceStatus, "present")
  # the D8 denominator is the organismQuantity; the raw tally survives as individualCount
  expect_equal(a$organismQuantity, 50)
  expect_equal(a$organismQuantityType, "individuals per 10 square metres of sea surface")
  expect_equal(a$individualCount, 10L)
})

test_that("occurrenceID is stable across calls and does not use obs_id", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  reg <- core_fixture_registries()
  a <- dwc_occurrence(con, "test_ds", life_stage = reg$life_stage)
  # re-number obs_id: an occurrence must keep its OBIS identity across releases
  DBI::dbExecute(con, "CREATE OR REPLACE TABLE obs_bio AS
    SELECT * REPLACE (obs_id + 1000 AS obs_id) FROM obs_bio")
  b <- dwc_occurrence(con, "test_ds", life_stage = reg$life_stage)
  expect_equal(sort(a$occurrenceID), sort(b$occurrenceID))
})

test_that("scientificNameID is empty for a taxon with no WoRMS id, never guessed", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "UPDATE taxon SET worms_id = NULL WHERE taxon_key = 'worms:2'")
  oc <- dwc_occurrence(con, "test_ds")
  b <- oc[oc$scientificName == "Sardinops sagax", ]
  expect_true(is.na(b$scientificNameID))
  expect_equal(b$taxonID, "worms:2")
})

test_that("a row with no taxon cannot be an Occurrence", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "UPDATE obs_bio SET taxon_key = NULL WHERE obs_id = 2")
  oc <- dwc_occurrence(con, "test_ds")
  expect_equal(nrow(oc), 2L)
  expect_false("Sardinops sagax" %in% oc$scientificName)
})

test_that("lifeStage follows the registry: label, verbatim substage, or remarks", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  reg <- core_fixture_registries()
  DBI::dbExecute(con, "UPDATE obs_bio SET life_stage = 'furcilia F1' WHERE obs_id = 1")
  DBI::dbExecute(con, "UPDATE obs_bio SET life_stage = 'invert'      WHERE obs_id = 2")
  DBI::dbExecute(con, "UPDATE obs_bio SET life_stage = 'nauplius'    WHERE obs_id = 3")
  oc <- dwc_occurrence(con, "test_ds", life_stage = reg$life_stage)
  # a substage S11 does not carve: the verbatim value IS the lifeStage
  expect_equal(oc$lifeStage[oc$eventID == "test_ds:net:1" &
                            oc$scientificName == "Engraulis mordax"], "furcilia F1")
  # `invert` is recorded as NOT a life stage: it goes to occurrenceRemarks
  inv <- oc[oc$scientificName == "Sardinops sagax", ]
  expect_true(is.na(inv$lifeStage))
  expect_match(inv$occurrenceRemarks, "not a life stage.*invert")
  # a value the registry has never seen is carried verbatim in remarks, not invented
  unk <- oc[oc$eventID == "test_ds:net:2", ]
  expect_true(is.na(unk$lifeStage))
  expect_match(unk$occurrenceRemarks, "verbatim life stage: nauplius")
})

# occurrenceStatus -----------------------------------------------------------------

test_that("a zeros-recorded dataset keeps its zeros as absent", {
  con <- new_core_fixture(zeros = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  expect_equal(dwc_absence_rule(con, "test_ds"), "zeros_recorded")
  oc <- dwc_occurrence(con, "test_ds")
  expect_equal(nrow(oc), 4L)
  expect_equal(as.integer(table(oc$occurrenceStatus)[c("absent", "present")]), c(1L, 3L))
  ab <- oc[oc$occurrenceStatus == "absent", ]
  expect_equal(ab$eventID, "test_ds:net:2")
  expect_equal(ab$scientificName, "Sardinops sagax")
})

test_that("a positive-only dataset derives NO absences by default", {
  con <- new_core_fixture(zeros = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  expect_equal(dwc_absence_rule(con, "test_ds"), "positive_only")
  oc <- dwc_occurrence(con, "test_ds")
  expect_equal(nrow(oc), 3L)
  expect_true(all(oc$occurrenceStatus == "present"))
})

test_that("absences = 'sample_root' derives them from the root, not from zero rows", {
  con <- new_core_fixture(zeros = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  reg <- core_fixture_registries()
  oc <- dwc_occurrence(con, "test_ds", life_stage = reg$life_stage,
                       absences = "sample_root")
  # vocabulary = 2 (worms:1 larva, worms:2 egg); 1 surveyed root; 2 positives on it
  # -> exactly 0 derived absences here, because both taxa were seen at that root
  expect_equal(nrow(oc), 3L)

  # add a second root that saw only worms:1: worms:2/egg is then a real absence there
  DBI::dbExecute(con, "INSERT INTO sample VALUES
    ('test_ds:site:2','site',NULL,'test_ds:site:2','test_ds','090.0 070.0',
     '2020-01-NODC',33.0,-121.0,NULL,NULL,NULL,NULL)")
  DBI::dbExecute(con, "INSERT INTO sample VALUES
    ('test_ds:net:3','net','test_ds:site:2','test_ds:site:2','test_ds','090.0 070.0',
     '2020-01-NODC',33.0,-121.0,TIMESTAMP '2020-01-03 10:00:00',0.0,210.0,'CB')")
  DBI::dbExecute(con, "INSERT INTO sample_root VALUES (2,'test_ds:site:2','test_ds','site')")
  DBI::dbExecute(con, "INSERT INTO obs_bio VALUES
    (5,'test_ds',2,'test_ds:net:3','worms:1','larva','abundance','count',2.0,NULL,10.0,20.0)")
  oc2 <- dwc_occurrence(con, "test_ds", life_stage = reg$life_stage,
                        absences = "sample_root")
  ab <- oc2[oc2$occurrenceStatus == "absent", ]
  expect_equal(nrow(ab), 1L)
  # an absence sits on the ROOT event: the release knows the root was sampled, not
  # which of its nets the missing taxon was looked for in
  expect_equal(ab$eventID, "test_ds:site:2")
  expect_equal(ab$scientificName, "Sardinops sagax")
  expect_equal(ab$lifeStage, "egg")
  expect_equal(ab$organismQuantity, 0)
  expect_equal(ab$individualCount, 0L)
})

test_that("deriving absences refuses to exceed max_absences", {
  con <- new_core_fixture(zeros = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  expect_error(dwc_occurrence(con, "test_ds", absences = "sample_root", max_absences = -1),
               "max_absences")
})

# eMoF ------------------------------------------------------------------------------

test_that("dwc_emof() carries the three grains with registry ids only", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  reg <- core_fixture_registries()
  oc <- dwc_occurrence(con, "test_ds", life_stage = reg$life_stage,
                       measurement_type = reg$measurement_type)
  mf <- dwc_emof(con, "test_ds", occurrence = oc, measurement_type = reg$measurement_type)

  # 3 sample_measurement rows (volume_sampled is the sampleSizeValue, not a repeat)
  # + 3 obs_attribute rows
  expect_equal(nrow(mf), 6L)
  expect_false("volume_sampled" %in% mf$measurementType)
  expect_equal(sum(duplicated(mf$measurementID)), 0L)

  shf <- mf[mf$measurementType == "std_haul_factor" & mf$eventID == "test_ds:net:1", ]
  expect_equal(shf$measurementValue, 5)
  expect_equal(shf$measurementUnit, "dimensionless")
  expect_equal(shf$measurementUnitID, "http://vocab.nerc.ac.uk/collection/P06/current/UUUU/")
  # the registry states no P01 concept for a CalCOFI standard haul factor: EMPTY
  expect_true(is.na(shf$measurementTypeID))
  expect_true(is.na(shf$occurrenceID))

  # a type WITH a physical unit: the bin_value is the value, the count a remark
  bl <- mf[mf$measurementType == "body_length", ]
  expect_equal(nrow(bl), 2L)
  expect_equal(sort(bl$measurementValue), c(12, 15))
  expect_true(all(bl$measurementUnit == "mm"))
  expect_true(all(bl$measurementTypeID ==
                  "http://vocab.nerc.ac.uk/collection/P01/current/OBSINDLX/"))
  expect_equal(bl$measurementRemarks[bl$measurementValue == 12], "3 individuals")
  # and it hangs off the occurrence it measures
  expect_equal(unique(bl$occurrenceID),
               oc$occurrenceID[oc$eventID == "test_ds:net:1" &
                               oc$scientificName == "Engraulis mordax"])

  # a type with NO unit is a categorical bin: the COUNT is the value, the label a remark
  st <- mf[mf$measurementType == "stage", ]
  expect_equal(nrow(st), 1L)
  expect_equal(st$measurementValue, 4)
  expect_equal(st$measurementUnit, "individuals")
  expect_equal(st$measurementUnitID, "http://vocab.nerc.ac.uk/collection/P06/current/UCNT/")
  expect_equal(st$measurementRemarks, "preflexion")
})

test_that("dwc_emof() fills no id without the registry", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  oc <- dwc_occurrence(con, "test_ds")
  mf <- dwc_emof(con, "test_ds", occurrence = oc)
  expect_false("measurementTypeID" %in% names(mf))
  expect_false("measurementUnitID" %in% names(mf))
})

# meta.xml --------------------------------------------------------------------------

test_that("dwc_meta_xml() maps every column and points the extensions at the core", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  reg <- core_fixture_registries()
  ev <- dwc_event(con, "test_ds", gear = reg$gear, measurement_type = reg$measurement_type)
  oc <- dwc_occurrence(con, "test_ds", life_stage = reg$life_stage,
                       measurement_type = reg$measurement_type)
  mf <- dwc_emof(con, "test_ds", occurrence = oc, measurement_type = reg$measurement_type)
  x <- dwc_meta_xml(list(event = ev, occurrence = oc, emof = mf))

  expect_match(x, 'rowType="http://rs.tdwg.org/dwc/terms/Event"', fixed = TRUE)
  expect_match(x, 'rowType="http://rs.tdwg.org/dwc/terms/Occurrence"', fixed = TRUE)
  expect_match(x, 'rowType="http://rs.iobis.org/obis/terms/ExtendedMeasurementOrFact"',
               fixed = TRUE)
  # the eMoF ids are OBIS terms, not TDWG ones
  expect_match(x, 'term="http://rs.iobis.org/obis/terms/measurementTypeID"', fixed = TRUE)
  expect_match(x, 'term="http://rs.iobis.org/obis/terms/measurementUnitID"', fixed = TRUE)

  d <- xml2::read_xml(x)
  ns <- c(d = "http://rs.tdwg.org/dwc/text/")
  core <- xml2::xml_find_first(d, "//d:core", ns)
  # <id> is eventID's index; every remaining column has a <field>
  expect_equal(xml2::xml_attr(xml2::xml_find_first(core, "d:id", ns), "index"),
               as.character(which(names(ev) == "eventID") - 1L))
  expect_equal(length(xml2::xml_find_all(core, "d:field", ns)), length(names(ev)))
  # an extension's <coreid> is eventID, and eventID gets no <field> of its own
  occ <- xml2::xml_find_all(d, "//d:extension", ns)[[1]]
  expect_equal(xml2::xml_attr(xml2::xml_find_first(occ, "d:coreid", ns), "index"),
               as.character(which(names(oc) == "eventID") - 1L))
  expect_equal(length(xml2::xml_find_all(occ, "d:field", ns)), length(names(oc)) - 1L)
})

test_that("dwc_meta_xml() errors on a column with no Darwin Core term", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  ev <- dwc_event(con, "test_ds")
  ev$notATerm <- "x"
  expect_error(dwc_meta_xml(list(event = ev)), "no Darwin Core term")
})

# the checks and the archive ----------------------------------------------------------

test_that("dwc_check() passes a well-formed archive and catches an orphan", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  reg <- core_fixture_registries()
  ev <- dwc_event(con, "test_ds", gear = reg$gear, measurement_type = reg$measurement_type)
  oc <- dwc_occurrence(con, "test_ds", life_stage = reg$life_stage,
                       measurement_type = reg$measurement_type)
  mf <- dwc_emof(con, "test_ds", occurrence = oc, measurement_type = reg$measurement_type)

  d <- dwc_check(ev, oc, mf, "test_ds")
  expect_false(any(d$level == "error"))
  expect_silent(assert_dwc(d, quiet = TRUE))

  oc$eventID[1] <- "test_ds:net:99"
  d2 <- dwc_check(ev, oc, mf, "test_ds")
  expect_true("orphan_occurrence" %in% d2$finding)
  expect_error(assert_dwc(d2, quiet = TRUE), "orphan_occurrence")
})

test_that("dwc_check() reports a missing WoRMS id as a warning, not an error", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "UPDATE taxon SET worms_id = NULL")
  ev <- dwc_event(con, "test_ds")
  oc <- dwc_occurrence(con, "test_ds")
  d <- dwc_check(ev, oc, NULL, "test_ds")
  r <- d[d$finding == "no_scientific_name_id", ]
  expect_equal(nrow(r), 1L)
  expect_equal(r$level, "warn")
  expect_equal(r$n, 3L)
})

test_that("dwc_datasets() measures the WoRMS filter and the absence rule", {
  con <- new_core_fixture(zeros = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  d <- dwc_datasets(con)
  expect_equal(d$dataset_key, "test_ds")
  expect_equal(d$n_obs, 4L)
  expect_equal(d$n_taxa, 2L)
  expect_equal(d$n_worms, 2L)
  expect_equal(d$n_no_worms, 0L)
  expect_equal(d$absence_rule, "zeros_recorded")

  # a dataset whose taxa resolve to nothing is not a candidate
  DBI::dbExecute(con, "UPDATE taxon SET worms_id = NULL")
  expect_equal(nrow(dwc_datasets(con)), 0L)
})

test_that("dwc_archive() writes the five files, a zip and a manifest", {
  skip_if_not_installed("zip")
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  reg <- core_fixture_registries()
  ev <- dwc_event(con, "test_ds", gear = reg$gear, measurement_type = reg$measurement_type)
  oc <- dwc_occurrence(con, "test_ds", life_stage = reg$life_stage,
                       measurement_type = reg$measurement_type)
  mf <- dwc_emof(con, "test_ds", occurrence = oc, measurement_type = reg$measurement_type)

  d <- withr::local_tempdir()
  eml <- file.path(d, "eml_src.xml")
  writeLines("<eml/>", eml)
  a <- dwc_archive(file.path(d, "test_ds"), ev, oc, mf, eml_path = eml,
                   dataset_key = "test_ds", version = "v2026.09.05")
  expect_true(file.exists(a$zip))
  expect_equal(basename(a$zip), "test_ds_v2026.09.05.zip")
  expect_equal(sort(zip::zip_list(a$zip)$filename),
               sort(c("event.csv", "occurrence.csv", "extendedMeasurementOrFact.csv",
                      "meta.xml", "eml.xml")))
  expect_equal(a$counts$event, nrow(ev))
  expect_equal(a$counts$occurrence, nrow(oc))
  expect_equal(a$counts$emof, nrow(mf))

  m <- jsonlite::fromJSON(a$manifest, simplifyVector = FALSE)
  expect_equal(m$dataset_key, "test_ds")
  expect_equal(m$version, "v2026.09.05")
  expect_equal(m$content_hash, a$content_hash)
  # nothing was uploaded, and the manifest says so rather than inventing a date
  expect_null(m$uploaded_utc)
  expect_null(m$ipt_resource)

  s <- dwc_manifest_status(a$manifest)
  expect_equal(s$status, "built, not uploaded")

  # the content hash is deterministic over the DATA: a re-run on unchanged rows
  # reproduces it, which is what tells a dataset page an upload is NOT due
  b <- dwc_archive(file.path(d, "test_ds2"), ev, oc, mf, eml_path = eml,
                   dataset_key = "test_ds", version = "v2026.09.05",
                   zip_path = file.path(d, "again.zip"))
  expect_equal(b$content_hash, a$content_hash)
})

test_that("a manifest whose uploaded bytes are these bytes reads as published", {
  d <- withr::local_tempdir()
  p <- file.path(d, "k_manifest.json")
  writeLines(jsonlite::toJSON(list(
    dataset_key = "k", version = "v2026.09.05", content_hash = "abc",
    uploaded_hash = "abc", uploaded_utc = "2026-09-01T00:00:00Z",
    ipt_resource = "calcofi_k", obis_dataset_id = "xyz"),
    auto_unbox = TRUE), p)
  expect_equal(dwc_manifest_status(p)$status, "published (v2026.09.05)")

  writeLines(jsonlite::toJSON(list(
    dataset_key = "k", version = "v2026.09.06", content_hash = "def",
    uploaded_hash = "abc", uploaded_utc = "2026-09-01T00:00:00Z"),
    auto_unbox = TRUE), p)
  expect_match(dwc_manifest_status(p)$status, "^stale")
})

test_that("the registries read with their real columns", {
  # the sibling checkout, whether the package sits at CalCOFI/calcofi4db or in a
  # worktree under CalCOFI/.worktrees/
  cand <- c(test_path("..", "..", "..", "workflows", "metadata"),
            test_path("..", "..", "..", "..", "workflows", "metadata"))
  md <- cand[dir.exists(cand)][1]
  skip_if(is.na(md), "workflows/metadata not beside the package")
  ls <- read_life_stage_registry(file.path(md, "life_stage.csv"))
  expect_true(all(DWC_LIFE_STAGE_COLS %in% names(ls)))
  # the two values recorded as NOT life stages carry neither a label nor a parent
  ns <- ls[ls$life_stage %in% c("damaged", "invert"), ]
  expect_equal(nrow(ns), 2L)
  expect_true(all(is.na(ns$dwc_lifeStage) & is.na(ns$life_stage_parent)))
})

test_that("dwc_event() closes a tree whose parent belongs to another dataset", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  # a foreign root, exactly the shape cdfw_dungeness-crab has: 306 examined
  # subsamples parented onto swfsc_ichthyo site occupations
  DBI::dbExecute(con, "INSERT INTO sample VALUES
    ('other_ds:site:9','site',NULL,'other_ds:site:9','other_ds','099.0 099.0',
     '2020-01-NODC',31.0,-119.0,TIMESTAMP '2020-01-04 09:00:00',NULL,NULL,NULL)")
  DBI::dbExecute(con, "UPDATE sample SET parent_sample_key = 'other_ds:site:9'
                       WHERE sample_key = 'test_ds:site:1'")

  ev <- dwc_event(con, "test_ds", cruises = FALSE)
  expect_true("other_ds:site:9" %in% ev$eventID)
  # the ancestor keeps ITS datasetID — the archive never claims an event it did
  # not collect
  expect_equal(ev$datasetID[ev$eventID == "other_ds:site:9"], "other_ds")
  expect_equal(ev$datasetID[ev$eventID == "test_ds:site:1"], "test_ds")
  # and nothing is an orphan any more
  oc <- dwc_occurrence(con, "test_ds")
  expect_false("orphan_event" %in% dwc_check(ev, oc, NULL, "test_ds")$finding)

  ev2 <- dwc_event(con, "test_ds", cruises = FALSE, close_tree = FALSE)
  expect_false("other_ds:site:9" %in% ev2$eventID)
  expect_true("orphan_event" %in%
              dwc_check(ev2, dwc_occurrence(con, "test_ds"), NULL, "test_ds")$finding)
})

test_that("some records missing a required field is a warning; all of them is an error", {
  skip_if_not_installed("obistools")
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  reg <- core_fixture_registries()
  ev <- dwc_event(con, "test_ds", gear = reg$gear, measurement_type = reg$measurement_type)
  oc <- dwc_occurrence(con, "test_ds", life_stage = reg$life_stage,
                       measurement_type = reg$measurement_type)
  expect_false(any(dwc_check(ev, oc, NULL, "test_ds")$level == "error"))

  # one taxon loses its WoRMS id: 1 of 3 records will not index at OBIS (OBIS
  # requires scientificNameID), the other two will, so the archive is still written.
  # Coordinates would be a poor test of the same thing — a leaf event INHERITS them
  # from its parent, which is exactly what .dwc_flatten() exists to model.
  DBI::dbExecute(con, "UPDATE taxon SET worms_id = NULL WHERE taxon_key = 'worms:2'")
  oc1 <- dwc_occurrence(con, "test_ds", life_stage = reg$life_stage,
                        measurement_type = reg$measurement_type)
  d1 <- dwc_check(ev, oc1, NULL, "test_ds")
  expect_true("incomplete_records" %in% d1$finding)
  expect_equal(d1$level[d1$finding == "incomplete_records"], "warn")
  expect_false(any(d1$level == "error"))

  # every event loses its date: nothing would index, so the archive is refused
  DBI::dbExecute(con, "UPDATE sample SET datetime = NULL")
  DBI::dbExecute(con, "UPDATE cruise SET date_min = NULL, date_max = NULL")
  ev2 <- dwc_event(con, "test_ds", gear = reg$gear)
  d2 <- dwc_check(ev2, oc, NULL, "test_ds")
  expect_true("missing_required_field" %in% d2$finding)
  expect_error(assert_dwc(d2, quiet = TRUE), "missing_required_field")
})

test_that("a dataset-local taxon key is dropped and counted, never shipped unnamed", {
  con <- new_core_fixture()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  # `cce-lter_zooscan:13`, `calcofi_phytoplankton:232` and friends are the real
  # shape: non-taxonomic classes that check_taxon_ids() allows by name and that
  # have no scientific name to publish
  DBI::dbExecute(con, "UPDATE obs_bio SET taxon_key = 'test_ds:13' WHERE obs_id = 2")
  oc <- dwc_occurrence(con, "test_ds")
  expect_equal(nrow(oc), 2L)
  expect_equal(attr(oc, "dwc_dropped"), 1L)
  d <- dwc_check(dwc_event(con, "test_ds"), oc, NULL, "test_ds")
  r <- d[d$finding == "dropped_no_taxon", ]
  expect_equal(r$n, 1L)
  expect_equal(r$level, "warn")
})
