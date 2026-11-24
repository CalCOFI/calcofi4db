# schema 1.2: the `display` block, `browse`, and `attribution.associated_parties`. The words were
# typed in the site's _data/dataset_extras.yml until 2026-11-24; each now has one home in
# workflows (notebook, sidecar or registry) and the record carries it.

cfx <- function(...) testthat::test_path("fixtures", "catalog", ...)
display_record <- function(reg = read_catalog_registries(cfx("metadata")))
  build_dataset_catalog(cfx("metadata.json"), cfx("coverage.json"), cfx("catalog.json"), reg)
ds_of <- function(rec, key) Filter(function(d) d$dataset_key == key, rec$datasets)[[1]]

test_that("the key lists carry the display keys, each on one side of the split", {
  expect_true(all(c("dataset_name_plain", "collection_method", "browse_group") %in% dataset_meta_structural_keys()))
  expect_true(all(c("description_short", "description_lede", "notes_md", "collection", "analysis",
                    "analysis_org", "collection_status", "references") %in% dataset_meta_descriptive_keys()))
  expect_length(intersect(dataset_meta_structural_keys(), dataset_meta_descriptive_keys()), 0)
  expect_equal(collection_statuses(), c("ongoing", "ended"))
})

test_that("read_notebook_display() reads the three notebook keys by dataset_key", {
  nb <- read_notebook_display(cfx())
  expect_equal(names(nb), "swfsc_ichthyo")
  expect_equal(nb$swfsc_ichthyo, list(dataset_name_plain = "Fish Egg and Larva Counts",
                                      collection_method = "net", browse_group = "fish-eggs-larvae"))
  expect_equal(read_notebook_display(withr::local_tempdir()), list())
})

test_that("display joins notebook, sidecar and registries for a dataset that declares them", {
  d <- ds_of(display_record(), "swfsc_ichthyo")$display
  expect_equal(d$name_plain, "Fish Egg and Larva Counts")
  expect_equal(d$collection_method, list(key = "net", label = "Net tow"))
  expect_equal(d$browse_group, "fish-eggs-larvae")
  expect_equal(d$description_short, "Fish eggs and larvae from net tows.")
  expect_null(d$description_lede)
  expect_equal(d$notes_md, "A tow with nothing in it is a true zero.")
  expect_equal(d$collection, "Bongo and manta net tows on CalCOFI cruises")
  expect_equal(d$analysis, "Visual microscopy"); expect_equal(d$analysis_org, "CalCOFI, NOAA SWFSC")
  expect_equal(d$collection_status, "ongoing")
  expect_equal(d$references, list(list(label = "Methods", url = "https://example.org/methods")))
  expect_equal(d$column_notes, list(tow_type = "Net type of the tow, for example CB (bongo)."))
})

test_that("a dataset that declares nothing gets a display block of nulls, never a guess", {
  d <- ds_of(display_record(), "calcofi_dic")$display
  for (k in c("name_plain", "description_short", "description_lede", "notes_md", "collection", "analysis",
              "analysis_org", "collection_method", "collection_status", "browse_group", "column_notes"))
    expect_null(d[[k]], info = k)
  expect_equal(d$references, list())
})

test_that("associated_parties reach the record's attribution", {
  rec <- display_record()
  expect_equal(ds_of(rec, "swfsc_ichthyo")$attribution$associated_parties,
               list(list(name = "Noelle Bowlin", organization = "NOAA SWFSC", email = NULL, role = "data provider")))
  expect_equal(ds_of(rec, "calcofi_dic")$attribution$associated_parties, list())
})

test_that("browse lists realms and groups in registry order", {
  b <- display_record()$browse
  expect_equal(vapply(b$realms, `[[`, "", "realm"), c("env", "bio"))
  expect_equal(vapply(b$groups, `[[`, "", "key"), c("ctd-bottle", "fish-eggs-larvae"))
  expect_equal(b$groups[[1]]$icon, "cat-physical"); expect_null(b$groups[[2]]$icon)
  expect_equal(b$groups[[2]]$line, "Eggs and larvae from net tows")
})

test_that("an unregistered method or group, or an unknown status, stops the read", {
  md <- withr::local_tempdir(); file.copy(cfx(), md, recursive = TRUE); root <- file.path(md, "catalog")
  qmd <- file.path(root, "ingest_swfsc_ichthyo.qmd"); orig <- readLines(qmd)
  writeLines(sub("collection_method: net", "collection_method: sledge", orig), qmd)
  expect_error(read_catalog_registries(file.path(root, "metadata")), "collection_method `sledge`")
  writeLines(sub("browse_group: fish-eggs-larvae", "browse_group: nowhere", orig), qmd)
  expect_error(read_catalog_registries(file.path(root, "metadata")), "browse_group `nowhere`")
  writeLines(orig, qmd)
  sc <- file.path(root, "metadata", "swfsc", "ichthyo", "dataset_meta.yml")
  writeLines(sub("collection_status: ongoing", "collection_status: archived", readLines(sc)), sc)
  expect_error(read_catalog_registries(file.path(root, "metadata")), "collection_status `archived`")
})

test_that("without the display registries the record still builds, with browse null", {
  md <- withr::local_tempdir(); file.copy(cfx(), md, recursive = TRUE); root <- file.path(md, "catalog")
  unlink(file.path(root, "metadata", c("collection_method.csv", "browse_group.csv", "browse_realm.csv")))
  rec <- display_record(read_catalog_registries(file.path(root, "metadata")))
  expect_null(rec$browse)
  expect_equal(ds_of(rec, "swfsc_ichthyo")$display$collection_method, list(key = "net", label = NULL))
})

test_that("the schema-1.2 record validates against its schema", {
  skip_if_not_installed("jsonvalidate")
  dir <- withr::local_tempdir(); write_dataset_catalog(display_record(), dir)
  expect_true(isTRUE(validate_dataset_catalog(file.path(dir, "datasets.json"))))
})
