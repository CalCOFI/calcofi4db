# The weekly observation of the external copies (plan 2026-09-05 § D-10/D-11,
# WS-M1). Every observer is exercised over an INJECTED fetch holding saved
# responses — the suite never touches a portal — and the two rules that matter
# have their own red test: a row is never deleted (an unanswered portal is
# `unreachable`, not `retired`), and a change is reported rather than applied.

dfx <- function(...) testthat::test_path("fixtures", "catalog", ...)

# a fetcher over saved responses, keyed by a substring of the URL
obs_fetch <- function(map, status_default = 404L) function(url, accept = NULL, method = "GET", ...) {
  for (pat in names(map)) if (grepl(pat, url, fixed = TRUE)) {
    v <- map[[pat]]
    if (is.numeric(v)) return(list(status = as.integer(v), content = "", url = url))
    return(list(status = 200L, content = v, url = url))
  }
  list(status = status_default, content = "", url = url)
}

reg_row <- function(...) {
  d <- list(dataset_key = "a_b", kind = "archive", portal = "edi", id = "", url = "", title = "",
            status = "external", superseded_by = "", observed_utc = "", notes = "")
  tibble::as_tibble(utils::modifyList(d, list(...)))
}

portals_fixture <- function() read_portal_registry(dfx("metadata", "portal.csv"))

# parsing ----------------------------------------------------------------------------

test_that(".edi_package_of() reads both EDI URL shapes and the registry id", {
  expect_equal(.edi_package_of("https://portal.edirepository.org/nis/mapbrowse?packageid=edi.109.4"),
               list(scope = "edi", id = "109", rev = 4L))
  expect_equal(.edi_package_of("https://portal.edirepository.org/nis/mapbrowse?scope=knb-lter-cce&identifier=78&revision=3"),
               list(scope = "knb-lter-cce", id = "78", rev = 3L))
  expect_equal(.edi_package_of("https://portal.edirepository.org/nis/mapbrowse?scope=knb-lter-cce&identifier=159")$rev,
               NA_integer_)
  expect_equal(.edi_package_of("https://example.org/x", "knb-lter-cce.255.3"),
               list(scope = "knb-lter-cce", id = "255", rev = 3L))
  expect_null(.edi_package_of("https://obis.org/dataset/abc"))
})

test_that(".erddap_das_url() finds the .das behind any ERDDAP page shape", {
  expect_equal(.erddap_das_url("https://erddap.calcofi.io/erddap/tabledap/calcofi_dic.html"),
               "https://erddap.calcofi.io/erddap/tabledap/calcofi_dic.das")
  expect_equal(.erddap_das_url("https://erddap.calcofi.io/erddap/info/calcofi_dic/index.html"),
               "https://erddap.calcofi.io/erddap/tabledap/calcofi_dic.das")
  expect_equal(.erddap_das_url("https://coastwatch.pfeg.noaa.gov/erddap/tabledap/erdCalCOFItows.graph"),
               "https://coastwatch.pfeg.noaa.gov/erddap/tabledap/erdCalCOFItows.das")
})

# the observers ----------------------------------------------------------------------

test_that("edi-pasta reports the newest revision, and says superseded when ours is older", {
  # the cite service answers per revision (PASTA's listing is 403 anonymously);
  # revisions are contiguous, so 1..3 answering and 4 not means 3 is newest
  f <- obs_fetch(c("knb-lter-cce.183.1" = "cite", "knb-lter-cce.183.2" = "cite", "knb-lter-cce.183.3" = "cite"))
  r <- .observe_edi(list(url = "https://portal.edirepository.org/nis/mapbrowse?packageid=knb-lter-cce.183.2", id = ""), f)
  expect_equal(r$status, "superseded")
  expect_equal(r$revision, "knb-lter-cce.183.3")
  expect_match(r$note, "newest revision is knb-lter-cce.183.3")
  r2 <- .observe_edi(list(url = "https://portal.edirepository.org/nis/mapbrowse?packageid=knb-lter-cce.183.3", id = ""), f)
  expect_equal(r2$status, "live")
  expect_true(is.na(r2$note))
})

test_that("an EDI rate limit is `unreachable`, never `retired` — the row survives", {
  r <- .observe_edi(list(url = "https://portal.edirepository.org/nis/mapbrowse?packageid=edi.109.4", id = ""),
                    obs_fetch(list(), status_default = 429L))
  expect_equal(r$status, "unreachable")
  expect_match(r$note, "150 requests")
  expect_false(r$status %in% "retired")
})

test_that("obis-api reads `updated` off the dataset id, never a text search", {
  j <- '{"results":[{"id":"0e223f55-c826-4513-ae9a-b04cbf2e189c","updated":"2026-04-15T10:00:00","records":463655,"title":"CalCOFI Fish Larvae & Egg Tows"}]}'
  r <- .observe_obis(list(id = "0e223f55-c826-4513-ae9a-b04cbf2e189c", url = "https://obis.org/dataset/0e223f55-c826-4513-ae9a-b04cbf2e189c"),
                     obs_fetch(c("api.obis.org/v3/dataset/0e223f55" = j)))
  expect_equal(r$status, "live")
  expect_equal(r$updated, "2026-04-15T10:00:00")
  expect_match(r$note, "463655")
  # OBIS answering with an empty result set means the dataset is gone
  r0 <- .observe_obis(list(id = "x", url = "https://obis.org/dataset/x"),
                      obs_fetch(c("api.obis.org" = '{"results":[]}')))
  expect_equal(r0$status, "retired")
})

test_that("erddap-das reads date_modified and time_coverage_end, and a 404 retires the id", {
  das <- 'Attributes {\n s {\n }\n NC_GLOBAL {\n String date_modified "2026-09-04T00:00:00Z";\n String time_coverage_end "2023-01-31T00:00:00Z";\n }\n}'
  r <- .observe_erddap(list(url = "https://erddap.calcofi.io/erddap/tabledap/calcofi_dic.html"),
                       obs_fetch(c("calcofi_dic.das" = das)))
  expect_equal(r$status, "live")
  expect_equal(r$updated, "2026-09-04T00:00:00Z")
  expect_match(r$note, "time_coverage_end 2023-01-31")
  r404 <- .observe_erddap(list(url = "https://erddap.calcofi.io/erddap/tabledap/gone.html"), obs_fetch(list()))
  expect_equal(r404$status, "retired")
  # a 503 is CoastWatch under load, not a retirement
  r503 <- .observe_erddap(list(url = "https://coastwatch.pfeg.noaa.gov/erddap/tabledap/erdCalCOFItows.html"),
                          obs_fetch(list(), status_default = 503L))
  expect_equal(r503$status, "unreachable")
})

test_that("doi, zenodo-api, ncbi-esummary and http each answer in their own idiom", {
  f_doi <- function(url, accept = NULL, method = "GET", ...)
    list(status = if (grepl("10.25921/3w9f-jd72", url)) 302L else 404L, content = "", url = url)
  expect_equal(.observe_doi(list(id = "10.25921/3w9f-jd72", url = "https://doi.org/10.25921/3w9f-jd72"), f_doi)$status, "live")
  expect_equal(.observe_doi(list(id = "10.9999/nope", url = "https://doi.org/10.9999/nope"), f_doi)$status, "retired")
  # a DOI that is only in the URL is still found
  expect_equal(.observe_doi(list(id = "", url = "https://doi.org/10.25921/3w9f-jd72"), f_doi)$status, "live")

  z <- '{"updated":"2026-09-05T12:00:00+00:00","metadata":{"version":"v2026.09.05"}}'
  rz <- .observe_zenodo(list(id = "22310858", url = "https://zenodo.org/records/22310858"),
                        obs_fetch(c("zenodo.org/api/records/22310858" = z)))
  expect_equal(rz$revision, "v2026.09.05"); expect_equal(rz$status, "live")

  n <- '{"result":{"555783":{"project_title":"NCOG 16S","registration_date":"2019-05-01"}}}'
  rn <- .observe_ncbi(list(id = "555783", url = "https://www.ncbi.nlm.nih.gov/bioproject/555783"),
                      obs_fetch(c("esummary.fcgi" = n)))
  expect_equal(rn$updated, "2019-05-01"); expect_match(rn$note, "NCOG")

  expect_equal(.observe_http(list(url = "https://x.example/ok"), obs_fetch(c("ok" = "hi")))$status, "live")
  expect_equal(.observe_http(list(url = "https://x.example/gone"), obs_fetch(list(), status_default = 410L))$status, "retired")
  expect_equal(.observe_http(list(url = "https://x.example/down"), obs_fetch(list(), status_default = NA))$status, "unreachable")
})

# the run ----------------------------------------------------------------------------

test_that("observe_distributions() dispatches by portal.csv's observe_method and keeps every row", {
  reg <- read_distribution_registry(dfx("metadata", "distribution.csv"))
  f <- obs_fetch(c(
    "api.obis.org" = '{"results":[{"updated":"2026-04-15T10:00:00","records":1,"title":"t"}]}',
    "cite.edirepository.org" = "cite",
    ".das" = 'NC_GLOBAL {\n String date_modified "2026-09-04";\n }'), status_default = 200L)
  o <- observe_distributions(reg, portals_fixture(), fetch = f, quiet = TRUE)
  expect_equal(nrow(o), nrow(reg))                        # nothing is dropped, ever
  expect_equal(names(o), OBSERVED_COLS)
  expect_true(all(o$status %in% observation_statuses()))
  expect_true(all(o$method %in% observe_methods()))
  expect_equal(o$method[o$portal == "obis"][1], "obis-api")
  expect_equal(o$method[o$portal == "edi"][1], "edi-pasta")
  expect_equal(o$method[o$portal == "erddap-noaa"][1], "erddap-das")
  expect_match(o$observed_utc[1], "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$")
  # the registry's own status travels beside the observation, never overwritten by it
  expect_true(all(o$registry_status %in% distribution_statuses()))
  expect_true("superseded" %in% o$registry_status)        # the legacy ERDDAP ids
})

test_that("an unknown portal or observe_method `none` never errors the run", {
  reg <- reg_row(portal = "datazoo", url = "https://oceaninformatics.ucsd.edu/datazoo/catalogs/ccelter/datasets/255")
  p <- tibble::tibble(portal = "datazoo", observe_method = "none")
  expect_equal(observe_distributions(reg, p, fetch = obs_fetch(list()), quiet = TRUE)$status, "skipped")
  # no portal registry at all: everything falls back to a plain liveness check
  o <- observe_distributions(reg, NULL, fetch = obs_fetch(c("datazoo" = "ok")), quiet = TRUE)
  expect_equal(o$method, "http"); expect_equal(o$status, "live")
  # an observer that throws is caught and reported, never fatal
  boom <- function(url, ...) stop("kaboom")
  expect_equal(observe_distributions(reg, NULL, fetch = boom, quiet = TRUE)$status, "unreachable")
})

test_that("distribution_targets() adds the holdings' source links and DOIs, deduplicated", {
  reg <- read_distribution_registry(dfx("metadata", "distribution.csv"))
  side <- list(
    cce_holding = list(status = "external", link_data_source = "https://portal.edirepository.org/nis/mapbrowse?packageid=knb-lter-cce.78.3",
                       doi = "10.6073/pasta/7f8e5d24e9b27ae695295a8ddc0809d1"),
    swfsc_ichthyo = list(status = "published", link_data_source = "https://example.org/not-a-holding"))
  t <- distribution_targets(reg, side)
  expect_true(nrow(t) > nrow(reg))
  expect_setequal(t$dataset_key[t$notes %in% "holding (plan D-11)"], c("cce_holding", "cce_holding"))
  expect_true("https://doi.org/10.6073/pasta/7f8e5d24e9b27ae695295a8ddc0809d1" %in% t$url)
  expect_false("https://example.org/not-a-holding" %in% t$url)   # an ingested dataset is not a holding
  expect_equal(nrow(distribution_targets(reg, NULL)), nrow(reg))
})

# the file ---------------------------------------------------------------------------

test_that("the observed file round-trips and reports what moved", {
  reg <- read_distribution_registry(dfx("metadata", "distribution.csv"))[1:3, ]
  f <- obs_fetch(c("api.obis.org" = '{"results":[{"updated":"2026-04-15T10:00:00","records":1,"title":"t"}]}'),
                 status_default = 200L)
  o <- observe_distributions(reg, portals_fixture(), fetch = f, quiet = TRUE)
  p <- withr::local_tempfile(fileext = ".json")
  write_distribution_observed(o, p)
  back <- read_distribution_observed(p)
  expect_equal(nrow(back), nrow(o))
  expect_setequal(back$url, o$url)
  j <- jsonlite::fromJSON(p, simplifyVector = FALSE)
  expect_equal(j$counts$rows, nrow(o))
  expect_equal(j$changes, list())
  expect_null(read_distribution_observed(withr::local_tempfile(fileext = ".json")))

  # a moved `updated` is a change; an outage is not
  prev <- back; prev$updated[prev$url == o$url[1]] <- "2020-01-01T00:00:00"
  ch <- distribution_changes(o, prev)
  expect_true(nrow(ch) >= 1)
  expect_true(all(ch$field %in% c("status", "revision", "updated")))
  expect_equal(ch$was[ch$url == o$url[1] & ch$field == "updated"], "2020-01-01T00:00:00")
  prev2 <- back; prev2$status <- "unreachable"
  expect_equal(nrow(distribution_changes(o, prev2)), 0)   # last week's outage is not this week's news
  expect_equal(nrow(distribution_changes(o, NULL)), 0)
})

test_that("a `retired` observation never removes the registry row", {
  reg <- reg_row(portal = "erddap-calcofi", id = "calcofi_dic_old", status = "superseded",
                 url = "https://erddap.calcofi.io/erddap/tabledap/calcofi_dic_old.html")
  p <- tibble::tibble(portal = "erddap-calcofi", observe_method = "erddap-das")
  o <- observe_distributions(reg, p, fetch = obs_fetch(list(), status_default = 404L), quiet = TRUE)
  expect_equal(o$status, "retired")
  expect_equal(nrow(o), 1)                                 # observed, not deleted
  expect_equal(o$registry_status, "superseded")
  f <- withr::local_tempfile(fileext = ".json")
  write_distribution_observed(o, f)
  expect_equal(read_distribution_observed(f)$url, reg$url)
})
