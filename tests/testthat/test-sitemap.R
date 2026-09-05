# The ODIS sitemap, generated from the record (plan 2026-09-05 § D-10, WS-M1).
# The rule with teeth: a `superseded` or `retired` external record stays in the
# registry and never reaches the sitemap. Everything here runs over a synthetic
# record — no network, no release.

smap_record <- function(...) utils::modifyList(list(
  schema_version = "1.0",
  release = list(version = "v2026.09.05", release_date = "2026-09-05"),
  datasets = list(
    list(dataset_key = "swfsc_ichthyo", dataset_name = "SWFSC Ichthyoplankton", visibility = "public",
         links = list(page = "https://calcofi.io/datasets/swfsc_ichthyo/"),
         distributions = list(
           list(kind = "download", url = "https://storage.googleapis.com/calcofi-db/x.parquet", status = "current"),
           list(kind = "service",  url = "https://erddap.calcofi.io/erddap/tabledap/swfsc_ichthyo.html",
                status = "current", portal = "erddap-calcofi", title = "ERDDAP"),
           list(kind = "archive",  url = "https://obis.org/dataset/0e223f55", status = "current",
                portal = "obis", title = "OBIS"),
           list(kind = "service",  url = "https://erddap.calcofi.io/erddap/tabledap/calcofi_casts.html",
                status = "superseded", portal = "erddap-calcofi", title = "legacy id"),
           list(kind = "mirror",   url = "https://coastwatch.pfeg.noaa.gov/erddap/tabledap/erdCalCOFItows.html",
                status = "retired", portal = "erddap-noaa", title = "retired mirror"),
           list(kind = "notebook", url = "https://calcofi.io/workflows/ingest_swfsc_ichthyo.html", status = "current"))),
    list(dataset_key = "calcofi_secret", dataset_name = "Internal", visibility = "internal",
         links = list(page = "https://calcofi.io/datasets/calcofi_secret/"),
         distributions = list(list(kind = "archive", url = "https://example.org/secret", status = "current")))),
  holdings = list(
    list(dataset_key = "calcofi_prodo", dataset_name = "PRODO", visibility = "public",
         links = list(page = "https://calcofi.io/datasets/calcofi_prodo/"),
         distributions = list(list(kind = "source", portal = "edi", title = "source",
                                   url = "https://portal.edirepository.org/nis/mapbrowse?packageid=knb-lter-cce.78.3",
                                   status = "external"))))), list(...))

test_that("the sitemap is the pages first, then every current/external record", {
  d <- build_datasets_sitemap(smap_record())
  expect_equal(names(d), SITEMAP_COLS)
  # two public pages (one dataset + one holding); the internal dataset is absent
  expect_equal(d$loc[d$kind == "page"],
               c("https://calcofi.io/datasets/swfsc_ichthyo/", "https://calcofi.io/datasets/calcofi_prodo/"))
  expect_false(any(grepl("calcofi_secret", d$loc)))
  expect_true(all(which(d$kind == "page") < min(which(d$kind != "page"))))
  # the external records that are records: ERDDAP, OBIS, the holding's EDI package
  expect_true("https://obis.org/dataset/0e223f55" %in% d$loc)
  expect_true("https://erddap.calcofi.io/erddap/tabledap/swfsc_ichthyo.html" %in% d$loc)
  expect_true(any(grepl("knb-lter-cce.78.3", d$loc)))
  # a parquet object and the ingest notebook are not dataset records
  expect_false(any(grepl("\\.parquet$", d$loc)))
  expect_false(any(grepl("/workflows/", d$loc)))
  # nor is the ERDDAP ISO 19115 XML: a sitemap lists pages, not files
  rec <- smap_record()
  rec$datasets[[1]]$distributions <- c(rec$datasets[[1]]$distributions, list(list(
    kind = "service", portal = "erddap-calcofi", status = "current", title = "ISO 19115",
    url = "https://erddap.calcofi.io/erddap/metadata/iso19115/xml/swfsc_ichthyo_iso19115.xml")))
  expect_false(any(grepl("iso19115", build_datasets_sitemap(rec)$loc)))
  expect_equal(unique(d$changefreq), "weekly")
  expect_equal(unique(d$lastmod[d$kind == "page"]), "2026-09-05")
})

test_that("a calcofi.org record page is an external record, not one of ours", {
  rec <- smap_record()
  rec$datasets[[1]]$distributions <- c(rec$datasets[[1]]$distributions, list(list(
    kind = "page", portal = "calcofi.org", title = "calcofi.org tile", status = "external",
    url = "https://calcofi.org/data/marine-ecosystem-data/fish-eggs-larvae/")))
  d <- build_datasets_sitemap(rec)
  expect_true("https://calcofi.org/data/marine-ecosystem-data/fish-eggs-larvae/" %in% d$loc)
  # `kind` alone would call it a page; the portal is what says whose page it is
  expect_equal(sum(d$kind == "page"), 3)
  expect_equal(sum(d$kind == "page" & d$portal == "calcofi.io"), 2)
  expect_equal(nrow(check_sitemap(d, network = FALSE)), 0)
})

test_that("a superseded or retired distribution is never listed", {
  d <- build_datasets_sitemap(smap_record())
  expect_false("https://erddap.calcofi.io/erddap/tabledap/calcofi_casts.html" %in% d$loc)
  expect_false("https://coastwatch.pfeg.noaa.gov/erddap/tabledap/erdCalCOFItows.html" %in% d$loc)
  # and the record still carries them — the sitemap filters, it does not delete
  rec <- smap_record()
  expect_true("superseded" %in% vapply(rec$datasets[[1]]$distributions, function(x) x$status, ""))
})

test_that("an observation supplies lastmod, and a retired observation drops the row", {
  obs <- tibble::tibble(
    dataset_key = c("swfsc_ichthyo", "swfsc_ichthyo"),
    url = c("https://obis.org/dataset/0e223f55", "https://erddap.calcofi.io/erddap/tabledap/swfsc_ichthyo.html"),
    status = c("live", "retired"), observed_utc = c("2026-09-05T00:00:00Z", "2026-09-05T00:00:00Z"),
    updated = c("2026-04-15T10:00:00", NA_character_), revision = NA_character_)
  d <- build_datasets_sitemap(smap_record(), observed = obs)
  expect_equal(d$lastmod[d$loc == "https://obis.org/dataset/0e223f55"], "2026-04-15")
  expect_false("https://erddap.calcofi.io/erddap/tabledap/swfsc_ichthyo.html" %in% d$loc)
  # no `updated` from the portal: the observation date is the next best answer
  obs2 <- obs[1, ]; obs2$updated <- NA_character_
  expect_equal(build_datasets_sitemap(smap_record(), observed = obs2)$lastmod[
    build_datasets_sitemap(smap_record(), observed = obs2)$loc == "https://obis.org/dataset/0e223f55"], "2026-09-05")
})

test_that("a sidecar edited after the release wins the page's lastmod", {
  d <- build_datasets_sitemap(smap_record(), edited = c(swfsc_ichthyo = "2026-09-30", calcofi_prodo = "2020-01-01"))
  expect_equal(d$lastmod[d$loc == "https://calcofi.io/datasets/swfsc_ichthyo/"], "2026-09-30")
  expect_equal(d$lastmod[d$loc == "https://calcofi.io/datasets/calcofi_prodo/"], "2026-09-05")
})

test_that("holdings can be left out, and every loc is unique", {
  d <- build_datasets_sitemap(smap_record(), include_holdings = FALSE)
  expect_false(any(grepl("calcofi_prodo", d$loc)))
  # the same URL under two datasets is listed once
  rec <- smap_record()
  rec$datasets[[2]]$visibility <- "public"
  rec$datasets[[2]]$distributions <- list(list(kind = "archive", url = "https://obis.org/dataset/0e223f55",
                                               status = "current", portal = "obis", title = "same record"))
  d2 <- build_datasets_sitemap(rec)
  expect_equal(sum(d2$loc == "https://obis.org/dataset/0e223f55"), 1)
})

test_that("write_sitemap_xml() writes valid sitemaps.org 0.9 and escapes what XML needs escaped", {
  d <- build_datasets_sitemap(smap_record())
  p <- withr::local_tempfile(fileext = ".xml")
  write_sitemap_xml(d, p)
  x <- readLines(p)
  expect_equal(x[1], '<?xml version="1.0" encoding="UTF-8"?>')
  expect_match(x[2], 'xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"')
  expect_equal(sum(grepl("<loc>", x)), nrow(d))
  expect_equal(tail(x, 1), "</urlset>")
  if (requireNamespace("xml2", quietly = TRUE)) {
    doc <- xml2::read_xml(p)
    expect_equal(xml2::xml_name(doc), "urlset")
    expect_equal(length(xml2::xml_find_all(doc, "//d1:url", xml2::xml_ns(doc))), nrow(d))
  }
  # an ampersand in a query string must not break the document
  d2 <- d[1, ]; d2$loc <- "https://portal.edirepository.org/nis/mapbrowse?scope=knb-lter-cce&identifier=78"
  write_sitemap_xml(d2, p)
  expect_match(paste(readLines(p), collapse = ""), "scope=knb-lter-cce&amp;identifier=78", fixed = TRUE)
  if (requireNamespace("xml2", quietly = TRUE)) expect_s3_class(xml2::read_xml(p), "xml_document")
})

test_that("check_sitemap() catches http, duplicates, a bad lastmod and dead URLs", {
  d <- build_datasets_sitemap(smap_record())
  ok <- check_sitemap(d, network = FALSE)
  expect_equal(nrow(ok[ok$level == "error", ]), 0)
  expect_silent(assert_sitemap(ok, quiet = TRUE))

  bad <- d
  bad$loc[2] <- "http://calcofi.io/datasets/calcofi_prodo/"
  bad$lastmod[3] <- "last week"
  bad <- dplyr::bind_rows(bad, bad[3, ])
  f <- check_sitemap(bad, network = FALSE)
  expect_true(all(c("not_https", "bad_lastmod", "duplicate_loc") %in% f$finding))
  expect_error(assert_sitemap(f, quiet = TRUE), "sitemap is invalid")

  # the pages must lead
  shuffled <- d[c(3, 1, 2, seq_len(nrow(d))[-(1:3)]), ]
  expect_true("pages_not_first" %in% check_sitemap(shuffled, network = FALSE)$finding)

  # the network half: 404 fails, a 503 only warns
  probe <- function(u) if (grepl("obis", u)) 404L else if (grepl("prodo", u)) 503L else 200L
  n <- check_sitemap(d, network = TRUE, probe = probe)
  expect_equal(n$finding[n$level == "error"], "url_dead")
  expect_true("url_unreachable" %in% n$finding[n$level == "warn"])
  expect_error(assert_sitemap(n, quiet = TRUE), "sitemap is invalid")

  # `allow_dead` forgives exactly the named URLs and nothing else: the calcofi.io
  # dataset pages 404 until the landing repo generates them (measured 2026-09-05)
  pages_404 <- check_sitemap(d, network = TRUE, probe = function(u) if (grepl("^https://calcofi\\.io/", u)) 404L else 200L)
  expect_equal(sum(pages_404$finding == "url_dead"), 2)
  expect_silent(assert_sitemap(pages_404, quiet = TRUE, allow_dead = "^https://calcofi\\.io/datasets/"))
  expect_error(assert_sitemap(n, quiet = TRUE, allow_dead = "^https://calcofi\\.io/datasets/"), "sitemap is invalid")
})
