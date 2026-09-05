# the ODIS sitemap, generated from the record --------------------------------------------
#
# `datasets/sitemap.xml` used to list 78 external dataset pages fetched from a
# hand-maintained Google Sheet of repositories, with `lastmod` = the run date.
# Since the plan of 2026-09-05 (§ D-10) it is a GENERATED VIEW of `datasets.json`:
# the calcofi.io dataset pages first — they are canonical, and the JSON-LD on each
# carries the external records as `sameAs` — then every `current` / `external`
# record of a CalCOFI dataset at another portal. A `superseded` or `retired`
# distribution is kept in the registry and kept OUT of the sitemap: pointing a
# crawler at a legacy ERDDAP id is how a retired copy outlives its successor.

SITEMAP_COLS <- c("loc", "lastmod", "changefreq", "kind", "dataset_key", "portal", "title")

# the distribution kinds that are a *record page* somewhere, and so belong in a
# sitemap: a parquet object or a netCDF file is a download, not a page to crawl
SITEMAP_KINDS <- c("service", "mirror", "source", "archive", "page")

#' Build the rows of `datasets/sitemap.xml` from the record
#'
#' @param record `datasets.json`: a path/URL or the parsed list
#' @param observed the tibble from [read_distribution_observed()], or NULL — its
#'   `updated` / `observed_utc` date is an external record's `lastmod`, and a row
#'   observed `retired` is dropped even when the registry still calls it current
#' @param edited a named character vector `dataset_key -> ISO date` of the
#'   descriptive sidecars' last edit; a page's `lastmod` is the later of that and
#'   the release date
#' @param include_holdings list the holdings' pages too (default TRUE — a holding
#'   has a page at the same URL shape, plan § D-11)
#' @param changefreq the `changefreq` written for every URL
#' @return A [tibble][tibble::tibble]: `loc`, `lastmod`, `changefreq`, `kind`
#'   (`page` for a calcofi.io dataset page, else the distribution's kind),
#'   `dataset_key`, `portal`, `title`. Pages first, in record order, then the
#'   external records; every `loc` unique.
#' @export
#' @concept catalog
build_datasets_sitemap <- function(record, observed = NULL, edited = NULL,
                                   include_holdings = TRUE, changefreq = "weekly") {
  record <- .read_json(record)
  rel    <- record[["release"]] %||% list()
  rdate  <- .s(rel[["release_date"]])
  obs_key <- if (is.null(observed) || !nrow(observed)) character() else paste(observed$dataset_key, observed$url)

  recs <- c(.rows(record[["datasets"]]),
            if (include_holdings) .rows(record[["holdings"]]) else list())
  pages <- list(); external <- list()
  for (r in recs) {
    if (!identical(.s(r[["visibility"]] %||% "public"), "public")) next
    key  <- .s(r[["dataset_key"]])
    page <- .s((r[["links"]] %||% list())[["page"]])
    ed   <- if (is.null(edited)) "" else .s(edited[[key]] %||% "")
    if (nzchar(page)) pages[[length(pages) + 1]] <- tibble::tibble(
      loc = page, lastmod = max(c(rdate, ed)[nzchar(c(rdate, ed))], ""),
      changefreq = changefreq, kind = "page", dataset_key = key, portal = "calcofi.io",
      title = .s(r[["dataset_name"]]))
    for (d in .rows(r[["distributions"]])) {
      url <- .s(d[["url"]])
      if (!nzchar(url) || !.s(d[["kind"]]) %in% SITEMAP_KINDS) next
      if (!.s(d[["status"]]) %in% c("current", "external")) next     # never superseded/retired
      lastmod <- rdate; note <- NA_character_
      i <- match(paste(key, url), obs_key)
      if (!is.na(i)) {
        if (identical(.s(observed$status[i]), "retired")) next       # the authority stopped answering
        up <- .s(observed$updated[i])
        d8 <- substr(if (grepl("^\\d{4}-\\d{2}-\\d{2}", up)) up else .s(observed$observed_utc[i]), 1, 10)
        if (grepl("^\\d{4}-\\d{2}-\\d{2}$", d8)) lastmod <- d8
      }
      external[[length(external) + 1]] <- tibble::tibble(
        loc = url, lastmod = lastmod, changefreq = changefreq, kind = .s(d[["kind"]]),
        dataset_key = key, portal = .s(d[["portal"]]), title = .s(d[["title"]]))
    }
  }
  out <- dplyr::bind_rows(c(pages, external))
  if (!nrow(out)) return(tibble::as_tibble(stats::setNames(
    replicate(length(SITEMAP_COLS), character(), simplify = FALSE), SITEMAP_COLS)))
  out[!duplicated(out$loc), SITEMAP_COLS]
}

#' Write a sitemap XML file (sitemaps.org 0.9)
#'
#' @param d the tibble from [build_datasets_sitemap()]
#' @param path the file to write
#' @return `path`, invisibly.
#' @export
#' @concept catalog
write_sitemap_xml <- function(d, path) {
  esc <- function(x) {
    x <- gsub("&", "&amp;", x, fixed = TRUE)
    x <- gsub("<", "&lt;", x, fixed = TRUE); x <- gsub(">", "&gt;", x, fixed = TRUE)
    gsub("'", "&apos;", gsub('"', "&quot;", x, fixed = TRUE), fixed = TRUE)
  }
  body <- sprintf("  <url>\n    <loc>%s</loc>\n    <lastmod>%s</lastmod>\n    <changefreq>%s</changefreq>\n  </url>",
                  esc(d$loc), d$lastmod, d$changefreq)
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  writeLines(c('<?xml version="1.0" encoding="UTF-8"?>',
               '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
               body, '</urlset>'), path, useBytes = TRUE)
  invisible(path)
}

#' The findings [check_sitemap()] can report
#' @return A named character vector, finding -> level.
#' @export
#' @concept catalog
sitemap_findings <- function() c(
  not_https      = "error",  # a `loc` that is not an absolute https URL
  duplicate_loc  = "error",  # the same URL twice
  bad_lastmod    = "error",  # a `lastmod` that is not W3C date
  no_pages       = "error",  # not one calcofi.io dataset page
  pages_not_first= "error",  # the pages must lead (they are the canonical records)
  url_dead       = "error",  # 404/410/451 from a ranged GET
  url_unreachable= "warn",   # no answer / 5xx — someone else's outage
  ok             = "ok")

#' Check a generated sitemap
#'
#' The structural half always runs (https, uniqueness, `lastmod` shape, the pages
#' leading); the network half asks every URL with a one-byte ranged GET, like the
#' index's link check, and is skipped when `CALCOFI_SKIP_LINK_CHECK` is set.
#'
#' @param d the tibble from [build_datasets_sitemap()]
#' @param network probe every URL (default: off when `CALCOFI_SKIP_LINK_CHECK`)
#' @param probe a function `url -> integer status` (the tests inject one)
#' @return A [tibble][tibble::tibble] `loc`, `finding`, `level`, `detail`.
#' @export
#' @concept catalog
check_sitemap <- function(d, network = !nzchar(Sys.getenv("CALCOFI_SKIP_LINK_CHECK")), probe = NULL) {
  out <- list()
  add <- function(loc, finding, detail = NA_character_) out[[length(out) + 1]] <<- tibble::tibble(
    loc = loc, finding = finding, level = unname(sitemap_findings()[finding]), detail = detail)
  for (i in seq_len(nrow(d))) {
    if (!grepl("^https://", d$loc[i])) add(d$loc[i], "not_https")
    if (!grepl("^\\d{4}-\\d{2}-\\d{2}(T.*)?$", d$lastmod[i])) add(d$loc[i], "bad_lastmod", d$lastmod[i])
  }
  dup <- unique(d$loc[duplicated(d$loc)])
  for (u in dup) add(u, "duplicate_loc")
  # the pages are a PREFIX of the file: they are the canonical records
  is_page <- d$kind == "page"
  if (!any(is_page)) add(NA_character_, "no_pages")
  else if (any(!is_page) && max(which(is_page)) > min(which(!is_page)))
    add(NA_character_, "pages_not_first", "a calcofi.io dataset page follows an external record")
  if (network) {
    if (is.null(probe)) probe <- function(u) .http_probe(u)
    for (u in unique(d$loc)) {
      st <- probe(u)
      if (!is.na(st) && st %in% c(404L, 410L, 451L)) add(u, "url_dead", paste("HTTP", st))
      else if (is.na(st) || st >= 500L) add(u, "url_unreachable", if (is.na(st)) "no answer" else paste("HTTP", st))
    }
  }
  if (!length(out)) return(tibble::tibble(loc = character(), finding = "ok", level = "ok", detail = character())[0, ])
  dplyr::bind_rows(out)
}

#' Stop when [check_sitemap()] found an error
#'
#' @param d the tibble from [check_sitemap()]
#' @param quiet suppress the summary line
#' @return `d`, invisibly.
#' @export
#' @concept catalog
assert_sitemap <- function(d, quiet = FALSE) {
  bad <- d[d$level == "error", , drop = FALSE]
  if (!quiet) message(sprintf("sitemap: %d error · %d warn", nrow(bad), sum(d$level == "warn")))
  if (nrow(bad))
    stop("sitemap is invalid:\n", paste(sprintf("  %s: %s", bad$loc, bad$finding), collapse = "\n"),
         call. = FALSE)
  invisible(d)
}
