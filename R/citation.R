# the attribution contract ------------------------------------------------------
#
# Every dataset in a release carries a citation that was CHECKED, a license from
# a REGISTRY, a MEASURED `source_accessed`, and the release cites itself. Until
# 2026-09-03 nothing validated any of it: 8 of 16 `citation_main` were empty,
# 3 licenses were the free text "CC BY 4.0", and `build_workflows_index.R`
# checked link shape and liveness only. The pieces here mirror that link check:
# a structural half that always runs (a year, a locator, a registered license)
# and a network half behind `CALCOFI_SKIP_LINK_CHECK` that asks the source's own
# authority (EDI's cite service, an ERDDAP `.das`, an NCEI landing page,
# DataCite) and reports DRIFT as a finding — never by writing into a notebook's
# YAML. The author's string is the record; the authority is a proposal.

#' @keywords internal
CC_ZENODO_CONCEPT_DOI <- "10.5281/zenodo.22281994"

#' @keywords internal
CC_RELEASE_PUBLISHER <- paste(
  "Scripps Institution of Oceanography, NOAA Fisheries, and",
  "California Department of Fish and Wildlife")

#' @keywords internal
CC_DB_SCHEMA_URL <- "https://calcofi.io/db-schema/"

#' @keywords internal
CC_RELEASES_HTTPS <- "https://storage.googleapis.com/calcofi-db/ducklake/releases"

LICENSE_COLS <- c("license", "name", "url", "status", "notes")

.s <- function(x) {
  if (is.null(x) || length(x) == 0) return("")
  x <- as.character(x)[1]
  if (is.na(x)) "" else trimws(x)
}

# one HTTP request via curl; never throws, a failure is status NA. `followlocation`
# is on for content (the DOI resolver redirects doi.org -> data.crosscite.org) and
# OFF for a HEAD, so that a DOI's own answer (30x = resolves, 404 = not minted) is
# what gets reported rather than the landing page's.
.http_get <- function(url, accept = NULL, method = "GET", timeout = 30) {
  if (!requireNamespace("curl", quietly = TRUE))
    stop("Package 'curl' is required for the network half of check_dataset_citation()",
         call. = FALSE)
  h <- curl::new_handle(
    followlocation = !identical(method, "HEAD"), timeout = timeout, connecttimeout = 10,
    useragent = "calcofi4db citation check (https://calcofi.io)")
  if (!is.null(accept)) curl::handle_setheaders(h, Accept = accept)
  if (identical(method, "HEAD")) curl::handle_setopt(h, nobody = TRUE)
  r <- tryCatch(curl::curl_fetch_memory(url, handle = h), error = function(e) NULL)
  if (is.null(r)) return(list(status = NA_integer_, content = "", url = url))
  content <- if (identical(method, "HEAD") || !length(r$content)) "" else rawToChar(r$content)
  Encoding(content) <- "UTF-8"
  list(status = as.integer(r$status_code), content = content, url = r$url)
}

# license registry ---------------------------------------------------------------

#' The allowed `status` values of `metadata/license.csv`
#' @return Character vector.
#' @export
#' @concept registry
license_statuses <- function() c("active", "deprecated")

#' Read `metadata/license.csv`, the registry of dataset licenses
#'
#' One row per SPDX-style id an ingest's `dataset_meta.license` may carry
#' (`CC-BY-4.0`, `CC0-1.0`, `CC-BY-NC-4.0`, `CC-BY-SA-4.0`, `US-PD`, `custom`,
#' `unknown`, …) with `name`, `url`, `status` (`active` | `deprecated`) and
#' `notes`. Read strictly (`na = ""`) and validated like every other registry:
#' sentinel strings, an unknown status or a duplicate id are errors. `custom`
#' requires a `license_url` on the dataset; `unknown` (or an empty license)
#' fails the index unless a `questions.csv` row is open on it — see
#' [check_dataset_citation()].
#'
#' @param path path to `metadata/license.csv`
#' @param validate error on a malformed registry (default TRUE)
#' @return A [tibble][tibble::tibble], all columns character.
#' @export
#' @concept registry
#' @importFrom readr read_csv cols col_character
read_license_registry <- function(path, validate = TRUE) {
  stopifnot("license.csv not found" = file.exists(path))
  d <- readr::read_csv(path, na = "", show_col_types = FALSE,
                       col_types = readr::cols(.default = readr::col_character()))
  miss <- setdiff(LICENSE_COLS, names(d))
  if (length(miss))
    stop("license registry ", path, " is missing column(s): ", paste(miss, collapse = ", "),
         "\n  Expected: ", paste(LICENSE_COLS, collapse = ", "), call. = FALSE)
  if (isTRUE(validate)) {
    check_registry_na_strings(d, path)
    bad <- setdiff(stats::na.omit(unique(d$status)), license_statuses())
    if (length(bad))
      stop("unknown license status in ", path, ": ", paste(bad, collapse = ", "),
           "\n  Allowed: ", paste(license_statuses(), collapse = " | "), call. = FALSE)
    dup <- unique(d$license[duplicated(d$license)])
    if (length(dup))
      stop("duplicate license id(s) in ", path, ": ", paste(dup, collapse = ", "), call. = FALSE)
    if (any(is.na(d$license) | !nzchar(d$license)))
      stop("empty license id in ", path, call. = FALSE)
  }
  d
}

# findings -----------------------------------------------------------------------

#' The findings `check_dataset_citation()` can report, with their level
#'
#' `error` findings fail the workflows index and the release unless an
#' `open`/`proposed` `questions.csv` row on the dataset covers the field;
#' `warn` findings are reported and never block.
#'
#' @return A named character vector, finding -> `"ok"` | `"error"` | `"warn"`.
#' @export
#' @concept registry
citation_findings <- function() c(
  ok                    = "ok",
  missing_citation      = "error",   # citation_main is empty
  no_year               = "error",   # no 4-digit year in the string
  no_locator            = "error",   # no DOI, no URL in the string, no link_data_source
  missing_license       = "error",   # license empty or `unknown`
  license_unregistered  = "error",   # not an active id in metadata/license.csv
  license_custom_no_url = "error",   # `custom` with no license_url
  doi_unresolved        = "error",   # not a bare DOI, or doi.org does not answer 200/30x
  authority_drift       = "warn",    # the source's own citation or license differs
  authority_unavailable = "warn")    # a resolver exists but could not be fetched

#' @rdname citation_findings
#' @export
citation_error_findings <- function() {
  f <- citation_findings()
  names(f)[f == "error"]
}

# which dataset_meta field a finding is about — the field a questions.csv row must
# name (or leave empty) to exempt it
.citation_finding_field <- c(
  missing_citation = "citation_main", no_year = "citation_main", no_locator = "citation_main",
  missing_license = "license", license_unregistered = "license", license_custom_no_url = "license",
  doi_unresolved = "doi", authority_drift = "citation_main", authority_unavailable = "citation_main")

#' Normalize a citation string for comparison
#'
#' Lower-case, markup and HTML entities removed, then everything but letters and
#' digits dropped — so a trailing period, a re-flowed line, `<i>` around a title
#' or an upper-cased DOI (doi.org content negotiation returns
#' `10.25921/3W9F-JD72`) is not drift. Author-name abbreviation IS drift, on
#' purpose: `Keeling, C.D.` and `Keeling, Charles D.` are different strings.
#'
#' @param x character
#' @return character of the same length
#' @export
#' @concept registry
normalize_citation <- function(x) {
  x <- tolower(as.character(x))
  x <- gsub("<[^>]+>", "", x)
  x <- .unescape_html(x)
  gsub("[^a-z0-9]", "", x)
}

.unescape_html <- function(x) {
  from <- c("&amp;", "&lt;", "&gt;", "&quot;", "&#39;", "&apos;", "&nbsp;")
  to   <- c("&",     "<",    ">",    "\"",     "'",     "'",      " ")
  for (i in seq_along(from)) x <- gsub(from[i], to[i], x, fixed = TRUE)
  x
}

.bare_doi <- function(x) {
  x <- .s(x)
  if (!nzchar(x)) return(NA_character_)
  if (grepl("^10\\.[0-9]{4,9}/\\S+$", x)) x else NA_character_
}

.find_doi <- function(x) {
  m <- regmatches(x, regexpr("10\\.[0-9]{4,9}/[^\\s\"<>]+", x, perl = TRUE))
  if (!length(m)) return(NA_character_)
  sub("[.,;)]+$", "", m)
}

# resolver parsers (pure, on saved responses) --------------------------------------

#' Parse a resolver's response into the fields the citation cache carries
#'
#' Each parser takes the raw text of one response and returns a list; they run
#' on saved responses in the tests, so no network is needed to pin them.
#' * `parse_edi_cite()` — EDI's cite service
#'   (`https://cite.edirepository.org/cite/<scope>.<id>.<rev>?style=ESIP`):
#'   the citation verbatim, plus the DOI and year it contains.
#' * `parse_erddap_das()` — an ERDDAP `.das`: the `NC_GLOBAL` string attributes
#'   as a named list (`title`, `institution`, `creator_name`, `license`,
#'   `citation` when a dataset declares one, …), multi-line values joined.
#' * `parse_ncei_landing()` — an NCEI landing page: its "Cite as:" block with
#'   the `[indicate subset used]` / `Accessed [date]` placeholders removed, plus
#'   the DOI.
#' * `parse_datacite()` — `https://api.datacite.org/dois/<doi>`: DOI, title,
#'   creators, publisher, year, URL and the SPDX `license` from `rightsList`
#'   (upper-cased to the registry's form, `CC-BY-4.0`).
#' * `parse_doi_bibliography()` — doi.org content negotiation
#'   (`Accept: text/x-bibliography; style=apa`): the formatted citation with
#'   markup and entities stripped.
#'
#' @param x the response body, one string
#' @return A list (or, for `parse_doi_bibliography()`, one string).
#' @export
#' @concept registry
parse_edi_cite <- function(x) {
  cit <- trimws(gsub("\\s+", " ", paste(x, collapse = " ")))
  year <- regmatches(cit, regexpr("\\b(19|20)[0-9]{2}\\b", cit))
  list(citation = cit, doi = .find_doi(cit),
       year = if (length(year)) year else NA_character_)
}

#' @rdname parse_edi_cite
#' @export
parse_erddap_das <- function(x) {
  x <- paste(x, collapse = "\n")
  g <- regmatches(x, regexpr("(?s)NC_GLOBAL\\s*\\{.*?\\n\\s*\\}", x, perl = TRUE))
  if (!length(g)) return(list())
  m <- gregexpr('String\\s+([A-Za-z0-9_]+)\\s+"((?:[^"\\\\]|\\\\.)*)"\\s*;', g, perl = TRUE)[[1]]
  if (m[1] < 0) return(list())
  starts <- attr(m, "capture.start"); lens <- attr(m, "capture.length")
  out <- list()
  for (i in seq_along(m)) {
    key <- substr(g, starts[i, 1], starts[i, 1] + lens[i, 1] - 1)
    val <- substr(g, starts[i, 2], starts[i, 2] + lens[i, 2] - 1)
    out[[key]] <- trimws(gsub("\\s+", " ", gsub("\\\\\"", "\"", val)))
  }
  out
}

#' @rdname parse_edi_cite
#' @export
parse_ncei_landing <- function(x) {
  t <- paste(x, collapse = " ")
  t <- gsub("<[^>]+>", " ", t)
  t <- .unescape_html(t)
  t <- gsub("\\s+", " ", t)
  m <- regmatches(t, regexpr("Cite as:\\s*(.*?)\\s*Accessed \\[date\\]\\.?", t, perl = TRUE))
  if (!length(m)) return(list(citation = NA_character_, doi = .find_doi(t)))
  cit <- sub("^Cite as:\\s*", "", m)
  cit <- sub("\\s*Accessed \\[date\\]\\.?$", "", cit)
  cit <- gsub("\\s*\\[indicate subset used\\]\\.?", "", cit)
  cit <- trimws(gsub("\\s+", " ", cit))
  list(citation = cit, doi = .find_doi(cit))
}

#' @rdname parse_edi_cite
#' @export
parse_datacite <- function(x) {
  j <- jsonlite::fromJSON(paste(x, collapse = "\n"), simplifyVector = FALSE)
  a <- j$data$attributes
  if (is.null(a)) return(list(doi = NA_character_, license = NA_character_))
  rights <- vapply(a$rightsList %||% list(), function(r) .s(r$rightsIdentifier), "")
  rights <- rights[nzchar(rights)]
  creators <- vapply(a$creators %||% list(), function(cr) .s(cr$name), "")
  list(
    doi       = .s(a$doi),
    title     = if (length(a$titles)) .s(a$titles[[1]]$title) else NA_character_,
    creators  = creators[nzchar(creators)],
    publisher = if (nzchar(.s(a$publisher))) .s(a$publisher) else NA_character_,
    year      = if (!is.null(a$publicationYear)) as.character(a$publicationYear) else NA_character_,
    url       = if (nzchar(.s(a$url))) .s(a$url) else NA_character_,
    license   = if (length(rights)) toupper(rights[1]) else NA_character_)
}

#' @rdname parse_edi_cite
#' @export
parse_doi_bibliography <- function(x) {
  t <- paste(x, collapse = " ")
  t <- gsub("<[^>]+>", "", t)
  t <- .unescape_html(t)
  trimws(gsub("\\s+", " ", t))
}

# resolvers ------------------------------------------------------------------------

.edi_cite_url <- function(scope, id, rev)
  sprintf("https://cite.edirepository.org/cite/%s.%s.%s?style=ESIP", scope, id, rev)

# Which authority speaks for a dataset, from its link_data_source (and, failing
# that, its DOI). NULL when nothing does — calcofi.org pages, DataZoo, a private
# collection export — and the check stays structural.
.citation_resolver <- function(link, doi) {
  link <- .s(link)
  if (grepl("edirepository\\.org", link)) {
    m <- regmatches(link, regexec("packageid=([A-Za-z0-9-]+)\\.([0-9]+)\\.([0-9]+)", link))[[1]]
    if (length(m) == 4)
      return(list(authority = "edi", scope = m[2], id = m[3], rev = m[4],
                  url = .edi_cite_url(m[2], m[3], m[4])))
    m <- regmatches(link, regexec("scope=([A-Za-z0-9-]+)&identifier=([0-9]+)", link))[[1]]
    if (length(m) == 3)
      return(list(authority = "edi", scope = m[2], id = m[3], rev = NA_character_, url = NA_character_))
    return(NULL)
  }
  if (grepl("ncei\\.noaa\\.gov", link)) return(list(authority = "ncei", url = link))
  if (grepl("/erddap/tabledap/", link)) {
    base <- sub("\\.(html|das|dds|json|csv|htmlTable|graph)$", "", link)
    return(list(authority = "erddap", url = paste0(base, ".das")))
  }
  if (!is.na(doi)) return(list(authority = "datacite", url = sprintf("https://doi.org/%s", doi)))
  NULL
}

# EDI's PASTA revision listing (`/package/eml/<scope>/<id>`) answers 403 to public
# access (measured 2026-09-03), so the newest revision is found by asking the cite
# service for rev 1, 2, … until it stops answering 200. Revisions are contiguous.
.edi_newest_revision <- function(scope, id, fetch, max_rev = 50L) {
  newest <- NA_integer_
  for (rev in seq_len(max_rev)) {
    r <- fetch(.edi_cite_url(scope, id, rev))
    if (!identical(as.integer(r$status), 200L)) break
    newest <- rev
  }
  newest
}

.new_cache_entry <- function(authority, url, doi) list(
  authority = authority, url = url, citation = NA_character_, license = NA_character_,
  license_scheme = NA_character_, creator = NA_character_, title = NA_character_,
  checked = format(Sys.Date()), doi = doi, doi_status = NA_integer_)

.ok200 <- function(r) identical(as.integer(r$status), 200L)
.fail <- function(what, r) list(error = sprintf(
  "%s: %s", what, if (is.na(r$status)) "no answer (timeout/DNS)" else paste("HTTP", r$status)))

# Fetch what the authority says about a dataset. Returns a cache entry, or
# list(error = "…") when the resolver could not be reached — in which case nothing
# is cached and the next run retries.
.fetch_authority <- function(res, doi, fetch) {
  authority <- res$authority
  entry <- .new_cache_entry(authority, res$url, doi)
  lookup_doi <- doi
  if (authority == "edi") {
    rev <- res$rev
    if (is.na(rev)) {
      rev <- .edi_newest_revision(res$scope, res$id, fetch)
      if (is.na(rev)) return(list(error = sprintf(
        "edi: no revision of %s.%s answers the cite service", res$scope, res$id)))
    }
    entry$url <- .edi_cite_url(res$scope, res$id, rev)
    r <- fetch(entry$url)
    if (!.ok200(r)) return(.fail(paste("edi", entry$url), r))
    p <- parse_edi_cite(r$content)
    entry$citation <- p$citation
    if (is.na(lookup_doi)) lookup_doi <- p$doi
  } else if (authority == "ncei") {
    r <- fetch(res$url)
    if (!.ok200(r)) return(.fail(paste("ncei", res$url), r))
    p <- parse_ncei_landing(r$content)
    entry$citation <- p$citation
    if (is.na(lookup_doi)) lookup_doi <- p$doi
  } else if (authority == "erddap") {
    r <- fetch(res$url)
    if (!.ok200(r)) return(.fail(paste("erddap", res$url), r))
    p <- parse_erddap_das(r$content)
    entry$title   <- if (nzchar(.s(p$title))) p$title else NA_character_
    entry$creator <- if (nzchar(.s(p$creator_name))) p$creator_name else
      if (nzchar(.s(p$publisher_name))) p$publisher_name else NA_character_
    if (nzchar(.s(p$license))) { entry$license <- p$license; entry$license_scheme <- "text" }
    # only a dataset that declares a `citation` global has an authority string
    if (nzchar(.s(p$citation))) entry$citation <- p$citation
  } else if (authority == "datacite") {
    r <- fetch(res$url, accept = "text/x-bibliography; style=apa")
    if (.ok200(r) && nzchar(trimws(r$content))) entry$citation <- parse_doi_bibliography(r$content)
  }
  # any DOI (declared, or the authority's own) is also a DataCite record: its
  # rightsList is the one place a license arrives as an SPDX id
  if (!is.na(lookup_doi)) {
    api <- sprintf("https://api.datacite.org/dois/%s", lookup_doi)
    r <- fetch(api)
    if (.ok200(r)) {
      p <- tryCatch(parse_datacite(r$content), error = function(e) NULL)
      if (!is.null(p)) {
        if (!is.na(p$license)) { entry$license <- p$license; entry$license_scheme <- "spdx" }
        if (is.na(entry$creator) && length(p$creators))
          entry$creator <- paste(p$creators, collapse = "; ")
        if (is.na(entry$title) && !is.na(p$title)) entry$title <- p$title
      }
    } else if (authority == "datacite" && (is.na(r$status) || r$status >= 500L)) {
      return(.fail(paste("datacite", api), r))
    }
  }
  # the DECLARED doi must resolve: doi.org's own answer, redirects not followed
  if (!is.na(doi)) {
    r <- fetch(sprintf("https://doi.org/%s", doi), method = "HEAD")
    if (is.na(r$status)) return(.fail(sprintf("doi.org/%s", doi), r))
    entry$doi_status <- as.integer(r$status)
  }
  entry
}

.read_cache_entry <- function(path) {
  j <- tryCatch(jsonlite::fromJSON(path, simplifyVector = TRUE), error = function(e) NULL)
  if (is.null(j)) return(NULL)
  e <- .new_cache_entry(NA_character_, NA_character_, NA_character_)
  for (k in names(e)) if (!is.null(j[[k]]) && length(j[[k]]) && !is.na(j[[k]][1])) e[[k]] <- j[[k]][1]
  e$doi_status <- suppressWarnings(as.integer(e$doi_status))
  e
}

.write_cache_entry <- function(entry, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(entry, path, auto_unbox = TRUE, pretty = TRUE, null = "null", na = "null")
  invisible(path)
}

# the open/proposed rows of a dataset's questions.csv that name the dataset table
.dataset_questions <- function(path) {
  if (is.null(path) || !file.exists(path)) return(NULL)
  q <- read_questions(path)
  q <- q[q$status %in% c("open", "proposed") &
           !is.na(q$related_table) & tolower(trimws(q$related_table)) == "dataset", , drop = FALSE]
  if (!nrow(q)) return(NULL)
  q
}

# check_dataset_citation ---------------------------------------------------------------

#' Check every dataset's citation, license and DOI, structurally and against its authority
#'
#' One row per (dataset, finding); a clean dataset has a single `ok` row. The
#' structural half always runs: `citation_main` must be non-empty
#' (`missing_citation`), carry a 4-digit year (`no_year`) and a locator — a DOI,
#' a URL in the string, or a `link_data_source` (`no_locator`); `license` must be
#' an active id in `metadata/license.csv` (`missing_license` when empty or
#' `unknown`, `license_unregistered` otherwise), and `custom` needs a
#' `license_url` (`license_custom_no_url`); `doi` must be bare (`10.…/…`).
#'
#' The network half (`network = TRUE`, i.e. not `CALCOFI_SKIP_LINK_CHECK`) asks
#' the source's own authority, chosen by `link_data_source`: EDI's cite service
#' (`packageid=<scope>.<id>.<rev>`, or the newest revision when only
#' `scope`/`identifier` are given), an NCEI landing page ("Cite as"), an ERDDAP
#' `.das` (globals), and DataCite for any DOI (its `rightsList` SPDX id, and
#' doi.org content negotiation for the formatted citation). A declared `doi` must
#' answer 200/30x at doi.org (`doi_unresolved`). Every fetch is cached in
#' `{cache_dir}/{provider}/{dataset}/citation_authority.json` (`authority`, `url`,
#' `citation`, `license`, `creator`, `title`, `checked`, …) so a re-run costs
#' nothing; pass `refresh = TRUE` to fetch again. A cached authority is compared
#' even when `network = FALSE`. A fetched citation (or SPDX license) that differs
#' from the declared one after [normalize_citation()] is `authority_drift`, with
#' both strings in `detail`; a resolver that cannot be reached is
#' `authority_unavailable`. **Nothing is ever written into a notebook's YAML** —
#' the author's string is the record, the authority is a proposal.
#'
#' A finding is `exempt` when the dataset's `questions.csv`
#' (`{cache_dir}/{provider}/{dataset}/questions.csv`) holds an `open` or
#' `proposed` row with `related_table = dataset` whose `related_field` is empty or
#' names the field the finding is about (`citation_main`, `license`, `doi`);
#' `question` carries the row's label. [assert_dataset_citation()] stops on any
#' non-exempt `error`-level row.
#'
#' @param ingest_yaml named list from [read_ingest_yaml()]
#' @param network fetch from the authorities (default TRUE); FALSE runs the
#'   structural half and compares against whatever is already cached
#' @param cache_dir the `metadata/` root: holds `license.csv`, each dataset's
#'   `questions.csv` and its `citation_authority.json` cache. Defaults to
#'   `metadata/` beside the first ingest's `.qmd`.
#' @param license_csv path to the license registry (default
#'   `{cache_dir}/license.csv`)
#' @param refresh ignore the cache and fetch again (default FALSE)
#' @param fetch the HTTP function, `function(url, accept = NULL, method = "GET",
#'   …)` returning `list(status, content, url)`; the default uses curl. The tests
#'   inject one that serves saved responses.
#' @param timeout seconds per request
#' @return A [tibble][tibble::tibble]: `dataset_key`, `finding`, `detail`,
#'   `authority`, `authority_citation`, `checked`, plus `level`
#'   (`ok`/`error`/`warn`, see [citation_findings()]), `exempt` and `question`.
#' @export
#' @concept registry
#' @seealso [read_license_registry()], [assert_dataset_citation()],
#'   [release_citation()]
check_dataset_citation <- function(ingest_yaml, network = TRUE, cache_dir = NULL,
                                   license_csv = NULL, refresh = FALSE, fetch = NULL,
                                   timeout = 30) {
  if (is.null(cache_dir)) {
    qmd <- unlist(lapply(ingest_yaml, function(cc) cc$qmd))
    if (!length(qmd))
      stop("cache_dir is required when the ingest_yaml carries no `qmd` paths", call. = FALSE)
    cache_dir <- file.path(dirname(qmd[1]), "metadata")
  }
  if (is.null(license_csv)) license_csv <- file.path(cache_dir, "license.csv")
  reg <- read_license_registry(license_csv)
  active <- reg$license[reg$status == "active"]
  if (is.null(fetch)) fetch <- function(url, accept = NULL, method = "GET", ...)
    .http_get(url, accept = accept, method = method, timeout = timeout)

  df <- ingest_yaml_to_dataset_df(ingest_yaml)
  levels <- citation_findings()
  rows <- list()
  row <- function(key, finding, detail, authority = NA_character_,
                  authority_citation = NA_character_, checked = NA_character_,
                  field = .citation_finding_field[finding]) {
    tibble::tibble(
      dataset_key = key, finding = finding, level = unname(levels[finding]),
      detail = detail, authority = authority, authority_citation = authority_citation,
      checked = checked, field = if (is.null(field) || is.na(field)) NA_character_ else unname(field))
  }

  for (i in seq_len(nrow(df))) {
    prov <- df$provider[i]; dset <- df$dataset[i]
    key  <- paste0(prov, "_", dset)
    cit  <- .s(df$citation_main[i]); lic <- .s(df$license[i])
    lic_url <- .s(df$license_url[i]); doi_raw <- .s(df$doi[i])
    link <- .s(df$link_data_source[i])
    found <- list()

    # structural: the citation ----
    if (!nzchar(cit)) {
      found[[length(found) + 1]] <- row(key, "missing_citation", "citation_main is empty")
    } else {
      if (!grepl("\\b(1[89]|20)[0-9]{2}\\b", cit))
        found[[length(found) + 1]] <- row(key, "no_year", "no 4-digit year in citation_main")
      has_doi <- nzchar(doi_raw) || !is.na(.find_doi(cit))
      has_url <- grepl("https?://", cit)
      if (!(has_doi || has_url || nzchar(link)))
        found[[length(found) + 1]] <- row(key, "no_locator",
          "no DOI and no URL in citation_main, and link_data_source is empty")
    }
    # structural: the license ----
    if (!nzchar(lic) || identical(tolower(lic), "unknown")) {
      found[[length(found) + 1]] <- row(key, "missing_license",
        if (!nzchar(lic)) "license is empty" else "license is declared `unknown`")
    } else if (!lic %in% active) {
      why <- if (lic %in% reg$license) "deprecated in metadata/license.csv" else
        "not in metadata/license.csv"
      found[[length(found) + 1]] <- row(key, "license_unregistered", sprintf(
        "license `%s` is %s; use one of: %s", lic, why, paste(active, collapse = ", ")))
    } else if (identical(lic, "custom") && !grepl("^https?://", lic_url)) {
      found[[length(found) + 1]] <- row(key, "license_custom_no_url",
        if (!nzchar(lic_url)) "license `custom` requires a license_url" else
          sprintf("license_url is not a URL: %s", lic_url))
    }
    # structural: the DOI shape ----
    doi <- .bare_doi(doi_raw)
    if (nzchar(doi_raw) && is.na(doi))
      found[[length(found) + 1]] <- row(key, "doi_unresolved", sprintf(
        "doi must be bare (10.xxxx/…), not `%s`", doi_raw))

    # the authority: cache, else fetch ----
    cache_path <- file.path(cache_dir, prov, dset, "citation_authority.json")
    res <- .citation_resolver(link, doi)
    entry <- NULL
    if (!isTRUE(refresh) && file.exists(cache_path)) entry <- .read_cache_entry(cache_path)
    if (is.null(entry) && isTRUE(network) && (!is.null(res) || !is.na(doi))) {
      if (is.null(res)) res <- list(authority = "datacite", url = sprintf("https://doi.org/%s", doi))
      got <- .fetch_authority(res, doi, fetch)
      if (!is.null(got$error)) {
        found[[length(found) + 1]] <- row(key, "authority_unavailable", got$error,
                                          authority = res$authority)
      } else {
        entry <- got
        .write_cache_entry(entry, cache_path)
      }
    }
    authority <- if (!is.null(entry)) entry$authority else NA_character_
    auth_cit  <- if (!is.null(entry)) entry$citation  else NA_character_
    checked   <- if (!is.null(entry)) entry$checked   else NA_character_

    # the declared DOI's answer ----
    if (!is.null(entry) && !is.na(doi) && !is.na(entry$doi_status) && identical(entry$doi, doi) &&
        !(entry$doi_status == 200L || (entry$doi_status >= 300L && entry$doi_status < 400L)))
      found[[length(found) + 1]] <- row(key, "doi_unresolved", sprintf(
        "https://doi.org/%s answered HTTP %d (checked %s)", doi, entry$doi_status, checked),
        authority = authority, authority_citation = auth_cit, checked = checked)

    # drift ----
    if (!is.null(entry) && !is.na(auth_cit) && nzchar(cit) &&
        normalize_citation(cit) != normalize_citation(auth_cit))
      found[[length(found) + 1]] <- row(key, "authority_drift",
        sprintf("declared: %s\nauthority (%s, %s): %s", cit, authority, checked, auth_cit),
        authority = authority, authority_citation = auth_cit, checked = checked,
        field = "citation_main")
    if (!is.null(entry) && identical(entry$license_scheme, "spdx") && !is.na(entry$license) &&
        nzchar(lic) && toupper(lic) != toupper(entry$license))
      found[[length(found) + 1]] <- row(key, "authority_drift",
        sprintf("license: declared %s; authority (%s, %s) says %s", lic, authority, checked, entry$license),
        authority = authority, authority_citation = auth_cit, checked = checked, field = "license")

    if (!length(found)) {
      what <- if (!is.null(entry)) sprintf("structural checks pass; authority %s checked %s", authority, checked) else
        if (is.null(res) && is.na(doi)) "structural checks pass; no authority resolves this source" else
        "structural checks pass; authority not fetched (network = FALSE, no cache)"
      found[[1]] <- row(key, "ok", what, authority = authority, authority_citation = auth_cit,
                        checked = checked, field = NA_character_)
    }

    # exemptions: an open/proposed question on the dataset table ----
    q <- .dataset_questions(file.path(cache_dir, prov, dset, "questions.csv"))
    for (j in seq_along(found)) {
      f <- found[[j]]
      # every row of the dataset says which authority was consulted (a structural
      # finding is raised before the fetch, so it is stamped here)
      if (is.na(f$authority)) f$authority <- authority
      if (is.na(f$authority_citation)) f$authority_citation <- auth_cit
      if (is.na(f$checked)) f$checked <- checked
      f$exempt <- FALSE; f$question <- NA_character_
      if (!is.null(q) && f$level == "error") {
        qf <- ifelse(is.na(q$related_field), "", trimws(q$related_field))
        hit <- !nzchar(qf) | qf == f$field
        if (any(hit)) { f$exempt <- TRUE; f$question <- paste(q$label[hit], collapse = "; ") }
      }
      found[[j]] <- f
    }
    rows <- c(rows, found)
  }
  out <- do.call(rbind, rows)
  out[, c("dataset_key", "finding", "detail", "authority", "authority_citation", "checked",
          "level", "exempt", "question", "field")]
}

#' Stop on any non-exempt error finding from [check_dataset_citation()]
#'
#' The one place the failure is formatted, shared by `build_workflows_index.R`
#' and `release_database.qmd`. Warn-level findings (drift, an unreachable
#' authority) are reported as messages and never stop.
#'
#' @param d the table from [check_dataset_citation()]
#' @param quiet suppress the messages for warn-level and exempt rows
#' @return `d`, invisibly, when nothing blocks.
#' @export
#' @concept registry
assert_dataset_citation <- function(d, quiet = FALSE) {
  fmt <- function(x) paste0("  ", x$dataset_key, "  ", x$finding, ": ",
                            gsub("\n", "\n      ", x$detail), collapse = "\n")
  warn <- d[d$level == "warn", , drop = FALSE]
  if (nrow(warn) && !quiet)
    message("citation check: ", nrow(warn), " warning(s) — the source's authority differs or ",
            "did not answer; review, do not paste:\n", fmt(warn))
  ex <- d[d$level == "error" & d$exempt, , drop = FALSE]
  if (nrow(ex) && !quiet)
    message("citation check: ", nrow(ex), " finding(s) exempt while a question is open/proposed: ",
            paste(sprintf("%s (%s, %s)", ex$dataset_key, ex$finding, ex$question), collapse = "; "))
  bad <- d[d$level == "error" & !d$exempt, , drop = FALSE]
  if (nrow(bad))
    stop("citation check: ", nrow(bad), " blocking finding(s):\n", fmt(bad),
         "\n  Fix the `calcofi.dataset_meta` field in the notebook, or file an open/proposed",
         " questions.csv row with related_table = dataset naming the field.", call. = FALSE)
  invisible(d)
}

# source_accessed: measured, never asserted ----------------------------------------------

#' When was a dataset's source last read? Measured from git
#'
#' The last commit date of `data/parquet/{provider}_{dataset}/manifest.json`
#' — the sidecar every ingest rewrites when it runs — is the best available
#' record of when the source was read, and it costs no ingest re-run. Method
#' `sidecar_commit`. An untracked sidecar, or a directory outside a repository,
#' yields `NA`.
#'
#' Prefer [resolve_source_accessed()], which takes an ingest's own
#' [stamp_source_access()] record from `metadata.json` when there is one.
#'
#' @param dir_parquet one or more sidecar directories
#'   (`data/parquet/{provider}_{dataset}`)
#' @param file the sidecar whose history is read (default `manifest.json`)
#' @return A [tibble][tibble::tibble]: `dataset_key` (the directory name),
#'   `source_accessed` (Date), `source_accessed_method`, `source_accessed_ref`
#'   (the commit).
#' @export
#' @concept registry
source_accessed_from_git <- function(dir_parquet, file = "manifest.json") {
  one <- function(dir) {
    # system2() goes through a shell, so the `|` in the format must be quoted or
    # it becomes a pipe
    out <- tryCatch(suppressWarnings(system2(
      "git", c("-C", shQuote(dir), "log", "-1", shQuote("--format=%cI|%H"), "--", shQuote(file)),
      stdout = TRUE, stderr = FALSE)), error = function(e) character())
    out <- out[nzchar(out)]
    if (!length(out) || !grepl("\\|", out[1]))
      return(c(NA_character_, NA_character_))
    p <- strsplit(out[1], "|", fixed = TRUE)[[1]]
    c(substr(p[1], 1, 10), p[2])
  }
  m <- unname(vapply(dir_parquet, one, character(2)))
  if (is.null(dim(m))) m <- matrix(m, nrow = 2)
  tibble::tibble(
    dataset_key            = basename(dir_parquet),
    source_accessed        = as.Date(unname(m[1, ])),
    source_accessed_method = ifelse(is.na(m[1, ]), NA_character_, "sidecar_commit"),
    source_accessed_ref    = unname(m[2, ]))
}

#' Record when an ingest read its sources
#'
#' Call it at the point the bytes come down (`urls`: method `download`,
#' accessed now) or, for archives kept on Drive, on the files themselves
#' (`files`: method `file_mtime`), and hand the result to
#' [build_metadata_json()]'s `sources` argument so it lands in the ingest's
#' `metadata.json` as `sources[]`. The release then takes the newest stamp as
#' the dataset's `source_accessed` ([resolve_source_accessed()]).
#'
#' @param files local files read as sources
#' @param urls URLs the sources were downloaded from
#' @return A [tibble][tibble::tibble]: `source`, `method`, `accessed` (POSIXct,
#'   UTC), `bytes`.
#' @export
#' @concept registry
stamp_source_access <- function(files = NULL, urls = NULL) {
  now <- Sys.time()
  rows <- list()
  if (length(files)) {
    info <- file.info(files)
    rows[[1]] <- tibble::tibble(source = as.character(files), method = "file_mtime",
                                accessed = as.POSIXct(info$mtime), bytes = as.numeric(info$size))
  }
  if (length(urls))
    rows[[length(rows) + 1]] <- tibble::tibble(source = as.character(urls), method = "download",
                                               accessed = rep(now, length(urls)), bytes = NA_real_)
  if (!length(rows))
    return(tibble::tibble(source = character(), method = character(),
                          accessed = as.POSIXct(character()), bytes = numeric()))
  out <- do.call(rbind, rows)
  attr(out$accessed, "tzone") <- "UTC"
  out
}

#' @rdname stamp_source_access
#' @param x a stamp table from [stamp_source_access()]
#' @return `sources_block()`: the list `build_metadata_json()` writes as
#'   `sources[]`.
#' @export
sources_block <- function(x) {
  if (is.null(x) || !nrow(x)) return(list())
  lapply(seq_len(nrow(x)), function(i) {
    e <- list(source = x$source[i], method = x$method[i],
              accessed = format(x$accessed[i], "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
    if (!is.na(x$bytes[i])) e$bytes <- x$bytes[i]
    e
  })
}

#' Resolve each dataset's `source_accessed`: the ingest's own stamp, else git
#'
#' Reads `metadata.json` `sources[]` (written by [build_metadata_json()] from
#' [stamp_source_access()]) and takes the newest stamp; where an ingest has none,
#' falls back to [source_accessed_from_git()].
#'
#' @inheritParams source_accessed_from_git
#' @return As [source_accessed_from_git()].
#' @export
#' @concept registry
resolve_source_accessed <- function(dir_parquet) {
  g <- source_accessed_from_git(dir_parquet)
  for (i in seq_along(dir_parquet)) {
    mj <- file.path(dir_parquet[i], "metadata.json")
    if (!file.exists(mj)) next
    src <- tryCatch(jsonlite::fromJSON(mj, simplifyVector = FALSE)$sources, error = function(e) NULL)
    if (!length(src)) next
    acc <- as.POSIXct(vapply(src, function(s) .s(s$accessed), ""),
                      format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
    if (all(is.na(acc))) next
    k <- which.max(acc)
    g$source_accessed[i]        <- as.Date(format(acc[k], "%Y-%m-%d", tz = "UTC"))
    g$source_accessed_method[i] <- .s(src[[k]]$method)
    g$source_accessed_ref[i]    <- NA_character_
  }
  g
}

# the release cites itself -----------------------------------------------------------------

#' The citation for a release of the integrated database
#'
#' Decided wording (2026-09-03): *CalCOFI (YYYY). CalCOFI Integrated Database,
#' release vYYYY.MM.DD \[Data set\]. Scripps Institution of Oceanography, NOAA
#' Fisheries, and California Department of Fish and Wildlife.
#' https://doi.org/<doi>* — the db-schema URL for the version until its Zenodo
#' DOI exists. `all_versions = TRUE` gives the concept-DOI form (no release in
#' the title, `10.5281/zenodo.22281994`).
#'
#' @param version `vYYYY.MM.DD`
#' @param date the release date (Date or `"YYYY-MM-DD"`); the year comes from
#'   the version when omitted
#' @param doi the version's DOI, when Zenodo has minted it
#' @param all_versions cite every version under the concept DOI
#' @return One string.
#' @export
#' @concept release
release_citation <- function(version, date = NULL, doi = NULL, all_versions = FALSE) {
  if (!grepl("^v20[0-9]{2}\\.[0-9]{2}(\\.[0-9]{2})?$", version))
    stop("version must be vYYYY.MM.DD, got `", version, "`", call. = FALSE)
  year <- if (!is.null(date) && !is.na(date) && nzchar(as.character(date)))
    format(as.Date(date), "%Y") else substr(version, 2, 5)
  if (isTRUE(all_versions))
    return(sprintf("CalCOFI (%s). CalCOFI Integrated Database [Data set]. %s. https://doi.org/%s",
                   year, CC_RELEASE_PUBLISHER, CC_ZENODO_CONCEPT_DOI))
  locator <- if (!is.null(doi) && !is.na(doi) && nzchar(doi)) paste0("https://doi.org/", doi) else
    sprintf("%s?v=%s", CC_DB_SCHEMA_URL, version)
  sprintf("CalCOFI (%s). CalCOFI Integrated Database, release %s [Data set]. %s. %s",
          year, version, CC_RELEASE_PUBLISHER, locator)
}

#' Write the release citation into a `catalog.json` list
#'
#' Sets `citation` (from [release_citation()]) and `concept_doi`; `doi` is set
#' when given and kept when the catalog already has one, and the citation uses
#' it. Everything else in the catalog is untouched.
#'
#' @param catalog the parsed catalog (list)
#' @param doi the version DOI, once Zenodo has minted it
#' @param concept_doi the concept DOI (all versions)
#' @return The catalog.
#' @export
#' @concept release
add_release_citation <- function(catalog, doi = NULL, concept_doi = CC_ZENODO_CONCEPT_DOI) {
  if (is.null(doi) || is.na(doi) || !nzchar(doi)) doi <- catalog$doi
  if (!is.null(doi)) catalog$doi <- doi
  catalog$concept_doi <- concept_doi
  catalog$citation <- release_citation(catalog$version, catalog$release_date, doi = doi)
  catalog
}

#' Find the Zenodo record (and DOI) minted for a release tag
#'
#' Zenodo's GitHub integration archives this repository at each release tag and
#' records `https://github.com/{repo}/tree/{tag}` as a related identifier.
#' `zenodo_doi_for_tag()` searches for that identifier, then falls back to the
#' concept's version listing matched on `metadata.version`. `NULL` when no
#' record exists yet (the DOI arrives minutes after the GitHub release).
#'
#' @param tag the release tag, e.g. `"v2026.09.03"`
#' @param repo the GitHub repository
#' @param concept_doi the concept DOI whose versions are listed as a fallback
#' @param fetch the HTTP function (see [check_dataset_citation()])
#' @return A list — `doi`, `concept_doi`, `record_id`, `version`, `url`,
#'   `title` — or `NULL`.
#' @export
#' @concept release
zenodo_doi_for_tag <- function(tag, repo = "CalCOFI/workflows",
                               concept_doi = CC_ZENODO_CONCEPT_DOI, fetch = NULL) {
  if (is.null(fetch)) fetch <- function(url, ...) .http_get(url, ...)
  q1 <- sprintf('related.identifier:"https://github.com/%s/tree/%s"', repo, tag)
  r <- fetch(paste0("https://zenodo.org/api/records?q=", utils::URLencode(q1, reserved = TRUE)))
  if (.ok200(r)) {
    hit <- zenodo_record_for_tag(r$content, tag, repo)
    if (!is.null(hit)) return(hit)
  }
  q2 <- sprintf('conceptdoi:"%s"', concept_doi)
  r <- fetch(paste0("https://zenodo.org/api/records?q=", utils::URLencode(q2, reserved = TRUE),
                    "&all_versions=true"))
  if (.ok200(r)) return(zenodo_record_for_tag(r$content, tag, repo))
  NULL
}

#' @rdname zenodo_doi_for_tag
#' @param json a Zenodo records search response (text)
#' @export
zenodo_record_for_tag <- function(json, tag, repo = "CalCOFI/workflows") {
  j <- tryCatch(jsonlite::fromJSON(json, simplifyVector = FALSE), error = function(e) NULL)
  hits <- j$hits$hits
  if (!length(hits)) return(NULL)
  tree <- sprintf("https://github.com/%s/tree/%s", repo, tag)
  for (h in hits) {
    rel <- vapply(h$metadata$related_identifiers %||% list(), function(r) .s(r$identifier), "")
    if (tree %in% rel || identical(.s(h$metadata$version), tag))
      return(list(doi = .s(h$doi), concept_doi = .s(h$conceptdoi), record_id = h$id,
                  version = .s(h$metadata$version), url = .s(h$doi_url),
                  title = .s(h$metadata$title)))
  }
  NULL
}

# .zenodo.json + CITATION.cff --------------------------------------------------------------

CC_RELEASE_CREATORS <- c(
  "Scripps Institution of Oceanography, UC San Diego",
  "NOAA Fisheries, Southwest Fisheries Science Center",
  "California Department of Fish and Wildlife")

CC_RELEASE_CURATORS <- list(
  list(name = "Best, Ben", type = "DataCurator", affiliation = "EcoQuants",
       orcid = "0000-0002-2686-0784"),
  list(name = "Huang, Betty", type = "DataCurator"))

CC_RELEASE_KEYWORDS <- c(
  "CalCOFI", "California Current", "oceanography", "CTD", "bottle", "ichthyoplankton",
  "zooplankton", "phytoplankton", "seabirds", "time series", "Parquet", "DuckDB")

# "J. Anthony Koslow" -> "Koslow, J. Anthony"; a name already in Family, Given form is kept
.family_given <- function(x) {
  x <- trimws(x)
  if (grepl(",", x, fixed = TRUE)) return(x)
  p <- strsplit(x, "\\s+")[[1]]
  if (length(p) < 2) return(x)
  paste0(p[length(p)], ", ", paste(p[-length(p)], collapse = " "))
}

.pi_contributors <- function(dataset_df) {
  pis <- unlist(strsplit(.s_vec(dataset_df$pi_names), ";"))
  pis <- unique(trimws(pis)); pis <- pis[nzchar(pis)]
  pis <- sort(vapply(pis, .family_given, ""))
  lapply(unname(pis), function(n) list(name = n, type = "DataCollector"))
}

.s_vec <- function(x) { x <- as.character(x); x[is.na(x)] <- ""; x }

#' Metadata for `.zenodo.json` and `CITATION.cff` at the workflows repo root
#'
#' Zenodo's GitHub integration fills a record from `.zenodo.json` at each
#' release tag; without it the alpha record came out as "CalCOFI/workflows:
#' initial Zenodo release", MIT, creators = the GitHub contributors (measured
#' 2026-09-03). `zenodo_metadata()` makes the record a **dataset**: title
#' "CalCOFI Integrated Database", creators = the three partners as
#' organisations, contributors = every dataset's PIs (`DataCollector`, from
#' `pi_names`) and the curators (`DataCurator`), license `cc-by-4.0` for the
#' record (the code stays MIT in `LICENSE`, said in the description), the GCS
#' release as `isSupplementTo` and db-schema as `isDocumentedBy`. `version` is
#' omitted unless given, because Zenodo takes it from the tag. `citation_cff()`
#' is the same record for GitHub's "Cite this repository", carrying the concept
#' DOI. [write_citation_files()] writes both.
#'
#' @param dataset_df the dataset table from [ingest_yaml_to_dataset_df()]
#'   (only `pi_names` is read)
#' @param version the release tag, when the record is for one release
#' @param publication_date `"YYYY-MM-DD"`, optional
#' @param curators the DataCurator contributors
#' @return A list, ready for `jsonlite::write_json(auto_unbox = TRUE)`.
#' @export
#' @concept release
zenodo_metadata <- function(dataset_df, version = NULL, publication_date = NULL,
                            curators = CC_RELEASE_CURATORS) {
  rel_data <- if (is.null(version)) paste0(CC_RELEASES_HTTPS, "/") else
    sprintf("%s/%s/catalog.json", CC_RELEASES_HTTPS, version)
  out <- list(
    title = "CalCOFI Integrated Database",
    upload_type = "dataset",
    description = paste0(
      "<p>The CalCOFI Integrated Database: sixteen CalCOFI-program datasets (bottle, CTD, ",
      "dissolved inorganic carbon, underway meteorology, ichthyoplankton, CUFES eggs, zooplankton, ",
      "euphausiids, phyllosoma, phytoplankton, picoplankton, mesopelagic fish, seabirds and ",
      "marine mammals, Dungeness crab megalopae) ingested into one schema and published as ",
      "versioned Parquet releases on Google Cloud Storage. This record archives the pipeline ",
      "repository at the release tag together with the release's <code>catalog.json</code> ",
      "(every object's path, size and sha256), <code>metadata.json</code> and release notes; ",
      "the Parquet objects themselves are on GCS and are verifiable through the catalog.</p>",
      "<p>The database is released under CC BY 4.0; each source dataset keeps its own license, ",
      "recorded per dataset in the release's <code>dataset</code> table. The pipeline code in ",
      "this repository is MIT-licensed (see <code>LICENSE</code>).</p>",
      "<p>Documentation: <a href=\"https://calcofi.io/db-schema/\">calcofi.io/db-schema</a>; ",
      "notebooks: <a href=\"https://calcofi.io/workflows/\">calcofi.io/workflows</a>.</p>"),
    creators = lapply(CC_RELEASE_CREATORS, function(n) list(name = n)),
    contributors = c(.pi_contributors(dataset_df), curators),
    license = "cc-by-4.0",
    access_right = "open",
    language = "eng",
    keywords = as.list(CC_RELEASE_KEYWORDS),
    related_identifiers = list(
      list(identifier = rel_data, relation = "isSupplementTo", resource_type = "dataset"),
      list(identifier = CC_DB_SCHEMA_URL, relation = "isDocumentedBy")))
  if (!is.null(version)) out$version <- version
  if (!is.null(publication_date)) out$publication_date <- publication_date
  out
}

#' @rdname zenodo_metadata
#' @param date_released `"YYYY-MM-DD"`
#' @param doi the DOI `CITATION.cff` carries (default: the concept DOI)
#' @export
citation_cff <- function(version, date_released, doi = CC_ZENODO_CONCEPT_DOI) {
  list(
    `cff-version` = "1.2.0",
    message = "If you use this database, please cite it as below and cite each source dataset you use (the release's `dataset` table carries their citations).",
    type = "dataset",
    title = "CalCOFI Integrated Database",
    abstract = "Sixteen CalCOFI-program datasets ingested into one schema and published as versioned Parquet releases; this repository is the pipeline that builds them.",
    authors = lapply(CC_RELEASE_CREATORS, function(n) list(name = n)),
    identifiers = list(list(type = "doi", value = doi, description = "Concept DOI (all versions)")),
    doi = doi,
    version = version,
    `date-released` = date_released,
    url = CC_DB_SCHEMA_URL,
    `repository-code` = "https://github.com/CalCOFI/workflows",
    license = "CC-BY-4.0",
    keywords = as.list(CC_RELEASE_KEYWORDS))
}

#' @rdname zenodo_metadata
#' @param dir the workflows repo root
#' @param zenodo_version the `version` written into `.zenodo.json` (default
#'   `NULL`: Zenodo takes it from the tag)
#' @return `write_citation_files()`: the two paths, named.
#' @export
write_citation_files <- function(dir, dataset_df, version, date_released,
                                 zenodo_version = NULL, doi = CC_ZENODO_CONCEPT_DOI) {
  pz <- file.path(dir, ".zenodo.json")
  jsonlite::write_json(zenodo_metadata(dataset_df, version = zenodo_version), pz,
                       auto_unbox = TRUE, pretty = TRUE)
  pc <- file.path(dir, "CITATION.cff")
  lines <- strsplit(yaml::as.yaml(citation_cff(version, date_released, doi = doi),
                                  indent.mapping.sequence = TRUE), "\n")[[1]]
  # a bare 2026-09-03 is a YAML timestamp to some loaders; CFF wants the string
  # (yaml::as.yaml usually quotes it already — only quote a bare value)
  lines <- sub("^date-released: ([^'\"].*)$", "date-released: '\\1'", lines)
  writeLines(c("# Generated by calcofi4db::write_citation_files() (scripts/build_citation_files.R); do not hand-edit.",
               lines), pc)
  c(".zenodo.json" = pz, "CITATION.cff" = pc)
}
