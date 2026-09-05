# the weekly observation of every external copy -----------------------------------------
#
# `metadata/distribution.csv` is the CURATED registry of the endpoints the release
# cannot measure itself (plan § D-1, D-10): the CoastWatch mirrors, the EDI / NCEI /
# DataZoo records, the OBIS dataset and its IPT resource, the legacy ERDDAP ids with
# their successor. What each of those portals says NOW is a different thing, and it
# changes without us: EDI publishes a revision, OBIS re-indexes, an ERDDAP id is
# dropped. `observe_distributions()` asks each portal by the method its `portal.csv`
# row declares and writes `metadata/distribution_observed.json`.
#
# Two rules, both from the plan:
#   * NOTHING IS EVER DELETED from the registry. A distribution the authority no
#     longer answers for is *observed* `retired` — the row stays, so a page can say
#     "was at X until 2026-08; now at Y".
#   * An unanswered request is `unreachable`, never `retired`. EDI's portal refuses
#     ranged GETs after ~150 requests in a day (measured 2026-09-05) and NOAA's
#     ERDDAPs 503 under load; retiring a row on someone else's bad afternoon would
#     rewrite our record with their outage.

#' The statuses an observation can report
#'
#' Deliberately NOT the registry's vocabulary ([distribution_statuses()]): the
#' registry says what we curate, an observation says what the portal answered.
#'
#' @return Character vector.
#' @export
#' @concept catalog
observation_statuses <- function() c("live", "superseded", "retired", "unreachable", "skipped")

#' The observation methods `portal.csv` may declare
#' @return Character vector.
#' @export
#' @concept catalog
observe_methods <- function() c(
  "edi-pasta", "doi", "obis-api", "ncbi-esummary", "zenodo-api", "erddap-das",
  "caloos", "http", "none")

OBSERVED_COLS <- c("dataset_key", "kind", "portal", "id", "url", "method", "registry_status",
                   "status", "observed_utc", "http_status", "revision", "updated", "note")

.obs_row <- function(status, http_status = NA_integer_, revision = NA_character_,
                     updated = NA_character_, note = NA_character_)
  list(status = status, http_status = as.integer(http_status), revision = as.character(revision),
       updated = as.character(updated), note = as.character(note))

# the EDI package a URL names: packageid=scope.id.rev, or scope=&identifier=&revision=
.edi_package_of <- function(url, id = NA_character_) {
  u <- .s(url)
  m <- regmatches(u, regexec("packageid=([A-Za-z0-9-]+)\\.([0-9]+)(?:\\.([0-9]+))?", u))[[1]]
  if (length(m) >= 3 && nzchar(m[2]))
    return(list(scope = m[2], id = m[3], rev = if (length(m) >= 4 && nzchar(m[4])) as.integer(m[4]) else NA_integer_))
  m <- regmatches(u, regexec("scope=([A-Za-z0-9-]+)&identifier=([0-9]+)(?:&revision=([0-9]+))?", u))[[1]]
  if (length(m) >= 3 && nzchar(m[2]))
    return(list(scope = m[2], id = m[3], rev = if (length(m) >= 4 && nzchar(m[4])) as.integer(m[4]) else NA_integer_))
  # else the registry's own id, `scope.identifier[.revision]`
  m <- regmatches(.s(id), regexec("^([A-Za-z0-9-]+)\\.([0-9]+)(?:\\.([0-9]+))?$", .s(id)))[[1]]
  if (length(m) >= 3 && nzchar(m[2]))
    return(list(scope = m[2], id = m[3], rev = if (length(m) >= 4 && nzchar(m[4])) as.integer(m[4]) else NA_integer_))
  NULL
}

# an ERDDAP dataset page/URL -> its .das
.erddap_das_url <- function(url) {
  u <- .s(url)
  u <- sub("/info/([^/]+)/index\\.html$", "/tabledap/\\1.das", u)
  sub("\\.(html|graph|json|csv|htmlTable|das|dds)$", ".das", u)
}

.das_global <- function(txt, key) {
  m <- regmatches(txt, regexec(sprintf('String %s "([^"]*)"', key), txt))[[1]]
  if (length(m) == 2) m[2] else NA_character_
}

# the observers ------------------------------------------------------------------------

.observe_edi <- function(row, fetch) {
  pkg <- .edi_package_of(row[["url"]], row[["id"]])
  if (is.null(pkg)) return(.obs_row("skipped", note = "no EDI package id in the URL or the registry id"))
  newest <- .edi_newest_revision(pkg$scope, pkg$id, fetch)
  if (is.na(newest))
    return(.obs_row("unreachable", note = paste0(
      "EDI's cite service did not answer for ", pkg$scope, ".", pkg$id,
      " (its portal refuses ranged GETs after ~150 requests in a day)")))
  rev <- pkg$rev
  st  <- if (!is.na(rev) && newest > rev) "superseded" else "live"
  note <- if (identical(st, "superseded"))
    sprintf("newest revision is %s.%s.%d; the registry names revision %d", pkg$scope, pkg$id, newest, rev) else NA_character_
  .obs_row(st, 200L, revision = sprintf("%s.%s.%d", pkg$scope, pkg$id, newest), note = note)
}

.observe_doi <- function(row, fetch) {
  doi <- .s(row[["id"]])
  if (!grepl("^10\\.", doi)) {
    u <- .s(row[["url"]])
    m <- regmatches(u, regexec("(10\\.[0-9]{4,9}/[^\\s\"'<>]+)", u))[[1]]
    doi <- if (length(m) >= 2) m[2] else ""
  }
  if (!nzchar(doi)) return(.observe_http(row, fetch))
  r <- fetch(paste0("https://doi.org/", doi), method = "HEAD")
  st <- as.integer(r$status)
  if (is.na(st)) return(.obs_row("unreachable", note = "doi.org did not answer"))
  if (st %in% c(200L, 301L, 302L, 303L, 307L, 308L)) return(.obs_row("live", st, note = paste0("doi:", doi)))
  .obs_row(if (st %in% c(404L, 410L)) "retired" else "unreachable", st, note = paste0("doi:", doi))
}

.observe_obis <- function(row, fetch) {
  id <- .s(row[["id"]])
  if (!nzchar(id)) return(.obs_row("skipped", note = "no OBIS dataset id"))
  r <- fetch(paste0("https://api.obis.org/v3/dataset/", id))
  st <- as.integer(r$status)
  if (!identical(st, 200L))
    return(.obs_row(if (identical(st, 404L)) "retired" else "unreachable", st,
                    note = "OBIS's text search never matches CalCOFI; the dataset id is the address"))
  j <- tryCatch(jsonlite::fromJSON(r$content, simplifyVector = FALSE), error = function(e) NULL)
  res <- .rows(j[["results"]])
  if (!length(res)) return(.obs_row("retired", st, note = "OBIS answered with no dataset"))
  d <- res[[1]]
  n <- .s(d[["records"]] %||% (d[["statistics"]] %||% list())[["Occurrence"]])
  .obs_row("live", st, updated = .s(d[["updated"]]),
           note = paste0(if (nzchar(n)) paste0(n, " records, ") else "", .s(d[["title"]])))
}

.observe_ncbi <- function(row, fetch) {
  id <- gsub("[^0-9]", "", .s(row[["id"]]))
  if (!nzchar(id)) return(.observe_http(row, fetch))
  r <- fetch(paste0("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
                    "?db=bioproject&retmode=json&id=", id))
  st <- as.integer(r$status)
  if (!identical(st, 200L)) return(.obs_row(if (identical(st, 404L)) "retired" else "unreachable", st))
  j <- tryCatch(jsonlite::fromJSON(r$content, simplifyVector = FALSE), error = function(e) NULL)
  rec <- (j[["result"]] %||% list())[[id]]
  if (is.null(rec)) return(.obs_row("retired", st, note = "no BioProject with that id"))
  .obs_row("live", st, updated = .s(rec[["registration_date"]]), note = .s(rec[["project_title"]]))
}

.observe_zenodo <- function(row, fetch) {
  id <- gsub("[^0-9]", "", .s(row[["id"]] %||% ""))
  if (!nzchar(id)) {
    m <- regmatches(.s(row[["url"]]), regexec("zenodo\\.(?:org/records?/|[0-9]*)([0-9]+)", .s(row[["url"]])))[[1]]
    id <- if (length(m) >= 2) m[2] else ""
  }
  if (!nzchar(id)) return(.observe_doi(row, fetch))
  r <- fetch(paste0("https://zenodo.org/api/records/", id))
  st <- as.integer(r$status)
  if (!identical(st, 200L)) return(.obs_row(if (identical(st, 404L)) "retired" else "unreachable", st))
  j <- tryCatch(jsonlite::fromJSON(r$content, simplifyVector = FALSE), error = function(e) NULL)
  .obs_row("live", st, revision = .s((j[["metadata"]] %||% list())[["version"]]), updated = .s(j[["updated"]]))
}

.observe_erddap <- function(row, fetch) {
  r <- fetch(.erddap_das_url(row[["url"]]))
  st <- as.integer(r$status)
  if (!identical(st, 200L))
    return(.obs_row(if (st %in% c(404L, 410L)) "retired" else "unreachable", st,
                    note = if (identical(st, 404L)) "the ERDDAP no longer serves this id" else NA_character_))
  txt <- r$content
  .obs_row("live", st, updated = .das_global(txt, "date_modified"),
           note = {
             tce <- .das_global(txt, "time_coverage_end")
             if (is.na(tce)) NA_character_ else paste("time_coverage_end", tce)
           })
}

.observe_http <- function(row, fetch) {
  r <- fetch(.s(row[["url"]]))
  st <- as.integer(r$status)
  if (is.na(st)) return(.obs_row("unreachable", st, note = "no answer (timeout/DNS)"))
  if (st %in% c(404L, 410L, 451L)) return(.obs_row("retired", st))
  if (st >= 400L) return(.obs_row("unreachable", st))
  .obs_row("live", st)
}

.observer_for <- function(method) switch(
  method,
  `edi-pasta`     = .observe_edi,
  doi             = .observe_doi,
  `obis-api`      = .observe_obis,
  `ncbi-esummary` = .observe_ncbi,
  `zenodo-api`    = .observe_zenodo,
  `erddap-das`    = .observe_erddap,
  caloos          = .observe_http,
  http            = .observe_http,
  .observe_http)

# what to observe ----------------------------------------------------------------------

#' Every external endpoint worth observing: the registry plus the holdings' links
#'
#' A holding (`status: planned | external | archived` in its descriptive sidecar)
#' has no release objects, so its only endpoints are its `link_data_source` and
#' its DOI — plan § D-11 asks for them to be observed exactly like an ingested
#' dataset's. Rows are deduplicated on `(dataset_key, url)`, the registry's row
#' winning (it carries the curated `id`, `title` and `status`).
#'
#' @param registry the tibble from [read_distribution_registry()]
#' @param sidecars the named list from [read_dataset_sidecars()], or NULL
#' @return A [tibble][tibble::tibble] with the registry's columns.
#' @export
#' @concept catalog
distribution_targets <- function(registry, sidecars = NULL) {
  reg <- tibble::as_tibble(registry)
  extra <- list()
  for (k in names(sidecars %||% list())) {
    y <- sidecars[[k]]
    if (!.s(y[["status"]]) %in% holding_statuses()) next
    link <- .s(y[["link_data_source"]])
    if (nzchar(link)) extra[[length(extra) + 1]] <- tibble::tibble(
      dataset_key = k, kind = "source", portal = classify_portal(link) %||% "other", id = "",
      url = link, title = "holding source", status = "external", superseded_by = "",
      observed_utc = "", notes = "holding (plan D-11)")
    doi <- .s(y[["doi"]])
    if (nzchar(doi)) extra[[length(extra) + 1]] <- tibble::tibble(
      dataset_key = k, kind = "archive", portal = "other", id = doi,
      url = paste0("https://doi.org/", doi), title = "holding DOI", status = "external",
      superseded_by = "", observed_utc = "", notes = "holding (plan D-11)")
  }
  if (length(extra)) {
    e <- dplyr::bind_rows(extra)
    e <- e[!paste(e$dataset_key, e$url) %in% paste(reg$dataset_key, reg$url), , drop = FALSE]
    reg <- dplyr::bind_rows(reg, e[, intersect(names(e), names(reg)), drop = FALSE])
  }
  reg[!duplicated(paste(reg$dataset_key, reg$url)), , drop = FALSE]
}

#' Ask every portal what it says about our external copies
#'
#' One observer per `portal.csv` `observe_method` — `edi-pasta` (the newest
#' revision through EDI's cite service, since PASTA's `/package/eml/{scope}/{id}`
#' answers 403 anonymously), `doi` (does doi.org resolve), `obis-api` (the
#' dataset's `updated`), `ncbi-esummary`, `zenodo-api`, `erddap-das`
#' (`date_modified` / `time_coverage_end`, and a 404 means the id is gone),
#' `caloos` / `http` (liveness). Nothing is written into the registry and no row
#' is ever dropped: the result is a parallel observation, and `retired` is a
#' *status*, not a deletion.
#'
#' @param registry the tibble from [read_distribution_registry()], or the union
#'   from [distribution_targets()]
#' @param portals the tibble from [read_portal_registry()], or NULL (every row is
#'   then observed by `http`)
#' @param fetch the HTTP function (see [check_dataset_citation()]); the tests
#'   inject one over saved responses, so the suite never touches the network
#' @param quiet suppress the per-row progress line
#' @return A [tibble][tibble::tibble]: the registry's key columns plus `method`,
#'   `registry_status`, `status` (one of [observation_statuses()]),
#'   `observed_utc`, `http_status`, `revision`, `updated`, `note`.
#' @export
#' @concept catalog
observe_distributions <- function(registry, portals = NULL, fetch = NULL, quiet = FALSE) {
  if (is.null(fetch)) fetch <- function(url, ...) .http_get(url, ...)
  d <- tibble::as_tibble(registry)
  stopifnot(all(c("dataset_key", "url") %in% names(d)))
  # `distribution.csv` names the portal FAMILY (`erddap-calcofi`), `portal.csv` the
  # portal (`erddap`) — one alias, so our own ERDDAP is asked by `.das` like NOAA's
  alias <- c("erddap-calcofi" = "erddap")
  method_of <- function(portal, url) {
    # a doi.org address is a DOI whatever the portal column says
    if (grepl("^https?://(dx\\.)?doi\\.org/", .s(url))) return("doi")
    if (is.null(portals) || !nrow(portals)) return("http")
    pk <- .s(portal); pk <- unname(alias[pk] %||% pk); if (is.na(pk)) pk <- .s(portal)
    i <- match(pk, portals$portal)
    m <- if (is.na(i)) "" else .s(portals$observe_method[i])
    if (!nzchar(m) || !m %in% observe_methods()) "http" else m
  }
  now <- format(as.POSIXct(Sys.time(), tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ")
  out <- vector("list", nrow(d))
  for (i in seq_len(nrow(d))) {
    row <- as.list(d[i, , drop = FALSE])
    m <- method_of(row[["portal"]], row[["url"]])
    o <- if (identical(m, "none")) .obs_row("skipped", note = "portal.csv declares observe_method `none`")
         else tryCatch(.observer_for(m)(row, fetch),
                       error = function(e) .obs_row("unreachable", note = paste("observer error:", conditionMessage(e))))
    out[[i]] <- tibble::tibble(
      dataset_key = .s(row[["dataset_key"]]), kind = .s(row[["kind"]]), portal = .s(row[["portal"]]),
      id = .s(row[["id"]]), url = .s(row[["url"]]), method = m,
      registry_status = .s(row[["status"]]), status = o$status, observed_utc = now,
      http_status = o$http_status, revision = o$revision, updated = o$updated, note = o$note)
    if (!quiet) message(sprintf("  %-28s %-14s %-11s %s", out[[i]]$dataset_key, m, o$status, out[[i]]$url))
  }
  if (!length(out)) return(tibble::as_tibble(stats::setNames(
    replicate(length(OBSERVED_COLS), character(), simplify = FALSE), OBSERVED_COLS)))
  dplyr::bind_rows(out)[, OBSERVED_COLS]
}

# the file -----------------------------------------------------------------------------

#' Read `metadata/distribution_observed.json`
#'
#' @param path the file
#' @return A [tibble][tibble::tibble] of the `rows`, or NULL when the file does
#'   not exist.
#' @export
#' @concept catalog
read_distribution_observed <- function(path) {
  if (!file.exists(path)) return(NULL)
  j <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  rows <- .rows(j[["rows"]])
  if (!length(rows)) return(NULL)
  dplyr::bind_rows(lapply(rows, function(r) tibble::as_tibble(lapply(
    stats::setNames(OBSERVED_COLS, OBSERVED_COLS),
    function(k) if (is.null(r[[k]])) NA else r[[k]][[1]]))))
}

#' What changed since the last observation
#'
#' A change is a proposal, never a silent edit (plan § D-11): a new EDI revision,
#' a dead link, a DOI newly minted or an ERDDAP `date_modified` that moved is one
#' row here, for the dataset's `questions.csv` and the provider's Sheet.
#'
#' @param observed the tibble from [observe_distributions()]
#' @param previous the tibble from [read_distribution_observed()], or NULL
#' @return A [tibble][tibble::tibble] `dataset_key`, `url`, `field`, `was`,
#'   `now` — empty when nothing moved (or when there is no previous file).
#' @export
#' @concept catalog
distribution_changes <- function(observed, previous = NULL) {
  empty <- tibble::tibble(dataset_key = character(), url = character(), field = character(),
                          was = character(), now = character())
  if (is.null(previous) || !nrow(previous)) return(empty)
  key <- function(d) paste(d$dataset_key, d$url)
  i <- match(key(observed), key(previous))
  out <- list()
  for (f in c("status", "revision", "updated")) {
    was <- ifelse(is.na(i), NA_character_, as.character(previous[[f]][i]))
    now <- as.character(observed[[f]])
    # an `unreachable` is somebody else's outage, not a change to report
    moved <- !is.na(i) & !identical(f, "x") & (was %||% "") != (now %||% "") &
      !(now %in% "unreachable") & !(was %in% "unreachable")
    moved[is.na(moved)] <- FALSE
    if (any(moved)) out[[f]] <- tibble::tibble(
      dataset_key = observed$dataset_key[moved], url = observed$url[moved], field = f,
      was = was[moved], now = now[moved])
  }
  if (!length(out)) empty else dplyr::bind_rows(out)
}

#' Write `metadata/distribution_observed.json`
#'
#' @param observed the tibble from [observe_distributions()]
#' @param path the file to write
#' @param changes the tibble from [distribution_changes()], or NULL
#' @return `path`, invisibly.
#' @export
#' @concept catalog
write_distribution_observed <- function(observed, path, changes = NULL) {
  obs <- observed[order(observed$dataset_key, observed$url), , drop = FALSE]
  x <- list(
    schema_version = "1.0",
    observed_utc = if (nrow(obs)) obs$observed_utc[1] else
      format(as.POSIXct(Sys.time(), tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ"),
    counts = as.list(c(rows = nrow(obs), table(factor(obs$status, levels = observation_statuses())))),
    changes = if (is.null(changes) || !nrow(changes)) list() else
      lapply(seq_len(nrow(changes)), function(i) as.list(changes[i, , drop = FALSE])),
    rows = lapply(seq_len(nrow(obs)), function(i) {
      r <- as.list(obs[i, , drop = FALSE])
      lapply(r, function(v) if (length(v) && is.na(v[[1]])) NULL else v[[1]])
    }))
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(x, path, auto_unbox = TRUE, pretty = TRUE, null = "null", na = "null")
  invisible(path)
}
