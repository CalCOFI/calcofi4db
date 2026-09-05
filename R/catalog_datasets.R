# the dataset catalog record ------------------------------------------------------
#
# One generated record per dataset_key — `datasets.json`, written beside
# `catalog.json` at release (plan 2026-09-05 "CalCOFI.io as a dataset catalog",
# § D-1, Appendix A). It JOINS what the release already measures (metadata.json's
# dataset block, coverage.json's roll-ups, catalog.json's content-addressed
# objects) with the reviewable registries in workflows/metadata/ (category,
# provider, license, dataset_status, distribution, portal, the per-dataset
# descriptive sidecar and questions) and with what the live services answer NOW
# (ERDDAP's allDatasets, the netCDF manifests). Nothing here is authored on a
# page: a dataset fact has exactly one home and the record points at it.
#
# Rules (Appendix A): every URL is absolute; `null`, never `""`; a field the
# release cannot supply is null (the page degrades), never invented;
# distributions[] lists endpoints that answered at release time, or that a
# registry row declares with its status. The builder never touches the network:
# the measured inputs (ERDDAP ids, netCDF manifests, since-versions) are fetched
# by small helpers with an injectable `fetch`, so the tests run on fixtures.

#' @keywords internal
CC_DATASETS_SCHEMA_VERSION <- "1.0"

#' @keywords internal
CC_DATASET_PAGE_BASE <- "https://calcofi.io/datasets/"

#' @keywords internal
CC_STORAGE_HTTPS <- "https://storage.googleapis.com/calcofi-db"

#' @keywords internal
CC_ERDDAP_BASE <- "https://erddap.calcofi.io/erddap"

#' @keywords internal
CC_NETCDF_HTTPS <- "https://storage.googleapis.com/calcofi-files-public/netcdf"

#' @keywords internal
CC_WORKFLOWS_ISSUES <- "https://github.com/CalCOFI/workflows/issues/"

DISTRIBUTION_COLS <- c("dataset_key", "kind", "portal", "id", "url", "title", "status",
                       "superseded_by", "observed_utc", "notes")
PORTAL_COLS       <- c("portal", "name", "kind", "url", "full_archive", "versioning", "doi_issued",
                       "query_by_xyt", "query_by_taxa", "multiple_formats", "api_access",
                       "harvests_from_us", "observe_method", "notes")
HOLDINGS_COLS     <- c("key", "name", "category", "provider", "status", "link", "doi", "module",
                       "lead_name", "lead_email", "lead_affiliation", "priority_caloos", "gh_issue",
                       "notes")
DATASET_STATUS_PUBLISH_COLS <- c("publish_obis", "publish_erddap", "publish_edi", "publish_ncei",
                                 "publish_caloos")

# vocabularies ---------------------------------------------------------------------

#' The controlled vocabularies of the dataset catalog registries
#'
#' `distribution_kinds()` — what a `metadata/distribution.csv` row (and a
#' `distributions[]` entry) is: `download` (bytes you can fetch), `service` (a
#' queryable endpoint), `mirror` (the same rows served by someone else),
#' `source` (where the ingest read from), `archive` (a DOI-minting deposit), and
#' — record-only, derived — `page` (a calcofi.org page) and `notebook` (the
#' ingest). `distribution_portals()` — the host families a URL is classified
#' into by [classify_portal()]. `distribution_statuses()` — `current` (answers
#' today), `superseded` (a newer record exists: see `superseded_by`), `retired`
#' (the authority no longer serves it), `external` (a portal we do not run,
#' listed as declared) and `planned`. `registration_statuses()` — what a
#' `registrations[]` row says: `published`, `planned`, `n/a`.
#' `holding_statuses()` — a dataset without a release: `planned`, `external`,
#' `archived`. `visibility_values()` — `public` | `internal` (Decision 25: an
#' internal dataset is in the record and flagged; every public surface skips it).
#'
#' @return Character vectors.
#' @export
#' @concept catalog
distribution_kinds <- function() c("download", "service", "mirror", "source", "archive", "page", "notebook")

#' @rdname distribution_kinds
#' @export
distribution_portals <- function() c(
  "erddap-calcofi", "erddap-noaa", "edi", "ncei", "obis", "ipt", "caloos", "datazoo", "ucsd-library",
  "zenodo", "ncbi", "calcofi.org", "gcs", "other")

#' @rdname distribution_kinds
#' @export
distribution_statuses <- function() c("current", "superseded", "retired", "external", "planned")

#' @rdname distribution_kinds
#' @export
registration_statuses <- function() c("published", "planned", "n/a")

#' @rdname distribution_kinds
#' @export
holding_statuses <- function() c("planned", "external", "archived")

#' @rdname distribution_kinds
#' @export
visibility_values <- function() c("public", "internal")

# the descriptive dataset_meta keys: provider-editable, sidecar-resident (plan § D-9)
#' The `dataset_meta` keys that live in the descriptive sidecar, not the notebook
#'
#' Plan § D-9 (Decision 14) splits an ingest's `calcofi.dataset_meta` in two:
#' the **structural** keys stay in the notebook YAML (`dataset_name`,
#' `dataset_name_short`, `category`, `color`, `tables`, `in_release` — what the
#' pipeline needs to run and group), and the **descriptive** keys — the ones a
#' provider edits in a Google Sheet — move to
#' `metadata/{provider}/{dataset}/dataset_meta.yml`. [read_calcofi_meta()]
#' merges the two; `scripts/build_workflows_index.R` errors when a descriptive
#' key is still found in a notebook.
#'
#' @return Character vector of key names.
#' @export
#' @concept catalog
dataset_meta_descriptive_keys <- function() c(
  "description", "abstract", "methods_md", "study_extent", "sampling_description",
  "quality_control_md", "maintenance", "creators", "associated_parties", "contact", "keywords",
  "keywords_gcmd", "funding", "acknowledgement", "citation_main", "citation_others", "license",
  "license_url", "doi", "pi_names", "link_calcofi_org", "link_data_source", "link_others",
  "coverage_temporal", "coverage_spatial", "visibility")

#' @rdname dataset_meta_descriptive_keys
#' @export
dataset_meta_structural_keys <- function() c(
  "dataset_name", "dataset_name_short", "category", "color", "tables", "in_release")

# small helpers --------------------------------------------------------------------

.has_value <- function(x) {
  # a scalar that is a value: not NULL, not NA, not ""
  if (is.null(x) || length(x) == 0) return(FALSE)
  x <- x[[1]]
  !(is.na(x) || (is.character(x) && !nzchar(trimws(x))))
}
.or_null <- function(x) if (.has_value(x)) x[[1]] else NULL
.chr_or_null <- function(x) if (.has_value(x)) as.character(x[[1]]) else NULL
.num_or_null <- function(x) if (.has_value(x)) as.numeric(x[[1]]) else NULL
.int_or_null <- function(x) if (.has_value(x)) as.integer(x[[1]]) else NULL

# a character vector -> a JSON array (I() keeps a length-1 vector an array under
# auto_unbox); empty -> an empty array, never null
.arr <- function(x) {
  x <- as.character(unlist(x))
  x <- x[!is.na(x) & nzchar(trimws(x))]
  I(unname(x))
}
# split a `;`-joined registry string into a JSON array
.split_semi <- function(x) {
  if (!.has_value(x)) return(.arr(character()))
  .arr(trimws(strsplit(as.character(x[[1]]), ";", fixed = TRUE)[[1]]))
}

# parse a JSON file/URL, or pass a list through; never simplify (shapes stay stable)
.read_json <- function(x) {
  if (is.character(x)) return(jsonlite::fromJSON(x, simplifyVector = FALSE))
  x
}

# rows of a JSON array that jsonlite may have simplified to a data.frame: always a
# list of lists
.rows <- function(x) {
  if (is.null(x)) return(list())
  if (is.data.frame(x)) return(lapply(seq_len(nrow(x)), function(i) as.list(x[i, , drop = FALSE])))
  x
}
# a named block (metadata.json datasets{}) or an array with provider/dataset — keyed
.keyed_datasets <- function(ds) {
  ds <- .rows(ds)
  if (!length(ds)) return(list())
  if (!is.null(names(ds)) && all(nzchar(names(ds)))) return(ds)
  keys <- vapply(ds, function(d) paste0(.s(d[["provider"]]), "_", .s(d[["dataset"]])), "")
  stats::setNames(ds, keys)
}

# one HTTP probe: a one-byte ranged GET (HEAD is refused by EDI's mapbrowse; a
# ranged GET answers 200/206 everywhere we link and never pulls a parquet). Returns
# the status, NA when the server did not answer.
.http_probe <- function(url, timeout = 30) {
  if (!requireNamespace("curl", quietly = TRUE))
    stop("Package 'curl' is required for the network half of check_dataset_catalog()", call. = FALSE)
  h <- curl::new_handle(range = "0-0", followlocation = TRUE, timeout = timeout,
                        connecttimeout = 10, useragent = "calcofi4db dataset catalog check (https://calcofi.io)")
  r <- tryCatch(curl::curl_fetch_memory(url, handle = h), error = function(e) NULL)
  if (is.null(r)) NA_integer_ else as.integer(r[["status_code"]])
}

# registries -------------------------------------------------------------------------

.read_registry_csv <- function(path, cols, what) {
  stopifnot(file.exists(path))
  d <- readr::read_csv(path, na = "", show_col_types = FALSE,
                       col_types = readr::cols(.default = readr::col_character()))
  miss <- setdiff(cols, names(d))
  if (length(miss))
    stop(what, " registry ", path, " is missing column(s): ", paste(miss, collapse = ", "),
         "\n  Expected: ", paste(cols, collapse = ", "), call. = FALSE)
  check_registry_na_strings(d, path)
  d
}

.check_vocab <- function(d, col, allowed, path) {
  bad <- setdiff(stats::na.omit(unique(d[[col]])), allowed)
  if (length(bad))
    stop("unknown `", col, "` in ", path, ": ", paste(bad, collapse = ", "),
         "\n  Allowed: ", paste(allowed, collapse = " | "), call. = FALSE)
  invisible(TRUE)
}

#' Read `metadata/distribution.csv`, the curated endpoints per dataset
#'
#' One row per external endpoint a dataset can be got or seen through that the
#' release cannot measure itself — a CoastWatch mirror, the EDI or NCEI record
#' the ingest read from, the OBIS dataset and its IPT resource, a legacy ERDDAP
#' id with its sunset — with `kind`, `portal`, `id`, `url`, `title`, `status`
#' (`current | superseded | retired | external | planned`), `superseded_by` (a
#' URL or a `dataset_key`), `observed_utc` (when the status was last confirmed;
#' [observe_distributions()] will refresh it) and `notes`. Nothing is ever
#' deleted from it: a dead endpoint becomes `retired` with the date, so a page
#' can say "was at X until …". Every vocabulary column is validated; an unknown
#' value errors.
#'
#' @param path path to `metadata/distribution.csv`
#' @return A [tibble][tibble::tibble], all columns character.
#' @export
#' @concept catalog
read_distribution_registry <- function(path) {
  d <- .read_registry_csv(path, DISTRIBUTION_COLS, "distribution")
  .check_vocab(d, "kind", setdiff(distribution_kinds(), c("page", "notebook")), path)
  .check_vocab(d, "portal", distribution_portals(), path)
  .check_vocab(d, "status", distribution_statuses(), path)
  bad <- d[["url"]][!is.na(d[["url"]]) & !grepl("^https?://", d[["url"]])]
  if (length(bad))
    stop("distribution.csv url(s) that are not absolute URLs: ", paste(bad, collapse = ", "), call. = FALSE)
  d
}

#' Read `metadata/portal.csv`, the portal capability registry
#'
#' The table `docs/portals.qmd` used to hand-maintain (`data/portal_comparison.csv`),
#' now a registry with the two columns the catalog needs: `harvests_from_us`
#' (what the portal reads from calcofi.io — `erddap-waf`, `sitemap-jsonld`,
#' `data.json`, `ipt`, `none`) and `observe_method` (how
#' [observe_distributions()] asks it what it holds now — `edi-pasta`, `doi`,
#' `obis-api`, `ncbi-esummary`, `zenodo-api`, `erddap-das`, `caloos`, `http`).
#'
#' @param path path to `metadata/portal.csv`
#' @return A [tibble][tibble::tibble], all columns character.
#' @export
#' @concept catalog
read_portal_registry <- function(path) {
  d <- .read_registry_csv(path, PORTAL_COLS, "portal")
  dup <- unique(d[["portal"]][duplicated(d[["portal"]])])
  if (length(dup)) stop("duplicate portal id(s) in ", path, ": ", paste(dup, collapse = ", "), call. = FALSE)
  d
}

#' Read `metadata/dataset_status.csv`, the pipeline-stage tracker
#'
#' One row per (provider, dataset) with the stage, priority, GitHub issue,
#' blockers and the `publish_*` registration columns (`publish_obis`,
#' `publish_erddap`, `publish_edi`, `publish_ncei`, `publish_caloos`), whose
#' cells read `done`, `n/a`, `planned` or `#38;#39 planned` — see
#' [parse_registration()].
#'
#' @param path path to `metadata/dataset_status.csv`
#' @return A [tibble][tibble::tibble] with `dataset_key` added, all columns character.
#' @export
#' @concept catalog
read_dataset_status <- function(path) {
  d <- .read_registry_csv(path, c("provider", "dataset", "gh_issue", "priority", "stage", "blockers",
                                  "updated", DATASET_STATUS_PUBLISH_COLS), "dataset_status")
  d[["dataset_key"]] <- paste0(d[["provider"]], "_", d[["dataset"]])
  d
}

#' Parse one `publish_*` cell of `dataset_status.csv`
#'
#' `done` → `published`; `n/a` (or empty) → `n/a`; anything naming `planned`
#' → `planned`; the `#NN` tokens become workflows issue URLs.
#'
#' @param x one cell
#' @return `list(status, issues)` — `issues` a character vector of URLs.
#' @export
#' @concept catalog
parse_registration <- function(x) {
  x <- .s(x)
  issues <- regmatches(x, gregexpr("#[0-9]+", x))[[1]]
  issues <- if (length(issues)) paste0(CC_WORKFLOWS_ISSUES, sub("#", "", issues, fixed = TRUE)) else character()
  status <- if (grepl("^done", x, ignore.case = TRUE) || grepl("published", x, ignore.case = TRUE)) "published" else
    if (grepl("planned", x, ignore.case = TRUE) || length(issues)) "planned" else "n/a"
  list(status = status, issues = issues)
}

#' Read one descriptive sidecar, `metadata/{provider}/{dataset}/dataset_meta.yml`
#'
#' The provider-editable half of a dataset's metadata (plan § D-9): abstract,
#' methods, creators, contact, keywords, licence, citation, DOI, links — and,
#' for a **holding** (a dataset with no release yet, § D-11), the structural
#' keys too plus `status: planned | external | archived`, `priority`, `owner`,
#' `next_step`, `gh_issue`, `module`. `visibility` defaults to `public`.
#' Validates the vocabularies; a licence outside `license.csv` is refused here
#' when the registry is given.
#'
#' @param path the YAML file
#' @param licenses optional character vector of active licence ids
#' @return A named list; `NULL` when the file does not exist.
#' @export
#' @concept catalog
read_dataset_sidecar <- function(path, licenses = NULL) {
  if (!file.exists(path)) return(NULL)
  y <- yaml::read_yaml(path) %||% list()
  if (!is.list(y)) stop("dataset sidecar is not a mapping: ", path, call. = FALSE)
  vis <- .s(y[["visibility"]])
  if (!nzchar(vis)) y[["visibility"]] <- "public"
  else if (!vis %in% visibility_values())
    stop("unknown visibility `", vis, "` in ", path, "; allowed: ",
         paste(visibility_values(), collapse = " | "), call. = FALSE)
  st <- .s(y[["status"]])
  if (nzchar(st) && !st %in% c(holding_statuses(), "published", "ingested"))
    stop("unknown status `", st, "` in ", path, "; a holding is one of: ",
         paste(holding_statuses(), collapse = " | "), call. = FALSE)
  lic <- .s(y[["license"]])
  if (!is.null(licenses) && nzchar(lic) && !lic %in% licenses)
    stop("license `", lic, "` in ", path, " is not an active id in metadata/license.csv", call. = FALSE)
  y
}

#' Read every descriptive sidecar under a `metadata/` root
#'
#' @param metadata_dir the `workflows/metadata` directory
#' @param licenses passed to [read_dataset_sidecar()]
#' @return A named list keyed by `dataset_key` (`{provider}_{dataset}` from the
#'   directory path); each element carries `provider`, `dataset` and `path`.
#' @export
#' @concept catalog
read_dataset_sidecars <- function(metadata_dir, licenses = NULL) {
  paths <- sort(Sys.glob(file.path(metadata_dir, "*", "*", "dataset_meta.yml")))
  out <- list()
  for (p in paths) {
    y <- read_dataset_sidecar(p, licenses)
    dataset  <- basename(dirname(p)); provider <- basename(dirname(dirname(p)))
    y[["provider"]] <- y[["provider"]] %||% provider
    y[["dataset"]]  <- y[["dataset"]]  %||% dataset
    if (!identical(y[["provider"]], provider) || !identical(y[["dataset"]], dataset))
      stop("sidecar ", p, " declares provider/dataset ", y[["provider"]], "/", y[["dataset"]],
           " but sits under ", provider, "/", dataset, call. = FALSE)
    y[["path"]] <- p
    out[[paste0(provider, "_", dataset)]] <- y
  }
  out
}

#' Read every registry the dataset catalog joins
#'
#' One call for `category.csv`, `provider.csv`, `license.csv`,
#' `dataset_status.csv`, `distribution.csv`, `portal.csv`, the descriptive
#' sidecars and a reader for each dataset's `questions.csv` — validated as they
#' are read, so an unregistered value fails here rather than in a page.
#'
#' @param metadata_dir the `workflows/metadata` directory
#' @return A list: `category`, `provider`, `license`, `dataset_status`,
#'   `distribution`, `portal` (tibbles), `sidecars` (named list),
#'   `questions(dataset_key)` (a function returning the open/proposed rows on
#'   `related_table = dataset`, or NULL) and `metadata_dir`.
#' @export
#' @concept catalog
read_catalog_registries <- function(metadata_dir) {
  stopifnot(dir.exists(metadata_dir))
  f <- function(x) file.path(metadata_dir, x)
  category <- .read_registry_csv(f("category.csv"), c("category", "order", "realm", "icon", "description"), "category")
  provider <- .read_registry_csv(f("provider.csv"), c("provider", "provider_short", "provider_name", "url", "status"), "provider")
  license  <- read_license_registry(f("license.csv"))
  active   <- license$license[license$status == "active"]
  sidecars <- read_dataset_sidecars(metadata_dir, licenses = active)
  # a holding's category and provider must be registered
  for (k in names(sidecars)) {
    y <- sidecars[[k]]
    if (!.s(y[["status"]]) %in% holding_statuses()) next
    if (!.s(y[["category"]]) %in% category$category)
      stop("holding ", k, " declares category `", .s(y[["category"]]), "` not in metadata/category.csv", call. = FALSE)
    if (!.s(y[["provider"]]) %in% provider$provider)
      stop("holding ", k, " declares provider `", .s(y[["provider"]]), "` not in metadata/provider.csv", call. = FALSE)
  }
  dist <- if (file.exists(f("distribution.csv"))) read_distribution_registry(f("distribution.csv")) else
    tibble::as_tibble(stats::setNames(replicate(length(DISTRIBUTION_COLS), character(), simplify = FALSE), DISTRIBUTION_COLS))
  portal <- if (file.exists(f("portal.csv"))) read_portal_registry(f("portal.csv")) else NULL
  bad_portal <- setdiff(unique(dist$portal), c(distribution_portals()))
  if (length(bad_portal)) stop("distribution.csv portal(s) not allowed: ", paste(bad_portal, collapse = ", "), call. = FALSE)
  questions <- function(dataset_key) {
    pd <- regmatches(dataset_key, regexec("^([^_]+)_(.+)$", dataset_key))[[1]]
    if (length(pd) != 3) return(NULL)
    .dataset_questions(file.path(metadata_dir, pd[2], pd[3], "questions.csv"))
  }
  questions_open <- function(dataset_key) {
    pd <- regmatches(dataset_key, regexec("^([^_]+)_(.+)$", dataset_key))[[1]]
    p <- file.path(metadata_dir, pd[2], pd[3], "questions.csv")
    if (length(pd) != 3 || !file.exists(p)) return(NA_integer_)
    q <- read_questions(p)
    sum(q[["status"]] %in% c("open", "proposed"))
  }
  list(category = category, provider = provider, license = license,
       dataset_status = read_dataset_status(f("dataset_status.csv")),
       distribution = dist, portal = portal, sidecars = sidecars,
       questions = questions, questions_open = questions_open, metadata_dir = metadata_dir)
}

# classification -----------------------------------------------------------------------

#' Which portal family a URL belongs to
#'
#' Host-based: `edi` (edirepository.org, pasta.lternet.edu), `ncei`,
#' `erddap-noaa` (coastwatch / oceanview / upwell `pfeg.noaa.gov` ERDDAPs),
#' `erddap-calcofi`, `datazoo` and the other oceaninformatics.ucsd.edu portals
#' (ZooDB, ZooScan), `ucsd-library`, `obis`, `ipt`, `caloos`, `zenodo`, `ncbi`,
#' `calcofi.org`, `gcs` (storage.googleapis.com / storage.calcofi.io), else
#' `other`. `NA` for an empty input.
#'
#' @param url character vector
#' @return character vector of the same length, values from [distribution_portals()].
#' @export
#' @concept catalog
classify_portal <- function(url) {
  vapply(as.character(url), function(u) {
    if (is.na(u) || !nzchar(trimws(u))) return(NA_character_)
    host <- tolower(sub("^https?://([^/]+).*$", "\\1", u))
    if (grepl("edirepository\\.org$|lternet\\.edu$", host)) return("edi")
    if (grepl("ncei\\.noaa\\.gov$|nodc\\.noaa\\.gov$", host)) return("ncei")
    if (grepl("pfeg\\.noaa\\.gov$|coastwatch\\.noaa\\.gov$", host)) return("erddap-noaa")
    if (grepl("^erddap\\.calcofi\\.io$", host)) return("erddap-calcofi")
    if (grepl("oceaninformatics\\.ucsd\\.edu$", host)) return("datazoo")
    if (grepl("library\\.ucsd\\.edu$", host)) return("ucsd-library")
    if (grepl("^(api\\.|www\\.)?obis\\.org$", host)) return("obis")
    if (grepl("^ipt", host)) return("ipt")
    if (grepl("caloos\\.org$", host)) return("caloos")
    if (grepl("zenodo\\.org$", host)) return("zenodo")
    if (grepl("ncbi\\.nlm\\.nih\\.gov$", host)) return("ncbi")
    if (grepl("^(www\\.)?calcofi\\.org$", host)) return("calcofi.org")
    if (grepl("^storage\\.googleapis\\.com$|^storage\\.calcofi\\.io$", host)) return("gcs")
    "other"
  }, character(1), USE.NAMES = FALSE)
}

# ERDDAP id -> grain, from the suffix the generic publisher uses
.erddap_grain <- function(id, key) {
  suffix <- if (startsWith(id, key)) substring(id, nchar(key) + 1) else id
  grains <- c("_sample" = "sampling events", "_attribute" = "length/stage frequency",
              "_full" = "full resolution (pre-thinning)")
  if (!nzchar(suffix)) return("observations")
  if (suffix %in% names(grains)) return(unname(grains[suffix]))
  sub("^_", "", suffix)
}

# measured inputs ----------------------------------------------------------------------

#' What ERDDAP serves now: `allDatasets` as a table
#'
#' @param base the ERDDAP base URL (`…/erddap`)
#' @param fetch the HTTP function (see [check_dataset_citation()]); the tests
#'   inject one that serves a saved CSV
#' @return A [tibble][tibble::tibble] `datasetID`, `title` — the `allDatasets`
#'   row itself dropped — or NULL when the server did not answer.
#' @export
#' @concept catalog
fetch_erddap_datasets <- function(base = CC_ERDDAP_BASE, fetch = NULL) {
  if (is.null(fetch)) fetch <- function(url, ...) .http_get(url, ...)
  r <- fetch(paste0(base, "/tabledap/allDatasets.csv?datasetID,title"))
  if (!.ok200(r) || !nzchar(r[["content"]])) return(NULL)
  parse_erddap_all_datasets(r[["content"]])
}

#' @rdname fetch_erddap_datasets
#' @param x the CSV text of `allDatasets.csv?datasetID,title`
#' @export
parse_erddap_all_datasets <- function(x) {
  d <- utils::read.csv(text = x, stringsAsFactors = FALSE, colClasses = "character", na.strings = character())
  d <- d[!is.na(d[["datasetID"]]) & nzchar(d[["datasetID"]]) & d[["datasetID"]] != "allDatasets", , drop = FALSE]
  tibble::tibble(datasetID = d[["datasetID"]], title = d[["title"]])
}

#' The netCDF `manifests.json` of every dataset published by `publish_to-netcdf.qmd`
#'
#' @param keys dataset keys (and any `{key}_full` variants) to look up
#' @param base the HTTPS root of `netcdf/`
#' @param fetch the HTTP function
#' @return A named list, one parsed `manifests.json` per key that answered.
#' @export
#' @concept catalog
fetch_netcdf_manifests <- function(keys, base = CC_NETCDF_HTTPS, fetch = NULL) {
  if (is.null(fetch)) fetch <- function(url, ...) .http_get(url, ...)
  out <- list()
  for (k in keys) {
    r <- fetch(sprintf("%s/%s/manifests.json", base, k))
    if (!.ok200(r) || !nzchar(r[["content"]])) next
    j <- tryCatch(jsonlite::fromJSON(r[["content"]], simplifyVector = FALSE), error = function(e) NULL)
    if (!is.null(j)) out[[k]] <- j
  }
  out
}

#' The first release each dataset appeared in
#'
#' Walks `versions.json` oldest → newest and reads each version's
#' `metadata.json` (retired versions keep their sidecars), recording the first
#' version whose `datasets` block names the key. ~30 small fetches at release
#' time; the tests inject a `fetch` over fixtures.
#'
#' @param versions the parsed `versions.json` (the list under `versions`, or the
#'   whole object)
#' @param base the HTTPS releases prefix
#' @param fetch the HTTP function
#' @param known a named character vector from a previous `datasets.json`
#'   (`dataset_key -> since_version`); those keys are not re-derived
#' @return A named character vector `dataset_key -> version`.
#' @export
#' @concept catalog
dataset_since_versions <- function(versions, base = CC_RELEASES_HTTPS, fetch = NULL, known = NULL) {
  if (is.null(fetch)) fetch <- function(url, ...) .http_get(url, ...)
  if (!is.null(versions$versions)) versions <- versions$versions
  vs <- vapply(.rows(versions), function(v) .s(v[["version"]]), "")
  vs <- sort(vs[nzchar(vs)])
  out <- if (is.null(known)) character() else known[!is.na(known)]
  for (v in vs) {
    r <- fetch(sprintf("%s/%s/metadata.json", base, v))
    if (!.ok200(r) || !nzchar(r[["content"]])) next
    j <- tryCatch(jsonlite::fromJSON(r[["content"]], simplifyVector = FALSE), error = function(e) NULL)
    keys <- names(.keyed_datasets(j[["datasets"]]))
    new <- setdiff(keys, names(out))
    if (length(new)) out[new] <- v
  }
  out
}

# the distributions of one dataset ------------------------------------------------------

.dist_row <- function(kind, url, ...) {
  x <- list(kind = kind, url = url, ...)
  # drop NULLs so a row carries only the fields it has (the schema allows any)
  x[!vapply(x, is.null, logical(1))]
}

#' Every endpoint of one dataset, measured and curated
#'
#' The `distributions[]` of a record (plan § D-1): the parquet objects that
#' belong to the dataset (its `dataset_key=` partitions and the whole tables
#' attributed to it), the CF netCDF from its `manifests.json`, the ERDDAP ids
#' that exist on erddap.calcofi.io now (by `dataset_key` prefix, with the ISO
#' 19115 record of the primary id), the ingest notebook, the calcofi.org page,
#' the source portal (`link_data_source`, classified by host) and the curated
#' rows of `metadata/distribution.csv` for the key — a legacy ERDDAP id is one
#' of those, listed with `legacy: true` and whether it is still `live`.
#'
#' @param key the `dataset_key`
#' @param ds the dataset's `metadata.json` block
#' @param objects the dataset's `objects[]` (from the builder)
#' @param erddap the table from [fetch_erddap_datasets()], or NULL
#' @param netcdf the list from [fetch_netcdf_manifests()], or NULL
#' @param curated the `distribution.csv` rows for this key (tibble, may be empty)
#' @param version the release version (selects the netCDF entry)
#' @param workflow_url the ingest notebook URL
#' @return A list of rows, each a named list (`kind`, `url`, …).
#' @export
#' @concept catalog
dataset_distributions <- function(key, ds, objects, erddap = NULL, netcdf = NULL, curated = NULL,
                                  version = NULL, workflow_url = NULL) {
  rows <- list()
  add <- function(r) rows[[length(rows) + 1]] <<- r
  # parquet objects ----
  for (o in objects)
    add(.dist_row("download", o[["url"]], format = "parquet", table = o[["table"]], scope = o[["scope"]],
                  shared = o[["shared"]], title = o[["title"]], bytes = o[["bytes"]], sha256 = o[["sha256"]],
                  since = o[["since"]], status = "current"))
  # CF netCDF: the entry for this release, else the newest ----
  for (nm in names(netcdf)) {
    if (!(nm == key || startsWith(nm, paste0(key, "_")))) next
    rel <- .rows(netcdf[[nm]]$releases)
    if (!length(rel)) next
    pick <- Filter(function(e) identical(.s(e[["db_release"]]), .s(version)), rel)
    if (!length(pick)) {
      # no netCDF for this release yet (publish_to-netcdf runs after the release): the newest
      # published one, by version string — which.max() is numeric-only and gave integer(0) here
      # (staging v2026.09.05, 2026-09-05)
      vv <- vapply(rel, function(e) .s(e[["version"]]), "")
      pick <- rel[order(vv, decreasing = TRUE)][1]
    }
    e <- pick[[1]]
    if (!nzchar(.s(e[["canonical_url"]]))) next
    add(.dist_row("download", e[["canonical_url"]], format = "netcdf", id = nm,
                  title = if (nm == key) "CF netCDF" else sprintf("CF netCDF (%s)", sub(paste0("^", key, "_"), "", nm)),
                  bytes = .num_or_null(e[["bytes"]]), sha256 = .chr_or_null(e[["sha256"]]),
                  cf_scope = .chr_or_null(e[["cf_scope"]]), release = .chr_or_null(e[["db_release"]]),
                  generated_utc = .chr_or_null(e[["generated_utc"]]), status = "current"))
  }
  # ERDDAP: the ids that exist now, by prefix ----
  erddap_ids <- character()
  if (!is.null(erddap) && nrow(erddap)) {
    ids <- erddap[["datasetID"]]
    # the generic publisher's ids only: {key}, {key}_sample, {key}_attribute, {key}_full — a legacy id
    # that happens to share the prefix (calcofi_dic_old, calcofi_phytoplankton_old) is registry-declared
    hit <- ids == key | ids %in% paste0(key, c("_sample", "_attribute", "_full"))
    for (i in which(hit)) {
      id <- ids[i]
      add(.dist_row("service", sprintf("%s/tabledap/%s.html", CC_ERDDAP_BASE, id), format = "erddap",
                    id = id, title = .chr_or_null(erddap$title[i]), grain = .erddap_grain(id, key),
                    info_url = sprintf("%s/info/%s/index.html", CC_ERDDAP_BASE, id), status = "current"))
      erddap_ids <- c(erddap_ids, id)
    }
    if (length(erddap_ids)) {
      primary <- if (key %in% erddap_ids) key else erddap_ids[1]
      add(.dist_row("service", sprintf("%s/metadata/iso19115/xml/%s_iso19115.xml", CC_ERDDAP_BASE, primary),
                    format = "iso19115", id = primary, title = "ISO 19115-3 metadata (ERDDAP)", status = "current"))
    }
  }
  # the notebook, the calcofi.org page, the source ----
  if (nzchar(.s(workflow_url)))
    add(.dist_row("notebook", workflow_url, format = "html", title = "ingest notebook", status = "current"))
  if (nzchar(.s(ds[["link_calcofi_org"]])))
    add(.dist_row("page", ds[["link_calcofi_org"]], portal = "calcofi.org", title = "calcofi.org data page", status = "external"))
  if (nzchar(.s(ds[["link_data_source"]])))
    add(.dist_row("source", ds[["link_data_source"]], portal = classify_portal(ds[["link_data_source"]]),
                  title = "source", status = "external"))
  # curated rows ----
  have_urls <- vapply(rows, function(r) .s(r[["url"]]), "")
  if (!is.null(curated) && nrow(curated)) {
    for (i in seq_len(nrow(curated))) {
      cr <- curated[i, ]
      u  <- .s(cr[["url"]])
      if (nzchar(u) && u %in% have_urls) next
      legacy <- identical(cr[["portal"]], "erddap-calcofi")
      r <- .dist_row(cr[["kind"]], .chr_or_null(cr[["url"]]), portal = .chr_or_null(cr[["portal"]]), id = .chr_or_null(cr[["id"]]),
                     title = .chr_or_null(cr[["title"]]), status = .chr_or_null(cr[["status"]]) %||% "external",
                     superseded_by = .chr_or_null(cr[["superseded_by"]]),
                     observed_utc = .chr_or_null(cr[["observed_utc"]]), notes = .chr_or_null(cr[["notes"]]))
      if (legacy) {
        r[["legacy"]] <- TRUE
        if (!is.null(erddap)) r[["live"]] <- .s(cr[["id"]]) %in% erddap$datasetID
      }
      add(r)
    }
  }
  rows
}

# the record ----------------------------------------------------------------------------

.provider_block <- function(key, provider) {
  i <- match(key, provider$provider)
  if (is.na(i)) return(list(key = key, short = NULL, name = NULL, url = NULL, registered = FALSE))
  list(key = key, short = .chr_or_null(provider$provider_short[i]), name = .chr_or_null(provider$provider_name[i]),
       url = .chr_or_null(provider$url[i]), registered = TRUE)
}
.category_block <- function(name, category) {
  i <- match(name, category$category)
  if (is.na(i)) return(list(name = .chr_or_null(name), realm = NULL, icon = NULL, order = NULL, registered = FALSE))
  list(name = name, realm = .chr_or_null(category$realm[i]), icon = .chr_or_null(category$icon[i]),
       order = .int_or_null(category$order[i]), registered = TRUE)
}

# coverage.json rolled up for one dataset
.coverage_block <- function(key, ds, cov, home_category, realm_hint = NULL) {
  cds  <- Filter(function(d) identical(.s(d[["dataset_key"]]), key), .rows(cov[["datasets"]]))
  cd   <- if (length(cds)) cds[[1]] else list()
  yrs  <- Filter(function(y) identical(.s(y[["dataset_key"]]), key), .rows(cov[["years"]]))
  years <- if (length(yrs)) data.frame(
    year = vapply(yrs, function(y) as.integer(y[["year"]]), 1L),
    n_obs = vapply(yrs, function(y) as.numeric(y[["n_obs"]]), 1),
    n_roots = vapply(yrs, function(y) as.numeric(y[["n_roots"]]), 1)) else
      data.frame(year = integer(), n_obs = numeric(), n_roots = numeric())
  n_stations <- sum(vapply(.rows(cov[["stations"]]), function(s)
    any(vapply(.rows(s[["datasets"]]), function(d) identical(.s(d[["dataset_key"]]), key), logical(1))), logical(1)))
  vars <- Filter(function(v) identical(.s(v[["dataset_key"]]), key), .rows(cov[["variables"]]))
  var_names <- vapply(vars, function(v) .s(v[["measurement_type"]]), "")
  dmin <- suppressWarnings(min(vapply(vars, function(v) .num_or_null(v[["depth_min_m"]]) %||% NA_real_, 1), na.rm = TRUE))
  dmax <- suppressWarnings(max(vapply(vars, function(v) .num_or_null(v[["depth_max_m"]]) %||% NA_real_, 1), na.rm = TRUE))
  realm <- .chr_or_null(cd[["realm"]]) %||% realm_hint
  # contributions: a variable homed in another category — env realm only (a shared
  # count type like `abundance` never makes a bio dataset a contributor; plan § D-3)
  contributes <- list()
  if (identical(realm, "env")) {
    vc <- vapply(vars, function(v) .s(v[["category"]]), "")
    other <- nzchar(vc) & vc != .s(home_category)
    if (any(other)) {
      by <- split(var_names[other], vc[other])
      contributes <- lapply(names(by), function(cn) list(category = cn, variables = .arr(sort(by[[cn]]))))
    }
  }
  taxa <- Filter(function(t) any(vapply(.rows(t[["datasets"]]), function(d) identical(.s(d[["dataset_key"]]), key), logical(1))),
                 .rows(cov[["taxa"]]))
  life_stages <- if (!is.null(cd[["life_stages"]])) .arr(unlist(cd[["life_stages"]])) else NULL
  bbox <- ds[["coverage_bbox"]]
  bbox <- if (is.null(bbox)) NULL else list(
    lat_min = .num_or_null(bbox$lat_min), lat_max = .num_or_null(bbox$lat_max),
    lon_min = .num_or_null(bbox$lon_min), lon_max = .num_or_null(bbox$lon_max))
  list(
    realm       = realm,
    temporal    = .chr_or_null(ds[["coverage_temporal_observed"]]) %||% .chr_or_null(ds[["coverage_temporal"]]),
    year_min    = .int_or_null(cd[["year_min"]]), year_max = .int_or_null(cd[["year_max"]]),
    years       = years,
    spatial     = .chr_or_null(ds[["coverage_spatial_observed"]]) %||% .chr_or_null(ds[["coverage_spatial"]]),
    bbox        = bbox,
    n_obs       = .num_or_null(cd[["n_obs"]]), n_roots = .num_or_null(cd[["n_roots"]]),
    n_stations  = as.integer(n_stations),
    n_variables = length(vars), n_taxa = length(taxa),
    depth_min_m = if (is.finite(dmin)) dmin else NULL,
    depth_max_m = if (is.finite(dmax)) dmax else NULL,
    variables   = .arr(sort(var_names)),
    life_stages = life_stages,
    contributes_to = contributes)
}

# the objects[] of one dataset from catalog.json: its partitions + the whole tables
# attributed to it in metadata.json
.dataset_objects <- function(key, ds, meta, catalog) {
  objs <- .catalog_objects(catalog)
  tabs <- .rows(catalog[["tables"]])
  tab_of <- stats::setNames(tabs, vapply(tabs, function(t) .s(t[["name"]]), ""))
  meta_tables <- meta[["tables"]] %||% list()
  contrib <- meta[["contributions"]] %||% list()
  owned <- character()
  for (t in as.character(unlist(ds[["tables"]]))) {
    mt <- meta_tables[[t]]
    if (is.null(mt)) next
    if (identical(.s(mt[["provider"]]), .s(ds[["provider"]])) && identical(.s(mt[["dataset"]]), .s(ds[["dataset"]]))) owned <- c(owned, t)
  }
  out <- list()
  if (!nrow(objs)) return(out)
  # partitions filed under this dataset
  part <- objs[!is.na(objs$partition_value) & objs$partition_value == key, , drop = FALSE]
  part <- part[vapply(seq_len(nrow(part)), function(i) {
    o <- .rows(tab_of[[part$table[i]]]$objects)
    any(vapply(o, function(x) identical(.s(x[["partition_by"]]), "dataset_key") && identical(.s(x[["partition_value"]]), key), logical(1)))
  }, logical(1)), , drop = FALSE]
  mk <- function(o, scope, shared) list(
    table = o[["table"]], scope = scope, shared = shared, path = o[["path"]],
    url = paste0(CC_STORAGE_HTTPS, "/", o[["path"]]),
    title = if (scope == "partition") sprintf("%s (this dataset's rows)", o[["table"]]) else sprintf("%s (whole table)", o[["table"]]),
    bytes = if (is.na(o[["bytes"]])) NULL else o[["bytes"]], sha256 = if (is.na(o[["sha256"]])) NULL else o[["sha256"]],
    since = if (is.na(o[["since"]])) NULL else o[["since"]])
  for (i in seq_len(nrow(part))) out[[length(out) + 1]] <- mk(as.list(part[i, ]), "partition", FALSE)
  for (t in owned) {
    tb <- tab_of[[t]]
    if (is.null(tb) || isTRUE(tb[["partitioned"]])) next
    w <- objs[objs$table == t & is.na(objs$partition_value), , drop = FALSE]
    by <- .rows(contrib[[t]]$by_dataset)
    shared <- length(by) > 1
    for (i in seq_len(nrow(w))) out[[length(out) + 1]] <- mk(as.list(w[i, ]), "table", shared)
  }
  out
}

.registrations <- function(key, st, curated, erddap_published, release_doi, concept_doi) {
  cur_url <- function(portal, kinds = c("archive", "mirror", "service")) {
    if (is.null(curated) || !nrow(curated)) return(NULL)
    h <- curated[curated$portal == portal & curated$kind %in% kinds & curated$status %in% c("current", "external"), , drop = FALSE]
    if (!nrow(h)) NULL else h[["url"]][1]
  }
  reg <- function(portal, col, url = NULL, note = NULL, force_status = NULL) {
    p <- parse_registration(if (is.null(st)) NA else st[[col]])
    status <- force_status %||% p[["status"]]
    r <- list(portal = portal, status = status, url = url,
              issue = if (length(p[["issues"]])) p[["issues"]][1] else NULL,
              issues = .arr(p[["issues"]]), note = note)
    r[!vapply(r, is.null, logical(1))]
  }
  obis_url <- cur_url("obis")
  list(
    reg("erddap", "publish_erddap",
        url = if (erddap_published) sprintf("%s/info/%s/index.html", CC_ERDDAP_BASE, key) else NULL,
        force_status = if (erddap_published) "published" else NULL),
    reg("obis", "publish_obis", url = obis_url, force_status = if (!is.null(obis_url)) "published" else NULL),
    reg("edi",  "publish_edi",  url = cur_url("edi", "archive")),
    reg("ncei", "publish_ncei", url = cur_url("ncei", "archive")),
    reg("caloos", "publish_caloos", url = cur_url("caloos", c("mirror", "service"))),
    list(portal = "zenodo",
         status = if (nzchar(.s(release_doi)) || nzchar(.s(concept_doi))) "published" else "planned",
         url = if (nzchar(.s(release_doi))) paste0("https://doi.org/", release_doi) else
           if (nzchar(.s(concept_doi))) paste0("https://doi.org/", concept_doi) else NULL,
         note = "as part of the integrated database release") |> (\(r) r[!vapply(r, is.null, logical(1))])())
}

.status_block <- function(key, st, registries) {
  q <- registries$questions(key)
  qd <- if (is.null(q)) list() else lapply(seq_len(nrow(q)), function(i)
    list(label = q[["label"]][i], field = .chr_or_null(q[["related_field"]][i]), status = q[["status"]][i]))
  gh <- .s(if (is.null(st)) NA else st[["gh_issue"]])
  list(
    stage    = .chr_or_null(if (is.null(st)) NULL else st[["stage"]]),
    priority = .chr_or_null(if (is.null(st)) NULL else st[["priority"]]),
    gh_issue = if (grepl("^#[0-9]+$", gh)) paste0(CC_WORKFLOWS_ISSUES, sub("#", "", gh)) else .chr_or_null(gh),
    blockers = .chr_or_null(if (is.null(st)) NULL else st[["blockers"]]),
    updated  = .chr_or_null(if (is.null(st)) NULL else st[["updated"]]),
    questions_open = { n <- registries$questions_open(key); if (is.na(n)) NULL else as.integer(n) },
    questions_dataset = qd)
}

.attribution_block <- function(ds, sc, license, source_accessed = NULL) {
  lic <- .chr_or_null(ds[["license"]]) %||% .chr_or_null(sc[["license"]])
  li  <- if (!is.null(lic)) match(lic, license$license) else NA
  creators <- lapply(.rows(sc[["creators"]]), function(cr) list(
    name = .chr_or_null(cr[["name"]]), organization = .chr_or_null(cr[["organization"]]),
    orcid = .chr_or_null(cr[["orcid"]]), email = .chr_or_null(cr[["email"]]), role = .chr_or_null(cr[["role"]])))
  list(
    citation_main   = .chr_or_null(ds[["citation_main"]]) %||% .chr_or_null(sc[["citation_main"]]),
    citation_others = .arr(ds[["citation_others"]] %||% sc[["citation_others"]]),
    doi             = .chr_or_null(ds[["doi"]]) %||% .chr_or_null(sc[["doi"]]),
    license         = lic,
    license_name    = if (!is.na(li)) .chr_or_null(license$name[li]) else NULL,
    license_url     = .chr_or_null(ds[["license_url"]]) %||% .chr_or_null(sc[["license_url"]]) %||%
                        (if (!is.na(li)) .chr_or_null(license$url[li]) else NULL),
    acknowledgement = .chr_or_null(ds[["acknowledgement"]]) %||% .chr_or_null(sc[["acknowledgement"]]),
    contact         = .chr_or_null(ds[["contact"]]) %||% .chr_or_null(sc[["contact"]]),
    pi_names        = .split_semi(ds[["pi_names"]] %||% sc[["pi_names"]]),
    creators        = creators,
    funding         = .chr_or_null(sc[["funding"]]),
    source_accessed = .chr_or_null(source_accessed))
}

.keywords_of <- function(sc) .arr(c(unlist(sc[["keywords_gcmd"]]), unlist(sc[["keywords"]])))

#' Build the dataset catalog record — `datasets.json`
#'
#' One record per `dataset_key` (plan § D-1, Appendix A), joined from the
#' release sidecars, the registries and the measured endpoints:
#'
#' * identity, description, attribution, links and `tables[]` from the
#'   `metadata.json` dataset block, with `provider` and `category` expanded
#'   from their registries and the descriptive sidecar's `keywords`,
#'   `creators[]`, `funding` and `visibility`;
#' * `coverage` rolled up from `coverage.json`: `years[]` (the sparkline),
#'   `n_stations`, `n_variables`, `n_taxa`, the depth span, `variables[]`,
#'   `life_stages[]` (when the coverage carries them) and `contributes_to[]`
#'   (env-realm variables homed in another category);
#' * `objects[]` from `catalog.json`: the dataset's `dataset_key=` partitions
#'   and the whole tables attributed to it, each with `bytes`, `sha256`, `since`
#'   and an absolute URL; `since_version` from `since`;
#' * `distributions[]` ([dataset_distributions()]): parquet, netCDF, the
#'   ERDDAP ids that exist now, the notebook, the calcofi.org page, the source
#'   and the curated `distribution.csv` rows;
#' * `registrations[]` from `dataset_status.csv`'s `publish_*` columns — with
#'   ERDDAP and OBIS *measured* (a served id / a curated OBIS row wins over the
#'   registry cell) and Zenodo from the release DOI;
#' * `status` from `dataset_status.csv` plus the open/proposed questions.
#'
#' `holdings[]` are the sidecars with `status: planned | external | archived`
#' (a dataset without a release, § D-11), and `reference[]` the cruise, ship,
#' grid and spatial tables, the boundary layers and the bathymetry (Decision
#' 20). Deterministic: no wall clock, sorted by key.
#'
#' @param meta the release `metadata.json` (path or parsed list)
#' @param coverage the release `coverage.json`
#' @param catalog the release `catalog.json`
#' @param registries from [read_catalog_registries()]
#' @param version the release version (default: the catalog's)
#' @param erddap the table from [fetch_erddap_datasets()] (NULL: no ERDDAP rows)
#' @param netcdf the list from [fetch_netcdf_manifests()]
#' @param since a named character vector `dataset_key -> first version`
#'   ([dataset_since_versions()])
#' @param source_accessed a named character vector `dataset_key -> YYYY-MM-DD`
#'   (the release `dataset` table's measured column)
#' @param spatial_layers the release `spatial_layers.json` (for `reference[]`)
#' @param bathymetry the `bathymetry/gebco_2025.json` manifest (for `reference[]`)
#' @param workflows_base the URL prefix of the rendered notebooks
#' @return A list ready for [write_dataset_catalog()] / `jsonlite::write_json(auto_unbox = TRUE)`.
#' @export
#' @concept catalog
#' @seealso [check_dataset_catalog()], [write_dataset_catalog()]
build_dataset_catalog <- function(meta, coverage, catalog, registries, version = NULL,
                                  erddap = NULL, netcdf = NULL, since = NULL, source_accessed = NULL,
                                  spatial_layers = NULL, bathymetry = NULL,
                                  workflows_base = "https://calcofi.io/workflows/") {
  meta <- .read_json(meta); coverage <- .read_json(coverage); catalog <- .read_json(catalog)
  if (!is.null(spatial_layers)) spatial_layers <- .read_json(spatial_layers)
  if (!is.null(bathymetry)) bathymetry <- .read_json(bathymetry)
  version <- version %||% .s(catalog[["version"]])
  stopifnot(nzchar(version))
  dsets <- .keyed_datasets(meta[["datasets"]])
  keys  <- sort(names(dsets))
  st_all <- registries$dataset_status
  dist   <- registries$distribution

  records <- lapply(keys, function(key) {
    ds <- dsets[[key]]
    sc <- registries$sidecars[[key]] %||% list()
    st <- if (key %in% st_all$dataset_key) as.list(st_all[match(key, st_all$dataset_key), ]) else NULL
    cur <- dist[dist$dataset_key == key, , drop = FALSE]
    cat_block <- .category_block(.s(ds[["category"]]), registries$category)
    objects <- .dataset_objects(key, ds, meta, catalog)
    wf <- .chr_or_null(ds[["workflow_url"]]) %||% paste0(workflows_base, "ingest_", key, ".html")
    dists <- dataset_distributions(key, ds, objects, erddap = erddap, netcdf = netcdf, curated = cur,
                                   version = version, workflow_url = wf)
    erddap_published <- any(vapply(dists, function(r) identical(r[["kind"]], "service") && identical(r[["format"]], "erddap"), logical(1)))
    list(
      dataset_key = key,
      provider    = .provider_block(.s(ds[["provider"]]), registries$provider),
      dataset     = .s(ds[["dataset"]]),
      dataset_name       = .chr_or_null(ds[["dataset_name"]]),
      dataset_name_short = .chr_or_null(ds[["dataset_name_short"]]),
      category    = cat_block,
      color       = .chr_or_null(ds[["color"]]),
      visibility  = .s(sc[["visibility"]] %||% "public"),
      description_md = .chr_or_null(ds[["description"]]) %||% .chr_or_null(sc[["abstract"]]) %||% .chr_or_null(sc[["description"]]),
      keywords    = .keywords_of(sc),
      attribution = .attribution_block(ds, sc, registries$license,
                                       source_accessed = if (!is.null(source_accessed)) source_accessed[key] else NULL),
      links = list(
        calcofi_org = .chr_or_null(ds[["link_calcofi_org"]]),
        data_source = .chr_or_null(ds[["link_data_source"]]),
        workflow    = wf,
        page        = paste0(CC_DATASET_PAGE_BASE, key, "/")),
      coverage    = .coverage_block(key, ds, coverage, .s(ds[["category"]]), realm_hint = .chr_or_null(cat_block$realm)),
      tables      = .arr(unlist(ds[["tables"]])),
      objects     = lapply(objects, function(o) o[c("table", "scope", "shared", "path", "url", "bytes", "sha256", "since")]),
      since_version = if (!is.null(since) && !is.na(since[key])) unname(since[key]) else NULL,
      distributions = dists,
      registrations = .registrations(key, st, cur, erddap_published, catalog[["doi"]], catalog[["concept_doi"]]),
      status = .status_block(key, st, registries))
  })

  holdings <- .holdings_records(registries)
  reference <- .reference_records(meta, catalog, spatial_layers, bathymetry)
  list(
    schema_version = CC_DATASETS_SCHEMA_VERSION,
    release = list(
      version = version, release_date = .chr_or_null(catalog[["release_date"]]),
      doi = .chr_or_null(catalog[["doi"]]), concept_doi = .chr_or_null(catalog[["concept_doi"]]),
      citation = .chr_or_null(catalog[["citation"]]),
      n_tables = length(.rows(catalog[["tables"]])),
      total_rows = .num_or_null(catalog[["total_rows"]]), total_size = .num_or_null(catalog[["total_size"]]),
      url = sprintf("%s/%s/", CC_RELEASES_HTTPS, version),
      catalog_url = sprintf("%s/%s/catalog.json", CC_RELEASES_HTTPS, version),
      schema_url = sprintf("%s?v=%s", CC_DB_SCHEMA_URL, version)),
    counts = list(datasets = length(records), holdings = length(holdings), reference = length(reference)),
    datasets = records, holdings = holdings, reference = reference)
}

# holdings: the sidecars with a holding status
.holdings_records <- function(registries) {
  sc_all <- registries$sidecars
  keys <- sort(names(sc_all)[vapply(sc_all, function(y) .s(y[["status"]]) %in% holding_statuses(), logical(1))])
  dist <- registries$distribution
  lapply(keys, function(key) {
    sc <- sc_all[[key]]
    cur <- dist[dist$dataset_key == key, , drop = FALSE]
    ds <- list(provider = sc[["provider"]], dataset = sc[["dataset"]], link_calcofi_org = sc[["link_calcofi_org"]],
               link_data_source = sc[["link_data_source"]])
    dists <- dataset_distributions(key, ds, list(), curated = cur)
    gh <- .s(sc[["gh_issue"]])
    list(
      dataset_key = key,
      provider    = .provider_block(.s(sc[["provider"]]), registries$provider),
      dataset     = .s(sc[["dataset"]]),
      dataset_name       = .chr_or_null(sc[["dataset_name"]]),
      dataset_name_short = .chr_or_null(sc[["dataset_name_short"]]),
      category    = .category_block(.s(sc[["category"]]), registries$category),
      visibility  = .s(sc[["visibility"]] %||% "public"),
      description_md = .chr_or_null(sc[["abstract"]]) %||% .chr_or_null(sc[["description"]]),
      keywords    = .keywords_of(sc),
      attribution = .attribution_block(list(), sc, registries$license),
      links = list(calcofi_org = .chr_or_null(sc[["link_calcofi_org"]]), data_source = .chr_or_null(sc[["link_data_source"]]),
                   page = paste0(CC_DATASET_PAGE_BASE, key, "/")),
      distributions = dists,
      status = list(
        stage = .s(sc[["status"]]), priority = .chr_or_null(sc[["priority"]]), owner = .chr_or_null(sc[["owner"]]),
        next_step = .chr_or_null(sc[["next_step"]]), module = .chr_or_null(sc[["module"]]),
        priority_caloos = .chr_or_null(sc[["priority_caloos"]]),
        gh_issue = if (grepl("^#?[0-9]+$", gh)) paste0(CC_WORKFLOWS_ISSUES, sub("#", "", gh)) else .chr_or_null(gh)))
  })
}

# reference[]: the tables that are not datasets, the boundary layers, the bathymetry
.reference_records <- function(meta, catalog, spatial_layers = NULL, bathymetry = NULL) {
  out <- list()
  tabs <- .rows(catalog[["tables"]])
  objs <- .catalog_objects(catalog)
  mt <- meta[["tables"]] %||% list()
  for (t in c("cruise", "ship", "grid", "spatial", "spatial_attribute")) {
    tb <- Filter(function(x) identical(.s(x[["name"]]), t), tabs)
    if (!length(tb)) next
    tb <- tb[[1]]
    o <- objs[objs$table == t, , drop = FALSE]
    out[[length(out) + 1]] <- list(
      key = t, kind = "table", name = .chr_or_null(mt[[t]]$name_long) %||% t,
      description_md = .chr_or_null(mt[[t]]$description_md), rows = .num_or_null(tb[["rows"]]),
      url = if (nrow(o)) paste0(CC_STORAGE_HTTPS, "/", o[["path"]][1]) else NULL,
      schema_url = paste0(CC_DB_SCHEMA_URL, "#", t))
  }
  if (!is.null(spatial_layers)) {
    base <- .s(spatial_layers[["pmtiles_base"]])
    for (ly in .rows(spatial_layers[["layers"]])) {
      out[[length(out) + 1]] <- list(
        key = .s(ly[["id"]]), kind = "layer", name = .chr_or_null(ly[["name"]]), group = .chr_or_null(ly[["group"]]),
        description_md = .chr_or_null(ly[["description"]]), attribution = .chr_or_null(ly[["attribution"]]),
        n_features = .int_or_null(ly[["n_features"]]), bbox = if (length(ly[["bbox"]]) == 4) I(as.numeric(unlist(ly[["bbox"]]))) else NULL,
        source = .chr_or_null(ly[["source"]]),
        url = if (nzchar(base) && nzchar(.s(ly[["source"]]))) paste0(base, ly[["source"]], ".pmtiles") else NULL,
        built = .chr_or_null(spatial_layers[["built"]]))
    }
  }
  if (!is.null(bathymetry)) {
    src <- bathymetry[["source"]] %||% list()
    prefix <- sub("^gs://", "https://storage.googleapis.com/", .s(bathymetry[["gcs_prefix"]]))
    files <- character()
    for (nm in names(bathymetry)) {
      b <- bathymetry[[nm]]
      if (is.list(b) && nzchar(.s(b[["path"]])) && grepl("\\.(pmtiles|tif)$", .s(b[["path"]])))
        files <- c(files, paste0(prefix, basename(.s(b[["path"]]))))
    }
    out[[length(out) + 1]] <- list(
      key = "gebco_2025", kind = "raster", name = "GEBCO 2025 bathymetry",
      description_md = .chr_or_null(src[["grid"]]), attribution = .chr_or_null(src[["citation"]]),
      license = .chr_or_null(src[["licence"]]), url = .chr_or_null(src[["url"]]),
      objects = .arr(files), built = .chr_or_null(bathymetry[["built"]]))
  }
  out
}

# holdings.csv: an index generated from the sidecars, never typed --------------------

#' The `holdings.csv` index, generated from the holding sidecars
#'
#' One row per sidecar with `status: planned | external | archived` (plan
#' § D-11): `key, name, category, provider, status, link, doi, module, lead_name,
#' lead_email, lead_affiliation, priority_caloos, gh_issue, notes`. The lead
#' columns come from the first `creators[]` entry; `link` is
#' `link_data_source`. [write_holdings_csv()] writes it with `na = ""`.
#'
#' @param registries from [read_catalog_registries()] (only `sidecars` is read)
#' @return A [tibble][tibble::tibble].
#' @export
#' @concept catalog
holdings_from_sidecars <- function(registries) {
  sc_all <- registries$sidecars
  keys <- sort(names(sc_all)[vapply(sc_all, function(y) .s(y[["status"]]) %in% holding_statuses(), logical(1))])
  rows <- lapply(keys, function(key) {
    y <- sc_all[[key]]
    lead <- .rows(y[["creators"]])
    lead <- if (length(lead)) lead[[1]] else list()
    tibble::tibble(
      key = key, name = .s(y[["dataset_name"]]), category = .s(y[["category"]]), provider = .s(y[["provider"]]),
      status = .s(y[["status"]]), link = .s(y[["link_data_source"]]), doi = .s(y[["doi"]]), module = .s(y[["module"]]),
      lead_name = .s(lead[["name"]]), lead_email = .s(lead[["email"]]), lead_affiliation = .s(lead[["organization"]]),
      priority_caloos = .s(y[["priority_caloos"]]), gh_issue = .s(y[["gh_issue"]]), notes = .s(y[["next_step"]]))
  })
  out <- if (length(rows)) do.call(rbind, rows) else
    tibble::as_tibble(stats::setNames(replicate(length(HOLDINGS_COLS), character(), simplify = FALSE), HOLDINGS_COLS))
  out[out == ""] <- NA_character_
  out
}

#' @rdname holdings_from_sidecars
#' @param path where to write `holdings.csv`
#' @export
write_holdings_csv <- function(registries, path) {
  d <- holdings_from_sidecars(registries)
  lines <- c("# GENERATED by calcofi4db::write_holdings_csv() from metadata/{provider}/{dataset}/dataset_meta.yml (status: planned | external | archived) — edit the sidecar, not this file",
             readr::format_csv(d, na = ""))
  writeLines(sub("\n$", "", paste(lines, collapse = "\n")), path)
  invisible(path)
}

# write --------------------------------------------------------------------------------

#' Write `datasets.json` and one `datasets/{key}.json` per dataset
#'
#' @param record from [build_dataset_catalog()]
#' @param dir the release directory
#' @return The paths written, invisibly (`datasets.json` first).
#' @export
#' @concept catalog
write_dataset_catalog <- function(record, dir) {
  dir.create(file.path(dir, "datasets"), recursive = TRUE, showWarnings = FALSE)
  main <- file.path(dir, "datasets.json")
  jsonlite::write_json(record, main, auto_unbox = TRUE, pretty = TRUE, digits = NA, null = "null", na = "null")
  paths <- main
  for (r in c(record[["datasets"]], record[["holdings"]])) {
    p <- file.path(dir, "datasets", paste0(r[["dataset_key"]], ".json"))
    jsonlite::write_json(list(schema_version = record[["schema_version"]], release = record[["release"]], dataset = r),
                         p, auto_unbox = TRUE, pretty = TRUE, digits = NA, null = "null", na = "null")
    paths <- c(paths, p)
  }
  invisible(paths)
}

# check --------------------------------------------------------------------------------

#' The findings `check_dataset_catalog()` can report, with their level
#'
#' `error` findings fail the release unless exempt (only `no_citation` can be:
#' an open/proposed `questions.csv` row on `related_table = dataset` naming
#' `citation_main`, or no field, covers it — the citation contract's rule);
#' `warn` findings never block.
#'
#' @return A named character vector, finding -> level.
#' @export
#' @concept catalog
catalog_findings <- function() c(
  ok                    = "ok",
  missing_name          = "error",   # dataset_name is null
  unregistered_category = "error",   # category not in metadata/category.csv
  unregistered_provider = "error",   # provider not in metadata/provider.csv
  missing_description   = "error",   # description_md is null
  missing_bbox          = "error",   # coverage.bbox null or incomplete
  no_download           = "error",   # no `download` distribution
  no_citation           = "error",   # citation_main null (exemptible)
  url_dead              = "error",   # 404 / 410 / 451
  url_unreachable       = "warn",    # 5xx, timeout, DNS
  invalid_visibility    = "error",   # not public | internal
  unregistered_license  = "error")   # license id not in metadata/license.csv

#' Check every record of the dataset catalog
#'
#' One row per (dataset, finding); a clean dataset has a single `ok` row. The
#' structural half always runs ([catalog_findings()]); the network half
#' (`network = TRUE`, i.e. not `CALCOFI_SKIP_LINK_CHECK`) probes every
#' distribution URL once with a one-byte ranged GET — 404/410/451 is `url_dead`
#' (error), 5xx / no answer is `url_unreachable` (warn); `retired` and
#' `superseded` rows are not probed (they are expected to be gone). Holdings
#' are checked for name, category, provider and their links.
#'
#' @param record from [build_dataset_catalog()] (or a `datasets.json` path)
#' @param registries from [read_catalog_registries()]; NULL trusts the record's
#'   own `registered` flags
#' @param network probe the URLs (default TRUE)
#' @param probe the probe function `function(url) status`; the tests inject one
#' @param timeout seconds per request
#' @return A [tibble][tibble::tibble]: `dataset_key`, `finding`, `level`,
#'   `detail`, `url`, `exempt`, `question`.
#' @export
#' @concept catalog
#' @seealso [assert_dataset_catalog()]
check_dataset_catalog <- function(record, registries = NULL, network = TRUE, probe = NULL, timeout = 30) {
  record <- .read_json(record)
  if (is.null(probe)) probe <- function(url) .http_probe(url, timeout = timeout)
  levels <- catalog_findings()
  rows <- list()
  row <- function(key, finding, detail, url = NA_character_, exempt = FALSE, question = NA_character_)
    tibble::tibble(dataset_key = key, finding = finding, level = unname(levels[finding]),
                   detail = detail, url = url, exempt = exempt, question = question)
  cats  <- if (!is.null(registries)) registries$category$category else NULL
  provs <- if (!is.null(registries)) registries$provider$provider else NULL
  lics  <- if (!is.null(registries)) registries$license$license[registries$license$status == "active"] else NULL
  probed <- list()
  probe_once <- function(u) {
    if (is.null(probed[[u]])) probed[[u]] <<- probe(u)
    probed[[u]]
  }
  check_urls <- function(key, dists) {
    out <- list()
    for (d in .rows(dists)) {
      u <- .s(d[["url"]])
      if (!nzchar(u) || .s(d[["status"]]) %in% c("retired", "superseded")) next
      if (!grepl("^https?://", u)) { out[[length(out) + 1]] <- row(key, "url_dead", "not an absolute URL", u); next }
      # prose in a link field ("A & B", "A (processed data); code at B") is a link that can never answer
      if (grepl("[[:space:]]", u)) { out[[length(out) + 1]] <- row(key, "url_dead", "not one URL (whitespace in the link field)", u); next }
      if (!isTRUE(network)) next
      st <- probe_once(u)
      if (!is.na(st) && st %in% c(404L, 410L, 451L))
        out[[length(out) + 1]] <- row(key, "url_dead", sprintf("HTTP %d (%s %s)", st, d[["kind"]], .s(d[["format"]]) %||% .s(d[["portal"]])), u)
      else if (is.na(st) || st >= 400L)
        out[[length(out) + 1]] <- row(key, "url_unreachable",
          sprintf("%s (%s)", if (is.na(st)) "no answer (timeout/DNS)" else paste("HTTP", st), d[["kind"]]), u)
    }
    out
  }
  registered <- function(block, allowed, field) {
    if (!is.null(allowed)) .s(block[[field]]) %in% allowed else isTRUE(block[["registered"]])
  }
  for (r in .rows(record[["datasets"]])) {
    key <- .s(r[["dataset_key"]]); found <- list()
    if (!nzchar(.s(r[["dataset_name"]]))) found[[length(found) + 1]] <- row(key, "missing_name", "dataset_name is null")
    if (!registered(r[["category"]], cats, "name"))
      found[[length(found) + 1]] <- row(key, "unregistered_category", sprintf("category `%s` is not in metadata/category.csv", .s(r[["category"]]$name)))
    if (!registered(r[["provider"]], provs, "key"))
      found[[length(found) + 1]] <- row(key, "unregistered_provider", sprintf("provider `%s` is not in metadata/provider.csv", .s(r[["provider"]]$key)))
    if (!nzchar(.s(r[["description_md"]]))) found[[length(found) + 1]] <- row(key, "missing_description", "description_md is null")
    bb <- r[["coverage"]]$bbox
    if (is.null(bb) || !all(vapply(c("lat_min", "lat_max", "lon_min", "lon_max"), function(k) .has_value(bb[[k]]), logical(1))))
      found[[length(found) + 1]] <- row(key, "missing_bbox", "coverage.bbox is null or incomplete")
    if (!any(vapply(.rows(r[["distributions"]]), function(d) identical(.s(d[["kind"]]), "download"), logical(1))))
      found[[length(found) + 1]] <- row(key, "no_download", "no `download` distribution (parquet or netCDF)")
    if (!nzchar(.s(r[["attribution"]]$citation_main))) {
      qd <- .rows(r[["status"]]$questions_dataset)
      hit <- vapply(qd, function(q) !nzchar(.s(q[["field"]])) || identical(.s(q[["field"]]), "citation_main"), logical(1))
      found[[length(found) + 1]] <- row(key, "no_citation", "attribution.citation_main is null",
        exempt = any(hit), question = if (any(hit)) paste(vapply(qd[hit], function(q) .s(q[["label"]]), ""), collapse = "; ") else NA_character_)
    }
    if (!.s(r[["visibility"]]) %in% visibility_values())
      found[[length(found) + 1]] <- row(key, "invalid_visibility", sprintf("visibility `%s`", .s(r[["visibility"]])))
    lic <- .s(r[["attribution"]]$license)
    if (nzchar(lic) && !is.null(lics) && !lic %in% lics)
      found[[length(found) + 1]] <- row(key, "unregistered_license", sprintf("license `%s` is not an active id in metadata/license.csv", lic))
    found <- c(found, check_urls(key, r[["distributions"]]))
    if (!length(found)) found[[1]] <- row(key, "ok", if (isTRUE(network)) "structural checks pass; every listed URL answers" else
      "structural checks pass; URLs not probed (network = FALSE)")
    rows <- c(rows, found)
  }
  for (h in .rows(record[["holdings"]])) {
    key <- .s(h[["dataset_key"]]); found <- list()
    if (!nzchar(.s(h[["dataset_name"]]))) found[[length(found) + 1]] <- row(key, "missing_name", "holding has no dataset_name")
    if (!registered(h[["category"]], cats, "name"))
      found[[length(found) + 1]] <- row(key, "unregistered_category", sprintf("category `%s` is not in metadata/category.csv", .s(h[["category"]]$name)))
    if (!registered(h[["provider"]], provs, "key"))
      found[[length(found) + 1]] <- row(key, "unregistered_provider", sprintf("provider `%s` is not in metadata/provider.csv", .s(h[["provider"]]$key)))
    found <- c(found, check_urls(key, h[["distributions"]]))
    if (!length(found)) found[[1]] <- row(key, "ok", "holding: name, category, provider and links check out")
    rows <- c(rows, found)
  }
  if (!length(rows)) return(row(character(), character(), character())[0, ])
  do.call(rbind, rows)
}

#' Stop on any non-exempt error finding from [check_dataset_catalog()]
#'
#' @param d the table from [check_dataset_catalog()]
#' @param quiet suppress the messages for warn-level and exempt rows
#' @return `d`, invisibly, when nothing blocks.
#' @export
#' @concept catalog
assert_dataset_catalog <- function(d, quiet = FALSE) {
  fmt <- function(x) paste0("  ", x[["dataset_key"]], "  ", x[["finding"]], ": ", x[["detail"]],
                            ifelse(is.na(x[["url"]]), "", paste0("  ", x[["url"]])), collapse = "\n")
  warn <- d[d[["level"]] == "warn", , drop = FALSE]
  if (nrow(warn) && !quiet)
    message("dataset catalog check: ", nrow(warn), " warning(s) — an endpoint did not answer cleanly; ",
            "retry before treating it as gone:\n", fmt(warn))
  ex <- d[d[["level"]] == "error" & d[["exempt"]], , drop = FALSE]
  if (nrow(ex) && !quiet)
    message("dataset catalog check: ", nrow(ex), " finding(s) exempt while a question is open/proposed: ",
            paste(sprintf("%s (%s, %s)", ex$dataset_key, ex$finding, ex$question), collapse = "; "))
  bad <- d[d[["level"]] == "error" & !d[["exempt"]], , drop = FALSE]
  if (nrow(bad))
    stop("dataset catalog check: ", nrow(bad), " blocking finding(s):\n", fmt(bad),
         "\n  Fix the registry / sidecar / notebook field, status the dead endpoint in metadata/distribution.csv,",
         " or file an open/proposed questions.csv row with related_table = dataset naming the field.", call. = FALSE)
  invisible(d)
}

#' Validate a `datasets.json` against the package's JSON schema
#'
#' The schema is `inst/schema/datasets.schema.json` (draft-07). Uses
#' \pkg{jsonvalidate} when installed; otherwise a structural check of the
#' required top-level and per-record keys, which is what the tests can always
#' run.
#'
#' @param x a `datasets.json` path, its text, or the record list
#' @param schema path to the schema file
#' @param verbose return the validator's error table on failure
#' @return `TRUE`, or stops with the first errors.
#' @export
#' @concept catalog
validate_dataset_catalog <- function(x, schema = system.file("schema", "datasets.schema.json", package = "calcofi4db"),
                                     verbose = TRUE) {
  stopifnot(file.exists(schema))
  txt <- if (is.character(x) && length(x) == 1 && file.exists(x)) paste(readLines(x, warn = FALSE, encoding = "UTF-8"), collapse = "\n") else
    if (is.character(x)) paste(x, collapse = "\n") else
      jsonlite::toJSON(x, auto_unbox = TRUE, digits = NA, null = "null", na = "null")
  if (requireNamespace("jsonvalidate", quietly = TRUE)) {
    v <- jsonvalidate::json_validator(schema, engine = "ajv")
    ok <- v(txt, verbose = verbose)
    if (!isTRUE(ok)) {
      err <- attr(ok, "errors")
      msg <- if (is.data.frame(err) && nrow(err)) paste(utils::head(paste(err$instancePath, err$message), 10), collapse = "\n  ") else "schema violation"
      stop("datasets.json does not validate against ", basename(schema), ":\n  ", msg, call. = FALSE)
    }
    return(TRUE)
  }
  j <- jsonlite::fromJSON(txt, simplifyVector = FALSE)
  need_top <- c("schema_version", "release", "datasets", "holdings", "reference")
  miss <- setdiff(need_top, names(j))
  if (length(miss)) stop("datasets.json is missing: ", paste(miss, collapse = ", "), call. = FALSE)
  need_rec <- c("dataset_key", "provider", "dataset", "category", "visibility", "attribution", "links",
                "coverage", "tables", "objects", "distributions", "registrations", "status")
  for (r in j[["datasets"]]) {
    miss <- setdiff(need_rec, names(r))
    if (length(miss)) stop("record ", .s(r[["dataset_key"]]), " is missing: ", paste(miss, collapse = ", "), call. = FALSE)
  }
  TRUE
}
