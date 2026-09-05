# the identifier a portal calls a dataset by ------------------------------------------
#
# Archives & portals on a calcofi.io dataset page lists Portal · Identifier · Title ·
# Status (UI plan 2026-09-05 § D-6, Decision 8), and the identifier is the thing a
# person actually needs: `edi.109.4`, `gov.noaa.nodc:0301029`, `erdCalCOFIlrvcnt`.
# `metadata/distribution.csv` carries it per curated endpoint; `registrations[]` did
# not, so the site derived it from the URL at build. Schema 1.1 puts it in the record
# and this is the rule that fills it — deliberately the SAME rule as the site's
# `CalCOFI.github.io/_plugins/derive_id.rb`, with the same nine cases pinned by tests
# on both sides, so the record and the page can never disagree about what OBIS calls
# a dataset.

#' The identifier a portal knows a dataset by, read off its own URL
#'
#' A fallback, never an override: a curated `id` in `metadata/distribution.csv` always
#' wins. Returns `NA_character_` when the URL says nothing — a portal home, a search
#' page — because a guessed identifier is worse than none.
#'
#' Recognised: EDI (`packageid=`, or `scope=&identifier=&revision=`), NCEI (`id=`),
#' OBIS (`/dataset/{uuid}`), a GBIF/OBIS IPT resource (`?r=`), any DOI
#' (`doi.org/10.…`), CalOOS (`#module-metadata/{uuid}`), any ERDDAP (a
#' `tabledap`/`griddap` page, an `info` page, or an ISO 19115 / FGDC document),
#' DataZoo (`/datasets/{n}`) and an NCBI BioProject.
#'
#' @param url character vector of URLs
#' @return character vector of the same length
#' @export
#' @concept catalog
#' @examples
#' derive_registration_id("https://obis.org/dataset/0e223f55-c826-4513-ae9a-b04cbf2e189c")
#' derive_registration_id("https://portal.edirepository.org/nis/mapbrowse?packageid=edi.109.4")
derive_registration_id <- function(url) {
  vapply(as.character(url), function(u) {
    if (is.na(u) || !nzchar(trimws(u))) return(NA_character_)
    g <- function(re) {
      m <- regmatches(u, regexec(re, u))[[1]]
      if (length(m) > 1 && nzchar(m[2])) utils::URLdecode(m[2]) else NA_character_
    }
    # EDI: two URL shapes for the same thing
    if (grepl("edirepository\\.org", u)) {
      pid <- g("[?&]packageid=([^&#]+)")
      if (!is.na(pid)) return(pid)
      scope <- g("[?&]scope=([^&#]+)"); ident <- g("[?&]identifier=([^&#]+)"); rev <- g("[?&]revision=([^&#]+)")
      if (!is.na(scope) && !is.na(ident))
        return(paste(stats::na.omit(c(scope, ident, rev)), collapse = "."))
      return(NA_character_)
    }
    if (grepl("ncei\\.noaa\\.gov", u)) return(g("[?&]id=([^&#]+)"))
    x <- g("obis\\.org/dataset/([0-9a-fA-F-]{36})");            if (!is.na(x)) return(x)
    if (grepl("ipt", u)) { x <- g("[?&]r=([^&#]+)");            if (!is.na(x)) return(x) }
    x <- g("doi\\.org/(10\\.[^[:space:]?#]+)");                 if (!is.na(x)) return(x)
    x <- g("#module-metadata/([0-9a-fA-F-]{36})");              if (!is.na(x)) return(x)
    x <- g("/(?:tabledap|griddap)/([A-Za-z0-9_.-]+?)(?:\\.[[:alnum:]]+)?(?:$|[?#])"); if (!is.na(x)) return(x)
    x <- g("/erddap/info/([A-Za-z0-9_.-]+)/");                  if (!is.na(x)) return(x)
    x <- g("/erddap/metadata/[[:alnum:]]+/xml/([A-Za-z0-9_.-]+?)_(?:iso19115|fgdc)\\.xml"); if (!is.na(x)) return(x)
    x <- g("datazoo/catalogs/[^/]+/datasets/([0-9]+)");         if (!is.na(x)) return(x)
    x <- g("ncbi\\.nlm\\.nih\\.gov/bioproject/([0-9]+)");       if (!is.na(x)) return(paste0("PRJNA", x))
    NA_character_
  }, character(1), USE.NAMES = FALSE)
}

# ERDDAP grain -> the sentence a reader needs -------------------------------------------
#
# The site rendered this from a hard-coded map marked `# until the record carries
# grain_description`; the meaning of a grain is a fact about the data, so it belongs
# here (UI plan § D-9).

#' What one ERDDAP grain means, in a sentence
#'
#' @param grain a grain from [.erddap_grain()] (`"observations"`, `"sampling events"`,
#'   `"length/stage frequency"`, `"full resolution (pre-thinning)"`, or a suffix the
#'   generic publisher coined)
#' @return character; `NA_character_` for a grain with no registered sentence, which
#'   `check_dataset_catalog()` reports as `grain_without_description`.
#' @export
#' @concept catalog
erddap_grain_description <- function(grain) {
  d <- c(
    "observations" = paste("one row per measurement — value, units and quality flag —",
                           "joined to the sampling event it was taken on"),
    "sampling events" = paste("one row per cast, tow or transect: when, where and how it",
                              "was sampled, with the effort that scales it"),
    "length/stage frequency" = paste("one row per size or stage class of a specimen, under the",
                                     "occurrence it belongs to"),
    "full resolution (pre-thinning)" = paste("the unthinned series — every bin as the instrument",
                                             "recorded it, before the release's depth thinning"))
  unname(ifelse(is.na(grain) | !(grain %in% names(d)), NA_character_, d[grain]))
}
