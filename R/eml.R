# one EML 2.2 document per dataset ---------------------------------------------------
#
# Plan 2026-09-05 "CalCOFI.io as a dataset catalog" § D-8, Decision 13: the EML
# document is the ONE metadata document the publishers share — the DwC-A's
# `eml.xml`, the EDI package, ERDDAP's globals and the dataset page's JSON-LD all
# derive from it, so none of them is typed twice. `publish_ichthyo_to-obis.qmd`
# built a valid EML 2.2 document the same way (`EML::write_eml()` +
# `eml_validate()`) but from strings hand-typed inside the notebook — sampling
# methods, study extent, rights, funding — which is the one place a provider
# cannot edit and the record cannot see.
#
# Everything here reads the generated record (`datasets.json`, R/catalog_datasets.R),
# the descriptive sidecar (`metadata/{provider}/{dataset}/dataset_meta.yml`) and the
# release's own `metadata.json` / `coverage.json`. **No metadata string is typed in
# this file.** The two constants that are not data — the EML `system` and the CalCOFI
# role address (Decision 23) — are declared once, below, and documented as such.
#
# A required EML element the record cannot supply is a FINDING, never a made-up
# value: `check_eml()` reports it the way `check_dataset_catalog()` reports a
# missing citation, and an open/proposed `questions.csv` row on
# `related_table = dataset` naming the field exempts it. Two fallbacks are
# *derivations from a registry*, not inventions, and each is reported at `warn`
# so it stays visible: an `organizationName`-only creator taken from
# `provider.csv` when the record carries neither `creators[]` nor `pi_names`, and
# the CalCOFI role address as `contact` when no provider address is on record.

#' @keywords internal
CC_EML_SYSTEM <- "calcofi.io"

#' The CalCOFI role address used as the EML contact of last resort
#'
#' Decision 23 of the 2026-09-05 dataset-catalog plan: `data@calcofi.io` is the
#' public contact for CalCOFI data, forwarded today, with
#' `calcofi-data@ucsd.edu` planned as a UCSD Google Group beside it. It is used
#' only when neither the dataset's own `contact` nor a creator email is on
#' record; `check_eml()` reports `contact_role_address` (warn) whenever it is,
#' so a provider address arriving later is an improvement anyone can see is due.
#'
#' @return A length-1 character vector.
#' @export
#' @concept eml
eml_contact_address <- function() "data@calcofi.io"

#' @keywords internal
CC_EML_CONTACT_ORG <- "CalCOFI"

# EML 2.2's `language` is ISO 639-2/B; every CalCOFI dataset is documented in English.
#' @keywords internal
CC_EML_LANGUAGE <- "eng"

#' @keywords internal
CC_ORCID_DIRECTORY <- "https://orcid.org/"

#' @keywords internal
CC_WORMS_PROVIDER <- "https://www.marinespecies.org"

#' @keywords internal
CC_ITIS_PROVIDER <- "https://www.itis.gov"

GEAR_COLS <- c("tow_type", "gear_name", "dwc_samplingProtocol", "nerc_l22", "datasets", "note")

# release column units -> EML standard unit names. Filled ONLY on an exact match,
# the same rule the NERC/DwC id columns follow (workflows/CLAUDE.md): a unit that
# no EML standard unit states exactly is emitted as a `customUnit` carrying the
# release's own string, never coerced onto a near-neighbour. `count/10m2` and
# `count/1000m3` are the standardized densities and have no EML standard unit.
EML_STANDARD_UNITS <- c(
  "m"               = "meter",
  "m3"              = "cubicMeter",
  "decimal degrees" = "degree",
  "count"           = "number",
  "individuals"     = "number",
  "km^2"            = "squareKilometers")

# DuckDB storage type -> EML measurementScale branch
.eml_scale_of <- function(data_type) {
  dt <- toupper(.s(data_type))
  if (grepl("^(TIMESTAMP|DATE|TIME)", dt)) return("dateTime")
  if (grepl("^(DOUBLE|FLOAT|REAL|DECIMAL|NUMERIC|BIGINT|INTEGER|SMALLINT|TINYINT|UBIGINT|UINTEGER|USMALLINT|UTINYINT|HUGEINT)", dt))
    return("ratio")
  "nominal"
}
.eml_number_type <- function(data_type) {
  dt <- toupper(.s(data_type))
  if (grepl("^(BIGINT|INTEGER|SMALLINT|TINYINT|HUGEINT)", dt)) return("integer")
  if (grepl("^(UBIGINT|UINTEGER|USMALLINT|UTINYINT)", dt)) return("whole")
  "real"
}
.eml_datetime_format <- function(data_type) {
  if (grepl("^DATE$", toupper(.s(data_type)))) "YYYY-MM-DD" else "YYYY-MM-DDThh:mm:ss"
}

# markdown -> plain paragraphs, for an EML <para>. Strips the inline syntax EML
# has no place for (emphasis, code ticks, link brackets) and keeps the link text
# plus its target, so nothing a reader needs is dropped.
.md_paras <- function(x) {
  x <- paste(as.character(unlist(x)), collapse = "\n")
  if (!nzchar(trimws(x))) return(character())
  ps <- strsplit(x, "\n[[:space:]]*\n")[[1]]
  ps <- vapply(ps, function(p) {
    p <- gsub("\\[([^]]+)\\]\\(([^)]+)\\)", "\\1 (\\2)", p)
    # `*` and backticks are markdown; `_` is NOT stripped — `sample_key`,
    # `parent_sample_key` and every other identifier in these descriptions would
    # otherwise arrive as `samplekey` in the attributeDefinition
    p <- gsub("[*`]{1,3}", "", p)
    p <- gsub("^[[:space:]]*#+[[:space:]]*", "", p)
    p <- gsub("[[:space:]]+", " ", p)
    trimws(p)
  }, "", USE.NAMES = FALSE)
  ps[nzchar(ps)]
}
.n_words <- function(x) {
  x <- trimws(paste(as.character(unlist(x)), collapse = " "))
  if (!nzchar(x)) return(0L)
  length(strsplit(x, "[[:space:]]+")[[1]])
}

# "Todd Martz" -> given/sur; "Martz, Todd" -> the same; a single token is a surname
.split_person <- function(nm) {
  nm <- trimws(.s(nm))
  if (!nzchar(nm)) return(NULL)
  if (grepl(",", nm, fixed = TRUE)) {
    parts <- trimws(strsplit(nm, ",", fixed = TRUE)[[1]])
    sur <- parts[1]; given <- paste(parts[-1], collapse = " ")
  } else {
    toks <- strsplit(nm, "[[:space:]]+")[[1]]
    sur <- toks[length(toks)]; given <- paste(toks[-length(toks)], collapse = " ")
  }
  out <- list(surName = sur)
  if (nzchar(trimws(given))) out <- c(list(givenName = trimws(given)), out)
  out
}

# one EML responsibleParty from a record creator {name, organization, orcid, email, role}
.eml_party <- function(name = NULL, organization = NULL, orcid = NULL, email = NULL,
                       position = NULL, role = NULL) {
  p <- list()
  ind <- .split_person(name)
  if (!is.null(ind)) p$individualName <- ind
  if (nzchar(.s(organization))) p$organizationName <- .s(organization)
  if (nzchar(.s(position))) p$positionName <- .s(position)
  if (nzchar(.s(email))) p$electronicMailAddress <- .s(email)
  if (nzchar(.s(orcid))) {
    id <- sub("^https?://orcid\\.org/", "", .s(orcid))
    p$userId <- list(directory = CC_ORCID_DIRECTORY, userId = id)
  }
  if (nzchar(.s(role))) p$role <- .s(role)
  if (!length(p)) NULL else p
}

# gear registry ------------------------------------------------------------------------

#' Read `metadata/gear.csv`, the net-gear registry
#'
#' One row per `sample.tow_type` code with `gear_name`, the
#' `dwc_samplingProtocol` sentence a Darwin Core / EML sampling description
#' needs, the NERC L22 device URI where one is exact, the `datasets` that use
#' the code (`;`-separated) and a `note`. [build_eml()] appends each of a
#' dataset's protocol sentences to its `samplingDescription`.
#'
#' @param path path to `metadata/gear.csv`
#' @return A [tibble][tibble::tibble], all columns character.
#' @export
#' @concept eml
read_gear_registry <- function(path) {
  .read_registry_csv(path, GEAR_COLS, "gear")
}

#' @rdname read_gear_registry
#' @param gear the tibble from [read_gear_registry()] (or NULL)
#' @param dataset_key the dataset to filter to
#' @return `dataset_gear()`: the rows whose `datasets` names `dataset_key`.
#' @export
dataset_gear <- function(gear, dataset_key) {
  if (is.null(gear) || !nrow(gear)) return(gear)
  hit <- vapply(gear[["datasets"]], function(d)
    dataset_key %in% trimws(strsplit(.s(d), ";", fixed = TRUE)[[1]]), logical(1))
  gear[hit, , drop = FALSE]
}

# the pieces of one document -----------------------------------------------------------

# creator[]: creators[] -> pi_names (+ the provider organization) -> the provider
# organization alone. The last is a registry value, not an invented one, and
# check_eml() reports it as `creator_from_provider`.
.eml_creators <- function(record) {
  att <- record[["attribution"]] %||% list()
  org <- .chr_or_null(record[["provider"]]$name) %||% .chr_or_null(record[["provider"]]$short)
  cre <- .rows(att[["creators"]])
  if (length(cre)) {
    # EML's <creator> is a plain responsibleParty: it takes NO <role> (only
    # associatedParty and project/personnel do), so the sidecar's role travels
    # beside the party and is applied where the schema allows it
    keep <- !vapply(cre, function(cr) is.null(.eml_party(
      name = cr[["name"]], organization = cr[["organization"]], orcid = cr[["orcid"]],
      email = cr[["email"]])), logical(1))
    cre <- cre[keep]
    if (length(cre)) {
      out <- lapply(cre, function(cr) .eml_party(
        name = cr[["name"]], organization = cr[["organization"]], orcid = cr[["orcid"]],
        email = cr[["email"]]))
      roles <- vapply(cre, function(cr) .s(cr[["role"]]), "")
      return(list(parties = out, roles = roles, source = "creators"))
    }
  }
  pis <- as.character(unlist(att[["pi_names"]]))
  pis <- trimws(pis[!is.na(pis) & nzchar(trimws(pis))])
  if (length(pis)) {
    out <- Filter(Negate(is.null), lapply(pis, function(p) .eml_party(name = p, organization = org)))
    if (length(out))
      return(list(parties = out, roles = rep("principalInvestigator", length(out)), source = "pi_names"))
  }
  if (!is.null(org))
    return(list(parties = list(list(organizationName = org)), roles = "originator", source = "provider"))
  list(parties = list(), roles = character(), source = "none")
}

# contact: the dataset's own address -> a creator with an email -> the CalCOFI role
# address (Decision 23)
.eml_contact <- function(record, creators) {
  att <- record[["attribution"]] %||% list()
  org <- .chr_or_null(record[["provider"]]$name) %||% .chr_or_null(record[["provider"]]$short)
  ct  <- .s(att[["contact"]])
  if (nzchar(ct)) {
    if (grepl("^https?://", ct))
      return(list(party = list(organizationName = org %||% CC_EML_CONTACT_ORG,
                               onlineUrl = ct), source = "dataset"))
    return(list(party = list(organizationName = org %||% CC_EML_CONTACT_ORG,
                             electronicMailAddress = ct), source = "dataset"))
  }
  with_mail <- Filter(function(p) nzchar(.s(p[["electronicMailAddress"]])), creators)
  if (length(with_mail)) return(list(party = with_mail[[1]], source = "creator"))
  list(party = list(organizationName = CC_EML_CONTACT_ORG,
                    electronicMailAddress = eml_contact_address()),
       source = "role")
}

# keywordSet: the GCMD terms under their thesaurus, everything else as free keywords
# (the record merges both into `keywords`, so a GCMD term is recognised by its own
# "EARTH SCIENCE >" stem; the sidecar, when given, separates them exactly)
.eml_keyword_sets <- function(record, sidecar = NULL) {
  kws <- as.character(unlist(record[["keywords"]]))
  gcmd <- as.character(unlist((sidecar %||% list())[["keywords_gcmd"]]))
  if (!length(gcmd)) gcmd <- grep("^EARTH SCIENCE", kws, value = TRUE)
  free <- setdiff(kws, gcmd)
  cat_name <- .chr_or_null(record[["category"]]$name)
  vars <- as.character(unlist(record[["coverage"]]$variables))
  free <- unique(c(free, cat_name, vars))
  free <- free[!is.na(free) & nzchar(trimws(free))]
  sets <- list()
  if (length(gcmd))
    sets[[length(sets) + 1]] <- list(keyword = I(unname(gcmd)),
                                     keywordThesaurus = "GCMD Science Keywords")
  if (length(free))
    sets[[length(sets) + 1]] <- list(keyword = I(unname(free)))
  sets
}

.eml_coverage <- function(record, coverage = NULL, dataset_key = NULL) {
  cov <- record[["coverage"]] %||% list()
  out <- list()
  bb <- cov[["bbox"]]
  if (!is.null(bb) && all(vapply(c("lat_min", "lat_max", "lon_min", "lon_max"),
                                 function(k) .has_value(bb[[k]]), logical(1)))) {
    desc <- .chr_or_null(cov[["spatial"]]) %||% .chr_or_null(record[["dataset_name"]])
    out$geographicCoverage <- list(
      geographicDescription = desc,
      boundingCoordinates = list(
        westBoundingCoordinate  = as.numeric(bb$lon_min),
        eastBoundingCoordinate  = as.numeric(bb$lon_max),
        northBoundingCoordinate = as.numeric(bb$lat_max),
        southBoundingCoordinate = as.numeric(bb$lat_min)))
  }
  y0 <- .int_or_null(cov[["year_min"]]); y1 <- .int_or_null(cov[["year_max"]])
  if (is.null(y0) || is.null(y1)) {
    # coverage.json carries no per-dataset year span for a few datasets, but the
    # measured `coverage.temporal` string ("1996 to 2022", "1939-05 to 2024-04")
    # does — take its first and last 4-digit years rather than dropping the
    # element. Still measured: nothing here supplies a year the record lacks.
    yrs <- regmatches(.s(cov[["temporal"]]), gregexpr("(18|19|20)[0-9]{2}", .s(cov[["temporal"]])))[[1]]
    if (length(yrs) >= 2) { y0 <- as.integer(yrs[1]); y1 <- as.integer(yrs[length(yrs)]) }
  }
  if (!is.null(y0) && !is.null(y1)) {
    # EML's calendarDate is xs:date or xs:gYear — a "YYYY-MM" span cannot be
    # expressed, and inventing a day-of-month would assert something measured
    # coverage never said. The observed years are the honest range.
    out$temporalCoverage <- list(rangeOfDates = list(
      beginDate = list(calendarDate = sprintf("%04d", y0)),
      endDate   = list(calendarDate = sprintf("%04d", y1))))
  }
  tx <- .eml_taxonomic_classification(coverage, dataset_key %||% .s(record[["dataset_key"]]))
  if (length(tx)) {
    gen <- .chr_or_null(record[["category"]]$name)
    out$taxonomicCoverage <- c(
      if (!is.null(gen)) list(generalTaxonomicCoverage = gen) else list(),
      list(taxonomicClassification = tx))
  }
  out
}

.eml_taxonomic_classification <- function(coverage, dataset_key) {
  if (is.null(coverage) || !nzchar(.s(dataset_key))) return(list())
  taxa <- Filter(function(t) any(vapply(.rows(t[["datasets"]]),
                                        function(d) identical(.s(d[["dataset_key"]]), dataset_key), logical(1))),
                 .rows(coverage[["taxa"]]))
  out <- list()
  for (t in taxa) {
    sci <- .s(t[["scientific_name"]])
    if (!nzchar(sci)) next
    key <- .s(t[["taxon_key"]])
    item <- list(taxonRankValue = sci)
    rk <- .s(t[["rank"]])
    if (nzchar(rk)) item <- c(list(taxonRankName = tolower(rk)), item)
    if (grepl("^worms:[0-9]+$", key))
      item$taxonId <- list(provider = CC_WORMS_PROVIDER, taxonId = sub("^worms:", "", key))
    else if (grepl("^itis:[0-9]+$", key))
      item$taxonId <- list(provider = CC_ITIS_PROVIDER, taxonId = sub("^itis:", "", key))
    cn <- .s(t[["common_name"]])
    if (nzchar(cn)) item$commonName <- cn
    out[[length(out) + 1]] <- item
  }
  out[order(vapply(out, function(x) .s(x[["taxonRankValue"]]), ""))]
}

# methods: the sidecar's narrative fields + the gear registry's protocol sentences
.eml_methods <- function(sidecar, gear_rows = NULL) {
  sc <- sidecar %||% list()
  steps <- list()
  m <- .md_paras(sc[["methods_md"]])
  if (length(m)) steps[[length(steps) + 1]] <- list(description = list(para = I(m)))
  protocols <- character()
  if (!is.null(gear_rows) && nrow(gear_rows))
    protocols <- unique(stats::na.omit(gear_rows[["dwc_samplingProtocol"]]))
  protocols <- protocols[nzchar(trimws(protocols))]
  if (!length(steps) && length(protocols))
    steps[[length(steps) + 1]] <- list(description = list(para = I(unname(protocols))))
  q <- .md_paras(sc[["quality_control_md"]])
  if (length(q))
    steps[[length(steps) + 1]] <- list(description = list(para = I(q)))
  if (!length(steps)) return(NULL)
  out <- list(methodStep = steps)
  ext <- .md_paras(sc[["study_extent"]])
  samp <- c(.md_paras(sc[["sampling_description"]]),
            if (length(steps) > 0 && length(protocols)) unname(protocols) else character())
  # EML requires BOTH studyExtent and samplingDescription inside <sampling>; emit the
  # block only when the record supplies both, never a placeholder for the missing half
  if (length(ext) && length(samp))
    out$sampling <- list(
      studyExtent = list(description = list(para = I(ext))),
      samplingDescription = list(para = I(samp)))
  out
}

# dataTable[]: the dataset's release tables, with the attributeList metadata.json's
# columns{} block describes ("{table}.{column}")
.eml_data_tables <- function(record, meta = NULL) {
  tabs <- as.character(unlist(record[["tables"]]))
  tabs <- tabs[!is.na(tabs) & nzchar(tabs)]
  if (!length(tabs)) return(list(tables = list(), undocumented = 0L, custom_units = character()))
  mt <- (meta %||% list())[["tables"]] %||% list()
  mc <- (meta %||% list())[["columns"]] %||% list()
  objs <- .rows(record[["objects"]])
  undoc <- 0L; custom <- character()
  out <- list()
  for (tb in tabs) {
    cn_all <- if (length(mc)) names(mc) else character()
    cols <- cn_all[startsWith(cn_all, paste0(tb, "."))]
    attrs <- list()
    for (cn in cols) {
      col <- mc[[cn]]
      nm  <- substring(cn, nchar(tb) + 2)
      lab <- .chr_or_null(col[["name_long"]])
      # attributeDefinition is required by EML; the column's own documented
      # description, else its long name, else the column name — every one of
      # those is a fact the release already carries
      def <- .chr_or_null(col[["description_md"]])
      if (is.null(def)) { undoc <- undoc + 1L; def <- lab %||% nm }
      a <- list(attributeName = nm)
      if (!is.null(lab)) a$attributeLabel <- lab
      a$attributeDefinition <- paste(.md_paras(def), collapse = " ")
      dt <- .s(col[["data_type"]])
      if (nzchar(dt)) a$storageType <- dt
      scale <- .eml_scale_of(dt)
      units <- .s(col[["units"]])
      if (scale == "ratio") {
        std <- if (nzchar(units)) unname(EML_STANDARD_UNITS[units]) else NA_character_
        u <- if (!is.na(std)) list(standardUnit = std) else
          if (nzchar(units)) { custom <- c(custom, units); list(customUnit = units) } else
            list(standardUnit = "dimensionless")
        a$measurementScale <- list(ratio = list(
          unit = u, numericDomain = list(numberType = .eml_number_type(dt))))
      } else if (scale == "dateTime") {
        a$measurementScale <- list(dateTime = list(formatString = .eml_datetime_format(dt)))
      } else {
        a$measurementScale <- list(nominal = list(nonNumericDomain = list(
          textDomain = list(definition = a$attributeDefinition))))
      }
      attrs[[length(attrs) + 1]] <- a
    }
    if (!length(attrs)) next
    e <- list(entityName = tb)
    d <- .chr_or_null(mt[[tb]]$description_md)
    if (!is.null(d)) e$entityDescription <- paste(.md_paras(d), collapse = " ")
    o <- Filter(function(x) identical(.s(x[["table"]]), tb), objs)
    if (length(o)) {
      o <- o[[1]]
      phys <- list(objectName = basename(.s(o[["path"]])))
      if (.has_value(o[["bytes"]])) phys$size <- list(unit = "bytes", size = as.character(o[["bytes"]]))
      if (.has_value(o[["sha256"]]))
        phys$authentication <- list(method = "SHA-256", authentication = .s(o[["sha256"]]))
      phys$dataFormat <- list(externallyDefinedFormat = list(formatName = "Apache Parquet"))
      if (.has_value(o[["url"]]))
        phys$distribution <- list(online = list(url = list(`function` = "download", url = .s(o[["url"]]))))
      e$physical <- phys
    }
    e$attributeList <- list(attribute = attrs)
    out[[length(out) + 1]] <- e
  }
  list(tables = out, undocumented = undoc, custom_units = unique(custom))
}

# build --------------------------------------------------------------------------------

#' Build one dataset's EML 2.2 document from the catalog record
#'
#' Plan § D-8: `eml/{dataset_key}.xml` in every release, generated from the
#' record and the descriptive sidecar so the DwC-A, the EDI package, ERDDAP's
#' globals and the page's JSON-LD all read one document. The mapping, field by
#' field:
#'
#' | EML | from |
#' |---|---|
#' | `packageId` / `system` | `{dataset_key}.{release version}` / `calcofi.io` |
#' | `dataset/alternateIdentifier` | `attribution.doi` (as a DOI URL) and `links.page` |
#' | `dataset/shortName` · `title` | `dataset_name_short` · `dataset_name` |
#' | `dataset/creator` | `attribution.creators[]`, else `pi_names` with the provider organization, else the provider organization alone |
#' | `dataset/pubDate` | the release `release_date` |
#' | `dataset/language` | `eng` |
#' | `dataset/abstract` | `description_md`, rendered to paragraphs |
#' | `dataset/keywordSet` | `keywords` — the GCMD terms under their thesaurus, plus the category and the observed variable names |
#' | `dataset/intellectualRights` · `licensed` | `attribution.license` / `license_name` / `license_url` (`metadata/license.csv`) |
#' | `dataset/distribution` | `links.page` |
#' | `dataset/coverage` | geographic from `coverage.bbox` + `coverage.spatial`; temporal from `coverage.year_min/max`; taxonomic from `coverage.json`'s `taxa[]` for this dataset (WoRMS / ITIS ids) |
#' | `dataset/maintenance` | the sidecar's `maintenance` |
#' | `dataset/contact` | `attribution.contact`, else a creator email, else [eml_contact_address()] |
#' | `dataset/methods` | the sidecar's `methods_md`, `quality_control_md`, `study_extent`, `sampling_description`, with `metadata/gear.csv`'s `dwc_samplingProtocol` sentences for the dataset's `tow_type`s |
#' | `dataset/project` | `dataset_name` + `attribution.funding` (else `acknowledgement`), personnel from the creators |
#' | `dataset/dataTable[]` | the record's `tables[]`, `attributeList` from `metadata.json`'s `columns{}` (label, definition, units, storage type), `physical` from the record's `objects[]` |
#' | `additionalMetadata` | the release citation, the dataset citation and the record's own provenance |
#'
#' Absent optional fields are omitted; a missing **required** field is a
#' [check_eml()] finding, never a placeholder.
#'
#' @param record one dataset record (an element of `datasets.json`'s `datasets[]`)
#' @param sidecar the dataset's `dataset_meta.yml` as a list (the narrative
#'   fields — `methods_md`, `study_extent`, `sampling_description`,
#'   `quality_control_md`, `maintenance`, `associated_parties` — live only there)
#' @param meta the release `metadata.json` (path or parsed list), for `columns{}`
#' @param coverage the release `coverage.json` (path or parsed list), for `taxa[]`
#' @param release the `release` block of `datasets.json` (version, date, citation);
#'   defaults to the record's own `release` element when it carries one
#' @param gear the tibble from [read_gear_registry()], or NULL
#' @return A named list ready for `EML::write_eml()`, carrying an
#'   `"eml_notes"` attribute (which fallbacks were used) that [check_eml()] reads.
#' @export
#' @concept eml
#' @seealso [check_eml()], [write_eml_files()]
build_eml <- function(record, sidecar = NULL, meta = NULL, coverage = NULL, release = NULL,
                      gear = NULL) {
  stopifnot(is.list(record), nzchar(.s(record[["dataset_key"]])))
  key <- .s(record[["dataset_key"]])
  meta <- if (is.null(meta)) NULL else .read_json(meta)
  coverage <- if (is.null(coverage)) NULL else .read_json(coverage)
  release <- release %||% record[["release"]] %||% list()
  sc <- sidecar %||% list()
  att <- record[["attribution"]] %||% list()

  cre <- .eml_creators(record)
  ct  <- .eml_contact(record, cre$parties)
  gear_rows <- dataset_gear(gear, key)

  ds <- list()
  sn <- .chr_or_null(record[["dataset_name_short"]])
  if (!is.null(sn)) ds$shortName <- sn
  ds$title <- .chr_or_null(record[["dataset_name"]])
  # alternateIdentifier: the DOI and the canonical dataset page
  alt <- c(if (nzchar(.s(att[["doi"]]))) paste0("https://doi.org/", .s(att[["doi"]])) else NULL,
           .chr_or_null(record[["links"]]$page))
  if (length(alt)) ds$alternateIdentifier <- I(unname(alt))
  if (length(cre$parties)) ds$creator <- cre$parties
  pd <- .chr_or_null(release[["release_date"]])
  if (!is.null(pd)) ds$pubDate <- pd
  ds$language <- CC_EML_LANGUAGE
  abs_paras <- .md_paras(record[["description_md"]])
  if (length(abs_paras)) ds$abstract <- list(para = I(abs_paras))
  ks <- .eml_keyword_sets(record, sc)
  if (length(ks)) ds$keywordSet <- ks
  # associated parties: the sidecar's, with their declared role
  ap <- Filter(Negate(is.null), lapply(.rows(sc[["associated_parties"]]), function(p) .eml_party(
    name = p[["name"]], organization = p[["organization"]], orcid = p[["orcid"]],
    email = p[["email"]], role = p[["role"]] %||% "associatedParty")))
  if (length(ap)) ds$associatedParty <- ap
  ack <- .chr_or_null(att[["acknowledgement"]])
  # intellectualRights: the licence name + URL the registry states, never a guess
  lic_id <- .chr_or_null(att[["license"]])
  lic_nm <- .chr_or_null(att[["license_name"]])
  lic_url <- .chr_or_null(att[["license_url"]])
  if (!is.null(lic_id) && !identical(lic_id, "unknown")) {
    rights <- paste(c(lic_nm %||% lic_id, if (!is.null(lic_url)) paste0("(", lic_url, ")")), collapse = " ")
    ds$intellectualRights <- list(para = I(rights))
    if (!is.null(lic_nm) && !is.null(lic_url) && !identical(lic_id, "custom"))
      ds$licensed <- list(licenseName = lic_nm, url = lic_url, identifier = lic_id)
  }
  page <- .chr_or_null(record[["links"]]$page)
  if (!is.null(page))
    ds$distribution <- list(scope = "document",
                            online = list(url = list(`function` = "information", url = page)))
  cov <- .eml_coverage(record, coverage, key)
  if (length(cov)) ds$coverage <- cov
  mnt <- .md_paras(sc[["maintenance"]])
  if (length(mnt)) ds$maintenance <- list(description = list(para = I(mnt)))
  ds$contact <- ct$party
  mth <- .eml_methods(sc, gear_rows)
  if (!is.null(mth)) ds$methods <- mth
  fund <- .md_paras(sc[["funding"]] %||% att[["funding"]] %||% ack)
  if (length(fund) && length(cre$parties))
    ds$project <- list(
      title = .chr_or_null(record[["dataset_name"]]) %||% key,
      personnel = lapply(seq_along(cre$parties), function(i) {
        p <- cre$parties[[i]]
        r <- if (i <= length(cre$roles)) .s(cre$roles[i]) else ""
        p$role <- if (nzchar(r)) r else "originator"
        p }),
      funding = list(para = I(fund)))
  dts <- .eml_data_tables(record, meta)
  if (length(dts$tables)) ds$dataTable <- dts$tables

  doc <- list(packageId = paste0(key, ".", .s(release[["version"]])),
              system = CC_EML_SYSTEM,
              dataset = ds)
  # additionalMetadata: the citations, so a copy of the document alone says how to
  # cite the dataset and the release it was cut from
  am <- list(datasetKey = key)
  if (nzchar(.s(release[["version"]]))) am$release <- .s(release[["version"]])
  if (nzchar(.s(release[["citation"]]))) am$releaseCitation <- .s(release[["citation"]])
  if (nzchar(.s(att[["citation_main"]]))) am$datasetCitation <- .s(att[["citation_main"]])
  oth <- as.character(unlist(att[["citation_others"]]))
  if (length(oth)) am$otherCitation <- I(unname(oth))
  if (!is.null(ack)) am$acknowledgement <- ack
  if (nzchar(.s(att[["source_accessed"]]))) am$sourceAccessed <- .s(att[["source_accessed"]])
  if (!is.null(page)) am$datasetPage <- page
  doc$additionalMetadata <- list(metadata = list(calcofi = am))

  attr(doc, "eml_notes") <- list(
    dataset_key = key, creator_source = cre$source, contact_source = ct$source,
    undocumented_attributes = dts$undocumented, custom_units = dts$custom_units,
    n_tables = length(dts$tables), n_taxa = length(cov$taxonomicCoverage$taxonomicClassification),
    has_license = !is.null(ds$intellectualRights), has_methods = !is.null(mth))
  doc
}

#' Build an EML document for every dataset in the catalog record
#'
#' @param catalog the record from [build_dataset_catalog()] (or a `datasets.json` path)
#' @param sidecars the named list from [read_dataset_sidecars()] (or a
#'   `registries` list, whose `sidecars` element is used)
#' @param meta,coverage,gear passed to [build_eml()]
#' @return A named list of EML documents, keyed by `dataset_key`.
#' @export
#' @concept eml
build_eml_catalog <- function(catalog, sidecars = NULL, meta = NULL, coverage = NULL, gear = NULL) {
  catalog <- .read_json(catalog)
  if (!is.null(sidecars) && !is.null(sidecars[["sidecars"]])) sidecars <- sidecars[["sidecars"]]
  meta <- if (is.null(meta)) NULL else .read_json(meta)
  coverage <- if (is.null(coverage)) NULL else .read_json(coverage)
  rel <- catalog[["release"]]
  recs <- .rows(catalog[["datasets"]])
  out <- list()
  for (r in recs) {
    k <- .s(r[["dataset_key"]])
    out[[k]] <- build_eml(r, sidecar = (sidecars %||% list())[[k]], meta = meta,
                          coverage = coverage, release = rel, gear = gear)
  }
  out
}

#' Write `eml/{dataset_key}.xml` for every built document
#'
#' `EML::write_eml()` renders the document; the file lands under `eml/` in the
#' release directory beside `datasets.json`.
#'
#' @param docs the named list from [build_eml_catalog()] (or one [build_eml()] document)
#' @param dir the release directory (an `eml/` subdirectory is created)
#' @return A named character vector of paths, keyed by `dataset_key`, invisibly.
#' @export
#' @concept eml
write_eml_files <- function(docs, dir) {
  if (!requireNamespace("EML", quietly = TRUE))
    stop("Package 'EML' is required to write EML documents", call. = FALSE)
  if (!is.null(docs[["dataset"]])) docs <- stats::setNames(list(docs), .s(attr(docs, "eml_notes")$dataset_key))
  d <- file.path(dir, "eml")
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  paths <- character()
  for (k in names(docs)) {
    p <- file.path(d, paste0(k, ".xml"))
    EML::write_eml(docs[[k]], p)
    paths[k] <- p
  }
  invisible(paths)
}

# check --------------------------------------------------------------------------------

#' The findings [check_eml()] can report, with their level
#'
#' `error` findings fail the release unless exempt (an open/proposed
#' `questions.csv` row on `related_table = dataset` naming the field, or naming
#' none, covers it — the same rule [check_dataset_catalog()] applies to
#' `no_citation`); `warn` findings never block but are always printed, because
#' each one names a real gap in the record.
#'
#' * `invalid_eml` — `EML::eml_validate()` rejected the written document.
#' * `no_title`, `no_abstract`, `no_pub_date`, `no_geographic_coverage`,
#'   `no_temporal_coverage`, `no_data_table` — an element EDI's EML checklist
#'   requires that the record could not supply.
#' * `no_creator` — no `creators[]`, no `pi_names` and no registered provider.
#' * `no_license` — no `license` on the record (exempt while a licence question is open).
#' * `short_abstract` — under 20 words (EDI's guidance; the record's own text, not a stub).
#' * `creator_from_provider` — the creator is the provider organization, because
#'   the record names no person.
#' * `contact_role_address` — the contact is [eml_contact_address()], because no
#'   provider address is on record.
#' * `creator_no_organization`, `no_keywords`, `no_methods`,
#'   `no_taxonomic_coverage`, `undocumented_attributes`, `custom_units` — a gap
#'   that weakens the document without invalidating it.
#'
#' @return A named character vector, finding -> level.
#' @export
#' @concept eml
eml_findings <- function() c(
  ok                      = "ok",
  invalid_eml             = "error",
  no_title                = "error",
  no_abstract             = "error",
  no_creator              = "error",
  no_pub_date             = "error",
  no_license              = "error",
  no_geographic_coverage  = "error",
  no_temporal_coverage    = "error",
  no_data_table           = "error",
  short_abstract          = "warn",
  creator_from_provider   = "warn",
  creator_no_organization = "warn",
  contact_role_address    = "warn",
  no_keywords             = "warn",
  no_methods              = "warn",
  no_taxonomic_coverage   = "warn",
  undocumented_attributes = "warn",
  custom_units            = "warn")

# which record fields a question must name to exempt a finding (an empty
# related_field exempts anything, as in check_dataset_catalog())
EML_FINDING_FIELDS <- list(
  no_creator = c("creators", "pi_names", "contact"),
  no_license = c("license", "license_url"))

#' Check one dataset's EML document
#'
#' The schema half is `EML::eml_validate()` — EML 2.2's XSDs ship with
#' \pkg{emld}, so nothing here touches the network. The rest is the required-element
#' checklist EDI's evaluate applies (see [eml_findings()]), read off the built
#' document rather than off the record, so what is asserted is what was written.
#'
#' @param doc an EML document from [build_eml()], or a path to a written `.xml`
#'   (the fallback notes are only available from the document)
#' @param path the written file to validate; defaults to `doc` when `doc` is a path
#' @param record the dataset's catalog record, for the question-row exemptions
#' @param validate run `EML::eml_validate()` (default TRUE)
#' @return A [tibble][tibble::tibble]: `dataset_key`, `finding`, `level`,
#'   `detail`, `exempt`, `question` — the shape [check_dataset_catalog()] returns.
#' @export
#' @concept eml
#' @seealso [assert_eml()]
check_eml <- function(doc, path = NULL, record = NULL, validate = TRUE) {
  if (is.character(doc) && length(doc) == 1 && file.exists(doc)) {
    path <- path %||% doc
    doc <- NULL
  }
  notes <- if (is.null(doc)) list() else (attr(doc, "eml_notes") %||% list())
  key <- .s(notes[["dataset_key"]])
  if (!nzchar(key)) key <- .s((record %||% list())[["dataset_key"]])
  if (!nzchar(key) && !is.null(path)) key <- sub("\\.xml$", "", basename(path))
  levels <- eml_findings()

  # a question naming one of the finding's fields (or naming none) exempts it
  qd <- .rows((record %||% list())[["status"]]$questions_dataset)
  exempt_for <- function(finding) {
    flds <- EML_FINDING_FIELDS[[finding]]
    if (is.null(flds) || !length(qd)) return(list(exempt = FALSE, question = NA_character_))
    hit <- vapply(qd, function(q) !nzchar(.s(q[["field"]])) || .s(q[["field"]]) %in% flds, logical(1))
    list(exempt = any(hit),
         question = if (any(hit)) paste(vapply(qd[hit], function(q) .s(q[["label"]]), ""), collapse = "; ") else NA_character_)
  }
  rows <- list()
  row <- function(finding, detail) {
    e <- exempt_for(finding)
    rows[[length(rows) + 1]] <<- tibble::tibble(
      dataset_key = key, finding = finding, level = unname(levels[finding]),
      detail = detail, exempt = e$exempt, question = e$question)
  }

  ds <- (doc %||% list())[["dataset"]] %||% list()
  if (!is.null(doc)) {
    if (!nzchar(.s(ds[["title"]]))) row("no_title", "dataset/title is empty (dataset_name is null)")
    abs_words <- .n_words(ds[["abstract"]]$para)
    if (abs_words == 0L) row("no_abstract", "dataset/abstract is empty (description_md is null)")
    else if (abs_words < 20L) row("short_abstract", sprintf("dataset/abstract is %d words (EDI asks for 20+)", abs_words))
    if (!length(ds[["creator"]])) row("no_creator", "no creators[], no pi_names and no registered provider")
    else {
      if (identical(.s(notes[["creator_source"]]), "provider"))
        row("creator_from_provider", "no creators[] or pi_names on the record; the creator is the provider organization")
      no_org <- vapply(.rows(ds[["creator"]]), function(p) !nzchar(.s(p[["organizationName"]])), logical(1))
      if (any(no_org)) row("creator_no_organization", sprintf("%d creator(s) with no organizationName", sum(no_org)))
    }
    if (!nzchar(.s(ds[["pubDate"]]))) row("no_pub_date", "dataset/pubDate is empty (no release_date)")
    if (is.null(ds[["intellectualRights"]])) row("no_license", "no intellectualRights (attribution.license is null)")
    if (!length(ds[["keywordSet"]])) row("no_keywords", "no keywordSet")
    if (is.null(ds[["coverage"]]$geographicCoverage))
      row("no_geographic_coverage", "no geographicCoverage (coverage.bbox is null or incomplete)")
    if (is.null(ds[["coverage"]]$temporalCoverage))
      row("no_temporal_coverage", "no temporalCoverage (coverage.year_min/year_max are null)")
    if (identical(.s((record %||% list())[["coverage"]]$realm), "bio") &&
        is.null(ds[["coverage"]]$taxonomicCoverage))
      row("no_taxonomic_coverage", "a bio dataset with no taxonomicCoverage (no taxa in coverage.json)")
    if (identical(.s(notes[["contact_source"]]), "role"))
      row("contact_role_address", sprintf("no dataset contact on record; using the CalCOFI role address %s", eml_contact_address()))
    if (is.null(ds[["methods"]]))
      row("no_methods", "no methods (no methods_md, quality_control_md or gear protocol on record)")
    if (!length(ds[["dataTable"]]))
      row("no_data_table", "no dataTable (no documented columns for this dataset's tables)")
    if (isTRUE(notes[["undocumented_attributes"]] > 0))
      row("undocumented_attributes", sprintf("%d attribute(s) fell back to the column name for attributeDefinition",
                                             as.integer(notes[["undocumented_attributes"]])))
    if (length(notes[["custom_units"]]))
      row("custom_units", sprintf("no EML standard unit states exactly: %s",
                                  paste(notes[["custom_units"]], collapse = ", ")))
  }
  if (isTRUE(validate) && !is.null(path)) {
    if (!requireNamespace("EML", quietly = TRUE))
      stop("Package 'EML' is required to validate EML documents", call. = FALSE)
    v <- EML::eml_validate(path)
    if (!isTRUE(as.logical(v))) {
      err <- attr(v, "errors")
      msg <- if (length(err)) paste(utils::head(as.character(err), 5), collapse = " | ") else "schema violation"
      row("invalid_eml", msg)
    }
  }
  if (!length(rows))
    return(tibble::tibble(dataset_key = key, finding = "ok", level = "ok",
                          detail = "EML 2.2 valid; every required element present",
                          exempt = FALSE, question = NA_character_))
  do.call(rbind, rows)
}

#' Check every dataset's EML document
#'
#' @param docs the named list from [build_eml_catalog()]
#' @param paths the named character vector from [write_eml_files()] (NULL: no
#'   schema validation)
#' @param catalog the catalog record, for each dataset's question rows
#' @return One [tibble][tibble::tibble] over every dataset.
#' @export
#' @concept eml
check_eml_catalog <- function(docs, paths = NULL, catalog = NULL) {
  catalog <- if (is.null(catalog)) NULL else .read_json(catalog)
  recs <- .rows((catalog %||% list())[["datasets"]])
  by_key <- stats::setNames(recs, vapply(recs, function(r) .s(r[["dataset_key"]]), ""))
  out <- lapply(names(docs), function(k)
    check_eml(docs[[k]], path = if (is.null(paths)) NULL else unname(paths[k]),
              record = by_key[[k]], validate = !is.null(paths)))
  if (!length(out)) return(check_eml(list(), validate = FALSE)[0, ])
  do.call(rbind, out)
}

#' Stop on any non-exempt error finding from [check_eml()]
#'
#' @param d the table from [check_eml()] / [check_eml_catalog()]
#' @param quiet suppress the messages for warn-level and exempt rows
#' @return `d`, invisibly, when nothing blocks.
#' @export
#' @concept eml
assert_eml <- function(d, quiet = FALSE) {
  fmt <- function(x) paste0("  ", x[["dataset_key"]], "  ", x[["finding"]], ": ", x[["detail"]], collapse = "\n")
  warn <- d[d[["level"]] == "warn", , drop = FALSE]
  if (nrow(warn) && !quiet)
    message("EML check: ", nrow(warn), " warning(s) — a gap in the record the document could not fill:\n", fmt(warn))
  ex <- d[d[["level"]] == "error" & d[["exempt"]], , drop = FALSE]
  if (nrow(ex) && !quiet)
    message("EML check: ", nrow(ex), " finding(s) exempt while a question is open/proposed: ",
            paste(sprintf("%s (%s, %s)", ex$dataset_key, ex$finding, ex$question), collapse = "; "))
  bad <- d[d[["level"]] == "error" & !d[["exempt"]], , drop = FALSE]
  if (nrow(bad))
    stop("EML check: ", nrow(bad), " blocking finding(s):\n", fmt(bad),
         "\n  Fill the field in metadata/{provider}/{dataset}/dataset_meta.yml, or file an open/proposed",
         " questions.csv row with related_table = dataset naming it. Never type a creator, contact,",
         " licence or keyword the source has not stated.", call. = FALSE)
  invisible(d)
}

# ERDDAP globals -----------------------------------------------------------------------

#' The ERDDAP global attributes of one dataset, from the same record
#'
#' Plan § D-8: ERDDAP's globals, the DwC-A's EML, the EDI package's EML and the
#' page's JSON-LD are rendered from one record, so none of them is typed twice.
#' `infoUrl` is the dataset page. `creator_*` follow the same order [build_eml()]
#' does (creators → pi_names → the provider organization); `creator_email` falls
#' back to [eml_contact_address()].
#'
#' @param record one dataset record
#' @return A named character vector of ERDDAP global attributes (absent values
#'   are omitted, never blank).
#' @export
#' @concept eml
erddap_globals <- function(record) {
  att <- record[["attribution"]] %||% list()
  cre <- .eml_creators(record)
  first <- if (length(cre$parties)) cre$parties[[1]] else list()
  nm <- if (!is.null(first[["individualName"]]))
    trimws(paste(.s(first$individualName$givenName), .s(first$individualName$surName))) else
      .s(first[["organizationName"]])
  ct <- .s(att[["contact"]])
  g <- c(
    title           = .s(record[["dataset_name"]]),
    summary         = paste(.md_paras(record[["description_md"]]), collapse = "\n\n"),
    institution     = .s(.chr_or_null(record[["provider"]]$name) %||% .chr_or_null(record[["provider"]]$short)),
    creator_name    = nm,
    creator_email   = if (nzchar(.s(first[["electronicMailAddress"]]))) .s(first[["electronicMailAddress"]])
                      else if (nzchar(ct) && !grepl("^https?://", ct)) ct else eml_contact_address(),
    creator_type    = if (!is.null(first[["individualName"]])) "person" else "institution",
    creator_url     = .s(record[["provider"]]$url),
    license         = paste(c(.s(att[["license_name"]]), .s(att[["license_url"]]))[nzchar(c(.s(att[["license_name"]]), .s(att[["license_url"]])))],
                            collapse = " "),
    keywords        = paste(as.character(unlist(record[["keywords"]])), collapse = ", "),
    keywords_vocabulary = if (any(grepl("^EARTH SCIENCE", as.character(unlist(record[["keywords"]]))))) "GCMD Science Keywords" else "",
    infoUrl         = .s(record[["links"]]$page),
    sourceUrl       = .s(record[["links"]]$data_source),
    acknowledgement = .s(att[["acknowledgement"]]),
    id              = .s(record[["dataset_key"]]),
    naming_authority = CC_EML_SYSTEM)
  g[nzchar(g)]
}
