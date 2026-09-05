# one Darwin Core Archive per biological dataset, from the core ------------------------
#
# Plan 2026-09-05 "CalCOFI.io as a dataset catalog" § D-8 (the mapping table and the
# `occurrenceStatus` rule), Decisions 10, 13 and 21; origin note
# `plans_todo/2026-09-03 Follow-on — generic publish_to-obis over the core.md`.
#
# `publish_ichthyo_to-obis.qmd` was the last per-dataset publisher: it read the
# `swfsc_ichthyo` SOURCE tables (`tbl_ichthyo`, `net`, `tow`, `site`, `species`,
# `lookup`) and hand-built the Event / Occurrence / eMoF triple, which is why nine
# other biological datasets had no OBIS route at all. Everything it hand-built now
# exists generically in the core:
#
#   Event core   <- `sample`'s adjacency list (+ `cruise` as the root event)
#   Occurrence   <- `obs_bio` (the bio realm with its gear, effort and D8 densities inline)
#   eMoF         <- `sample_measurement` (event grain) + `obs_attribute` (occurrence
#                   grain) + `obs_env` rows on the same events
#
# and the vocabulary ids live in the registries the release already publishes:
# `gear.csv` (`dwc_samplingProtocol`, NERC L22), `life_stage.csv` (`dwc_lifeStage`,
# NERC S11), `measurement_type.csv` (`nerc_p01` / `units_nerc_p06` ->
# `measurementTypeID` / `measurementUnitID`).
#
# THREE RULES THIS FILE DOES NOT BEND:
#
# 1. **A controlled-vocabulary id is filled only on an exact match** (workflows
#    CLAUDE.md). Every id emitted here is looked up in a registry; an empty cell
#    means "no concept says exactly this" and travels as an empty column, never as
#    an invented URI or a near-neighbour.
# 2. **`occurrenceStatus` is emitted honestly.** A dataset that records its zeros
#    (`cufes`, `phytoplankton`, `zoodb`, `zooscan`, `phyllosoma`, `dungeness-crab`)
#    keeps them as `absent`. A positive-only dataset (`ichthyo`, `euphausiids`,
#    `bird-mammal`, `mesopelagic-fish`) has NO zero rows at all — a surveyed-empty
#    tow simply has no row — so an absence can only be DERIVED, from `sample_root`
#    minus the positives, and only for a dataset whose protocol sorts every sample
#    for the whole vocabulary. That is a claim about the protocol, not about the
#    data, so `dwc_occurrence()` derives absences only when explicitly asked
#    (`absences = "sample_root"`) and refuses to emit more than `max_absences`.
#    See [dwc_absence_rule()].
# 3. **No metadata string is typed here.** `eml.xml` comes from the release's own
#    `eml/{dataset_key}.xml` (R/eml.R, D-8); the archive only copies it in.

DWC_LIFE_STAGE_COLS <- c(
  "life_stage", "dwc_lifeStage", "nerc_s11", "life_stage_parent", "datasets", "note")

# the DwC / OBIS term URI for every column this file can emit. An unmapped column is
# an ERROR in dwc_meta_xml(), never a silently dropped field: a column the IPT cannot
# read is a column nobody at OBIS will ever ask about.
DWC_TERM_NS   <- "http://rs.tdwg.org/dwc/terms/"
DWC_IOBIS_NS  <- "http://rs.iobis.org/obis/terms/"
DWC_DCTERM_NS <- "http://purl.org/dc/terms/"

# columns whose term is NOT {DWC_TERM_NS}{column}
DWC_TERM_OVERRIDE <- c(
  measurementTypeID  = paste0(DWC_IOBIS_NS, "measurementTypeID"),
  measurementUnitID  = paste0(DWC_IOBIS_NS, "measurementUnitID"),
  measurementValueID = paste0(DWC_IOBIS_NS, "measurementValueID"),
  modified           = paste0(DWC_DCTERM_NS, "modified"),
  type               = paste0(DWC_DCTERM_NS, "type"),
  license            = paste0(DWC_DCTERM_NS, "license"),
  rightsHolder       = paste0(DWC_DCTERM_NS, "rightsHolder"),
  bibliographicCitation = paste0(DWC_DCTERM_NS, "bibliographicCitation"))

# every Darwin Core term this file emits, by row type. The list IS the contract:
# dwc_meta_xml() maps a column only if it appears here.
DWC_TERMS_EVENT <- c(
  "eventID", "parentEventID", "eventType", "eventDate", "samplingProtocol",
  "sampleSizeValue", "sampleSizeUnit", "eventRemarks", "locationID",
  "decimalLatitude", "decimalLongitude", "geodeticDatum", "footprintWKT",
  "minimumDepthInMeters", "maximumDepthInMeters", "datasetID")
DWC_TERMS_OCCURRENCE <- c(
  "occurrenceID", "eventID", "basisOfRecord", "occurrenceStatus",
  "scientificName", "scientificNameID", "taxonID", "taxonRank", "kingdom",
  "phylum", "class", "order", "family", "vernacularName", "lifeStage",
  "individualCount", "organismQuantity", "organismQuantityType", "occurrenceRemarks")
DWC_TERMS_EMOF <- c(
  "eventID", "occurrenceID", "measurementID", "measurementType",
  "measurementTypeID", "measurementValue", "measurementValueID",
  "measurementUnit", "measurementUnitID", "measurementRemarks")

# `basisOfRecord` for a net-tow / transect observation identified by a human. Not a
# choice this file re-decides per dataset: every CalCOFI biological dataset in the
# core is a sorted, human-identified sample, and the published ichthyo archive says
# the same.
DWC_BASIS_OF_RECORD <- "HumanObservation"

# the release's coordinates are EPSG:4326 (`sample.geom` is declared so); the datum
# is a property of the release, not an assertion about a dataset
DWC_GEODETIC_DATUM <- "WGS84"

# `volume_sampled` becomes the event's sampleSizeValue, so it is NOT repeated as an
# eMoF row. Every other sample_measurement type is.
DWC_SAMPLE_SIZE_TYPE <- "volume_sampled"

DWC_WORMS_LSID <- "urn:lsid:marinespecies.org:taxname:"

# registries ---------------------------------------------------------------------------

#' Read `metadata/life_stage.csv`, the life-stage registry
#'
#' One row per distinct `obs.life_stage` value, with the Darwin Core label
#' (`dwc_lifeStage`) and the NERC S11 concept URI where one is exact, plus
#' `life_stage_parent` for a substage S11 does not carve (`furcilia F1` ->
#' `furcilia`). Two values are recorded as **not life stages** — euphausiid
#' `damaged` and ichthyo `invert` — and carry neither a label nor a parent;
#' [dwc_occurrence()] sends those to `occurrenceRemarks`, never to `lifeStage`.
#'
#' @param path path to `metadata/life_stage.csv`
#' @return A [tibble][tibble::tibble], all columns character.
#' @export
#' @concept dwc
#' @seealso [read_gear_registry()], [dwc_occurrence()]
read_life_stage_registry <- function(path) {
  .read_registry_csv(path, DWC_LIFE_STAGE_COLS, "life stage")
}

#' Read the three registries a Darwin Core Archive needs
#'
#' A convenience over [read_gear_registry()], [read_life_stage_registry()] and
#' [read_measurement_type()] so a notebook names the metadata directory once.
#'
#' @param dir the `metadata/` directory
#' @return A named list: `gear`, `life_stage`, `measurement_type`.
#' @export
#' @concept dwc
dwc_registries <- function(dir) {
  list(
    gear             = read_gear_registry(file.path(dir, "gear.csv")),
    life_stage       = read_life_stage_registry(file.path(dir, "life_stage.csv")),
    measurement_type = read_measurement_type(file.path(dir, "measurement_type.csv")))
}

# small helpers ------------------------------------------------------------------------

# an ISO 8601 UTC instant; NA stays NA (an event with no time says so)
.dwc_iso <- function(x) {
  out <- rep(NA_character_, length(x))
  ok <- !is.na(x)
  if (any(ok)) out[ok] <- format(as.POSIXct(x[ok], tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ")
  out
}

# a deterministic, release-stable id: the same natural key always yields the same id,
# so an occurrence keeps its OBIS identity across releases. `obs_id` is NOT used —
# it is assigned at freeze time and would re-key every occurrence each release.
.dwc_id <- function(prefix, ...) {
  parts <- lapply(list(...), function(x) ifelse(is.na(x), "", as.character(x)))
  key <- do.call(paste, c(parts, list(sep = "|")))
  paste0(prefix, ":", vapply(key, function(k) digest::digest(k, algo = "md5"), "",
                             USE.NAMES = FALSE))
}

# NA-safe registry lookup: value where the registry states one, NA otherwise. NEVER a
# default, never a near-neighbour — an empty cell means "no concept says exactly this".
.dwc_lookup <- function(x, from, to) {
  i <- match(x, from)
  out <- to[i]
  out[!is.na(out) & !nzchar(trimws(out))] <- NA_character_
  unname(out)
}

.dwc_sql_str <- function(x) paste0("'", gsub("'", "''", x, fixed = TRUE), "'")

.dwc_has_table <- function(con, name) {
  n <- DBI::dbGetQuery(con, paste0(
    "SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_name = ",
    .dwc_sql_str(name)))$n
  isTRUE(n > 0)
}

# drop all-NA columns: an empty column in a DwC-A is a field the IPT maps and every
# consumer then sees as blank. Terms the archive must always carry are kept.
.dwc_drop_empty <- function(d, keep = character()) {
  drop <- vapply(names(d), function(nm)
    !(nm %in% keep) && all(is.na(d[[nm]])), logical(1))
  d[, !drop, drop = FALSE]
}

# the datasets ------------------------------------------------------------------------

#' The biological datasets a Darwin Core Archive can be built for
#'
#' Decision 21 (Ben, 2026-09-05): **one IPT resource per dataset whose taxa resolve
#' to WoRMS**. This measures that from the release rather than listing it: a dataset
#' is a candidate when it has rows in `obs_bio` and at least one of the taxa it
#' observed carries a WoRMS id. `cce-lter_picoplankton-bacteria` (flow-cytometry
#' groups, environmental realm) and `sio_pic-zooplankton` (no taxa) have no `obs_bio`
#' rows at all, so they fall out by construction rather than by a hard-coded exclusion.
#'
#' `n_no_worms` is the count of observed taxa with no WoRMS id: those occurrences ship
#' with `scientificNameID` empty (never a guessed LSID), and [dwc_check()] reports it.
#'
#' @param con a DBI connection to the release (`calcofi4r::cc_get_db()`)
#' @return A data frame, one row per candidate dataset: `dataset_key`, `n_obs`,
#'   `n_taxa`, `n_worms`, `n_no_worms`, `n_no_taxon`, `absence_rule`.
#' @export
#' @concept dwc
#' @seealso [dwc_absence_rule()], [dwc_event()], [dwc_occurrence()]
dwc_datasets <- function(con) {
  d <- DBI::dbGetQuery(con, "
    WITH tx AS (
      SELECT DISTINCT dataset_key, taxon_key FROM obs_bio WHERE taxon_key IS NOT NULL)
    SELECT o.dataset_key,
           COUNT(*)                                                       AS n_obs,
           SUM(CASE WHEN o.taxon_key IS NULL THEN 1 ELSE 0 END)           AS n_no_taxon,
           SUM(CASE WHEN o.value = 0 THEN 1 ELSE 0 END)                   AS n_zero
    FROM obs_bio o GROUP BY 1")
  tx <- DBI::dbGetQuery(con, "
    WITH tx AS (
      SELECT DISTINCT dataset_key, taxon_key FROM obs_bio WHERE taxon_key IS NOT NULL)
    SELECT tx.dataset_key, COUNT(*) AS n_taxa,
           SUM(CASE WHEN t.worms_id IS NOT NULL THEN 1 ELSE 0 END) AS n_worms
    FROM tx LEFT JOIN taxon t USING (taxon_key) GROUP BY 1")
  d <- merge(d, tx, by = "dataset_key", all.x = TRUE)
  d$n_taxa    <- ifelse(is.na(d$n_taxa), 0L, as.integer(d$n_taxa))
  d$n_worms   <- ifelse(is.na(d$n_worms), 0L, as.integer(d$n_worms))
  d$n_no_worms   <- d$n_taxa - d$n_worms
  d$absence_rule <- ifelse(d$n_zero > 0, "zeros_recorded", "positive_only")
  d$n_zero <- NULL
  d <- d[d$n_worms > 0, , drop = FALSE]
  d <- d[order(d$dataset_key), c("dataset_key", "n_obs", "n_taxa", "n_worms",
                                 "n_no_worms", "n_no_taxon", "absence_rule")]
  rownames(d) <- NULL
  d
}

#' Which `occurrenceStatus` rule a dataset falls under
#'
#' Measured from the data, never declared:
#'
#' * `"zeros_recorded"` — the dataset has zero-valued `obs_bio` rows, so a sample
#'   that was examined and held none of a taxon is already in the release. Those
#'   rows become `occurrenceStatus = "absent"` and nothing is derived.
#' * `"positive_only"` — the dataset has no zero rows: a surveyed-empty sample
#'   simply has no row. `occurrenceStatus` is `"present"` for every row it does
#'   have, and an absence exists only if the protocol sorted every sample for the
#'   whole vocabulary — a claim about the protocol that the release cannot make.
#'   [dwc_occurrence()] therefore derives absences ONLY when asked
#'   (`absences = "sample_root"`).
#'
#' @param con a DBI connection to the release
#' @param dataset_key the dataset
#' @return `"zeros_recorded"` or `"positive_only"`.
#' @export
#' @concept dwc
dwc_absence_rule <- function(con, dataset_key) {
  n <- DBI::dbGetQuery(con, paste0(
    "SELECT SUM(CASE WHEN value = 0 THEN 1 ELSE 0 END) AS n FROM obs_bio ",
    "WHERE dataset_key = ", .dwc_sql_str(dataset_key)))$n
  if (isTRUE(as.numeric(n) > 0)) "zeros_recorded" else "positive_only"
}

# Event core ---------------------------------------------------------------------------

#' Build the Darwin Core Event core for one dataset
#'
#' From `sample`'s adjacency list — the same rows every consumer reads — plus the
#' `cruise` reference as the root event, plus the effort denominator from
#' `sample_measurement`:
#'
#' | Darwin Core | core |
#' |---|---|
#' | `eventID` | `sample.sample_key` |
#' | `parentEventID` | `sample.parent_sample_key`, or the row's `cruise_key` for a root |
#' | `eventType` | `sample.sample_type` (`"cruise"` for a cruise event) |
#' | `eventDate` | `sample.datetime` as ISO 8601 UTC; a cruise's `date_min/date_max` span |
#' | `decimalLatitude` / `decimalLongitude` | `sample.latitude` / `sample.longitude` |
#' | `minimumDepthInMeters` / `maximumDepthInMeters` | `sample.depth_min_m` / `depth_max_m` |
#' | `locationID` | `sample.site_key` |
#' | `samplingProtocol` | `gear.csv` `dwc_samplingProtocol` for `sample.tow_type` |
#' | `sampleSizeValue` / `sampleSizeUnit` | `sample_measurement` `volume_sampled` + its registry unit |
#' | `geodeticDatum` | `"WGS84"` — the release's own CRS |
#' | `datasetID` | `dataset_key` |
#'
#' **Cruise events.** A dataset's root samples carry `cruise_key` but no parent, so
#' `cruises = TRUE` (the default) emits one event per cruise the dataset touches and
#' parents the roots to it. That is a derivation from a column already on the row,
#' not an invention, and it is what makes an archive's events group the way a reader
#' expects. `cruises = FALSE` leaves the roots parentless.
#'
#' Nothing here asserts `countryCode`, `waterBody` or `coordinateUncertaintyInMeters`:
#' the release measures none of them, and the ichthyo notebook's hand-typed values
#' were dataset metadata living where no provider could edit them (D-8).
#'
#' @param con a DBI connection to the release
#' @param dataset_key the dataset
#' @param gear the registry from [read_gear_registry()], or NULL (no `samplingProtocol`)
#' @param measurement_type the registry from [read_measurement_type()], or NULL
#'   (`sampleSizeUnit` is then empty)
#' @param cruises emit a cruise root event per `cruise_key` and parent the roots to it
#' @param close_tree pull in an ancestor event that belongs to ANOTHER dataset, so
#'   the archive's `parentEventID`s all resolve. `sample_key` is globally unique and
#'   a `parent_sample_key` may point across datasets — `cdfw_dungeness-crab` parents
#'   306 examined subsamples onto `swfsc_ichthyo` **site** occupations, and
#'   `calcofi_dic` parents 6 bottles onto `calcofi_bottle` casts (that is how the
#'   DIC/bottle dedup works). In the release those are not orphans; in a
#'   single-dataset archive they would be, so the ancestors travel with their own
#'   `datasetID` rather than the pointer being dropped.
#' @return A data frame of Event-core rows, all-NA columns dropped.
#' @export
#' @concept dwc
#' @seealso [dwc_occurrence()], [dwc_emof()], [dwc_archive()]
dwc_event <- function(con, dataset_key, gear = NULL, measurement_type = NULL,
                      cruises = TRUE, close_tree = TRUE) {
  k <- .dwc_sql_str(dataset_key)
  size_sql <- if (.dwc_has_table(con, "sample_measurement")) paste0("
    LEFT JOIN (SELECT sample_key, measurement_value FROM sample_measurement
               WHERE dataset_key = ", k, "
                 AND measurement_type = ", .dwc_sql_str(DWC_SAMPLE_SIZE_TYPE), ") v
           ON v.sample_key = s.sample_key") else ""
  size_col <- if (nzchar(size_sql)) "v.measurement_value" else "NULL::DOUBLE"
  sel <- paste0("
    SELECT s.dataset_key, s.sample_key, s.parent_sample_key, s.sample_type, s.cruise_key,
           s.site_key, s.datetime, s.latitude, s.longitude, s.depth_min_m, s.depth_max_m,
           s.tow_type, ", size_col, " AS sample_size
    FROM sample s", size_sql, "
    WHERE ")
  d <- DBI::dbGetQuery(con, paste0(sel, "s.dataset_key = ", k, " ORDER BY s.sample_key"))

  if (isTRUE(close_tree)) {
    # follow parent_sample_key out of the dataset until the tree is closed. Bounded
    # by the number of event levels, so it cannot loop on a cycle.
    for (step in seq_len(8L)) {
      need <- setdiff(stats::na.omit(d$parent_sample_key), d$sample_key)
      if (!length(need)) break
      up <- DBI::dbGetQuery(con, paste0(
        sel, "s.sample_key IN (", paste(.dwc_sql_str(need), collapse = ", "),
        ") ORDER BY s.sample_key"))
      if (!nrow(up)) break
      d <- rbind(d, up)
    }
  }

  ev <- data.frame(
    eventID              = d$sample_key,
    parentEventID        = d$parent_sample_key,
    eventType            = d$sample_type,
    eventDate            = .dwc_iso(d$datetime),
    decimalLatitude      = d$latitude,
    decimalLongitude     = d$longitude,
    geodeticDatum        = ifelse(is.na(d$latitude), NA_character_, DWC_GEODETIC_DATUM),
    minimumDepthInMeters = d$depth_min_m,
    maximumDepthInMeters = d$depth_max_m,
    locationID           = d$site_key,
    samplingProtocol     = NA_character_,
    sampleSizeValue      = d$sample_size,
    sampleSizeUnit       = NA_character_,
    # an ancestor pulled in from another dataset keeps ITS datasetID, so an archive
    # never claims an event it did not collect
    datasetID            = d$dataset_key,
    stringsAsFactors     = FALSE)

  if (!is.null(gear) && nrow(gear))
    ev$samplingProtocol <- .dwc_lookup(d$tow_type, gear[["tow_type"]],
                                       gear[["dwc_samplingProtocol"]])
  if (!is.null(measurement_type) && nrow(measurement_type)) {
    u <- .dwc_lookup(DWC_SAMPLE_SIZE_TYPE, measurement_type[["measurement_type"]],
                     measurement_type[["units"]])
    ev$sampleSizeUnit <- ifelse(is.na(ev$sampleSizeValue), NA_character_, u)
  }

  if (isTRUE(cruises) && .dwc_has_table(con, "cruise")) {
    roots <- is.na(ev$parentEventID) & !is.na(d$cruise_key)
    if (any(roots)) {
      keys <- sort(unique(d$cruise_key[roots]))
      cr <- DBI::dbGetQuery(con, paste0("
        SELECT cruise_key, date_min, date_max, ship_name FROM cruise
        WHERE cruise_key IN (", paste(.dwc_sql_str(keys), collapse = ", "), ")
        ORDER BY cruise_key"))
      if (nrow(cr)) {
        span <- ifelse(
          is.na(cr$date_min), NA_character_,
          ifelse(is.na(cr$date_max) | cr$date_max == cr$date_min,
                 as.character(cr$date_min),
                 paste0(cr$date_min, "/", cr$date_max)))
        cev <- ev[0, , drop = FALSE][seq_len(nrow(cr)), , drop = FALSE]
        cev[] <- lapply(cev, function(x) x[NA_integer_])
        cev$eventID       <- cr$cruise_key
        cev$parentEventID <- NA_character_
        cev$eventType     <- "cruise"
        cev$eventDate     <- span
        cev$datasetID     <- dataset_key
        ev$parentEventID[roots] <- d$cruise_key[roots]
        # a root whose cruise has no `cruise` row keeps no parent, rather than
        # pointing at an event the archive does not contain
        ev$parentEventID[roots & !(d$cruise_key %in% cr$cruise_key)] <- NA_character_
        ev <- rbind(cev, ev)
      }
    }
  }
  rownames(ev) <- NULL
  .dwc_drop_empty(ev, keep = c("eventID", "parentEventID", "eventType", "eventDate"))
}

# Occurrence extension ------------------------------------------------------------------

#' Build the Darwin Core Occurrence extension for one dataset
#'
#' From `obs_bio` joined to `taxon`, with `lifeStage` from `life_stage.csv` and the
#' D8 denominator as `organismQuantity`:
#'
#' | Darwin Core | core |
#' |---|---|
#' | `occurrenceID` | md5 of `(sample_key, taxon_key, life_stage, measurement_type, depth_bin, ordinal)` — stable across releases (`obs_id` is not) |
#' | `eventID` | `obs_bio.sample_key`, or the root's `sample_key` for a derived absence |
#' | `scientificName` / `taxonID` / `taxonRank` / lineage | `taxon` |
#' | `scientificNameID` | the WoRMS LSID of `taxon.worms_id`; **empty when the taxon has none** |
#' | `lifeStage` | `life_stage.csv` `dwc_lifeStage`, else the verbatim value where the registry gives it a `life_stage_parent`; a value the registry records as *not a life stage* goes to `occurrenceRemarks` |
#' | `organismQuantity` / `organismQuantityType` | `density_per_10m2`, else `density_per_1000m3`, else `value` + its registry `units` |
#' | `occurrenceStatus` | `"present"` where `value > 0`, `"absent"` where `value = 0` |
#'
#' **The absence rule.** See [dwc_absence_rule()]. `absences = "none"` (the default)
#' emits only rows the release holds. `absences = "sample_root"` additionally emits
#' one `absent` row for every (surveyed root sample x observed taxon/stage) pair with
#' no positive row — the `sample_root` minus positives rule. It is correct only for a
#' dataset whose protocol sorts every sample for its whole vocabulary, so it is never
#' the default, and `max_absences` (5,000,000) stops a large vocabulary from turning
#' a survey into a hundred million assertions nobody made.
#'
#' @param con a DBI connection to the release
#' @param dataset_key the dataset
#' @param life_stage the registry from [read_life_stage_registry()], or NULL
#' @param measurement_type the registry from [read_measurement_type()], or NULL
#' @param absences `"none"` or `"sample_root"`
#' @param max_absences refuse to derive more absences than this
#' @return A data frame of Occurrence rows, all-NA columns dropped.
#' @export
#' @concept dwc
#' @seealso [dwc_event()], [dwc_emof()], [dwc_absence_rule()]
dwc_occurrence <- function(con, dataset_key, life_stage = NULL, measurement_type = NULL,
                           absences = c("none", "sample_root"), max_absences = 5e6) {
  absences <- match.arg(absences)
  k <- .dwc_sql_str(dataset_key)
  d <- DBI::dbGetQuery(con, paste0("
    SELECT o.obs_id, o.sample_key, o.taxon_key, o.life_stage, o.measurement_type,
           o.units, o.value, o.depth_bin, o.density_per_10m2, o.density_per_1000m3,
           t.scientific_name, t.worms_id, t.rank, t.kingdom, t.phylum, t.class,
           t.order_taxon, t.family, t.common_name
    FROM obs_bio o LEFT JOIN taxon t USING (taxon_key)
    WHERE o.dataset_key = ", k, "
    ORDER BY o.sample_key, o.taxon_key, o.life_stage, o.measurement_type, o.obs_id"))
  # a row with no taxon, or whose taxon_key names no `taxon` row, cannot be an
  # Occurrence — DwC requires a name. Dropped here and REPORTED by dwc_check()
  # through the `dwc_dropped` attribute, never shipped as an unnamed record and
  # never lost without a count.
  keep <- !is.na(d$taxon_key) & !is.na(d$scientific_name)
  n_dropped <- sum(!keep)
  d <- d[keep, , drop = FALSE]

  occ <- .dwc_occurrence_rows(d, dataset_key, life_stage, measurement_type)

  if (identical(absences, "sample_root")) {
    ab <- .dwc_absence_rows(con, dataset_key, max_absences, life_stage, measurement_type)
    if (nrow(ab)) occ <- rbind(occ, ab[, names(occ), drop = FALSE])
  }
  rownames(occ) <- NULL
  occ <- .dwc_drop_empty(occ, keep = c("occurrenceID", "eventID", "basisOfRecord",
                                       "occurrenceStatus", "scientificName"))
  attr(occ, "dwc_dropped") <- as.integer(n_dropped)
  occ
}

# the shared projection: one obs_bio-shaped frame -> Occurrence rows
.dwc_occurrence_rows <- function(d, dataset_key, life_stage, measurement_type) {
  n <- nrow(d)
  if (!n) return(data.frame(
    occurrenceID = character(), eventID = character(), basisOfRecord = character(),
    occurrenceStatus = character(), scientificName = character(),
    scientificNameID = character(), taxonID = character(), taxonRank = character(),
    kingdom = character(), phylum = character(), class = character(),
    order = character(), family = character(), vernacularName = character(),
    lifeStage = character(), individualCount = integer(),
    organismQuantity = numeric(), organismQuantityType = character(),
    occurrenceRemarks = character(), stringsAsFactors = FALSE))
  # the grain is (sample, taxon, stage, type, depth bin); a handful of datasets carry
  # a duplicate of it, so an ordinal within the grain keeps the id unique AND stable
  grain <- paste(d$sample_key, d$taxon_key, d$life_stage, d$measurement_type,
                 d$depth_bin, sep = "|")
  ord <- stats::ave(seq_len(n), grain, FUN = seq_along)

  ls <- .dwc_life_stage(d$life_stage, life_stage)
  q  <- .dwc_quantity(d, measurement_type)

  data.frame(
    occurrenceID     = .dwc_id(paste0(dataset_key, ":occ"), d$sample_key, d$taxon_key,
                               d$life_stage, d$measurement_type, d$depth_bin, ord),
    eventID          = d$sample_key,
    basisOfRecord    = rep(DWC_BASIS_OF_RECORD, n),
    occurrenceStatus = ifelse(is.na(d$value), "present",
                              ifelse(d$value > 0, "present", "absent")),
    scientificName       = d$scientific_name,
    scientificNameID     = ifelse(is.na(d$worms_id), NA_character_,
                                  paste0(DWC_WORMS_LSID, d$worms_id)),
    taxonID              = d$taxon_key,
    taxonRank            = d$rank,
    kingdom              = d$kingdom,
    phylum               = d$phylum,
    class                = d$class,
    order                = d$order_taxon,
    family               = d$family,
    vernacularName       = d$common_name,
    lifeStage            = ls$life_stage,
    individualCount      = q$count,
    organismQuantity     = q$value,
    organismQuantityType = q$type,
    occurrenceRemarks    = ls$remarks,
    stringsAsFactors     = FALSE)
}

# lifeStage from the registry. Three outcomes, all of them the registry's own:
#   a `dwc_lifeStage`             -> that label
#   no label but a parent stage   -> the verbatim value (a real stage S11 does not carve)
#   neither (recorded as NOT a life stage, or absent from the registry) -> remarks
.dwc_life_stage <- function(x, reg) {
  n <- length(x)
  out <- rep(NA_character_, n); rem <- rep(NA_character_, n)
  if (is.null(reg) || !nrow(reg)) return(list(life_stage = out, remarks = rem))
  lab <- .dwc_lookup(x, reg[["life_stage"]], reg[["dwc_lifeStage"]])
  par <- .dwc_lookup(x, reg[["life_stage"]], reg[["life_stage_parent"]])
  known <- x %in% reg[["life_stage"]]
  out[!is.na(lab)] <- lab[!is.na(lab)]
  sub <- is.na(lab) & !is.na(par)
  out[sub] <- x[sub]
  not_stage <- !is.na(x) & known & is.na(lab) & is.na(par)
  rem[not_stage] <- paste0("verbatim life stage (not a life stage): ", x[not_stage])
  unknown <- !is.na(x) & !known
  rem[unknown] <- paste0("verbatim life stage: ", x[unknown])
  list(life_stage = out, remarks = rem)
}

# organismQuantity: the D8 denominator first (the standardized density the dataset
# was designed around), the raw value and its registry unit otherwise.
#
# `individualCount` carries the RAW count alongside it, so standardizing the quantity
# never costs a re-user the number that was actually tallied — emitted only where the
# registry says the type's unit is a count AND the value is a whole number, so a
# density-grained type (`count/m2`) never arrives as if it were specimens counted.
DWC_COUNT_UNITS <- c("count", "individuals")
# unit string -> organismQuantityType. A rename into DwC's own wording, not a
# conversion: "count" and "individuals" are the same thing said twice.
DWC_QUANTITY_TYPE <- c(count = "individuals", individuals = "individuals")

.dwc_quantity <- function(d, measurement_type) {
  n <- nrow(d)
  d10  <- if (!is.null(d$density_per_10m2))   d$density_per_10m2   else rep(NA_real_, n)
  d1k  <- if (!is.null(d$density_per_1000m3)) d$density_per_1000m3 else rep(NA_real_, n)
  units <- if (!is.null(measurement_type) && nrow(measurement_type))
    .dwc_lookup(d$measurement_type, measurement_type[["measurement_type"]],
                measurement_type[["units"]]) else d$units
  units <- ifelse(is.na(units), d$units, units)
  qtype <- ifelse(units %in% names(DWC_QUANTITY_TYPE),
                  unname(DWC_QUANTITY_TYPE[units]), units)
  val <- ifelse(!is.na(d10), d10, ifelse(!is.na(d1k), d1k, d$value))
  typ <- ifelse(!is.na(d10), "individuals per 10 square metres of sea surface",
                ifelse(!is.na(d1k), "individuals per 1000 cubic metres", qtype))
  typ[is.na(val)] <- NA_character_
  cnt <- rep(NA_integer_, n)
  is_count <- !is.na(units) & units %in% DWC_COUNT_UNITS &
    !is.na(d$value) & d$value >= 0 & abs(d$value - round(d$value)) < 1e-9
  cnt[is_count] <- as.integer(round(d$value[is_count]))
  list(value = val, type = typ, count = cnt)
}

# `sample_root` minus the positives, guarded. Absences attach to the ROOT event: the
# release knows the root was sampled, not which of its nets a missing taxon was
# looked for in.
.dwc_absence_rows <- function(con, dataset_key, max_absences, life_stage, measurement_type) {
  k <- .dwc_sql_str(dataset_key)
  if (!.dwc_has_table(con, "sample_root"))
    stop("absences = \"sample_root\" needs the supplemental `sample_root` table; ",
         "open the release with supplemental = TRUE", call. = FALSE)
  n <- DBI::dbGetQuery(con, paste0("
    WITH surveyed AS (
      SELECT DISTINCT sr.root_sample_key
      FROM sample_root sr JOIN obs_bio o ON o.root_id = sr.root_id
      WHERE sr.dataset_key = ", k, "),
    vocab AS (
      SELECT DISTINCT taxon_key, life_stage, measurement_type
      FROM obs_bio WHERE dataset_key = ", k, " AND taxon_key IS NOT NULL),
    pos AS (
      SELECT DISTINCT sr.root_sample_key, o.taxon_key, o.life_stage, o.measurement_type
      FROM obs_bio o JOIN sample_root sr ON sr.root_id = o.root_id
      WHERE o.dataset_key = ", k, ")
    SELECT (SELECT COUNT(*) FROM surveyed) * (SELECT COUNT(*) FROM vocab)
           - (SELECT COUNT(*) FROM pos) AS n"))$n
  if (isTRUE(as.numeric(n) > max_absences))
    stop("deriving absences for ", dataset_key, " would emit ", format(n, big.mark = ","),
         " rows (max_absences = ", format(max_absences, big.mark = ","), "). A vocabulary ",
         "this wide is not sorted for in every sample; raise max_absences only when the ",
         "provider has confirmed that it is.", call. = FALSE)
  d <- DBI::dbGetQuery(con, paste0("
    WITH surveyed AS (
      SELECT DISTINCT sr.root_sample_key
      FROM sample_root sr JOIN obs_bio o ON o.root_id = sr.root_id
      WHERE sr.dataset_key = ", k, "),
    vocab AS (
      SELECT DISTINCT taxon_key, life_stage, measurement_type, units
      FROM obs_bio WHERE dataset_key = ", k, " AND taxon_key IS NOT NULL),
    pos AS (
      SELECT DISTINCT sr.root_sample_key, o.taxon_key, o.life_stage, o.measurement_type
      FROM obs_bio o JOIN sample_root sr ON sr.root_id = o.root_id
      WHERE o.dataset_key = ", k, ")
    SELECT s.root_sample_key AS sample_key, v.taxon_key, v.life_stage,
           v.measurement_type, v.units, 0.0 AS value,
           NULL::INTEGER AS depth_bin, NULL::DOUBLE AS density_per_10m2,
           NULL::DOUBLE AS density_per_1000m3, NULL::BIGINT AS obs_id,
           t.scientific_name, t.worms_id, t.rank, t.kingdom, t.phylum, t.class,
           t.order_taxon, t.family, t.common_name
    FROM surveyed s
    CROSS JOIN vocab v
    LEFT JOIN pos p ON p.root_sample_key = s.root_sample_key
                   AND p.taxon_key = v.taxon_key
                   AND p.life_stage IS NOT DISTINCT FROM v.life_stage
                   AND p.measurement_type = v.measurement_type
    LEFT JOIN taxon t ON t.taxon_key = v.taxon_key
    WHERE p.root_sample_key IS NULL AND t.scientific_name IS NOT NULL
    ORDER BY s.root_sample_key, v.taxon_key, v.life_stage, v.measurement_type"))
  .dwc_occurrence_rows(d, dataset_key, life_stage, measurement_type)
}

# eMoF extension -------------------------------------------------------------------------

#' Build the ExtendedMeasurementOrFact extension for one dataset
#'
#' Three grains, all with `measurementTypeID` / `measurementUnitID` from
#' `measurement_type.csv`'s `nerc_p01` / `units_nerc_p06` — **empty where the
#' registry states no exact concept, never invented**:
#'
#' * **event** — `sample_measurement` rows for the dataset, minus `volume_sampled`
#'   (which is the event's `sampleSizeValue`, not a repeat measurement).
#' * **occurrence** — `obs_attribute` rows, joined to their occurrence on
#'   `(sample_key, taxon_key, life_stage)`. Which of a bin's two numbers is the
#'   `measurementValue` is decided by the REGISTRY, not per dataset: a type with a
#'   physical `units` (`body_length` mm, `carapace_length` mm) puts `bin_value` in
#'   `measurementValue` and the bin's `count` in `measurementRemarks`; a type with no
#'   unit (`stage`, `behavior`) is a categorical bin, so the `count` is the value and
#'   the `bin_label` is the remark.
#' * **event, environmental** — `obs_env` rows sitting on one of this dataset's own
#'   events (`sample_key`). Empty for a dataset that measures no environment itself.
#'
#' @param con a DBI connection to the release
#' @param dataset_key the dataset
#' @param occurrence the frame from [dwc_occurrence()] — needed to resolve
#'   `occurrenceID`; without it the `obs_attribute` grain is skipped
#' @param measurement_type the registry from [read_measurement_type()], or NULL
#' @param env include `obs_env` rows on the dataset's own events
#' @return A data frame of eMoF rows, all-NA columns dropped.
#' @export
#' @concept dwc
#' @seealso [dwc_event()], [dwc_occurrence()]
dwc_emof <- function(con, dataset_key, occurrence = NULL, measurement_type = NULL,
                     env = TRUE) {
  k <- .dwc_sql_str(dataset_key)
  mt <- measurement_type
  type_id <- function(x) if (is.null(mt)) rep(NA_character_, length(x)) else
    .dwc_lookup(x, mt[["measurement_type"]], mt[["nerc_p01"]])
  unit_id <- function(x) if (is.null(mt)) rep(NA_character_, length(x)) else
    .dwc_lookup(x, mt[["measurement_type"]], mt[["units_nerc_p06"]])
  units_of <- function(x) if (is.null(mt)) rep(NA_character_, length(x)) else
    .dwc_lookup(x, mt[["measurement_type"]], mt[["units"]])

  out <- list()

  # 1. event grain -- sample_measurement
  if (.dwc_has_table(con, "sample_measurement")) {
    sm <- DBI::dbGetQuery(con, paste0("
      SELECT sample_key, measurement_type, measurement_value, measurement_qual
      FROM sample_measurement
      WHERE dataset_key = ", k, " AND measurement_type <> ",
      .dwc_sql_str(DWC_SAMPLE_SIZE_TYPE), "
      ORDER BY sample_key, measurement_type"))
    if (nrow(sm)) out$sample_measurement <- data.frame(
      eventID            = sm$sample_key,
      occurrenceID       = NA_character_,
      measurementID      = .dwc_id(paste0(dataset_key, ":mof"), "sample_measurement",
                                   sm$sample_key, sm$measurement_type),
      measurementType    = sm$measurement_type,
      measurementTypeID  = type_id(sm$measurement_type),
      measurementValue   = sm$measurement_value,
      measurementValueID = NA_character_,
      measurementUnit    = units_of(sm$measurement_type),
      measurementUnitID  = unit_id(sm$measurement_type),
      measurementRemarks = ifelse(is.na(sm$measurement_qual), NA_character_,
                                  paste0("quality flag: ", sm$measurement_qual)),
      stringsAsFactors   = FALSE)
  }

  # 2. occurrence grain -- obs_attribute
  if (.dwc_has_table(con, "obs_attribute") && !is.null(occurrence) && nrow(occurrence)) {
    oa <- DBI::dbGetQuery(con, paste0("
      SELECT sample_key, taxon_key, life_stage, measurement_type, bin_value, bin_label,
             count AS bin_count, measurement_qual
      FROM obs_attribute WHERE dataset_key = ", k, "
      ORDER BY sample_key, taxon_key, life_stage, measurement_type, bin_value"))
    if (nrow(oa)) {
      # occurrence rows keyed on the same grain, minus the bin: taxonID + eventID +
      # lifeStage identify the occurrence an attribute row belongs to
      okey <- paste(occurrence$eventID, occurrence$taxonID,
                    ifelse(is.na(occurrence$lifeStage), "", occurrence$lifeStage), sep = "|")
      # a substage's occurrence carries the verbatim value, so match on the raw
      # obs_bio life_stage too by re-deriving both forms
      akey <- paste(oa$sample_key, oa$taxon_key,
                    ifelse(is.na(oa$life_stage), "", oa$life_stage), sep = "|")
      i <- match(akey, okey)
      units <- units_of(oa$measurement_type)
      is_binned <- is.na(units)   # no physical unit -> the bin is categorical
      out$obs_attribute <- data.frame(
        eventID            = oa$sample_key,
        occurrenceID       = occurrence$occurrenceID[i],
        measurementID      = .dwc_id(paste0(dataset_key, ":mof"), "obs_attribute",
                                     oa$sample_key, oa$taxon_key, oa$life_stage,
                                     oa$measurement_type, oa$bin_value),
        measurementType    = oa$measurement_type,
        measurementTypeID  = type_id(oa$measurement_type),
        measurementValue   = ifelse(is_binned, oa$bin_count, oa$bin_value),
        measurementValueID = NA_character_,
        measurementUnit    = ifelse(is_binned, "individuals", units),
        measurementUnitID  = ifelse(is_binned, unit_id("abundance"),
                                    unit_id(oa$measurement_type)),
        measurementRemarks = ifelse(
          is_binned,
          ifelse(is.na(oa$bin_label), paste0(oa$measurement_type, ": ", oa$bin_value),
                 oa$bin_label),
          paste0(oa$bin_count, " individuals")),
        stringsAsFactors   = FALSE)
    }
  }

  # 3. event grain -- obs_env on this dataset's own events
  if (isTRUE(env) && .dwc_has_table(con, "obs_env")) {
    oe <- DBI::dbGetQuery(con, paste0("
      SELECT e.sample_key, e.measurement_type, e.value, e.units, e.measurement_qual
      FROM obs_env e
      WHERE e.sample_key IN (SELECT sample_key FROM sample WHERE dataset_key = ", k, ")
      ORDER BY e.sample_key, e.measurement_type"))
    if (nrow(oe)) out$obs_env <- data.frame(
      eventID            = oe$sample_key,
      occurrenceID       = NA_character_,
      measurementID      = .dwc_id(paste0(dataset_key, ":mof"), "obs_env",
                                   oe$sample_key, oe$measurement_type, oe$value),
      measurementType    = oe$measurement_type,
      measurementTypeID  = type_id(oe$measurement_type),
      measurementValue   = oe$value,
      measurementValueID = NA_character_,
      measurementUnit    = oe$units,
      measurementUnitID  = unit_id(oe$measurement_type),
      measurementRemarks = ifelse(is.na(oe$measurement_qual), NA_character_,
                                  paste0("quality flag: ", oe$measurement_qual)),
      stringsAsFactors   = FALSE)
  }

  if (!length(out)) return(data.frame(
    eventID = character(), occurrenceID = character(), measurementID = character(),
    measurementType = character(), measurementTypeID = character(),
    measurementValue = numeric(), measurementValueID = character(),
    measurementUnit = character(), measurementUnitID = character(),
    measurementRemarks = character(), stringsAsFactors = FALSE))
  d <- do.call(rbind, unname(out))
  rownames(d) <- NULL
  .dwc_drop_empty(d, keep = c("eventID", "occurrenceID", "measurementID",
                              "measurementType", "measurementValue"))
}

# meta.xml -------------------------------------------------------------------------------

#' The Darwin Core term URI for every column the archive can carry
#'
#' Column name -> term URI. Most are `http://rs.tdwg.org/dwc/terms/{column}`; the
#' OBIS eMoF ids and the Dublin Core terms are the exceptions
#' (`DWC_TERM_OVERRIDE`). A column absent from this map is an ERROR in
#' [dwc_meta_xml()] rather than a dropped field.
#'
#' @return A named character vector, `column -> term URI`.
#' @export
#' @concept dwc
dwc_term_map <- function() {
  cols <- unique(c(DWC_TERMS_EVENT, DWC_TERMS_OCCURRENCE, DWC_TERMS_EMOF))
  out <- stats::setNames(paste0(DWC_TERM_NS, cols), cols)
  ov <- intersect(names(DWC_TERM_OVERRIDE), cols)
  out[ov] <- DWC_TERM_OVERRIDE[ov]
  out
}

# the three row types, in archive order
DWC_ROW_TYPES <- list(
  event = list(file = "event.csv", id = "eventID",
               rowType = "http://rs.tdwg.org/dwc/terms/Event"),
  occurrence = list(file = "occurrence.csv", id = "occurrenceID", coreid = "eventID",
                    rowType = "http://rs.tdwg.org/dwc/terms/Occurrence"),
  emof = list(file = "extendedMeasurementOrFact.csv", id = "measurementID",
              coreid = "eventID",
              rowType = "http://rs.iobis.org/obis/terms/ExtendedMeasurementOrFact"))

#' Generate `meta.xml` for an Event-core archive
#'
#' Maps each written CSV's columns to their Darwin Core term URIs. **A column with
#' no term is an error**: the ichthyo notebook only `message()`d about one, so a
#' renamed column would have shipped as a field the IPT silently ignores.
#'
#' @param tables a named list of the archive's data frames: `event` (the core) and
#'   any of `occurrence`, `emof`
#' @param terms the map from [dwc_term_map()]
#' @return The `meta.xml` document as a length-1 character string.
#' @export
#' @concept dwc
#' @seealso [dwc_archive()]
dwc_meta_xml <- function(tables, terms = dwc_term_map()) {
  stopifnot("meta.xml needs an `event` core" = !is.null(tables[["event"]]))
  fields <- function(cols, skip = NULL) {
    idx <- seq_along(cols) - 1L
    keep <- !(cols %in% skip)
    miss <- setdiff(cols[keep], names(terms))
    if (length(miss))
      stop("no Darwin Core term for column(s): ", paste(miss, collapse = ", "),
           "\n  Add them to DWC_TERMS_* in R/dwc.R, or stop emitting them.", call. = FALSE)
    paste(sprintf('    <field index="%d" term="%s"/>', idx[keep], terms[cols[keep]]),
          collapse = "\n")
  }
  hdr <- paste0(
    'encoding="UTF-8" fieldsTerminatedBy="," linesTerminatedBy="\\n" ',
    "fieldsEnclosedBy='\"' ignoreHeaderLines=\"1\"")

  ev <- DWC_ROW_TYPES$event
  cols <- names(tables$event)
  core <- sprintf(paste0(
    '  <core %s rowType="%s">\n',
    '    <files>\n      <location>%s</location>\n    </files>\n',
    '    <id index="%d" />\n%s\n  </core>'),
    hdr, ev$rowType, ev$file, which(cols == ev$id) - 1L, fields(cols))

  ext <- character()
  for (nm in c("occurrence", "emof")) {
    d <- tables[[nm]]
    if (is.null(d) || !nrow(d)) next
    spec <- DWC_ROW_TYPES[[nm]]
    cols <- names(d)
    ext <- c(ext, sprintf(paste0(
      '  <extension %s rowType="%s">\n',
      '    <files>\n      <location>%s</location>\n    </files>\n',
      '    <coreid index="%d" />\n%s\n  </extension>'),
      hdr, spec$rowType, spec$file, which(cols == spec$coreid) - 1L,
      fields(cols, skip = spec$coreid)))
  }
  paste0(
    '<?xml version="1.0" encoding="UTF-8"?>\n',
    '<archive xmlns="http://rs.tdwg.org/dwc/text/"\n',
    '         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n',
    '         xsi:schemaLocation="http://rs.tdwg.org/dwc/text/ ',
    'http://rs.tdwg.org/dwc/text/tdwg_dwc_text.xsd">\n',
    paste(c(core, ext), collapse = "\n"), "\n</archive>\n")
}

# the checks -------------------------------------------------------------------------------

#' The findings [dwc_check()] can report, with their level
#'
#' `error` findings mean **no archive is written** for that dataset — the whole
#' point of gating: a broken archive at OBIS is worse than a missing one.
#'
#' * `orphan_event` — a `parentEventID` naming no event in the core (`obistools::check_eventids()`).
#' * `orphan_occurrence` — an occurrence whose `eventID` is not in the core.
#' * `orphan_emof` — an eMoF row whose `eventID` / `occurrenceID` is not in the archive.
#' * `missing_required_field` — `obistools::check_fields()` at level `error` on
#'   **every** occurrence: nothing in the archive would index at OBIS, so writing it
#'   would publish an empty dataset. `calcofi_phytoplankton` is here at v2026.09.05
#'   — all 409 `region_pool` samples carry no `datetime`, so no occurrence has an
#'   `eventDate`.
#' * `incomplete_records` — the same check failing on SOME occurrences: those records
#'   will not index at OBIS and the rest will, so the archive is written and the count
#'   is reported. A gap in the release, not a fault of the mapping.
#' * `bad_event_date` — `obistools::check_eventdate()` rejected a value.
#' * `duplicate_id` — a repeated `eventID` or `occurrenceID`.
#' * `no_occurrence` — the dataset produced no occurrence rows at all.
#' * `no_scientific_name_id` — occurrences whose taxon has no WoRMS id, so
#'   `scientificNameID` is empty (warn — never a guessed LSID).
#' * `no_life_stage_id`, `no_measurement_type_id`, `no_measurement_unit_id` — a
#'   registry states no exact concept for a value the archive emits (warn).
#' * `dropped_no_taxon` — `obs_bio` rows with no taxon, which cannot be Occurrences (warn).
#' * `no_event_date`, `no_coordinates` — events missing either (warn: OBIS accepts
#'   the archive, a consumer will notice).
#'
#' @return A named character vector, finding -> level.
#' @export
#' @concept dwc
dwc_findings <- function() c(
  ok                     = "ok",
  orphan_event           = "error",
  orphan_occurrence      = "error",
  orphan_emof            = "error",
  missing_required_field = "error",
  bad_event_date         = "error",
  duplicate_id           = "error",
  no_occurrence          = "error",
  incomplete_records     = "warn",
  no_scientific_name_id  = "warn",
  no_life_stage_id       = "warn",
  no_measurement_type_id = "warn",
  no_measurement_unit_id = "warn",
  dropped_no_taxon       = "warn",
  no_event_date          = "warn",
  no_coordinates         = "warn")

#' Check an archive's three tables before it is written
#'
#' Runs the `obistools` gate (`check_eventids()`, `check_extension_eventids()`,
#' `check_fields()`, `check_eventdate()`) plus the referential and
#' controlled-vocabulary checks the release can make itself. No network: taxon names
#' are checked against the release's own `taxon` table, not `obistools::match_taxa()`,
#' which is an interactive WoRMS call.
#'
#' @param event,occurrence,emof the frames from [dwc_event()] / [dwc_occurrence()] /
#'   [dwc_emof()]
#' @param dataset_key named in the findings
#' @return A data frame: `dataset_key`, `finding`, `level`, `n`, `detail`. One `ok`
#'   row when nothing is found.
#' @export
#' @concept dwc
#' @seealso [assert_dwc()], [dwc_findings()]
dwc_check <- function(event, occurrence = NULL, emof = NULL, dataset_key = NA_character_) {
  f <- dwc_findings()
  out <- list()
  add <- function(finding, n, detail = NA_character_) {
    if (!isTRUE(n > 0)) return(invisible(NULL))
    out[[length(out) + 1L]] <<- data.frame(
      dataset_key = dataset_key, finding = finding, level = unname(f[finding]),
      n = as.integer(n), detail = detail, stringsAsFactors = FALSE)
  }

  add("duplicate_id", sum(duplicated(event$eventID)), "eventID")
  par <- event$parentEventID[!is.na(event$parentEventID)]
  add("orphan_event", sum(!(par %in% event$eventID)), "parentEventID not in the core")
  # a date or a place is inherited DOWN the event tree, so only a leaf event lacking
  # one is a gap; a cruise or a station occupation that leaves both to its tows is
  # how a DwC-A event hierarchy is meant to read
  leaf <- !(event$eventID %in% par)
  add("no_event_date", sum(leaf & is.na(event$eventDate)), "leaf events")
  if (!is.null(event$decimalLatitude))
    add("no_coordinates",
        sum(leaf & (is.na(event$decimalLatitude) | is.na(event$decimalLongitude))),
        "leaf events")

  if (requireNamespace("obistools", quietly = TRUE)) {
    ce <- tryCatch(obistools::check_eventids(event), error = function(e) NULL)
    if (!is.null(ce) && nrow(ce)) add("orphan_event", nrow(ce), "obistools::check_eventids")
    # check_eventdate() flags an EMPTY date as an unparseable one; a parent event with
    # no time of its own (an ichthyo `site`) is a gap, not a malformed value, and is
    # already counted as `no_event_date`. Only non-empty values are parsed here.
    dated <- event[!is.na(event$eventDate), , drop = FALSE]
    if (nrow(dated)) {
      cd <- tryCatch(obistools::check_eventdate(dated), error = function(e) NULL)
      if (!is.null(cd) && nrow(cd)) add("bad_event_date", nrow(cd), "event.eventDate")
    }
  }

  if (is.null(occurrence) || !nrow(occurrence)) {
    add("no_occurrence", 1L, "obs_bio has no usable rows for this dataset")
  } else {
    add("duplicate_id", sum(duplicated(occurrence$occurrenceID)), "occurrenceID")
    add("orphan_occurrence", sum(!(occurrence$eventID %in% event$eventID)))
    add("dropped_no_taxon", attr(occurrence, "dwc_dropped") %||% 0L,
        "obs_bio rows whose taxon_key names no taxon")
    add("no_scientific_name_id",
        if (is.null(occurrence$scientificNameID)) nrow(occurrence)
        else sum(is.na(occurrence$scientificNameID)))
    if (!is.null(occurrence$lifeStage))
      add("no_life_stage_id", sum(!is.na(occurrence$occurrenceRemarks) &
                                  grepl("^verbatim life stage", occurrence$occurrenceRemarks)))
    if (requireNamespace("obistools", quietly = TRUE)) {
      # check_fields() asks for the fields an INDEXED OBIS record must carry, so it
      # is run on the flattened occurrence — the occurrence with the date and
      # coordinates it inherits up the event chain — not on either table alone. Run
      # on the Event core it demands `scientificName`; run on the raw Occurrence
      # extension it demands the `eventDate` the core supplies. Both are false alarms.
      flat <- .dwc_flatten(event, occurrence)
      cf <- tryCatch(obistools::check_fields(flat, level = "error"),
                     error = function(e) NULL)
      if (!is.null(cf) && nrow(cf)) {
        # check_fields() returns one row per (record, missing field); what matters
        # is how many RECORDS are affected, and whether that is all of them —
        # an archive where nothing would index is broken, one where 1,563 of 49,572
        # positions lack coordinates is a reported gap in the release
        n_rec <- length(unique(cf$row))
        detail <- paste0("flattened occurrence: ", paste(unique(cf$field), collapse = ", "),
                         " (", n_rec, " of ", nrow(occurrence), " records)")
        if (n_rec >= nrow(occurrence)) add("missing_required_field", n_rec, detail)
        else add("incomplete_records", n_rec, detail)
      }
      cx <- tryCatch(obistools::check_extension_eventids(event, occurrence),
                     error = function(e) NULL)
      if (!is.null(cx) && nrow(cx))
        add("orphan_occurrence", nrow(cx), "obistools::check_extension_eventids")
    }
  }

  if (!is.null(emof) && nrow(emof)) {
    ev <- emof$eventID[!is.na(emof$eventID)]
    add("orphan_emof", sum(!(ev %in% event$eventID)), "eventID")
    oc <- emof$occurrenceID[!is.na(emof$occurrenceID)]
    if (length(oc) && !is.null(occurrence))
      add("orphan_emof", sum(!(oc %in% occurrence$occurrenceID)), "occurrenceID")
    if (!is.null(emof$measurementTypeID))
      add("no_measurement_type_id", sum(is.na(emof$measurementTypeID)))
    else add("no_measurement_type_id", nrow(emof))
    if (!is.null(emof$measurementUnitID))
      add("no_measurement_unit_id", sum(is.na(emof$measurementUnitID)))
    else add("no_measurement_unit_id", nrow(emof))
  }

  if (!length(out)) return(data.frame(
    dataset_key = dataset_key, finding = "ok", level = "ok", n = 0L,
    detail = NA_character_, stringsAsFactors = FALSE))
  d <- do.call(rbind, out)
  rownames(d) <- NULL
  d
}

# An occurrence with the date and coordinates it INHERITS from its event chain --
# what an OBIS record actually looks like once indexed. A DwC-A event is allowed to
# leave a child's date and place to its parent, so a check that reads one table on
# its own reports gaps the archive does not have.
.dwc_flatten <- function(event, occurrence) {
  inh <- c("eventDate", "decimalLatitude", "decimalLongitude",
           "minimumDepthInMeters", "maximumDepthInMeters")
  inh <- intersect(inh, names(event))
  ev <- event[, unique(c("eventID", "parentEventID", inh)), drop = FALSE]
  # walk up the chain, filling each still-empty field from the parent (the depth of
  # a CalCOFI event tree is 3-4, so a bounded loop is enough and cannot cycle)
  cur <- ev
  for (step in seq_len(8L)) {
    need <- Reduce(`|`, lapply(inh, function(f) is.na(cur[[f]])))
    if (!any(need) || all(is.na(cur$parentEventID))) break
    i <- match(cur$parentEventID, ev$eventID)
    for (f in inh) {
      fill <- is.na(cur[[f]]) & !is.na(i)
      cur[[f]][fill] <- ev[[f]][i[fill]]
    }
    cur$parentEventID <- ev$parentEventID[i]
  }
  j <- match(occurrence$eventID, cur$eventID)
  out <- occurrence
  for (f in inh) out[[f]] <- cur[[f]][j]
  out
}

#' Stop when a Darwin Core check found an error
#'
#' @param d the frame from [dwc_check()]
#' @param quiet suppress the printed summary
#' @return `d`, invisibly.
#' @export
#' @concept dwc
assert_dwc <- function(d, quiet = FALSE) {
  bad <- d[d$level == "error", , drop = FALSE]
  if (!quiet && nrow(d)) print(d)
  if (nrow(bad))
    stop("Darwin Core check failed for ", paste(unique(bad$dataset_key), collapse = ", "),
         ": ", paste(unique(bad$finding), collapse = ", "), call. = FALSE)
  invisible(d)
}

# the archive ------------------------------------------------------------------------------

#' Write a Darwin Core Archive and its manifest
#'
#' Writes `event.csv`, `occurrence.csv`, `extendedMeasurementOrFact.csv`, `meta.xml`
#' and `eml.xml` into `dir`, zips them (flat, no directory entries — what the IPT
#' expects), and writes `manifest.json` beside the zip.
#'
#' **The manifest is how a dataset page knows an upload is due** (D-8): it records
#' `content_hash` (an md5 over the three CSVs, deterministic — the same rows always
#' hash the same), the release `version`, and the `ipt_resource` / `obis_dataset_id` /
#' `uploaded_utc` of the *published* copy. Those last three are never invented here:
#' they are carried forward from an existing `manifest.json` (or supplied by the
#' caller from `distribution.csv`), so a freshly built archive that has never been
#' uploaded says `uploaded_utc: null`, and `registrations[]` reads that as
#' "not published" rather than as a date nobody set.
#'
#' The upload itself is a deliberate manual act through the OBIS-USA IPT
#' (Decision 10, `docs/portals.qmd` § OBIS). Nothing in this function talks to a portal.
#'
#' @param dir the directory to write into (created)
#' @param event,occurrence,emof the checked frames
#' @param eml_path path to the release's `eml/{dataset_key}.xml`, copied in as
#'   `eml.xml`; NULL writes no metadata document (and says so)
#' @param dataset_key,version named in the manifest and the zip name
#' @param ipt_resource,obis_dataset_id the published copy's ids, from
#'   `distribution.csv`; NULL keeps whatever an existing manifest holds
#' @param zip_path where to write the archive; defaults to
#'   `{dirname(dir)}/{dataset_key}_{version}.zip`
#' @return A list: `zip`, `manifest` (the path), `content_hash`, `counts`.
#' @export
#' @concept dwc
#' @seealso [dwc_check()], [dwc_meta_xml()]
dwc_archive <- function(dir, event, occurrence = NULL, emof = NULL, eml_path = NULL,
                        dataset_key = NA_character_, version = NA_character_,
                        ipt_resource = NULL, obis_dataset_id = NULL, zip_path = NULL) {
  stopifnot(is.data.frame(event), nrow(event) > 0)
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  tables <- list(event = event, occurrence = occurrence, emof = emof)
  files <- character()
  for (nm in names(DWC_ROW_TYPES)) {
    d <- tables[[nm]]
    if (is.null(d) || !nrow(d)) next
    p <- file.path(dir, DWC_ROW_TYPES[[nm]]$file)
    utils::write.csv(d, p, row.names = FALSE, na = "", quote = TRUE, fileEncoding = "UTF-8")
    files <- c(files, p)
  }
  meta <- file.path(dir, "meta.xml")
  writeLines(dwc_meta_xml(tables), meta, useBytes = TRUE)
  files <- c(files, meta)

  if (length(eml_path) == 1 && is.na(eml_path)) eml_path <- NULL
  if (!is.null(eml_path) && file.exists(eml_path)) {
    file.copy(eml_path, file.path(dir, "eml.xml"), overwrite = TRUE)
    files <- c(files, file.path(dir, "eml.xml"))
  } else if (!is.null(eml_path)) {
    warning("no EML at ", eml_path, " — the archive ships without eml.xml; build it with ",
            "build_eml_catalog() + write_eml_files()", call. = FALSE)
  }

  # deterministic over the DATA, so a re-run on unchanged rows reproduces the hash
  # (meta.xml and eml.xml carry no row content)
  csvs <- files[grepl("\\.csv$", files)]
  content_hash <- digest::digest(
    paste(vapply(sort(csvs), function(p) digest::digest(file = p, algo = "md5"), ""),
          collapse = ""), algo = "md5")

  if (is.null(zip_path))
    zip_path <- file.path(dirname(dir), paste0(dataset_key, "_", version, ".zip"))
  dir.create(dirname(zip_path), recursive = TRUE, showWarnings = FALSE)
  if (file.exists(zip_path)) unlink(zip_path)
  if (requireNamespace("zip", quietly = TRUE)) {
    zip::zip(zip_path, files = basename(files), root = dir)
  } else {
    utils::zip(zip_path, files = files, flags = "-jq")
  }

  mpath <- file.path(dirname(zip_path), paste0(dataset_key, "_manifest.json"))
  prev <- if (file.exists(mpath)) jsonlite::fromJSON(mpath, simplifyVector = FALSE) else list()
  counts <- list(event = nrow(event),
                 occurrence = if (is.null(occurrence)) 0L else nrow(occurrence),
                 emof = if (is.null(emof)) 0L else nrow(emof))
  # uploaded_utc survives only while the bytes do: a changed content_hash means the
  # published copy is stale, and saying "uploaded" of these bytes would be false
  same <- identical(.s(prev[["content_hash"]]), content_hash)
  man <- list(
    dataset_key     = dataset_key,
    version         = version,
    content_hash    = content_hash,
    archive         = basename(zip_path),
    counts          = counts,
    ipt_resource    = ipt_resource    %||% .or_null(prev[["ipt_resource"]]),
    obis_dataset_id = obis_dataset_id %||% .or_null(prev[["obis_dataset_id"]]),
    uploaded_utc    = if (same) .or_null(prev[["uploaded_utc"]]) else NULL,
    uploaded_hash   = if (same) .or_null(prev[["uploaded_hash"]]) else
                      .or_null(prev[["uploaded_hash"]]),
    generated_utc   = format(as.POSIXct(Sys.time(), tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ"))
  writeLines(jsonlite::toJSON(man, auto_unbox = TRUE, pretty = TRUE, null = "null"), mpath)

  list(zip = zip_path, manifest = mpath, content_hash = content_hash, counts = counts)
}

#' Read a Darwin Core Archive manifest and say whether the OBIS copy is current
#'
#' Feeds `registrations[]` (D-8): `published (vX)` when the uploaded bytes are these
#' bytes, `stale — data changed in vY` when they are not, `built, not uploaded` when
#' nothing was ever uploaded.
#'
#' @param path a `{dataset_key}_manifest.json`
#' @return A one-row data frame: `dataset_key`, `version`, `content_hash`,
#'   `ipt_resource`, `obis_dataset_id`, `uploaded_utc`, `status`.
#' @export
#' @concept dwc
dwc_manifest_status <- function(path) {
  m <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  up <- .s(m[["uploaded_utc"]])
  status <- if (!nzchar(up)) "built, not uploaded"
            else if (identical(.s(m[["uploaded_hash"]]), .s(m[["content_hash"]])))
              paste0("published (", .s(m[["version"]]), ")")
            else paste0("stale — data changed in ", .s(m[["version"]]))
  data.frame(
    dataset_key     = .s(m[["dataset_key"]]),
    version         = .s(m[["version"]]),
    content_hash    = .s(m[["content_hash"]]),
    ipt_resource    = .s(m[["ipt_resource"]]),
    obis_dataset_id = .s(m[["obis_dataset_id"]]),
    uploaded_utc    = up,
    status          = status,
    stringsAsFactors = FALSE)
}
