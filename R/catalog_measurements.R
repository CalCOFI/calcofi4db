# the measurements catalog record --------------------------------------------------
#
# One generated record per release — `measurements.json`, written beside
# `taxa.json` at release (plan 2026-09-10 "Measurements catalog — the
# environment's Species …", § D2 and D4, Appendix A). It is the species
# catalog's pattern with `obs_env.measurement_type` in the place of
# `obs_bio.taxon_key` and `measurement_type.csv` in the place of `taxon`: one
# entry per key, every number read from the release, nothing authored on a page.
#
# What it holds, and why that shape:
#
# * A **measurement** is one page. Its key is the registry's `variable` where
#   one is set (the crosswalk that says two datasets measure the same thing
#   comparably — the bottle's `temperature` and the CTD's `temperature_ave` are
#   both `temperature`), else the `measurement_type` itself. 79 keys over the 84
#   series of v2026.09.06.
# * A **series** is one `measurement_type × dataset_key` present in `obs_env` —
#   "what the source called it", the `dataset_taxon` of this catalog. It carries
#   the dataset's own column and flag column, its counts by year, month, depth
#   band and flag code, and the observed quantiles.
# * A registry row that belongs to one of these datasets but never reaches
#   `obs_env` — a raw CTD sensor, a METS thermosalinograph past the first — gets
#   no page: it is listed under its dataset in `datasets[].full_resolution_only[]`
#   (the `vocabulary_only[]` of this catalog), which is where a reader looks for
#   the full-resolution product it does live in.
# * `related[]` names the other keys that share a NERC P01 concept and are kept
#   apart on purpose, with the reason ([measurement_related_reasons()]). P01
#   identity is necessary evidence that two series measure the same quantity and
#   never sufficient: the CTD files' own bottle table is plausibly the same
#   physical bottles as the bottle dataset (merging them would double count), a
#   replicate is not a mean, an underway intake is not a cast.
#
# The arithmetic the record checks on itself: the sum of `series[].n_values`
# over every key is `obs_env`'s row count, and every series' `years{}` sums to
# its own `n_values` less the rows carrying no year.
#
# Deterministic: no wall clock, no network. `measurements[]` is ordered by the
# category registry's `order`, then by `totals.n_values` descending, then by key;
# `series[]` by `n_values` descending; `datasets[]` follows the record's catalog
# order.

#' @keywords internal
CC_MEASUREMENTS_SCHEMA_VERSION <- "1.0"

# the eight depth bands, on `depth_min_m`: left-closed, right-open, the last open
#' @keywords internal
CC_MEASUREMENT_DEPTH_BANDS <- c("0-10", "10-50", "50-100", "100-200",
                                "200-500", "500-1000", "1000-2000", "2000+")

# the naming conventions of the measurement vocabulary that the flags and the
# `related[]` reasons read. These are conventions of `measurement_type.csv`
# itself, not per-dataset arms: `btl_*` / `*_btl` is a bottle table carried
# inside another dataset's files, `*_rep1` a replicate beside a mean, `r_*` a
# reported pre-QC twin.
#' @keywords internal
CC_MEASUREMENT_BOTTLE_RE    <- "(^btl_)|(_btl$)|(_btl_)"
#' @keywords internal
CC_MEASUREMENT_REPLICATE_RE <- "_rep[0-9]*$"
#' @keywords internal
CC_MEASUREMENT_PRE_QC_RE    <- "^r_"

# a value at or above this, in a series with no declared bound, reads as a
# possible fill sentinel (the 99.00 degC bottle temperature of v2026.09.06)
#' @keywords internal
CC_MEASUREMENT_SENTINEL <- 99

# helpers ----------------------------------------------------------------------------

# a scalar for the record: NULL / zero-length / NA / "" all become NA, which
# jsonlite writes as `null` under `na = "null"`
.mm_val <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA)
  x <- x[[1]]
  if (is.character(x) && !is.na(x) && !nzchar(trimws(x))) return(NA)
  if (is.na(x)) NA else x
}

.mm_chr <- function(x) {
  v <- .mm_val(x)
  if (is.na(v)) NA_character_ else trimws(as.character(v))
}

.mm_int <- function(x) {
  v <- .mm_val(x)
  if (is.na(v)) return(NA_integer_)
  v <- suppressWarnings(as.numeric(v))
  if (is.na(v)) NA_integer_ else if (abs(v) <= .Machine$integer.max) as.integer(v) else v
}

.mm_num <- function(x) {
  v <- .mm_val(x)
  if (is.na(v)) return(NA_real_)
  v <- suppressWarnings(as.numeric(v))
  if (is.na(v) || !is.finite(v)) NA_real_ else v
}

.mm_lgl <- function(x) {
  v <- .mm_val(x)
  if (is.na(v)) return(NA)
  if (is.logical(v)) return(v)
  isTRUE(tolower(trimws(as.character(v))) %in% c("true", "t", "yes", "1"))
}

# min/max over the values that are values: min(numeric(0)) is Inf with a warning
.mm_min <- function(x) { x <- x[!is.na(x)]; if (length(x)) min(x) else NA }
.mm_max <- function(x) { x <- x[!is.na(x)]; if (length(x)) max(x) else NA }

# `years{}` / `qual{}` must be JSON objects even when empty: a zero-length NAMED
# list is written `{}`, a bare `list()` is written `[]`
.mm_obj <- function(nm, n) {
  o <- stats::setNames(as.list(as.integer(n)), as.character(nm))
  if (!length(o)) o <- stats::setNames(list(), character())
  o[order(names(o))]
}

# the key a measurement_type belongs to: the registry's `variable` where set
.mm_key_of <- function(measurement_type, variable) {
  v <- trimws(as.character(variable))
  ifelse(is.na(v) | !nzchar(v), as.character(measurement_type), v)
}

# vocabulary ---------------------------------------------------------------------------

#' The `flags[]` a `measurements.json` series can carry
#'
#' A `series[]` entry is one `measurement_type × dataset` of `obs_env` — what a
#' dataset calls a measurement. A flag says something about that series a reader
#' of the page needs in order to read its numbers correctly, so a measurement
#' page can show a quiet pill rather than a footnote nobody writes:
#'
#' * `sensor_mean` — the registry's `derivation` says the series is a mean of a
#'   sensor pair (`temperature_ave`, "Mean of the two temperature sensors"). The
#'   per-sensor series ride the full-resolution product.
#' * `replicate` — a `…_rep1` / `…_rep2` replicate beside a mean of the same
#'   quantity (`alkalinity_rep1`), never itself the mean.
#' * `reported_pre_qc` — an `r_*` series: the value the source reported before
#'   quality control, kept beside the QC'd one.
#' * `no_bound` — the registry declares neither `valid_min` nor `valid_max`, so
#'   nothing in the pipeline can call one of its values impossible.
#' * `sentinel_suspected` — the observed maximum exceeds the declared
#'   `valid_max`, or (with no bound declared at all) reaches
#'   `CC_MEASUREMENT_SENTINEL` (99), the shape a fill value takes. On
#'   v2026.09.06 the bottle's `temperature` reads 99.00 degC — one corrupt 2020
#'   cast, fixed at the ingest for the next release.
#' * `no_flag_at_grain` — the registry names no `_qual_column`, so the series
#'   reaches the release carrying no quality code. The CTD's `temperature_ave`
#'   is the headline case: the sensor flags ride `temperature_1` / `temperature_2`
#'   in `obs_ctd_full`.
#' * `no_p01` — no `nerc_p01` concept. Under the registry's exact-match rule
#'   that means "no concept says exactly this", never "not looked at".
#'
#' @return A character vector, the `series[].flags` enum of
#'   `measurements.schema.json`.
#' @export
#' @concept release
#' @examples
#' measurement_series_flags()
measurement_series_flags <- function()
  c("sensor_mean", "replicate", "reported_pre_qc", "no_bound",
    "sentinel_suspected", "no_flag_at_grain", "no_p01")

#' The `flags[]` a `measurements.json` measurement (a page) can carry
#'
#' One flag, and it is about the record rather than the data: `no_label` says no
#' `metadata/variable.csv` row supplies a display label for the key, so `label`
#' fell back to the canonical series' registry `description` — a column note, not
#' a title. The builder never invents a label.
#'
#' @return A character vector, the measurement-level `flags` enum.
#' @export
#' @concept release
#' @examples
#' measurement_flags()
measurement_flags <- function() "no_label"

#' Why two measurement keys sharing a NERC P01 concept are kept apart
#'
#' P01 identity says two series name the same quantity. It does not say they may
#' be pooled, and `related[]` is where the record states the difference the
#' crosswalk rule (plan 2026-09-10 § D3) refused to merge across:
#'
#' * `underway_vs_cast` — one side is an underway intake, the other a cast
#'   (`sst_c` beside `temperature`). Different sampling entirely.
#' * `same_bottles` — one side is a `btl_*` / `*_btl` series: another dataset's
#'   files carrying their own copy of the bottle table. Plausibly the same
#'   physical bottles, so merging them would double count.
#' * `replicate_vs_mean` — one side is a `…_rep` replicate, the other a mean
#'   (`alkalinity_rep1` beside `alkalinity`).
#' * `pre_qc_twin` — one side is an `r_*` series reported before quality control.
#'
#' They are tested in that order, because the first that applies is the
#' coarsest difference: an underway intake beside a cast's bottle table is
#' `underway_vs_cast`, not `same_bottles`.
#'
#' A pair sharing a P01 that matches none of the four gets **no** `related[]`
#' entry: the vocabulary states no reason, and the record never invents one.
#'
#' @return A character vector, the `related[].why` enum.
#' @export
#' @concept release
#' @examples
#' measurement_related_reasons()
measurement_related_reasons <- function()
  c("underway_vs_cast", "same_bottles", "replicate_vs_mean", "pre_qc_twin")

# build ---------------------------------------------------------------------------------

#' Build the measurements catalog record (`measurements.json`)
#'
#' One entry per measurement key the release's `obs_env` carries — the registry's
#' `variable` where set, else the `measurement_type` — with its series (one per
#' `measurement_type × dataset`), each series' counts by year, calendar month,
#' depth band and quality code, its observed quantiles, the registry's declared
#' bounds, the source and flag columns, the NERC ids, and the other keys sharing
#' its P01 concept that are deliberately kept apart.
#'
#' Everything is read: `obs_env` (and, when present, `climatology` and the
#' `obs_*_full` supplementals) on `con`, the dataset names, colours, categories
#' and order from `record` (the `datasets.json` the release has just written, so
#' a dataset dot cannot disagree between `/datasets/` and `/measurements/`), and
#' the vocabulary from the registries. Nothing is authored and nothing is
#' fetched.
#'
#' Six grouped queries do the counting — never one per key — and every per-series
#' lookup is a split index, so the builder is linear in the number of `obs_env`
#' groups.
#'
#' **The label.** `label` comes from `metadata/variable.csv` when that registry
#' carries a row for the key. Absent a row it falls back to the canonical series'
#' registry `description` and the measurement carries the `no_label` flag: a
#' column note is not a title, and the builder does not invent one.
#'
#' @param con a DBI connection holding the release table `obs_env` (and,
#'   optionally, `climatology` and the `obs_*_full` supplementals)
#' @param record the `datasets.json` record — a path or the list from
#'   [build_dataset_catalog()] — read for `datasets[].dataset_name_short`,
#'   `color` and `category`, and for the catalog order of `datasets[]`
#' @param measurement_type the measurement vocabulary, from
#'   [read_measurement_type()] (`metadata/measurement_type.csv`)
#' @param variable the label registry (`metadata/variable.csv`): a data frame
#'   with at least `variable` and `label`. `NULL` (the default) means no registry
#'   is available and every measurement falls back to its canonical series'
#'   description with the `no_label` flag.
#' @param category the category registry (`metadata/category.csv`): `category`,
#'   `order`, `realm`, `icon` (and optionally `key`)
#' @param release_version the release version (default: the record's)
#' @param release_date the release date, `YYYY-MM-DD` (default: the record's)
#' @param underway_datasets dataset keys whose series are underway intakes
#'   rather than casts, which is what makes a shared P01 `underway_vs_cast`.
#'   Named here rather than inferred, because nothing in the release states it.
#' @param supplemental_tables named character vector mapping a registry
#'   `_source_table` to the full-resolution release table its non-released series
#'   live in; the names are also what makes a canonical series absent from
#'   `obs_env` a `full_resolution_only[]` row rather than an empty one.
#' @return A list ready for [write_measurements_catalog()] /
#'   `jsonlite::write_json(auto_unbox = TRUE)`, validating against
#'   `inst/schema/measurements.schema.json`.
#' @export
#' @concept release
#' @seealso [write_measurements_catalog()], [validate_measurements_catalog()],
#'   [check_measurements_catalog()], [measurement_series_flags()]
#' @importFrom DBI dbGetQuery dbListTables dbListFields dbExecute dbWriteTable dbRemoveTable
build_measurements_catalog <- function(con, record, measurement_type, variable = NULL,
                                       category, release_version = NULL, release_date = NULL,
                                       underway_datasets = "calcofi_mets",
                                       supplemental_tables = c(ctd_raw = "obs_ctd_full",
                                                               mets_measurement = "obs_mets_full")) {
  stopifnot(
    "build_measurements_catalog(): `con` must be an open DBI connection to the release tables" =
      inherits(con, "DBIConnection"),
    "build_measurements_catalog(): `measurement_type` must be a data frame with a measurement_type column" =
      is.data.frame(measurement_type) && "measurement_type" %in% names(measurement_type),
    "build_measurements_catalog(): `category` must be a data frame with a category column" =
      is.data.frame(category) && "category" %in% names(category))
  record <- .read_json(record)
  release_version <- .s(release_version %||% record[["release"]][["version"]])
  release_date    <- .s(release_date    %||% record[["release"]][["release_date"]])
  stopifnot(
    "build_measurements_catalog(): no release version (pass `release_version`, or a record with release$version)" =
      nzchar(release_version),
    "build_measurements_catalog(): no release date (pass `release_date`, or a record with release$release_date)" =
      nzchar(release_date))

  need <- c("dataset_key", "measurement_type", "sample_key", "root_id", "grid_key",
            "cruise_key", "datetime", "year", "depth_min_m", "depth_max_m", "value",
            "measurement_qual", "qual_ok")
  miss <- setdiff(need, DBI::dbListFields(con, "obs_env"))
  if (length(miss))
    stop("build_measurements_catalog(): obs_env is missing ", paste(miss, collapse = ", "),
         call. = FALSE)

  # the registry, with every column the builder reads present ---------------------
  mt <- as.data.frame(measurement_type, stringsAsFactors = FALSE)
  for (cl in c("description", "units", "valid_min", "valid_max", "derivation",
               "is_canonical", "_source_column", "_qual_column", "_source_table",
               "_source_datasets", "category", "variable", "nerc_p01", "units_nerc_p06"))
    if (!cl %in% names(mt)) mt[[cl]] <- NA
  mt_i <- stats::setNames(seq_len(nrow(mt)), mt[["measurement_type"]])
  reg  <- function(type, col) mt[[col]][mt_i[[type]]]

  # what the release measures -----------------------------------------------------
  ser <- DBI::dbGetQuery(con, "
    SELECT dataset_key, measurement_type,
           CAST(count(*)                    AS INTEGER) AS n_values,
           CAST(count(DISTINCT sample_key)  AS INTEGER) AS n_samples,
           CAST(count(DISTINCT root_id)     AS INTEGER) AS n_roots,
           CAST(count(DISTINCT grid_key)    AS INTEGER) AS n_cells,
           CAST(count(DISTINCT cruise_key)  AS INTEGER) AS n_cruises,
           CAST(min(year) AS INTEGER) AS year_min, CAST(max(year) AS INTEGER) AS year_max,
           min(depth_min_m) AS depth_min_m, max(depth_max_m) AS depth_max_m,
           CAST(count(*) FILTER (WHERE qual_ok) AS INTEGER) AS qual_ok_n,
           min(value)                     FILTER (WHERE isfinite(value)) AS v_min,
           quantile_cont(value, 0.05)     FILTER (WHERE isfinite(value)) AS v_p05,
           quantile_cont(value, 0.50)     FILTER (WHERE isfinite(value)) AS v_p50,
           quantile_cont(value, 0.95)     FILTER (WHERE isfinite(value)) AS v_p95,
           max(value)                     FILTER (WHERE isfinite(value)) AS v_max
    FROM obs_env GROUP BY 1, 2 ORDER BY 1, 2")
  stopifnot("build_measurements_catalog(): obs_env carries no rows" = nrow(ser) > 0)
  sy <- DBI::dbGetQuery(con, "
    SELECT dataset_key, measurement_type, CAST(year AS INTEGER) AS year,
           CAST(count(*) AS INTEGER) AS n
    FROM obs_env WHERE year IS NOT NULL GROUP BY 1, 2, 3 ORDER BY 1, 2, 3")
  sm <- DBI::dbGetQuery(con, "
    SELECT dataset_key, measurement_type,
           CAST(EXTRACT(month FROM datetime) AS INTEGER) AS month,
           CAST(count(*) AS INTEGER) AS n
    FROM obs_env WHERE datetime IS NOT NULL GROUP BY 1, 2, 3 ORDER BY 1, 2, 3")
  sd <- DBI::dbGetQuery(con, "
    SELECT dataset_key, measurement_type,
           CASE WHEN depth_min_m <    10 THEN '0-10'
                WHEN depth_min_m <    50 THEN '10-50'
                WHEN depth_min_m <   100 THEN '50-100'
                WHEN depth_min_m <   200 THEN '100-200'
                WHEN depth_min_m <   500 THEN '200-500'
                WHEN depth_min_m <  1000 THEN '500-1000'
                WHEN depth_min_m <  2000 THEN '1000-2000'
                ELSE '2000+' END AS band,
           CAST(count(*) AS INTEGER) AS n
    FROM obs_env WHERE depth_min_m IS NOT NULL AND isfinite(depth_min_m)
    GROUP BY 1, 2, 3 ORDER BY 1, 2, 3")
  sq <- DBI::dbGetQuery(con, "
    SELECT dataset_key, measurement_type,
           COALESCE(CAST(measurement_qual AS VARCHAR), 'none') AS qual,
           CAST(count(*) AS INTEGER) AS n
    FROM obs_env GROUP BY 1, 2, 3 ORDER BY 1, 2, 3")
  n_obs_env <- as.numeric(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM obs_env")$n)

  # the keys ----------------------------------------------------------------------
  unknown <- setdiff(ser[["measurement_type"]], mt[["measurement_type"]])
  if (length(unknown))
    stop("build_measurements_catalog(): obs_env carries measurement_type(s) absent from the registry: ",
         paste(utils::head(unknown, 8), collapse = ", "), call. = FALSE)
  ser[["key"]] <- .mm_key_of(ser[["measurement_type"]],
                             vapply(ser[["measurement_type"]], function(t) .mm_chr(reg(t, "variable")), ""))
  types <- sort(unique(ser[["measurement_type"]]))
  map   <- data.frame(measurement_type = types,
                      mkey = .mm_key_of(types, vapply(types, function(t) .mm_chr(reg(t, "variable")), "")),
                      stringsAsFactors = FALSE)

  # totals at the key grain: distinct samples and roots cannot be summed over
  # series (two series of one dataset would share sample_keys), so the key is
  # pushed into SQL as a temp mapping and counted once
  DBI::dbWriteTable(con, "_mm_map", map, temporary = TRUE, overwrite = TRUE)
  on.exit(try(DBI::dbRemoveTable(con, "_mm_map"), silent = TRUE), add = TRUE)
  tot <- DBI::dbGetQuery(con, "
    SELECT m.mkey AS key,
           CAST(count(*)                      AS INTEGER) AS n_values,
           CAST(count(DISTINCT o.sample_key)  AS INTEGER) AS n_samples,
           CAST(count(DISTINCT o.root_id)     AS INTEGER) AS n_roots,
           CAST(count(DISTINCT o.dataset_key) AS INTEGER) AS n_datasets,
           CAST(min(o.year) AS INTEGER) AS year_min, CAST(max(o.year) AS INTEGER) AS year_max,
           min(o.depth_min_m) AS depth_min_m, max(o.depth_max_m) AS depth_max_m
    FROM obs_env o JOIN _mm_map m USING (measurement_type)
    GROUP BY 1 ORDER BY 1")
  tot_i <- stats::setNames(seq_len(nrow(tot)), tot[["key"]])

  # the climatology, and the full-resolution supplementals ------------------------
  tbls <- DBI::dbListTables(con)
  clim <- if ("climatology" %in% tbls)
    DBI::dbGetQuery(con, "SELECT DISTINCT measurement_type FROM climatology")$measurement_type else
      character()
  full_tbls <- grep("^obs_[a-z0-9]+_full$", tbls, value = TRUE)
  full_rows <- if (length(full_tbls)) sum(vapply(full_tbls, function(t)
    as.numeric(DBI::dbGetQuery(con, paste0('SELECT count(*) AS n FROM "', t, '"'))$n), 0)) else NA_real_

  # the categories ----------------------------------------------------------------
  cg    <- as.data.frame(category, stringsAsFactors = FALSE)
  for (cl in c("order", "realm", "icon", "key")) if (!cl %in% names(cg)) cg[[cl]] <- NA
  # `[[` on a named atomic vector ERRORS on a missing name before any default can
  # apply, so every lookup here is a match(), never `cg_i[[name]]`
  cat_of <- function(name) {
    i <- match(.s(name), as.character(cg[["category"]]))
    if (is.na(i))
      return(list(key = NA_character_, name = .mm_chr(name), icon = NA_character_,
                  realm = NA_character_, order = NA_integer_))
    list(key   = .mm_chr(cg[["key"]][i]),   name  = .mm_chr(cg[["category"]][i]),
         icon  = .mm_chr(cg[["icon"]][i]),  realm = .mm_chr(cg[["realm"]][i]),
         order = .mm_int(cg[["order"]][i]))
  }

  # the label registry ------------------------------------------------------------
  vr <- if (is.null(variable)) NULL else as.data.frame(variable, stringsAsFactors = FALSE)
  if (!is.null(vr)) {
    stopifnot("build_measurements_catalog(): `variable` needs a `variable` column" =
                "variable" %in% names(vr))
    if (!"label" %in% names(vr)) vr[["label"]] <- NA_character_
  }
  label_of <- function(key) {
    if (is.null(vr)) return(NA_character_)
    i <- match(key, as.character(vr[["variable"]]))
    if (is.na(i)) NA_character_ else .mm_chr(vr[["label"]][i])
  }

  # split indexes, taken once ------------------------------------------------------
  pair <- function(dataset_key, measurement_type) paste(dataset_key, measurement_type, sep = "\r")
  ser[["pk"]] <- pair(ser[["dataset_key"]], ser[["measurement_type"]])
  sy_by <- split(seq_len(nrow(sy)), pair(sy[["dataset_key"]], sy[["measurement_type"]]))
  sm_by <- split(seq_len(nrow(sm)), pair(sm[["dataset_key"]], sm[["measurement_type"]]))
  sd_by <- split(seq_len(nrow(sd)), pair(sd[["dataset_key"]], sd[["measurement_type"]]))
  sq_by <- split(seq_len(nrow(sq)), pair(sq[["dataset_key"]], sq[["measurement_type"]]))

  # the P01 graph: which keys share a concept, and why they are kept apart ---------
  p01_of_series <- vapply(ser[["measurement_type"]], function(t) .mm_chr(reg(t, "nerc_p01")), "")
  key_p01 <- tapply(p01_of_series, ser[["key"]], function(x) {
    x <- x[!is.na(x)]
    if (length(x)) x[[1]] else NA_character_
  })
  key_types <- split(ser[["measurement_type"]], ser[["key"]])
  key_dsets <- split(ser[["dataset_key"]], ser[["key"]])
  why_of <- function(a, b) {
    ta <- key_types[[a]]; tb <- key_types[[b]]
    da <- key_dsets[[a]]; db <- key_dsets[[b]]
    uw <- function(d) any(d %in% underway_datasets)
    if (xor(uw(da), uw(db)))                                          return("underway_vs_cast")
    if (any(grepl(CC_MEASUREMENT_BOTTLE_RE,    c(ta, tb))))           return("same_bottles")
    if (any(grepl(CC_MEASUREMENT_REPLICATE_RE, c(ta, tb))))           return("replicate_vs_mean")
    if (any(grepl(CC_MEASUREMENT_PRE_QC_RE,    c(ta, tb))))           return("pre_qc_twin")
    NA_character_
  }

  # the datasets, in the record's catalog order ------------------------------------
  rec_order <- vapply(.rows(record[["datasets"]]), function(d) .s(d[["dataset_key"]]), "")
  ds_seen   <- unique(ser[["dataset_key"]])
  ds_keys   <- c(intersect(rec_order, ds_seen), sort(setdiff(ds_seen, rec_order)))
  ds_meta   <- .mm_dataset_meta(record)

  # measurements[] -----------------------------------------------------------------
  keys <- sort(unique(ser[["key"]]))
  build_series <- function(i) {
    dk <- ser[["dataset_key"]][i]; tp <- ser[["measurement_type"]][i]; pk <- ser[["pk"]][i]
    yr <- sy_by[[pk]]; mo <- sm_by[[pk]]; db <- sd_by[[pk]]; qu <- sq_by[[pk]]
    months <- integer(12)
    if (length(mo)) months[sm[["month"]][mo]] <- as.integer(sm[["n"]][mo])
    bands <- stats::setNames(as.list(integer(length(CC_MEASUREMENT_DEPTH_BANDS))),
                             CC_MEASUREMENT_DEPTH_BANDS)
    if (length(db)) for (j in db) bands[[sd[["band"]][j]]] <- as.integer(sd[["n"]][j])
    obs_max <- .mm_num(ser[["v_max"]][i])
    list(measurement_type = tp,
         dataset_key      = dk,
         source_column    = .mm_chr(reg(tp, "_source_column")),
         qual_column      = .mm_chr(reg(tp, "_qual_column")),
         description      = .mm_chr(reg(tp, "description")),
         units            = .mm_chr(reg(tp, "units")),
         nerc_p01         = .mm_chr(reg(tp, "nerc_p01")),
         is_canonical     = .mm_lgl(reg(tp, "is_canonical")),
         n_values   = .mm_int(ser[["n_values"]][i]),
         n_samples  = .mm_int(ser[["n_samples"]][i]),
         n_roots    = .mm_int(ser[["n_roots"]][i]),
         n_cells    = .mm_int(ser[["n_cells"]][i]),
         n_cruises  = .mm_int(ser[["n_cruises"]][i]),
         year_min   = .mm_int(ser[["year_min"]][i]),
         year_max   = .mm_int(ser[["year_max"]][i]),
         depth_min_m = .mm_num(ser[["depth_min_m"]][i]),
         depth_max_m = .mm_num(ser[["depth_max_m"]][i]),
         years  = .mm_obj(sy[["year"]][yr], sy[["n"]][yr]),
         months = I(as.integer(months)),
         depth_bands = bands,
         qual = .mm_obj(sq[["qual"]][qu], sq[["n"]][qu]),
         qual_ok_n = .mm_int(ser[["qual_ok_n"]][i]),
         observed = list(min = .mm_num(ser[["v_min"]][i]), p05 = .mm_num(ser[["v_p05"]][i]),
                         p50 = .mm_num(ser[["v_p50"]][i]), p95 = .mm_num(ser[["v_p95"]][i]),
                         max = obs_max),
         flags = .arr(.mm_series_flags(tp, mt, mt_i, obs_max)))
  }

  measurements <- lapply(keys, function(k) {
    rr <- which(ser[["key"]] == k)
    rr <- rr[order(-ser[["n_values"]][rr], ser[["dataset_key"]][rr], ser[["measurement_type"]][rr])]
    # the canonical series speaks for the key: the registry's own is_canonical
    # where one of them has it, else the largest
    canon <- rr[[1]]
    is_c  <- vapply(rr, function(i) isTRUE(.mm_lgl(reg(ser[["measurement_type"]][i], "is_canonical"))), logical(1))
    if (any(is_c)) canon <- rr[which(is_c)[[1]]]
    ct <- ser[["measurement_type"]][canon]
    lab <- label_of(k)
    m_flags <- character()
    if (is.na(lab)) { lab <- .mm_chr(reg(ct, "description")); m_flags <- "no_label" }
    bounded <- rr[!is.na(mt[["valid_min"]][mt_i[ser[["measurement_type"]][rr]]]) |
                    !is.na(mt[["valid_max"]][mt_i[ser[["measurement_type"]][rr]]])]
    ti <- tot_i[[k]]
    p  <- unname(key_p01[[k]])
    rel <- list()
    if (!is.na(p)) {
      others <- sort(setdiff(names(key_p01)[!is.na(key_p01) & key_p01 == p], k))
      for (o in others) {
        w <- why_of(k, o)
        if (!is.na(w)) rel[[length(rel) + 1]] <- list(key = o, why = w)
      }
    }
    list(key   = k,
         slug  = k,
         label = lab,
         description   = .mm_chr(reg(ct, "description")),
         units         = .mm_chr(reg(ct, "units")),
         units_nerc_p06 = .mm_chr(reg(ct, "units_nerc_p06")),
         nerc_p01      = p,
         category      = cat_of(.mm_chr(reg(ct, "category"))),
         is_unified    = length(unique(ser[["measurement_type"]][rr])) > 1,
         derivation    = .mm_chr(reg(ct, "derivation")),
         climatology   = any(ser[["measurement_type"]][rr] %in% clim),
         bounds = list(
           valid_min = .mm_num(.mm_min(as.numeric(mt[["valid_min"]][mt_i[ser[["measurement_type"]][rr]]]))),
           valid_max = .mm_num(.mm_max(as.numeric(mt[["valid_max"]][mt_i[ser[["measurement_type"]][rr]]]))),
           declared_by = .arr(unique(ser[["measurement_type"]][bounded]))),
         totals = list(
           n_values   = .mm_int(tot[["n_values"]][ti]),
           n_samples  = .mm_int(tot[["n_samples"]][ti]),
           n_roots    = .mm_int(tot[["n_roots"]][ti]),
           n_datasets = .mm_int(tot[["n_datasets"]][ti]),
           year_min   = .mm_int(tot[["year_min"]][ti]),
           year_max   = .mm_int(tot[["year_max"]][ti]),
           depth_min_m = .mm_num(tot[["depth_min_m"]][ti]),
           depth_max_m = .mm_num(tot[["depth_max_m"]][ti])),
         series  = lapply(rr, build_series),
         related = if (length(rel)) rel else list(),
         flags   = .arr(m_flags))
  })

  # category order, then the biggest key first, then the key ------------------------
  ord_cat <- vapply(measurements, function(m) {
    o <- .mm_int(m[["category"]][["order"]]); if (is.na(o)) .Machine$integer.max else o }, 0)
  ord_n   <- vapply(measurements, function(m) {
    v <- .mm_val(m[["totals"]][["n_values"]]); if (is.na(v)) 0 else as.numeric(v) }, 0)
  ord_k   <- vapply(measurements, function(m) m[["key"]], "")
  measurements <- measurements[order(ord_cat, -ord_n, ord_k)]

  # datasets[] ----------------------------------------------------------------------
  ds_of_type <- strsplit(ifelse(is.na(mt[["_source_datasets"]]), "", as.character(mt[["_source_datasets"]])), ";")
  datasets <- lapply(ds_keys, function(dk) {
    m  <- ds_meta[[dk]]
    rr <- which(ser[["dataset_key"]] == dk)
    # a registry row this dataset declares that never reaches obs_env: a raw
    # sensor, a thermosalinograph past the first. It gets no page; it is here so
    # a reader can find the full-resolution product it does live in.
    claims <- vapply(ds_of_type, function(v) dk %in% trimws(v), logical(1))
    fro <- which(claims & !mt[["measurement_type"]] %in% ser[["measurement_type"]][rr] &
                   (!vapply(mt[["is_canonical"]], function(x) isTRUE(.mm_lgl(x)), logical(1)) |
                      as.character(mt[["_source_table"]]) %in% names(supplemental_tables)))
    fro <- fro[order(mt[["measurement_type"]][fro])]
    list(dataset_key        = dk,
         dataset_name_short = if (is.null(m)) NA_character_ else m$dataset_name_short,
         color              = if (is.null(m)) NA_character_ else m$color,
         category           = if (is.null(m))
           list(key = NA_character_, name = NA_character_, icon = NA_character_,
                realm = NA_character_, order = NA_integer_) else m$category,
         n_series   = length(rr),
         n_values   = .mm_int(sum(ser[["n_values"]][rr])),
         year_min   = .mm_int(.mm_min(ser[["year_min"]][rr])),
         year_max   = .mm_int(.mm_max(ser[["year_max"]][rr])),
         full_resolution_only = lapply(fro, function(j) {
           st <- .mm_chr(mt[["_source_table"]][j])
           hit <- match(.s(st), names(supplemental_tables))
           list(measurement_type = .mm_chr(mt[["measurement_type"]][j]),
                description      = .mm_chr(mt[["description"]][j]),
                table            = if (is.na(hit)) st else .mm_chr(unname(supplemental_tables[hit])))
         }))
  })

  list(schema_version = CC_MEASUREMENTS_SCHEMA_VERSION,
       release = list(version = release_version, release_date = release_date),
       counts = list(
         measurements = length(measurements),
         series       = nrow(ser),
         datasets     = length(ds_keys),
         obs_env_rows = .mm_int(n_obs_env),
         full_rows    = if (is.na(full_rows)) NA_integer_ else .mm_int(full_rows),
         pages        = length(measurements)),
       datasets     = datasets,
       measurements = measurements,
       flags        = .arr(measurement_series_flags()))
}

# the flags of one series against its registry row
.mm_series_flags <- function(type, mt, mt_i, obs_max) {
  i <- mt_i[[type]]
  f <- character()
  drv <- .mm_chr(mt[["derivation"]][i])
  vmin <- suppressWarnings(as.numeric(.mm_val(mt[["valid_min"]][i])))
  vmax <- suppressWarnings(as.numeric(.mm_val(mt[["valid_max"]][i])))
  no_bound <- is.na(vmin) && is.na(vmax)
  if (!is.na(drv) && grepl("mean of", drv, ignore.case = TRUE))     f <- c(f, "sensor_mean")
  if (grepl(CC_MEASUREMENT_REPLICATE_RE, type))                     f <- c(f, "replicate")
  if (grepl(CC_MEASUREMENT_PRE_QC_RE, type))                        f <- c(f, "reported_pre_qc")
  if (no_bound)                                                     f <- c(f, "no_bound")
  if (!is.na(obs_max) && ((!is.na(vmax) && obs_max > vmax) ||
                          (no_bound && obs_max >= CC_MEASUREMENT_SENTINEL)))
    f <- c(f, "sentinel_suspected")
  if (is.na(.mm_chr(mt[["_qual_column"]][i])))                      f <- c(f, "no_flag_at_grain")
  if (is.na(.mm_chr(mt[["nerc_p01"]][i])))                          f <- c(f, "no_p01")
  f[order(match(f, measurement_series_flags()))]
}

# name / colour / category for the dots: the release record is the authority (it
# is what the dataset pages already read)
.mm_dataset_meta <- function(record) {
  out <- list()
  for (d in .rows(record[["datasets"]])) {
    k <- .s(d[["dataset_key"]])
    if (!nzchar(k)) next
    ct <- d[["category"]]
    out[[k]] <- list(
      dataset_name_short = .mm_chr(d[["dataset_name_short"]]),
      color              = .mm_chr(d[["color"]]),
      category = list(key = .mm_chr(ct[["key"]]), name = .mm_chr(ct[["name"]]),
                      icon = .mm_chr(ct[["icon"]]), realm = .mm_chr(ct[["realm"]]),
                      order = .mm_int(ct[["order"]])))
  }
  out
}

# write -----------------------------------------------------------------------------

#' Write `measurements.json`
#'
#' Minified, not pretty-printed: the record is read by a build, never by a
#' person. `na = "null"` is what turns the record's `NA` scalars into the JSON
#' `null` the schema declares.
#'
#' @param record from [build_measurements_catalog()]
#' @param dir the release directory (created if missing)
#' @return The path written, invisibly.
#' @export
#' @concept release
#' @seealso [build_measurements_catalog()]
write_measurements_catalog <- function(record, dir) {
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  path <- file.path(dir, "measurements.json")
  jsonlite::write_json(record, path, auto_unbox = TRUE, digits = NA, null = "null", na = "null")
  invisible(path)
}

# validate --------------------------------------------------------------------------

#' Validate a `measurements.json` against the package's JSON schema
#'
#' The schema is `inst/schema/measurements.schema.json` (draft-07). Uses
#' \pkg{jsonvalidate} when installed; otherwise a structural check of the
#' required top-level and per-measurement keys, which is what the tests can
#' always run.
#'
#' @param x a `measurements.json` path, its text, or the record list
#' @param schema path to the schema file
#' @param verbose return the validator's error table on failure
#' @return `TRUE`, or stops with the first errors.
#' @export
#' @concept release
#' @seealso [build_measurements_catalog()], [check_measurements_catalog()]
validate_measurements_catalog <- function(
    x, schema = system.file("schema", "measurements.schema.json", package = "calcofi4db"),
    verbose = TRUE) {
  stopifnot("validate_measurements_catalog(): the schema file was not found" = file.exists(schema))
  txt <- if (is.character(x) && length(x) == 1 && file.exists(x))
    paste(readLines(x, warn = FALSE, encoding = "UTF-8"), collapse = "\n") else
      if (is.character(x)) paste(x, collapse = "\n") else
        jsonlite::toJSON(x, auto_unbox = TRUE, digits = NA, null = "null", na = "null")
  if (requireNamespace("jsonvalidate", quietly = TRUE)) {
    v  <- jsonvalidate::json_validator(schema, engine = "ajv")
    ok <- v(txt, verbose = verbose)
    if (!isTRUE(ok)) {
      err <- attr(ok, "errors")
      msg <- if (is.data.frame(err) && nrow(err))
        paste(utils::head(paste(err$instancePath, err$message), 10), collapse = "\n  ") else "schema violation"
      stop("measurements.json does not validate against ", basename(schema), ":\n  ", msg, call. = FALSE)
    }
    return(TRUE)
  }
  j <- jsonlite::fromJSON(txt, simplifyVector = FALSE)
  miss <- setdiff(c("schema_version", "release", "counts", "datasets", "measurements"), names(j))
  if (length(miss)) stop("measurements.json is missing: ", paste(miss, collapse = ", "), call. = FALSE)
  need <- c("key", "slug", "label", "units", "nerc_p01", "category", "is_unified",
            "bounds", "totals", "series", "related", "flags")
  for (m in j[["measurements"]]) {
    miss <- setdiff(need, names(m))
    if (length(miss))
      stop("measurement ", .s(m[["key"]]), " is missing: ", paste(miss, collapse = ", "), call. = FALSE)
  }
  TRUE
}

# check -----------------------------------------------------------------------------

#' The checks [check_measurements_catalog()] runs, with their level
#'
#' Every `error` finding stops the release: the record is generated, so a failure
#' is a bug in the generator or a break in the release's own tables, never
#' something a registry row can excuse.
#'
#' @return A named character vector, check -> level.
#' @export
#' @concept release
measurements_catalog_checks <- function() c(
  obs_env_rows   = "error",   # counts$obs_env_rows == obs_env rows
  series_total   = "error",   # sum of series[].n_values over keys == counts$obs_env_rows
  series_count   = "error",   # counts$series == the number of series[] entries
  measurements   = "error",   # counts$measurements == length(measurements) == counts$pages
  totals_agree   = "error",   # each key's totals$n_values == the sum of its series
  years_sum      = "error",   # each series' years{} sums to n_values less its year-less rows
  slugs_unique   = "error",   # one page directory per measurement
  types_known    = "error",   # every series[].measurement_type is in the registry
  datasets_known = "error",   # every dataset_key is in the release record
  dataset_meta   = "error",   # every dataset has a short name and a #rrggbb colour
  flags_known    = "error",   # every series flag is in measurement_series_flags()
  related_known  = "error")   # every related[].why is in measurement_related_reasons()

#' Check a `measurements.json` against the release it was built from
#'
#' One row per check with an `ok` flag, so a release chunk can print the table
#' whether or not it passes. The counts are re-measured against `obs_env` when
#' `con` is given (the release's own connection, or a promoted release read
#' back); without it the arithmetic that lives inside the record is still
#' checked.
#'
#' @param record from [build_measurements_catalog()] (or a path or URL)
#' @param con a DBI connection holding `obs_env`; NULL to check only what the
#'   record can prove about itself
#' @param dataset_record the `datasets.json` record (or its path) whose
#'   `datasets[]` every `dataset_key` must appear in; NULL to skip that check
#' @param measurement_type the registry every `series[].measurement_type` must be
#'   in (a data frame, or NULL to skip)
#' @return A [tibble][tibble::tibble]: `check`, `level`, `ok`, `expected`,
#'   `observed`, `detail`.
#' @export
#' @concept release
#' @seealso [assert_measurements_catalog()], [measurements_catalog_checks()]
check_measurements_catalog <- function(record, con = NULL, dataset_record = NULL,
                                       measurement_type = NULL) {
  record <- .read_json(record)
  levels <- measurements_catalog_checks()
  rows <- list()
  add <- function(check, ok, expected = NA_character_, observed = NA_character_, detail = "") {
    rows[[length(rows) + 1]] <<- tibble::tibble(
      check = check, level = unname(levels[check]), ok = isTRUE(ok),
      expected = as.character(expected), observed = as.character(observed), detail = detail)
  }
  num <- function(x) { v <- .mm_val(x); if (is.na(v)) 0 else as.numeric(v) }
  ms     <- .rows(record[["measurements"]])
  counts <- record[["counts"]]
  keys   <- vapply(ms, function(m) .s(m[["key"]]), "")
  slugs  <- vapply(ms, function(m) .s(m[["slug"]]), "")
  series <- lapply(ms, function(m) .rows(m[["series"]]))
  n_ser  <- sum(vapply(series, length, 0L))
  ser_n  <- sum(unlist(lapply(series, function(ss) vapply(ss, function(s) num(s[["n_values"]]), 0))), 0)

  if (!is.null(con)) {
    n_obs <- as.numeric(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM obs_env")$n)
    add("obs_env_rows", isTRUE(num(counts[["obs_env_rows"]]) == n_obs),
        n_obs, counts[["obs_env_rows"]], "rows of obs_env")
  } else {
    add("obs_env_rows", TRUE, counts[["obs_env_rows"]], counts[["obs_env_rows"]],
        "not re-measured (no connection)")
  }
  add("series_total", isTRUE(ser_n == num(counts[["obs_env_rows"]])),
      counts[["obs_env_rows"]], ser_n, "sum of series[].n_values over every key")
  add("series_count", isTRUE(num(counts[["series"]]) == n_ser),
      n_ser, counts[["series"]], "counts$series is the number of series[] entries")
  add("measurements", isTRUE(num(counts[["measurements"]]) == length(ms) &&
                               num(counts[["pages"]]) == length(ms)),
      length(ms), counts[["measurements"]], "counts$measurements and counts$pages are length(measurements[])")

  bad_tot <- keys[vapply(seq_along(ms), function(i)
    !isTRUE(num(ms[[i]][["totals"]][["n_values"]]) ==
              sum(vapply(series[[i]], function(s) num(s[["n_values"]]), 0), 0)), logical(1))]
  add("totals_agree", length(bad_tot) == 0, length(ms), length(ms) - length(bad_tot),
      if (length(bad_tot)) paste("totals.n_values != sum(series):",
                                 paste(utils::head(bad_tot, 5), collapse = ", ")) else
                                   "every key's totals.n_values is the sum of its series")

  bad_yr <- character()
  for (i in seq_along(ms)) for (s in series[[i]]) {
    ys <- sum(unlist(s[["years"]]), 0)
    if (ys > num(s[["n_values"]]))
      bad_yr <- c(bad_yr, paste0(.s(s[["dataset_key"]]), ":", .s(s[["measurement_type"]])))
  }
  add("years_sum", length(bad_yr) == 0, n_ser, n_ser - length(bad_yr),
      if (length(bad_yr)) paste("years{} exceeds n_values for:",
                                paste(utils::head(bad_yr, 5), collapse = ", ")) else
                                  "every series' years{} sums to at most its n_values")

  add("slugs_unique", anyDuplicated(slugs) == 0L, length(slugs), length(unique(slugs)),
      if (anyDuplicated(slugs) > 0L)
        paste("duplicate slug(s):", paste(utils::head(unique(slugs[duplicated(slugs)]), 5), collapse = ", ")) else
          "one page directory per measurement")

  types <- unique(unlist(lapply(series, function(ss) vapply(ss, function(s) .s(s[["measurement_type"]]), ""))))
  if (!is.null(measurement_type)) {
    unk <- setdiff(types, as.character(measurement_type[["measurement_type"]]))
    add("types_known", length(unk) == 0, length(types), length(unk),
        if (length(unk)) paste("not in measurement_type.csv:", paste(utils::head(unk, 5), collapse = ", ")) else
          "every series[].measurement_type is registered")
  } else {
    add("types_known", TRUE, length(types), 0, "registry not supplied")
  }

  ds  <- .rows(record[["datasets"]])
  dsk <- vapply(ds, function(d) .s(d[["dataset_key"]]), "")
  in_ms <- unique(unlist(lapply(series, function(ss) vapply(ss, function(s) .s(s[["dataset_key"]]), ""))))
  if (!is.null(dataset_record)) {
    dataset_record <- .read_json(dataset_record)
    known <- vapply(.rows(dataset_record[["datasets"]]), function(d) .s(d[["dataset_key"]]), "")
    unk <- setdiff(unique(c(dsk, in_ms)), known)
    add("datasets_known", length(unk) == 0, length(unique(c(dsk, in_ms))), length(unk),
        if (length(unk)) paste("not in datasets.json:", paste(unk, collapse = ", ")) else
          "every dataset_key is in the release record")
  } else {
    unk <- setdiff(in_ms, dsk)
    add("datasets_known", length(unk) == 0, length(in_ms), length(unk),
        if (length(unk)) paste("measured but not in datasets[]:", paste(unk, collapse = ", ")) else
          "every dataset a series names is in datasets[] (datasets.json not supplied)")
  }
  no_meta <- dsk[!vapply(ds, function(d)
    nzchar(.s(d[["dataset_name_short"]])) && grepl("^#[0-9a-fA-F]{6}$", .s(d[["color"]])), logical(1))]
  add("dataset_meta", length(no_meta) == 0, length(dsk), length(dsk) - length(no_meta),
      if (length(no_meta)) paste("no short name or #rrggbb colour:", paste(no_meta, collapse = ", ")) else
        "every dataset carries a short name and a colour")

  flags <- unlist(lapply(series, function(ss) unlist(lapply(ss, function(s) unlist(s[["flags"]])))))
  flags <- c(flags, unlist(lapply(ms, function(m) unlist(m[["flags"]]))))
  unk <- setdiff(unique(flags), c(measurement_series_flags(), measurement_flags()))
  add("flags_known", length(unk) == 0, length(flags), length(unk),
      if (length(unk)) paste("unknown flag(s):", paste(unk, collapse = ", ")) else
        sprintf("%d flag(s), all in the vocabulary", length(flags)))

  whys <- unlist(lapply(ms, function(m) vapply(.rows(m[["related"]]), function(r) .s(r[["why"]]), "")))
  unk <- setdiff(unique(whys), measurement_related_reasons())
  add("related_known", length(unk) == 0, length(whys), length(unk),
      if (length(unk)) paste("unknown related why:", paste(unk, collapse = ", ")) else
        sprintf("%d related pair(s), all with a stated reason", length(whys)))

  do.call(rbind, rows)
}

#' Stop on any failing check from [check_measurements_catalog()]
#'
#' @param d the table from [check_measurements_catalog()]
#' @param quiet suppress the passing summary
#' @return `d`, invisibly, when nothing blocks.
#' @export
#' @concept release
#' @seealso [check_measurements_catalog()]
assert_measurements_catalog <- function(d, quiet = FALSE) {
  bad <- d[!d[["ok"]] & d[["level"]] == "error", , drop = FALSE]
  if (nrow(bad))
    stop("measurements catalog check: ", nrow(bad), " blocking finding(s):\n",
         paste0("  ", bad[["check"]], ": ", bad[["detail"]],
                "  (expected ", bad[["expected"]], ", got ", bad[["observed"]], ")", collapse = "\n"),
         "\n  The record is generated: fix build_measurements_catalog() or the release's obs_env table.",
         call. = FALSE)
  if (!quiet) message("measurements catalog check: ", nrow(d), " check(s) pass")
  invisible(d)
}
