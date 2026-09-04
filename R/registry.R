# shared metadata registries: safe read + safe append ---------------------------
#
# The registries in `metadata/` are hand-editable CSVs that are ALSO written back
# by ingest notebooks. That round trip is where they get corrupted: R's
# `readr::write_csv()` defaults to `na = "NA"`, so an empty cell comes back as the
# two-character string "NA". Nothing complains, because `read_csv()` reads "NA"
# straight back to NA — the damage is invisible from R.
#
# It is NOT invisible downstream. `release_database.qmd` loads
# `measurement_type.csv` with DuckDB's `read_csv_auto`, whose default `nullstr` is
# the empty string only, so "NA" lands in the released `measurement_type` table as
# a literal 2-character value and ships to the schema site and every consumer.
# 161 rows were in that state when this was found.
#
# So: writes go through [register_measurement_types()] (which always writes
# `na = ""`), and reads go through [read_measurement_type()] (which refuses to
# return a registry containing sentinel strings). Belt and braces, because the
# writer and the reader are usually different notebooks.

#' Reject sentinel strings that should have been empty cells
#'
#' Guards a metadata registry against the `write_csv(na = "NA")` round-trip
#' described in `R/registry.R`. Errors listing the offending columns and rows
#' rather than returning quietly, because the whole failure mode is silence.
#'
#' @param df a data.frame read from a registry CSV
#' @param path optional source path, used in the error message
#' @param sentinels character strings that must never appear as literal values
#'   (default `"NA"`, `"NaN"`, `"NULL"`, `"N/A"`, `"na"`)
#' @param cols character columns to check (default: all character columns)
#'
#' @return `df`, invisibly and unchanged, when clean
#' @export
#' @concept registry
#' @examples
#' \dontrun{
#' check_registry_na_strings(readr::read_csv("metadata/measurement_type.csv"))
#' }
check_registry_na_strings <- function(df, path = NULL,
                                      sentinels = c("NA", "NaN", "NULL", "N/A", "na"),
                                      cols = NULL) {
  if (is.null(cols)) {
    cols <- names(df)[vapply(df, is.character, logical(1))]
  }
  hits <- list()
  for (cl in intersect(cols, names(df))) {
    v <- df[[cl]]
    bad <- which(!is.na(v) & trimws(v) %in% sentinels)
    if (length(bad)) hits[[cl]] <- bad
  }
  if (length(hits)) {
    detail <- paste(vapply(names(hits), function(cl) sprintf(
      "%s (%d row%s, e.g. row %d = %s)", cl, length(hits[[cl]]),
      if (length(hits[[cl]]) == 1) "" else "s",
      hits[[cl]][1], encodeString(df[[cl]][hits[[cl]][1]], quote = "\"")), ""),
      collapse = "; ")
    stop(
      "registry contains literal sentinel strings where cells should be empty",
      if (!is.null(path)) paste0(" in ", path) else "", ":\n  ", detail,
      "\n  Almost certainly written by write_csv() without `na = \"\"`. DuckDB's",
      " read_csv_auto does NOT treat \"NA\" as NULL, so these would ship to the",
      " release as literal values.\n  Fix: rewrite the file with",
      " readr::write_csv(x, path, na = \"\"), and append to registries via",
      " calcofi4db::register_measurement_types() rather than by hand.",
      call. = FALSE)
  }
  invisible(df)
}

#' Read `metadata/measurement_type.csv`, refusing a corrupted registry
#'
#' The canonical measurement vocabulary, validated on the way in via
#' [check_registry_na_strings()] so a `write_csv(na = "NA")` round trip fails here
#' instead of silently reaching the release.
#'
#' The read is deliberately **strict**: `na = ""`, so only genuinely empty cells
#' become `NA`. `read_csv()`'s default is `na = c("", "NA")`, which converts the
#' literal string `"NA"` back to `NA` — meaning a default read *cannot see* this
#' corruption, and no validator downstream of one ever could. DuckDB, which is not
#' so forgiving, is where the damage surfaces.
#'
#' @param path path to `metadata/measurement_type.csv`
#' @param validate error on sentinel strings (default TRUE). Only set FALSE to
#'   inspect a file you already know is broken.
#'
#' @return A [tibble][tibble::tibble] of the registry, with empty cells as `NA`.
#' @export
#' @concept registry
#' @importFrom readr read_csv cols
read_measurement_type <- function(path, validate = TRUE) {
  stopifnot("measurement_type.csv not found" = file.exists(path))
  # na = "" is load-bearing, not stylistic — see above
  d <- readr::read_csv(path, na = "", show_col_types = FALSE)
  if (isTRUE(validate)) check_registry_na_strings(d, path)
  d
}

#' Append new measurement types to the shared registry, safely
#'
#' Replaces the read / `bind_rows` / `write_csv` cycle that each ingest used to
#' hand-roll. Reads and validates the registry, appends only types not already
#' present, and writes with `na = ""` so empty cells stay empty. Writes nothing
#' when there is nothing new to add.
#'
#' @param new_types data.frame of candidate rows; must have a `measurement_type`
#'   column. Columns absent from the registry are dropped with a warning, so a
#'   stray column cannot silently widen the registry.
#' @param path path to `metadata/measurement_type.csv`
#' @param quiet suppress the "added N type(s)" message
#'
#' @return The full updated registry (invisibly if nothing changed), suitable for
#'   `dbWriteTable(con, "measurement_type", ...)`.
#' @export
#' @concept registry
#' @importFrom readr write_csv
#' @examples
#' \dontrun{
#' d_meas_type <- register_measurement_types(
#'   my_new_types, here::here("metadata/measurement_type.csv"))
#' dbWriteTable(con, "measurement_type", as.data.frame(d_meas_type), overwrite = TRUE)
#' }
register_measurement_types <- function(new_types, path, quiet = FALSE) {
  d <- read_measurement_type(path)
  if (is.null(new_types) || !nrow(new_types)) return(d)
  stopifnot("new_types needs a measurement_type column" =
              "measurement_type" %in% names(new_types))

  extra <- setdiff(names(new_types), names(d))
  if (length(extra)) {
    warning("dropping column(s) not in the registry: ",
            paste(extra, collapse = ", "), call. = FALSE)
    new_types <- new_types[, setdiff(names(new_types), extra), drop = FALSE]
  }

  add <- new_types[!new_types$measurement_type %in% d$measurement_type, , drop = FALSE]
  # a duplicate WITHIN new_types would slip past the check above
  add <- add[!duplicated(add$measurement_type), , drop = FALSE]
  if (!nrow(add)) {
    if (!quiet) message("measurement_type registry unchanged (no new types)")
    return(d)
  }

  out <- dplyr::bind_rows(d, add)
  out <- out[order(out$measurement_type), , drop = FALSE]
  # na = "" is the whole point: the default na = "NA" is what corrupts this file
  readr::write_csv(out, path, na = "")
  # re-read so the caller gets exactly what is now on disk, and so a bad write
  # trips the validator immediately rather than at some later reader
  out <- read_measurement_type(path)
  if (!quiet)
    message(glue::glue(
      "measurement_type registry: added {nrow(add)} type(s) — ",
      "{paste(add$measurement_type, collapse = ', ')}"))
  out
}

#' Declare `valid_min` / `valid_max` on measurement types that already exist
#'
#' [register_measurement_types()] only ever *appends* — by design, so an ingest
#' cannot silently rewrite a type another dataset relies on. That leaves no way to
#' do the thing the bounds convention asks for most often: put a bound on a type
#' that is already registered without one. All 73 unbounded types at v2026.08.07
#' were in exactly that state, so "declare it with `register_measurement_types()`"
#' was advice that could not be followed.
#'
#' This is the narrow, auditable counterpart: it changes **only** the four bound
#' columns, only on rows that already exist, and it refuses an unknown
#' `measurement_type` rather than quietly adding one — a typo would otherwise
#' create a bound-carrying orphan that no observation ever matches.
#'
#' @param bounds data.frame with `measurement_type` and at least one of
#'   `valid_min`, `valid_max`, `valid_depth_min_m`, `valid_depth_max_m`. `NA`
#'   leaves that bound undeclared; supply only the side you can defend.
#' @param path path to `metadata/measurement_type.csv`
#' @param overwrite allow replacing a bound that is already declared (default
#'   FALSE). A declared bound has been agreed with a provider, so changing it is
#'   a deliberate act, not a side effect of re-running an ingest.
#' @param quiet suppress the summary message
#'
#' @return The full updated registry, invisibly if nothing changed.
#' @export
#' @concept registry
#' @seealso [check_measurement_bounds()], which is what consumes these.
#' @importFrom readr write_csv
#' @examples
#' \dontrun{
#' declare_measurement_bounds(
#'   data.frame(measurement_type = "zooscan_abundance", valid_min = 0),
#'   here::here("metadata/measurement_type.csv"))
#' }
declare_measurement_bounds <- function(bounds, path, overwrite = FALSE,
                                       quiet = FALSE) {
  d <- read_measurement_type(path)
  if (is.null(bounds) || !nrow(bounds)) return(invisible(d))
  stopifnot("bounds needs a measurement_type column" =
              "measurement_type" %in% names(bounds))

  cols <- intersect(c("valid_min", "valid_max",
                      "valid_depth_min_m", "valid_depth_max_m"), names(bounds))
  if (!length(cols))
    stop("bounds has none of valid_min / valid_max / valid_depth_min_m / ",
         "valid_depth_max_m", call. = FALSE)

  unknown <- setdiff(bounds$measurement_type, d$measurement_type)
  if (length(unknown))
    stop("not in the registry: ", paste(unknown, collapse = ", "),
         "\n  A bound on an unregistered type would never match an observation.",
         "\n  Add the type first with register_measurement_types().", call. = FALSE)

  dup <- unique(bounds$measurement_type[duplicated(bounds$measurement_type)])
  if (length(dup))
    stop("duplicate measurement_type in bounds: ", paste(dup, collapse = ", "),
         call. = FALSE)

  # A registry predating the bound columns gains them, rather than the update
  # silently no-op'ing against a NULL column. Added in PAIRS, because the
  # documented schema has both halves and a file carrying `valid_min` with no
  # `valid_max` column reads as "no maximum is possible here" rather than "no
  # maximum has been declared yet".
  for (pair in list(c("valid_min", "valid_max"),
                    c("valid_depth_min_m", "valid_depth_max_m")))
    if (any(pair %in% cols))
      for (cl in pair) if (!cl %in% names(d)) d[[cl]] <- NA_real_

  i <- match(bounds$measurement_type, d$measurement_type)
  changed <- character()
  for (cl in cols) {
    new <- suppressWarnings(as.numeric(bounds[[cl]]))
    old <- suppressWarnings(as.numeric(d[[cl]][i]))
    # only touch rows where a value is actually supplied
    set <- !is.na(new)
    clash <- set & !is.na(old) & old != new
    if (any(clash) && !isTRUE(overwrite))
      stop("would change an already-declared ", cl, " for: ",
           paste(bounds$measurement_type[clash], collapse = ", "),
           "\n  A declared bound has been agreed; pass overwrite = TRUE to change it.",
           call. = FALSE)
    hit <- set & (is.na(old) | old != new)
    if (any(hit)) {
      d[[cl]][i[hit]] <- new[hit]
      changed <- c(changed, bounds$measurement_type[hit])
    }
  }

  if (!length(changed)) {
    if (!quiet) message("measurement_type bounds unchanged")
    return(invisible(d))
  }

  # na = "" — the whole reason R/registry.R exists
  readr::write_csv(d, path, na = "")
  d <- read_measurement_type(path)
  changed <- unique(changed)
  if (!quiet)
    message(glue::glue(
      "measurement_type bounds: declared on {length(changed)} type(s) — ",
      "{paste(utils::head(changed, 8), collapse = ', ')}",
      "{if (length(changed) > 8) glue::glue(' … +{length(changed) - 8} more') else ''}"))
  d
}

#' Replace a measurement type's definition while keeping its curated columns
#'
#' Ten ingests "register" their types by deleting the existing row and binding a
#' freshly-built literal in its place:
#'
#' ```r
#' d_meas_type |> filter(measurement_type != "euphausiid_abundance") |>
#'   bind_rows(euph_types)          # <- literal, no valid_min/valid_max
#' ```
#'
#' Every column the literal omits is destroyed on each re-run. That is how a
#' provider-agreed `valid_min` silently disappeared from `euphausiid_abundance`
#' and the four picoplankton types during the v2026.08.08 re-render: the ingests
#' had not changed, but a curated column had been added underneath them. Only
#' `ingest_calcofi_mets.qmd` did the preserve-and-merge dance by hand.
#'
#' Use this instead of `filter(... != x) |> bind_rows(new)`. It replaces the
#' definition columns the ingest owns and carries the curated ones forward from
#' the row being replaced, so a re-run cannot quietly narrow the registry.
#'
#' @param d the current registry (a data.frame, e.g. from
#'   [read_measurement_type()])
#' @param new_types data.frame of definitions to upsert; needs `measurement_type`
#' @param authoritative registry-owned columns (default
#'   [declarable_measurement_fields()]): the existing registry value wins
#'   whenever it is non-NA, even over an explicit value in `new_types`, because
#'   only [declare_measurement_fields()] may set them. A type new to the
#'   registry takes the literal's value.
#' @param preserve columns to carry forward from the existing row when
#'   `new_types` does not supply a non-`NA` value. Defaults to the bound columns —
#'   the ones an ingest never authors and a provider has agreed.
#'
#' @return The updated registry, sorted by `measurement_type`.
#' @export
#' @concept registry
#' @seealso [declare_measurement_bounds()] to set a bound,
#'   [register_measurement_types()] to append a genuinely new type.
#' @examples
#' \dontrun{
#' d_meas_type <- upsert_measurement_types(d_meas_type, euph_types)
#' readr::write_csv(d_meas_type, meas_type_csv, na = "")
#' }
upsert_measurement_types <- function(
    d, new_types,
    preserve = c("valid_min", "valid_max",
                 "valid_depth_min_m", "valid_depth_max_m"),
    authoritative = declarable_measurement_fields()) {

  if (is.null(new_types) || !nrow(new_types)) return(d)
  stopifnot("new_types needs a measurement_type column" =
              "measurement_type" %in% names(new_types))
  dup <- unique(new_types$measurement_type[duplicated(new_types$measurement_type)])
  if (length(dup))
    stop("duplicate measurement_type in new_types: ",
         paste(dup, collapse = ", "), call. = FALSE)

  for (cl in preserve) {
    if (!cl %in% names(d)) next
    old <- d[[cl]][match(new_types$measurement_type, d$measurement_type)]
    if (!cl %in% names(new_types)) {
      new_types[[cl]] <- old
    } else {
      # an explicit value in new_types wins; NA means "not authored here"
      keep <- is.na(new_types[[cl]])
      new_types[[cl]][keep] <- old[keep]
    }
  }

  # the registry-owned fields (set only through declare_measurement_fields()):
  # the registry's value wins whenever it has one, even over an explicit value in
  # the ingest's literal — an ingest re-run must never undo a declaration. Until
  # 4.0.0 these were not preserved at all, so re-rendering
  # ingest_cce-lter_euphausiids.qmd blanked `category`, `variable` and
  # `units_nerc_p06` on euphausiid_abundance (found 2026-09-04, WS-E Phase 3b).
  # A type new to the registry takes whatever the literal supplies.
  for (cl in setdiff(authoritative, preserve)) {
    if (!cl %in% names(d)) next
    old <- d[[cl]][match(new_types$measurement_type, d$measurement_type)]
    if (!cl %in% names(new_types)) {
      new_types[[cl]] <- old
    } else {
      keep <- !is.na(old)
      new_types[[cl]][keep] <- old[keep]
    }
  }

  out <- dplyr::bind_rows(
    d[!d$measurement_type %in% new_types$measurement_type, , drop = FALSE],
    new_types)
  out[order(out$measurement_type), , drop = FALSE]
}

#' The columns [declare_measurement_fields()] is allowed to touch
#'
#' `category` / `variable` were the original two (the Explorer's *Browse* tab);
#' `derivation` / `is_canonical` were added in calcofi4db 3.29.0 so the bottle
#' `r_*` pre-QC types could record "interpolated to standard depth, not an input
#' for further interpolation" and flip `is_canonical` to FALSE without a bare
#' `write_csv()` (WS-G, 2026-09-03). Both new columns are treated as character —
#' `is_canonical` is stored in the CSV as the literal string `"TRUE"`/`"FALSE"`,
#' matching how [read_measurement_type()] reads it, so no type coercion happens
#' here that the registry's own round trip does not already do.
#'
#' `nerc_p01` / `units_nerc_p06` followed in 3.32.0 (WS-H2, pre-release decision
#' D-S2): the controlled-vocabulary ids a portal export needs — OBIS/DwC eMoF's
#' `measurementTypeID` (NERC BODC Parameter Usage Vocabulary P01) and
#' `measurementUnitID` (NERC P06). Both hold the **full concept URI**, and both
#' are filled only on an exact vocabulary match, so an empty cell means "no
#' concept states exactly what this type is", never "not looked at".
#'
#' @return Character vector of the allowed field names.
#' @keywords internal
declarable_measurement_fields <- function()
  c("category", "variable", "derivation", "is_canonical", "nerc_p01", "units_nerc_p06")

#' The NERC NVS collections [declare_measurement_fields()] validates against
#'
#' Maps each vocabulary column to the collection its URIs must come from. A typo
#' in a concept id is invisible (it is just a string), but a URI in the wrong
#' collection — a P06 unit where a P01 parameter belongs — is exactly the kind of
#' mistake that reaches a portal export intact, so the prefix is checked.
#'
#' @return Named character vector: column name -> required URI prefix.
#' @keywords internal
nerc_uri_prefixes <- function() c(
  nerc_p01       = "http://vocab.nerc.ac.uk/collection/P01/current/",
  units_nerc_p06 = "http://vocab.nerc.ac.uk/collection/P06/current/")

#' Declare `category` / `variable` / `derivation` / `is_canonical` / NERC ids on measurement types that already exist
#'
#' Six descriptive columns, none of them an ingest's own definition:
#' * `category` — one of the registered categories (`metadata/category.csv`:
#'   *Physical Oceanography*, *Nutrients & Chemistry*, *Carbonate System*,
#'   *Productivity & Pigments*, *Meteorology & Sea State*, …), read by the
#'   CalCOFI Explorer's *Browse* tab (explorer UI plan D14).
#' * `variable` — the crosswalk that says which types measure the same thing
#'   comparably across datasets (`temperature` for the bottle's `temperature`
#'   and the CTD's `temperature_ave`), which the explorer carried in
#'   `src/variables.ts` as a stopgap.
#' * `derivation` — free text saying how a *derived* type was produced (the
#'   `_cruise_corr` vs `_sta_corr` distinction, or that a pre-QC `r_*` type is
#'   "interpolated to standard depth and carries no quality code by design").
#' * `is_canonical` — whether the type reaches the default `obs`/`ctd_thin`
#'   selection; a provider-confirmed fact like "the bottle's `r_*` series are
#'   interpolated, so they are not canonical" belongs here, not in an ingest's
#'   own literal.
#' * `nerc_p01` — the NERC BODC Parameter Usage Vocabulary (P01) concept URI
#'   that a DwC/OBIS eMoF export emits as `measurementTypeID`.
#' * `units_nerc_p06` — the NERC P06 unit concept URI, emitted as
#'   `measurementUnitID`.
#'
#' The two vocabulary columns are validated against
#' [nerc_uri_prefixes()]: a value must be a full concept URI in the right
#' collection (`.../collection/P01/current/<CODE>/`). They are filled **only on
#' an exact vocabulary match** — a concept every one of whose stated facets
#' (quantity, matrix, phase, method) the registry or the dataset's documented
#' protocol actually supplies. A generic concept is an exact match at coarser
#' specificity (`TEMPPR01`, *Temperature of the water body*); a concept that
#' adds a facet nobody recorded is not. So an empty cell means "no concept says
#' exactly this", never "not looked at", and inventing one to fill the column is
#' the same mistake as inventing a bound to quiet
#' [check_measurement_bounds()].
#'
#' Like [declare_measurement_bounds()] this changes **only** these columns,
#' only on rows that already exist, refuses an unknown `measurement_type`, and
#' writes with `na = ""`. A registry predating a column gains it.
#'
#' @param fields data.frame with `measurement_type` and at least one of
#'   `category`, `variable`, `derivation`, `is_canonical`, `nerc_p01`,
#'   `units_nerc_p06` ([declarable_measurement_fields()]). `NA` leaves that
#'   field as it is.
#' @param path path to `metadata/measurement_type.csv`
#' @param categories the allowed `category` values — the `category` column of
#'   `metadata/category.csv`; `NULL` skips the check (not recommended)
#' @param overwrite allow replacing a value that is already declared (default
#'   FALSE)
#' @param quiet suppress the summary message
#'
#' @return The full updated registry, invisibly if nothing changed.
#' @export
#' @concept registry
#' @seealso [build_coverage()], which puts `category`/`variable` onto
#'   `coverage.json`'s `variables[]`.
#' @importFrom readr write_csv
declare_measurement_fields <- function(fields, path, categories = NULL, overwrite = FALSE, quiet = FALSE) {
  d <- read_measurement_type(path)
  if (is.null(fields) || !nrow(fields)) return(invisible(d))
  stopifnot("fields needs a measurement_type column" = "measurement_type" %in% names(fields))
  cols <- intersect(declarable_measurement_fields(), names(fields))
  if (!length(cols))
    stop("fields has none of ", paste(declarable_measurement_fields(), collapse = " / "), call. = FALSE)
  unknown <- setdiff(fields$measurement_type, d$measurement_type)
  if (length(unknown))
    stop("not in the registry: ", paste(unknown, collapse = ", "),
         "\n  Add the type first with register_measurement_types().", call. = FALSE)
  dup <- unique(fields$measurement_type[duplicated(fields$measurement_type)])
  if (length(dup)) stop("duplicate measurement_type in fields: ", paste(dup, collapse = ", "), call. = FALSE)
  if ("category" %in% cols && !is.null(categories)) {
    bad <- setdiff(stats::na.omit(unique(as.character(fields$category))), categories)
    if (length(bad))
      stop("category not in the registry (metadata/category.csv): ", paste(bad, collapse = ", "), call. = FALSE)
  }
  # a NERC id is just a string, so a wrong-collection URI would ship intact
  for (cl in intersect(names(nerc_uri_prefixes()), cols)) {
    pre <- unname(nerc_uri_prefixes()[cl])
    v   <- stats::na.omit(unique(as.character(fields[[cl]])))
    v   <- v[nzchar(trimws(v))]
    bad <- v[!grepl(paste0("^", gsub("([.|()\\^{}+$*?\\[\\]])", "\\\\\\1", pre), "[A-Za-z0-9_]+/$"), v)]
    if (length(bad))
      stop(cl, " must be a full NERC concept URI of the form ", pre, "<CODE>/ — got: ",
           paste(bad, collapse = ", "), call. = FALSE)
  }
  for (cl in declarable_measurement_fields()) if (!cl %in% names(d)) d[[cl]] <- NA_character_
  i <- match(fields$measurement_type, d$measurement_type)
  changed <- character()
  for (cl in cols) {
    new <- as.character(fields[[cl]]); new[!nzchar(trimws(new)) | is.na(new)] <- NA_character_
    old <- as.character(d[[cl]][i])
    set <- !is.na(new)
    clash <- set & !is.na(old) & old != new
    if (any(clash) && !isTRUE(overwrite))
      stop("would change an already-declared ", cl, " for: ", paste(fields$measurement_type[clash], collapse = ", "),
           "\n  Pass overwrite = TRUE to change it.", call. = FALSE)
    hit <- set & (is.na(old) | old != new)
    if (any(hit)) { d[[cl]][i[hit]] <- new[hit]; changed <- c(changed, fields$measurement_type[hit]) }
  }
  if (!length(changed)) { if (!quiet) message("measurement_type fields unchanged"); return(invisible(d)) }
  readr::write_csv(d, path, na = "")
  d <- read_measurement_type(path)
  if (!quiet) message(glue::glue("measurement_type registry: declared {paste(cols, collapse = ' / ')} on {length(unique(changed))} type(s)"))
  d
}
