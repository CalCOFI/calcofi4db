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
