# measurement-face registries: chem, method, scale, why, face -------------------
#
# Five small CSVs under `metadata/` back "the face" every measurement page shows
# (2026-09-11 plan "Measurement faces …", WS-MF1, D1-D4 D7, Appendix A):
#
#   measurement_chem.csv    key,chebi_id,role,mass_fraction,via,source,source_url,note
#   measurement_method.csv  dataset_key,measurement_type,platform,instrument,nerc_l22,
#                           principle,steps,wavelength_nm,precision,bibkeys,
#                           calcofi_org_url,text_fragment,source,source_url
#   measurement_scale.csv   key,value,lo,hi,label,kind,how,source,source_url
#   measurement_why.csv     key,rank,kind,text,bibkeys,source_url,eov,goos_doc
#   measurement_face.csv    key,face_kind,face_of,stands_in_note,source
#
# Same round-trip discipline as every other registry in this package (R/registry.R):
# `na = ""` on every write (readr's `na = "NA"` default is how a registry ships the
# literal string "NA" to DuckDB's `read_csv_auto`, whose `nullstr` default is the
# empty string only), a `#`-prefixed header comment naming the writer, and a strict
# read that refuses a corrupted file. Unlike `variable.csv`'s register_variables()
# (append-only, refuses a duplicate key), these five are filled by hand across a
# long-running workstream, so the register_* helpers here **append-or-update by the
# row's natural key** — a second call for the same key replaces that key's row(s)
# rather than erroring, so a workstream can be re-run without hand-editing the CSV
# first. `validate_measurement_faces()` is the standing-invariant check (never a
# controlled-vocabulary id filled without an exact match; every row sourced).

#' @importFrom readr read_csv write_csv format_csv cols col_character col_double col_integer
#' @importFrom dplyr bind_rows anti_join
NULL

# vocabularies (Appendix A) -----------------------------------------------------

#' @keywords internal
.measurement_chem_role_vocab <- function()
  c("the_thing", "component", "conjugate", "method_product", "one_of")

#' @keywords internal
.measurement_chem_via_vocab <- function() c("nerc_s27", "registry")

#' @keywords internal
.measurement_method_platform_vocab <- function()
  c("bottle", "ctd", "underway", "lab", "net", "mast")

#' @keywords internal
.measurement_scale_kind_vocab <- function()
  c("familiar", "physical", "threshold", "computed")

#' @keywords internal
.measurement_why_kind_vocab <- function() c("authored", "goos", "wikipedia", "calcofi")

#' @keywords internal
.measurement_face_kind_vocab <- function()
  c("structure", "composition", "scale", "organism", "standsin", "none")

# a `nerc_l22` (or any NERC concept URI) is a URI of this shape, never a bare code
#' @keywords internal
.nerc_concept_uri_re <- function(collection)
  paste0("^http://vocab\\.nerc\\.ac\\.uk/collection/", collection, "/current/[A-Za-z0-9_]+/$")

# column shapes -------------------------------------------------------------------

#' @keywords internal
.measurement_chem_cols <- function()
  c("key", "chebi_id", "role", "mass_fraction", "via", "source", "source_url", "note")

#' @keywords internal
.measurement_method_cols <- function()
  c("dataset_key", "measurement_type", "platform", "instrument", "nerc_l22", "principle",
    "steps", "wavelength_nm", "precision", "bibkeys", "calcofi_org_url", "text_fragment",
    "source", "source_url")

#' @keywords internal
.measurement_scale_cols <- function()
  c("key", "value", "lo", "hi", "label", "kind", "how", "source", "source_url")

#' @keywords internal
.measurement_why_cols <- function()
  c("key", "rank", "kind", "text", "bibkeys", "source_url", "eov", "goos_doc")

#' @keywords internal
.measurement_face_cols <- function()
  c("key", "face_kind", "face_of", "stands_in_note", "source")

#' @keywords internal
.measurement_col_types <- function(cols) {
  numeric_cols <- c("mass_fraction", "wavelength_nm", "value", "lo", "hi", "rank")
  spec <- lapply(cols, function(cl) if (cl %in% numeric_cols) readr::col_double() else readr::col_character())
  names(spec) <- cols
  do.call(readr::cols, spec)
}

#' @keywords internal
.measurement_header_comment <- function(file, cols, note) {
  paste0("# metadata/", file, " — ", note, " Columns: ", paste(cols, collapse = ", "),
         ". Read with calcofi4db::read_measurement_", sub("^measurement_", "", sub("\\.csv$", "", file)),
         "(), appended with calcofi4db::register_measurement_",
         sub("^measurement_", "", sub("\\.csv$", "", file)), "() — never a bare write_csv().")
}

# generic read/register/bootstrap ------------------------------------------------

#' @keywords internal
.read_measurement_registry <- function(path, cols, validate = TRUE) {
  stopifnot("registry file not found" = file.exists(path))
  d <- readr::read_csv(path, na = "", comment = "#", show_col_types = FALSE,
                        col_types = .measurement_col_types(cols))
  missing_cols <- setdiff(cols, names(d))
  if (length(missing_cols))
    stop(path, " is missing column(s): ", paste(missing_cols, collapse = ", "), call. = FALSE)
  if (isTRUE(validate)) check_registry_na_strings(d, path)
  d[, cols, drop = FALSE]
}

#' @keywords internal
.bootstrap_measurement_registry <- function(path, cols, header_comment) {
  if (file.exists(path)) return(invisible(NULL))
  empty <- stats::setNames(
    as.data.frame(replicate(length(cols), character(0), simplify = FALSE)), cols)
  writeLines(c(header_comment, readr::format_csv(empty, na = "")), path)
  invisible(NULL)
}

#' Append-or-update rows in a measurement-face registry by natural key
#'
#' Unlike [register_variables()] (append-only, refuses a duplicate), the five
#' measurement-face registries are filled incrementally by hand and by a
#' workstream that may be re-run, so a row whose natural key (`key_cols`)
#' matches an existing row **replaces** it; a genuinely new key is appended.
#' Always writes `na = ""`.
#'
#' @keywords internal
.register_measurement_registry <- function(new_rows, path, cols, key_cols, header_comment, label, quiet = FALSE) {
  .bootstrap_measurement_registry(path, cols, header_comment)
  d <- .read_measurement_registry(path, cols)
  if (is.null(new_rows) || !nrow(new_rows)) return(d)

  missing_key <- setdiff(key_cols, names(new_rows))
  if (length(missing_key))
    stop("new_rows needs column(s): ", paste(missing_key, collapse = ", "), call. = FALSE)

  extra <- setdiff(names(new_rows), cols)
  if (length(extra)) {
    warning("dropping column(s) not in the registry: ", paste(extra, collapse = ", "), call. = FALSE)
    new_rows <- new_rows[, setdiff(names(new_rows), extra), drop = FALSE]
  }
  for (cl in setdiff(cols, names(new_rows))) new_rows[[cl]] <- NA_character_
  new_rows <- new_rows[, cols, drop = FALSE]

  dup_key <- do.call(paste, c(new_rows[key_cols], sep = ""))
  if (any(duplicated(dup_key)))
    stop("duplicate natural key in new_rows: ",
         paste(unique(dup_key[duplicated(dup_key)]), collapse = "; "), call. = FALSE)

  old_key <- if (nrow(d)) do.call(paste, c(d[key_cols], sep = "")) else character(0)
  replaced <- sum(dup_key %in% old_key)
  d <- d[!old_key %in% dup_key, , drop = FALSE]
  out <- dplyr::bind_rows(d, new_rows)
  out <- out[do.call(order, out[key_cols]), , drop = FALSE]

  writeLines(c(header_comment, readr::format_csv(out, na = "")), path)
  out <- .read_measurement_registry(path, cols)
  if (!quiet)
    message(glue::glue(
      "{label}: added {nrow(new_rows) - replaced} new, updated {replaced} existing row(s)"))
  out
}

# measurement_chem.csv -------------------------------------------------------------

#' Read `metadata/measurement_chem.csv`
#' @param path path to `metadata/measurement_chem.csv`
#' @param validate error on sentinel strings (default TRUE)
#' @return tibble
#' @export
#' @concept registry
read_measurement_chem <- function(path, validate = TRUE)
  .read_measurement_registry(path, .measurement_chem_cols(), validate)

#' Append-or-update `metadata/measurement_chem.csv` by (key, chebi_id, role)
#'
#' `role` is one of `the_thing | component | conjugate | method_product | one_of`
#' and `via` one of `nerc_s27 | registry` (Appendix A). Refuses a row whose `role`
#' or `via` is outside that vocabulary, or whose `source` is empty — a
#' measurement-face row without a source is the one thing this registry never
#' ships (WS-MF1 gate).
#'
#' @param new_rows data.frame with `key`, `chebi_id`, `role` and any of
#'   `mass_fraction`, `via`, `source`, `source_url`, `note`
#' @param path path to `metadata/measurement_chem.csv`
#' @param quiet suppress the added/updated message
#' @return the full updated registry
#' @export
#' @concept registry
register_measurement_chem <- function(new_rows, path, quiet = FALSE) {
  if (!is.null(new_rows) && nrow(new_rows)) {
    bad_role <- setdiff(stats::na.omit(new_rows$role), .measurement_chem_role_vocab())
    if (length(bad_role)) stop("measurement_chem role outside Appendix A vocabulary: ",
                               paste(bad_role, collapse = ", "), call. = FALSE)
    if ("via" %in% names(new_rows)) {
      bad_via <- setdiff(stats::na.omit(new_rows$via), .measurement_chem_via_vocab())
      if (length(bad_via)) stop("measurement_chem via outside Appendix A vocabulary: ",
                                paste(bad_via, collapse = ", "), call. = FALSE)
    }
    if (!"source" %in% names(new_rows) || any(is.na(new_rows$source) | !nzchar(new_rows$source)))
      stop("measurement_chem: every row needs a source (never a guess)", call. = FALSE)
  }
  .register_measurement_registry(
    new_rows, path, .measurement_chem_cols(), c("key", "chebi_id", "role"),
    .measurement_header_comment("measurement_chem.csv", .measurement_chem_cols(),
      "one row per chemical entity behind a measurement key's face (D1-D2): role in\n# the_thing | component | conjugate | method_product | one_of; via in nerc_s27 | registry."),
    "measurement_chem registry", quiet)
}

# measurement_method.csv -----------------------------------------------------------

#' Read `metadata/measurement_method.csv`
#' @param path path to `metadata/measurement_method.csv`
#' @param validate error on sentinel strings (default TRUE)
#' @return tibble
#' @export
#' @concept registry
read_measurement_method <- function(path, validate = TRUE)
  .read_measurement_registry(path, .measurement_method_cols(), validate)

#' Append-or-update `metadata/measurement_method.csv` by (dataset_key, measurement_type)
#'
#' One row per series in `measurements.json` (D4): `platform` in `bottle | ctd |
#' underway | lab | net | mast`; `nerc_l22` only on an exact device match (empty
#' otherwise, per the `metadata-registries` skill's exact-match rule); `steps`
#' and `bibkeys` are `" | "`-joined lists on one CSV cell. A method the source
#' cannot support ships with `principle` empty and `source = "not found"` —
#' never a guess.
#'
#' @param new_rows data.frame with `dataset_key`, `measurement_type` and any
#'   other column of `.measurement_method_cols()`
#' @param path path to `metadata/measurement_method.csv`
#' @param quiet suppress the added/updated message
#' @return the full updated registry
#' @export
#' @concept registry
register_measurement_method <- function(new_rows, path, quiet = FALSE) {
  if (!is.null(new_rows) && nrow(new_rows) && "platform" %in% names(new_rows)) {
    bad <- setdiff(stats::na.omit(new_rows$platform), .measurement_method_platform_vocab())
    if (length(bad)) stop("measurement_method platform outside vocabulary: ",
                          paste(bad, collapse = ", "), call. = FALSE)
  }
  if (!is.null(new_rows) && nrow(new_rows)) {
    if (!"source" %in% names(new_rows) || any(is.na(new_rows$source) | !nzchar(new_rows$source)))
      stop("measurement_method: every row needs a source (\"not found\" is allowed; blank is not)", call. = FALSE)
  }
  .register_measurement_registry(
    new_rows, path, .measurement_method_cols(), c("dataset_key", "measurement_type"),
    .measurement_header_comment("measurement_method.csv", .measurement_method_cols(),
      "one row per series (dataset_key x measurement_type, D4): platform, instrument, nerc_l22\n# (exact match only), principle paraphrased from the source, steps and bibkeys ' | '-joined."),
    "measurement_method registry", quiet)
}

# measurement_scale.csv ------------------------------------------------------------

#' Read `metadata/measurement_scale.csv`
#' @param path path to `metadata/measurement_scale.csv`
#' @param validate error on sentinel strings (default TRUE)
#' @return tibble
#' @export
#' @concept registry
read_measurement_scale <- function(path, validate = TRUE)
  .read_measurement_registry(path, .measurement_scale_cols(), validate)

#' Append-or-update `metadata/measurement_scale.csv` by (key, label)
#'
#' `kind` is one of `familiar | physical | threshold | computed` (D7). A
#' `computed` mark's `value` is recomputed at build time from `how` (the
#' function and its inputs) and is never trusted from the CSV — this registry
#' holds the recipe and a value for reference, not the release's number.
#'
#' @param new_rows data.frame with `key`, `label` and any other column of
#'   `.measurement_scale_cols()`
#' @param path path to `metadata/measurement_scale.csv`
#' @param quiet suppress the added/updated message
#' @return the full updated registry
#' @export
#' @concept registry
register_measurement_scale <- function(new_rows, path, quiet = FALSE) {
  if (!is.null(new_rows) && nrow(new_rows)) {
    bad <- setdiff(stats::na.omit(new_rows$kind), .measurement_scale_kind_vocab())
    if (length(bad)) stop("measurement_scale kind outside Appendix A vocabulary: ",
                          paste(bad, collapse = ", "), call. = FALSE)
    if (!"source" %in% names(new_rows) || any(is.na(new_rows$source) | !nzchar(new_rows$source)))
      stop("measurement_scale: every mark needs a source", call. = FALSE)
  }
  .register_measurement_registry(
    new_rows, path, .measurement_scale_cols(), c("key", "label"),
    .measurement_header_comment("measurement_scale.csv", .measurement_scale_cols(),
      "the familiar-scale marks for a key with a face (D7): kind in familiar | physical |\n# threshold | computed; a computed mark's value is recomputed at build from `how`, never trusted from this CSV."),
    "measurement_scale registry", quiet)
}

# measurement_why.csv --------------------------------------------------------------

#' Read `metadata/measurement_why.csv`
#' @param path path to `metadata/measurement_why.csv`
#' @param validate error on sentinel strings (default TRUE)
#' @return tibble
#' @export
#' @concept registry
read_measurement_why <- function(path, validate = TRUE)
  .read_measurement_registry(path, .measurement_why_cols(), validate)

#' Append-or-update `metadata/measurement_why.csv` by (key, rank)
#'
#' `rank` 1 is the pick shown on the page; ranks 2+ are alternatives under a
#' collapsed details element (D5). Owned by WS-MF2 — WS-MF1 creates the file
#' with its header only.
#'
#' @param new_rows data.frame with `key`, `rank` and any other column of
#'   `.measurement_why_cols()`
#' @param path path to `metadata/measurement_why.csv`
#' @param quiet suppress the added/updated message
#' @return the full updated registry
#' @export
#' @concept registry
register_measurement_why <- function(new_rows, path, quiet = FALSE) {
  if (!is.null(new_rows) && nrow(new_rows) && "kind" %in% names(new_rows)) {
    bad <- setdiff(stats::na.omit(new_rows$kind), .measurement_why_kind_vocab())
    if (length(bad)) stop("measurement_why kind outside Appendix A vocabulary: ",
                          paste(bad, collapse = ", "), call. = FALSE)
  }
  .register_measurement_registry(
    new_rows, path, .measurement_why_cols(), c("key", "rank"),
    .measurement_header_comment("measurement_why.csv", .measurement_why_cols(),
      "why a key matters (D5): rank 1 is the page's pick, one per key; ranks 2+ are\n# alternatives (kind in authored | goos | wikipedia | calcofi) under a collapsed details element. Filled by WS-MF2."),
    "measurement_why registry", quiet)
}

# measurement_face.csv -------------------------------------------------------------

#' Read `metadata/measurement_face.csv`
#' @param path path to `metadata/measurement_face.csv`
#' @param validate error on sentinel strings (default TRUE)
#' @return tibble
#' @export
#' @concept registry
read_measurement_face <- function(path, validate = TRUE)
  .read_measurement_registry(path, .measurement_face_cols(), validate)

#' Append-or-update `metadata/measurement_face.csv` by key
#'
#' `face_kind` in `structure | composition | scale | organism | standsin |
#' none` (D2-D3). A `standsin` row's `face_of` names the key whose face it
#' borrows and `stands_in_note` is the chip's short text; a row that is not
#' `standsin` never carries `face_of` (no borrowed ids on a key that has its
#' own concept, D3).
#'
#' @param new_rows data.frame with `key` and any other column of
#'   `.measurement_face_cols()`
#' @param path path to `metadata/measurement_face.csv`
#' @param quiet suppress the added/updated message
#' @return the full updated registry
#' @export
#' @concept registry
register_measurement_face <- function(new_rows, path, quiet = FALSE) {
  if (!is.null(new_rows) && nrow(new_rows)) {
    bad <- setdiff(stats::na.omit(new_rows$face_kind), .measurement_face_kind_vocab())
    if (length(bad)) stop("measurement_face face_kind outside Appendix A vocabulary: ",
                          paste(bad, collapse = ", "), call. = FALSE)
    if (!"source" %in% names(new_rows) || any(is.na(new_rows$source) | !nzchar(new_rows$source)))
      stop("measurement_face: every row needs a source", call. = FALSE)
  }
  .register_measurement_registry(
    new_rows, path, .measurement_face_cols(), "key",
    .measurement_header_comment("measurement_face.csv", .measurement_face_cols(),
      "every measurement key's face_kind (D2-D3): structure | composition | scale |\n# organism | standsin | none. A standsin row's face_of names the key it borrows; stands_in_note is the chip text."),
    "measurement_face registry", quiet)
}

# the standing check ----------------------------------------------------------------

#' Validate the five measurement-face registries against Appendix A
#'
#' Returns findings rather than stopping (mirrors `check_measurement_bounds()`):
#' a workstream fills the registries, runs this, and resolves every row it
#' reports before calling itself done. Checks, one row of output each:
#'
#' * a `measurement_chem` / `measurement_method` / `measurement_scale` /
#'   `measurement_face` row with an empty `source`
#' * a value outside its column's Appendix A vocabulary (chem `role`/`via`,
#'   method `platform`, scale `kind`, why `kind`, face `face_kind`)
#' * a `measurement_why` key with zero, or more than one, `rank == 1` row
#' * a `measurement_face` key absent from `measurement_type.csv`
#' * a `nerc_l22` (method) that is not an exact-form L22 concept URI
#'
#' @param chem_path,method_path,scale_path,why_path,face_path paths to the
#'   five registries
#' @param measurement_type_path path to `metadata/measurement_type.csv`, used
#'   to check `measurement_face` keys exist. `NULL` skips that check.
#'
#' @return a tibble of findings (`registry`, `key`, `rule`, `detail`); zero
#'   rows means clean.
#' @export
#' @concept registry
validate_measurement_faces <- function(chem_path, method_path, scale_path, why_path, face_path,
                                        measurement_type_path = NULL) {
  chem   <- read_measurement_chem(chem_path)
  method <- read_measurement_method(method_path)
  scale  <- read_measurement_scale(scale_path)
  why    <- read_measurement_why(why_path)
  face   <- read_measurement_face(face_path)

  f <- list()
  add <- function(registry, key, rule, detail)
    f[[length(f) + 1]] <<- data.frame(registry = registry, key = key, rule = rule, detail = detail,
                                       stringsAsFactors = FALSE)

  # empty source
  no_src <- function(d, registry, keycol)
    if (nrow(d)) { bad <- is.na(d$source) | !nzchar(d$source)
                   if (any(bad)) add(registry, d[[keycol]][bad], "missing_source", "source is empty") }
  no_src(chem,   "measurement_chem",   "key")
  no_src(method, "measurement_method", "measurement_type")
  no_src(scale,  "measurement_scale",  "key")
  no_src(face,   "measurement_face",   "key")

  # vocabulary
  vocab_check <- function(d, col, vocab, registry, keycol)
    if (nrow(d) && col %in% names(d)) {
      bad <- !is.na(d[[col]]) & nzchar(d[[col]]) & !d[[col]] %in% vocab
      if (any(bad)) add(registry, d[[keycol]][bad], paste0("bad_", col),
                        paste0(d[[col]][bad], " not in {", paste(vocab, collapse = "|"), "}"))
    }
  vocab_check(chem, "role", .measurement_chem_role_vocab(), "measurement_chem", "key")
  vocab_check(chem, "via", .measurement_chem_via_vocab(), "measurement_chem", "key")
  vocab_check(method, "platform", .measurement_method_platform_vocab(), "measurement_method", "measurement_type")
  vocab_check(scale, "kind", .measurement_scale_kind_vocab(), "measurement_scale", "key")
  vocab_check(why, "kind", .measurement_why_kind_vocab(), "measurement_why", "key")
  vocab_check(face, "face_kind", .measurement_face_kind_vocab(), "measurement_face", "key")

  # measurement_why: exactly one rank == 1 per key
  if (nrow(why)) {
    one <- why[!is.na(why$rank) & why$rank == 1, , drop = FALSE]
    if (nrow(one)) {
      r1 <- stats::aggregate(rank ~ key, data = one, FUN = length)
      zero <- setdiff(unique(why$key), r1$key)
      two_plus <- r1$key[r1$rank > 1]
    } else {
      r1 <- data.frame(key = character(), rank = integer())
      zero <- unique(why$key)
      two_plus <- character()
    }
    if (length(zero)) add("measurement_why", zero, "rank1_count", "zero rank == 1 rows")
    if (length(two_plus)) add("measurement_why", two_plus, "rank1_count", "more than one rank == 1 row")
  }

  # measurement_face key must exist in measurement_type.csv
  if (!is.null(measurement_type_path) && nrow(face)) {
    mt <- read_measurement_type(measurement_type_path)
    orphan <- setdiff(face$key, mt$measurement_type)
    if (length(orphan)) add("measurement_face", orphan, "unknown_key",
                            "measurement_face key not in measurement_type.csv")
  }

  # nerc_l22 must be an exact-form L22 concept URI
  if (nrow(method) && "nerc_l22" %in% names(method)) {
    l22 <- method$nerc_l22
    set <- !is.na(l22) & nzchar(l22)
    bad <- set & !grepl(.nerc_concept_uri_re("L22"), l22)
    if (any(bad)) add("measurement_method", method$measurement_type[bad], "bad_nerc_l22",
                      paste0(l22[bad], " is not an L22 concept URI"))
  }

  if (!length(f)) return(tibble::tibble(registry = character(), key = character(),
                                         rule = character(), detail = character()))
  tibble::as_tibble(do.call(rbind, f))
}
