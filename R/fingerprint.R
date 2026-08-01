# input fingerprinting --------------------------------------------------------
#
# An ingest notebook is expensive (`ingest_calcofi_ctd-cast.qmd` is ~1 hour of
# download → parse → pivot) and is re-rendered for reasons that have nothing to do
# with its inputs: a narrative edit, a new diagnostic, a fixed typo. Re-running the
# heavy path in that case buys nothing, and the cost is that the notebook stops
# being usable as a living document — you do not add a paragraph to something that
# takes an hour to check.
#
# So: hash what the outputs actually depend on — the source file list and the
# metadata registries — and record it next to the outputs. Unchanged fingerprint +
# complete outputs means the heavy path can be skipped and the notebook rendered
# from what is already on disk.
#
# This is the same idea `write_parquet_outputs()` already applies per table (a
# content hash so an unchanged partition is not re-uploaded), lifted one level up
# to the whole ingest.
#
# A MISSING FILE IS PART OF THE FINGERPRINT, recorded as "<missing>" rather than
# skipped. Deleting a registry must invalidate the outputs; a fingerprint that
# ignores absent inputs would treat "the corrections file was removed" as no change.

#' Fingerprint the inputs an ingest's outputs depend on
#'
#' @param files paths to hash by content (missing paths are recorded as
#'   `"<missing>"`, which still changes the fingerprint)
#' @param values additional values to fold in — a scraped URL list, a package
#'   version, anything not on disk. Coerced to character and hashed in order, so
#'   sort first if the order is not meaningful.
#'
#' @return list with `hash` (a single string) and `parts` (named character vector,
#'   one entry per input, for reporting *what* changed)
#' @export
#' @concept ingest
#' @examples
#' \dontrun{
#' input_fingerprint(
#'   files  = c("metadata/measurement_type.csv", "metadata/measurement_qual.csv"),
#'   values = sort(d_zips$url))
#' }
input_fingerprint <- function(files = character(), values = character()) {
  files <- as.character(files)
  parts <- character(0)

  if (length(files)) {
    md5 <- unname(tools::md5sum(files))
    md5[is.na(md5)] <- "<missing>"
    names(md5) <- files
    parts <- c(parts, md5)
  }
  if (length(values)) {
    v <- rlang::hash(as.character(values))
    names(v) <- "<values>"
    parts <- c(parts, v)
  }

  list(hash  = rlang::hash(paste(names(parts), parts, sep = "=", collapse = "\n")),
       parts = parts)
}

#' Read a previously recorded input fingerprint
#'
#' @param path JSON file written by [write_input_fingerprint()]
#'
#' @return the recorded list (`hash`, `parts`, `recorded_at`), or `NULL` when the
#'   file is absent or unreadable — both mean "no usable prior state", which must
#'   fall through to a full run rather than error
#' @export
#' @concept ingest
read_input_fingerprint <- function(path) {
  if (!file.exists(path)) return(NULL)
  tryCatch(jsonlite::fromJSON(path), error = function(e) NULL)
}

#' Record an input fingerprint next to an ingest's outputs
#'
#' Written only after the outputs it describes are complete: a fingerprint saved
#' before the run finishes would let the next render skip a heavy path that never
#' actually produced anything.
#'
#' @param path JSON file to write
#' @param fp output of [input_fingerprint()]
#'
#' @return `path`, invisibly
#' @export
#' @concept ingest
write_input_fingerprint <- function(path, fp) {
  jsonlite::write_json(
    list(hash        = fp$hash,
         recorded_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
         parts       = as.list(fp$parts)),
    path, auto_unbox = TRUE, pretty = TRUE)
  invisible(path)
}

#' Which inputs changed since a recorded fingerprint
#'
#' @param fp output of [input_fingerprint()]
#' @param prior output of [read_input_fingerprint()]; `NULL` means everything is
#'   new
#'
#' @return character vector of input names that were added, removed or changed
#' @export
#' @concept ingest
changed_inputs <- function(fp, prior) {
  if (is.null(prior) || is.null(prior$parts)) return(names(fp$parts))
  old <- unlist(prior$parts)
  new <- fp$parts
  nms <- union(names(old), names(new))
  nms[vapply(nms, function(n) !identical(unname(old[n]), unname(new[n])), logical(1))]
}
