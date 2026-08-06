# stage.R — where bulk parquet lands locally on its way to GCS.
#
# The repo holds the reviewable half of an ingest's output (manifest.json,
# metadata.json, relationships.json — kilobytes, diffable, committed). The
# parquet is the other half: 24 GB across 16 datasets, rewritten wholesale on
# every run, and already published to gs://calcofi-db/. It has no business
# inside a git working tree, where it sat one `git add -A` away from history and
# forced a blanket ignore rule that swept the sidecars out of version control
# along with it.

#' Local Staging Root for Bulk Data
#'
#' Directory where bulk parquet is written on its way to GCS. Read from the
#' `CALCOFI_STAGE_DIR` environment variable, falling back to `~/_big/calcofi`.
#'
#' @details
#' Set this in `~/.Renviron` to a path that is **neither a git working tree nor
#' a synced cloud folder** — a 24 GB tree inside either one is a problem for
#' that tool, not a feature:
#'
#' ```
#' CALCOFI_STAGE_DIR=/Users/bbest/_big/calcofi
#' ```
#'
#' The fallback keeps a fresh clone working without setup. It is deliberately
#' outside the repo, so the failure mode of forgetting to set the variable is
#' "writes to an unexpected but harmless place", never "writes 24 GB back into
#' git".
#'
#' @return Absolute path, with `~` expanded. Not created.
#' @export
#' @concept cloud
#' @examples
#' cc_stage_dir()
cc_stage_dir <- function() {
  d <- Sys.getenv("CALCOFI_STAGE_DIR", "")
  if (!nzchar(trimws(d))) d <- "~/_big/calcofi"
  path.expand(trimws(d))
}

#' Path Within the Local Staging Root
#'
#' Joins path components onto [cc_stage_dir()].
#'
#' @param ... Path components, e.g. `"parquet", "calcofi_dic"`.
#' @param create If `TRUE`, create the directory (recursively) if absent.
#'   Use on a directory you are about to write into.
#'
#' @return Absolute path.
#' @export
#' @concept cloud
#' @examples
#' cc_stage_path("parquet", "calcofi_dic")
cc_stage_path <- function(..., create = FALSE) {
  p <- file.path(cc_stage_dir(), ...)
  if (isTRUE(create) && !dir.exists(p)) dir.create(p, recursive = TRUE)
  p
}
