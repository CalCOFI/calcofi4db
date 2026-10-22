# source archive: every file an ingest reads, kept once by content ----------------------

#' Where a source file is archived on GCS
#'
#' Sources are kept by content, not by date: `{prefix}/{provider}/{dataset}/{md5}/{file}`.
#' A provider that re-publishes a corrected file under the same name gets a second
#' object beside the first, never an overwrite, and a file read by many renders is
#' stored once.
#'
#' @param md5 hexadecimal MD5 of the file
#' @param file the file's base name
#' @param provider,dataset the ingest's provider and dataset
#' @param prefix archive prefix within the bucket (default `"archive/sources"`)
#' @return object paths, relative to the bucket
#' @export
#' @concept archive
source_archive_path <- function(md5, file, provider, dataset, prefix = "archive/sources") {
  stopifnot(length(provider) == 1, length(dataset) == 1,
            all(grepl("^[0-9a-f]{32}$", md5)), all(nzchar(file)))
  file.path(prefix, provider, dataset, md5, basename(file))
}

#' Decide what archiving each source file needs
#'
#' Pure: compares the local files with what the archive (and optionally a mirror
#' such as `_sync/`) already holds. A file whose archive path exists is `present`;
#' one the mirror holds with the same name and MD5 is `copy` (server side, no bytes
#' leave the machine); anything else is `upload`.
#'
#' @param local tibble with `file` (base name), `md5` (hex) and `archive` (object
#'   path from [source_archive_path()])
#' @param archived object paths already under the archive prefix
#' @param mirror optional tibble of mirror objects: `name` (object path) and `md5`
#'   (hex)
#' @return `local` with `action` (`present`, `copy`, `upload`) and `copy_from`
#'   (the mirror object path for a `copy`, else `NA`)
#' @export
#' @concept archive
plan_source_archive <- function(local, archived = character(), mirror = NULL) {
  stopifnot(all(c("file", "md5", "archive") %in% names(local)))
  key_local <- paste(basename(local$file), local$md5)
  copy_from <- rep(NA_character_, nrow(local))
  if (!is.null(mirror) && nrow(mirror)) {
    key_mirror <- paste(basename(mirror$name), mirror$md5)
    copy_from  <- mirror$name[match(key_local, key_mirror)]
  }
  present <- local$archive %in% archived
  local$action    <- ifelse(present, "present", ifelse(is.na(copy_from), "upload", "copy"))
  local$copy_from <- ifelse(local$action == "copy", copy_from, NA_character_)
  local
}

#' GCS base64 MD5 to hexadecimal
#' @noRd
md5_base64_to_hex <- function(x) {
  vapply(x, function(b) {
    if (is.na(b) || !nzchar(b)) return(NA_character_)
    paste(format(as.hexmode(as.integer(jsonlite::base64_dec(b))), width = 2), collapse = "")
  }, character(1), USE.NAMES = FALSE)
}

#' Keep every source file an ingest reads in an immutable GCS archive
#'
#' A file fetched from the web lives only where the render ran until something
#' copies it off the machine; on the Mac mini, `dir_data` is a plain folder that
#' nothing syncs back to GCS, so a provider that later withdraws a file takes the
#' only copy with it (calcofi.org's 1995-98 FinalQC zips, 2026-10-20). Call this
#' after the downloads, on the files the ingest reads. Each file lands once at
#' [source_archive_path()]: an object already there costs nothing, one the `_sync/`
#' mirror holds is copied server side, anything else is uploaded. A failed copy or
#' upload stops the render.
#'
#' The result is a [stamp_source_access()] table with the archive columns added,
#' ready for [build_metadata_json()]'s `sources` argument, so `metadata.json`
#' `sources[]` says which bytes were read (`md5`), from where (`url`) and where the
#' copy is kept (`archive`).
#'
#' @param files local source files
#' @param provider,dataset the ingest's provider and dataset
#' @param urls where each file was fetched from (`NA` for a file that arrived by
#'   hand or from Drive); recycled to `files`
#' @param bucket GCS bucket (default `"calcofi-files-public"`; an
#'   `in_release: false` dataset goes to `calcofi-files-private`, see
#'   [gcs_bucket_for_dataset()])
#' @param prefix archive prefix (default `"archive/sources"`)
#' @param mirror_prefix a mirror to copy from server side, e.g.
#'   `"_sync/calcofi/ctd-cast/download"`; `NULL` for none
#' @param dry_run if `TRUE`, plan only: nothing is copied or uploaded
#' @return A tibble: `source` (base name), `method` (`file_mtime`), `accessed`,
#'   `bytes`, `md5`, `url`, `archive` (`gs://` URI) and `action`.
#' @export
#' @concept archive
archive_sources <- function(files, provider, dataset, urls = NA_character_,
                            bucket = "calcofi-files-public", prefix = "archive/sources",
                            mirror_prefix = NULL, dry_run = FALSE) {
  stopifnot(length(files) > 0, all(file.exists(files)))
  # an unreleased dataset's sources are kept in the private twin, never the public archive
  to <- gcs_bucket_for_dataset(bucket, paste0(provider, "_", dataset))
  if (is.na(to)) stop(glue::glue("{provider}_{dataset} is in_release: false and has no private ",
                                 "bucket for gs://{bucket}"), call. = FALSE)
  if (!identical(to, bucket)) {
    message(glue::glue("{provider}_{dataset} is in_release: false: gs://{bucket} -> gs://{to}"))
    bucket <- to
  }
  urls  <- rep_len(as.character(urls), length(files))
  info  <- file.info(files)
  md5   <- unname(tools::md5sum(files))
  local <- tibble::tibble(
    file    = files,
    md5     = md5,
    archive = source_archive_path(md5, files, provider, dataset, prefix))

  archived <- list_gcs_files(bucket, prefix = file.path(prefix, provider, dataset, ""))$name
  mirror <- NULL
  if (!is.null(mirror_prefix)) {
    m <- list_gcs_files(bucket, prefix = paste0(sub("/$", "", mirror_prefix), "/"))
    if (nrow(m)) mirror <- tibble::tibble(name = m$name, md5 = md5_base64_to_hex(m$md5))
  }
  plan <- plan_source_archive(local, archived, mirror)

  if (!dry_run) {
    gcloud <- find_gcloud()
    for (i in which(plan$action != "present")) {
      dst <- glue::glue("gs://{bucket}/{plan$archive[i]}")
      status <- if (plan$action[i] == "copy") {
        system2(gcloud, c("storage", "cp", shQuote(glue::glue("gs://{bucket}/{plan$copy_from[i]}")),
                          shQuote(dst)), stdout = FALSE, stderr = FALSE)
      } else {
        system2(gcloud, c("storage", "cp", shQuote(plan$file[i]), shQuote(dst)),
                stdout = FALSE, stderr = FALSE)
      }
      if (!identical(as.integer(status), 0L))
        stop(glue::glue("archiving {basename(plan$file[i])} to {dst} failed ",
                        "(gcloud exit {status}); the source is not kept anywhere but here"))
    }
    n <- table(factor(plan$action, c("present", "copy", "upload")))
    message(glue::glue("archive_sources {provider}/{dataset}: {n[['present']]} present, ",
                       "{n[['copy']]} copied, {n[['upload']]} uploaded"))
  }

  accessed <- as.POSIXct(info$mtime)
  attr(accessed, "tzone") <- "UTC"
  tibble::tibble(
    source   = basename(files),
    method   = "file_mtime",
    accessed = accessed,
    bytes    = as.numeric(info$size),
    md5      = md5,
    url      = urls,
    archive  = paste0("gs://", bucket, "/", plan$archive),
    action   = plan$action)
}
