# GCS archive operations for calcofi data workflow
# Manages immutable archives of source data files

#' convert base64-encoded md5 (from GCS) to hex string (from tools::md5sum)
#' @noRd
md5_base64_to_hex <- function(b64) {
  vapply(b64, function(x) {
    if (is.na(x)) return(NA_character_)
    raw <- jsonlite::base64_dec(x)
    paste0(format(as.hexmode(as.integer(raw)), width = 2), collapse = "")
  }, character(1), USE.NAMES = FALSE)
}

#' Get latest archive timestamp from GCS
#'
#' Finds the most recent archive timestamp in the GCS bucket.
#'
#' @param gcs_bucket GCS bucket name (default: "calcofi-files-public")
#' @param archive_prefix Prefix for archive folder (default: "archive")
#'
#' @return Character string with latest timestamp, or NULL if no archives exist
#' @export
#' @concept archive
#'
#' @examples
#' \dontrun{
#' latest <- get_latest_archive_timestamp()
#' # "2026-02-02_121557"
#' }
get_latest_archive_timestamp <- function(
    gcs_bucket     = "calcofi-files-public",
    archive_prefix = "archive") {

  gcs_path <- glue::glue("gs://{gcs_bucket}/{archive_prefix}/")

  # list top-level archive directories via simple gcloud ls (fast, non-recursive)
  gcloud <- find_gcloud()
  ls_out <- tryCatch(
    system2(gcloud, c("storage", "ls", gcs_path),
            stdout = TRUE, stderr = TRUE),
    error = function(e) character())

  if (length(ls_out) == 0) return(NULL)

  # extract timestamps from paths like "gs://bucket/archive/2026-02-02_121557/"
  timestamps <- stringr::str_extract(
    ls_out, "\\d{4}-\\d{2}-\\d{2}_\\d{6}") |>
    stats::na.omit() |>
    unique() |>
    sort(decreasing = TRUE)

  if (length(timestamps) == 0) {
    return(NULL)
  }

  timestamps[1]
}

#' Get archive manifest (file metadata)
#'
#' Retrieves metadata (size, md5, path) for all CSV files in a GCS archive.
#' The md5 hash is converted from GCS base64 encoding to hex for comparison
#' with \code{tools::md5sum()}.
#'
#' @param archive_timestamp Archive timestamp (e.g., "2026-02-02_121557")
#' @param provider Data provider (e.g., "swfsc")
#' @param dataset Dataset name (e.g., "ichthyo")
#' @param gcs_bucket GCS bucket name
#' @param archive_prefix Archive folder prefix
#'
#' @return Tibble with columns: name, size, md5, gcs_path
#' @export
#' @concept archive
#'
#' @examples
#' \dontrun{
#' manifest <- get_archive_manifest(
#'   archive_timestamp = "2026-02-02_121557",
#'   provider = "swfsc",
#'   dataset = "ichthyo")
#' }
#' @importFrom tibble tibble
#' @importFrom dplyr filter mutate select
#' @importFrom stringr str_detect
get_archive_manifest <- function(
    archive_timestamp,
    provider,
    dataset,
    gcs_bucket     = "calcofi-files-public",
    archive_prefix = "archive") {

  prefix <- glue::glue("{archive_prefix}/{archive_timestamp}/{provider}/{dataset}/")

  files <- tryCatch({
    list_gcs_files(gcs_bucket, prefix = prefix)
  }, error = function(e) {
    return(tibble::tibble(
      name = character(), size = numeric(), md5 = character()))
  })

  if (nrow(files) == 0) {
    return(tibble::tibble(
      name     = character(),
      size     = numeric(),
      md5      = character(),
      gcs_path = character()))
  }

  files |>
    dplyr::filter(stringr::str_detect(name, "\\.csv$")) |>
    dplyr::mutate(
      filename = basename(name),
      md5      = md5_base64_to_hex(md5),
      gcs_path = glue::glue("gs://{gcs_bucket}/{name}")) |>
    dplyr::select(name = filename, size, md5, gcs_path)
}

#' Get local file manifest
#'
#' Retrieves metadata (size, mtime, md5, path) for all CSV files in a local
#' directory. Uses \code{tools::md5sum()} for content hashing and
#' \code{file.mtime()} for modification timestamps.
#'
#' @param dir_csv Local directory containing CSV files
#'
#' @return Tibble with columns: name, size, mtime, md5, local_path
#' @export
#' @concept archive
#'
#' @examples
#' \dontrun{
#' manifest <- get_local_manifest("/path/to/csv/files")
#' }
#' @importFrom tibble tibble
get_local_manifest <- function(dir_csv) {
  csv_files <- list.files(dir_csv, pattern = "\\.csv$", full.names = TRUE)

  if (length(csv_files) == 0) {
    return(tibble::tibble(
      name       = character(),
      size       = numeric(),
      mtime      = as.POSIXct(character()),
      md5        = character(),
      local_path = character()))
  }

  tibble::tibble(
    name       = basename(csv_files),
    size       = file.size(csv_files),
    mtime      = file.mtime(csv_files),
    md5        = unname(tools::md5sum(csv_files)),
    local_path = csv_files)
}

#' Compare local files with GCS archive
#'
#' Compares local CSV files with a GCS archive to detect changes.
#' Uses md5 hash as primary comparison when available, with file size as
#' fallback when md5 is unavailable (e.g., gcloud CLI fallback).
#'
#' @param dir_csv Local directory containing CSV files
#' @param archive_timestamp Archive timestamp to compare against
#' @param provider Data provider
#' @param dataset Dataset name
#' @param gcs_bucket GCS bucket name
#' @param archive_prefix Archive folder prefix
#'
#' @return List with:
#'   \itemize{
#'     \item \code{matches}: Logical, TRUE if local matches archive
#'     \item \code{local_manifest}: Tibble of local files
#'     \item \code{archive_manifest}: Tibble of archive files
#'     \item \code{added}: Files in local but not archive
#'     \item \code{removed}: Files in archive but not local
#'     \item \code{changed}: Files with different content (md5) or size
#'   }
#' @export
#' @concept archive
#'
#' @examples
#' \dontrun{
#' comparison <- compare_local_vs_archive(
#'   dir_csv = "/path/to/csv",
#'   archive_timestamp = "2026-02-02_121557",
#'   provider = "swfsc",
#'   dataset = "ichthyo")
#'
#' if (!comparison$matches) {
#'   message("Local files have changed since archive")
#' }
#' }
#' @importFrom dplyr anti_join inner_join filter select
compare_local_vs_archive <- function(
    dir_csv,
    archive_timestamp,
    provider,
    dataset,
    gcs_bucket     = "calcofi-files-public",
    archive_prefix = "archive") {

  local_manifest <- get_local_manifest(dir_csv)

  archive_manifest <- get_archive_manifest(
    archive_timestamp = archive_timestamp,
    provider          = provider,
    dataset           = dataset,
    gcs_bucket        = gcs_bucket,
    archive_prefix    = archive_prefix)

  # find differences
  added   <- dplyr::anti_join(local_manifest, archive_manifest, by = "name")
  removed <- dplyr::anti_join(archive_manifest, local_manifest, by = "name")

  # find content changes in common files (md5 primary, size fallback)
  common <- dplyr::inner_join(
    local_manifest   |> dplyr::select(
      name, local_size = size, local_md5 = md5),
    archive_manifest |> dplyr::select(
      name, archive_size = size, archive_md5 = md5),
    by = "name")

  changed <- common |>
    dplyr::filter(
      # md5 mismatch (definitive when both available)
      (!is.na(local_md5) & !is.na(archive_md5) & local_md5 != archive_md5) |
      # size fallback when md5 unavailable
      ((is.na(local_md5) | is.na(archive_md5)) & local_size != archive_size))

  matches <- nrow(added) == 0 && nrow(removed) == 0 && nrow(changed) == 0

  list(
    matches          = matches,
    local_manifest   = local_manifest,
    archive_manifest = archive_manifest,
    added            = added,
    removed          = removed,
    changed          = changed)
}

#' Sync local files to GCS archive
#'
#' Creates a new timestamped archive in GCS from local files.
#' Only creates a new archive if local files differ from the latest archive.
#' The archive timestamp is derived from the maximum file modification time
#' (not render/wall-clock time), so re-rendering with unchanged files produces
#' the same timestamp and skips upload.
#'
#' @param dir_csv Local directory containing CSV files
#' @param provider Data provider
#' @param dataset Dataset name
#' @param gcs_bucket GCS bucket name
#' @param archive_prefix Archive folder prefix
#' @param force Force creation even if files match latest archive
#'
#' @return List with:
#'   \itemize{
#'     \item \code{archive_timestamp}: Timestamp of the archive used/created
#'     \item \code{archive_path}: Full GCS path to archive
#'     \item \code{created_new}: Logical, TRUE if new archive was created
#'     \item \code{files_uploaded}: Number of files uploaded (0 if using existing)
#'   }
#' @export
#' @concept archive
#'
#' @examples
#' \dontrun{
#' result <- sync_to_gcs_archive(
#'   dir_csv  = "/path/to/csv",
#'   provider = "swfsc",
#'   dataset  = "ichthyo")
#'
#' message(glue("Using archive: {result$archive_timestamp}"))
#' }
#' @importFrom glue glue
sync_to_gcs_archive <- function(
    dir_csv,
    provider,
    dataset,
    gcs_bucket     = "calcofi-files-public",
    archive_prefix = "archive",
    force          = FALSE) {

  # check latest archive
  latest_timestamp <- get_latest_archive_timestamp(gcs_bucket, archive_prefix)

  if (!is.null(latest_timestamp) && !force) {
    # compare with latest archive
    comparison <- compare_local_vs_archive(
      dir_csv           = dir_csv,
      archive_timestamp = latest_timestamp,
      provider          = provider,
      dataset           = dataset,
      gcs_bucket        = gcs_bucket,
      archive_prefix    = archive_prefix)

    if (comparison$matches) {
      message(glue::glue(
        "Local files match archive {latest_timestamp}, no sync needed"))
      return(list(
        archive_timestamp = latest_timestamp,
        archive_path      = glue::glue(
          "gs://{gcs_bucket}/{archive_prefix}/{latest_timestamp}/{provider}/{dataset}"),
        created_new       = FALSE,
        files_uploaded    = 0L))
    }

    # report changes
    if (nrow(comparison$added) > 0) {
      message(glue::glue(
        "New files: {paste(comparison$added$name, collapse = ', ')}"))
    }
    if (nrow(comparison$removed) > 0) {
      message(glue::glue(
        "Removed files: {paste(comparison$removed$name, collapse = ', ')}"))
    }
    if (nrow(comparison$changed) > 0) {
      message(glue::glue(
        "Changed files: {paste(comparison$changed$name, collapse = ', ')}"))
    }
  }

  # get local files
  local_manifest <- get_local_manifest(dir_csv)

  if (nrow(local_manifest) == 0) {
    stop("No CSV files found in local directory")
  }

  # use max file mtime as archive timestamp (not wall-clock time)
  max_mtime     <- max(local_manifest$mtime)
  new_timestamp <- format(max_mtime, "%Y-%m-%d_%H%M%S")

  # check if archive at this mtime-based timestamp already exists
  existing <- get_archive_manifest(
    archive_timestamp = new_timestamp,
    provider          = provider,
    dataset           = dataset,
    gcs_bucket        = gcs_bucket,
    archive_prefix    = archive_prefix)

  if (nrow(existing) > 0) {
    message(glue::glue(
      "Archive already exists at {new_timestamp} ",
      "(file modification date), skipping upload"))
    return(list(
      archive_timestamp = new_timestamp,
      archive_path      = glue::glue(
        "gs://{gcs_bucket}/{archive_prefix}/{new_timestamp}/{provider}/{dataset}"),
      created_new       = FALSE,
      files_uploaded    = 0L))
  }

  archive_base <- glue::glue(
    "{archive_prefix}/{new_timestamp}/{provider}/{dataset}")

  message(glue::glue("Creating new archive: {new_timestamp}"))

  # upload each file
  files_uploaded <- 0L
  for (i in seq_len(nrow(local_manifest))) {
    local_path <- local_manifest$local_path[i]
    filename   <- local_manifest$name[i]
    gcs_path   <- glue::glue("gs://{gcs_bucket}/{archive_base}/{filename}")

    tryCatch({
      put_gcs_file(local_path, gcs_path)
      files_uploaded <- files_uploaded + 1L
      message(glue::glue("  Uploaded: {filename}"))
    }, error = function(e) {
      warning(glue::glue("Failed to upload {filename}: {e$message}"))
    })
  }

  list(
    archive_timestamp = new_timestamp,
    archive_path      = glue::glue("gs://{gcs_bucket}/{archive_base}"),
    created_new       = TRUE,
    files_uploaded    = files_uploaded)
}

#' Download archive to local directory
#'
#' Downloads CSV files from a GCS archive to a local directory.
#'
#' @param archive_timestamp Archive timestamp
#' @param provider Data provider
#' @param dataset Dataset name
#' @param local_dir Local directory to download to (default: temp directory)
#' @param gcs_bucket GCS bucket name
#' @param archive_prefix Archive folder prefix
#'
#' @return Path to local directory containing downloaded files
#' @export
#' @concept archive
#'
#' @examples
#' \dontrun{
#' local_dir <- download_archive(
#'   archive_timestamp = "2026-02-02_121557",
#'   provider = "swfsc",
#'   dataset = "ichthyo")
#' }
#' @importFrom glue glue
download_archive <- function(
    archive_timestamp,
    provider,
    dataset,
    local_dir      = NULL,
    gcs_bucket     = "calcofi-files-public",
    archive_prefix = "archive") {

  # set default local directory
  if (is.null(local_dir)) {
    local_dir <- file.path(tempdir(), "archive", archive_timestamp, provider, dataset)
  }

  # create directory
  if (!dir.exists(local_dir)) {
    dir.create(local_dir, recursive = TRUE)
  }

  # get archive manifest
  manifest <- get_archive_manifest(
    archive_timestamp = archive_timestamp,
    provider          = provider,
    dataset           = dataset,
    gcs_bucket        = gcs_bucket,
    archive_prefix    = archive_prefix)

  if (nrow(manifest) == 0) {
    stop(glue::glue("No files found in archive {archive_timestamp}/{provider}/{dataset}"))
  }

  message(glue::glue("Downloading {nrow(manifest)} files from archive {archive_timestamp}..."))

  # download each file
  for (i in seq_len(nrow(manifest))) {
    gcs_path   <- manifest$gcs_path[i]
    filename   <- manifest$name[i]
    local_path <- file.path(local_dir, filename)

    # skip if already exists with same size
    if (file.exists(local_path) && file.size(local_path) == manifest$size[i]) {
      next
    }

    tryCatch({
      get_gcs_file(gcs_path, local_path = local_path, overwrite = TRUE)
    }, error = function(e) {
      warning(glue::glue("Failed to download {filename}: {e$message}"))
    })
  }

  message("Download complete")
  local_dir
}

#' Remove duplicate archives from GCS
#'
#' Compares md5 hashes across archive timestamps for a given provider/dataset.
#' When multiple archives have identical content, keeps the earliest and
#' removes the rest.
#'
#' @param provider Data provider (e.g., "swfsc")
#' @param dataset Dataset name (e.g., "ichthyo")
#' @param gcs_bucket GCS bucket name
#' @param archive_prefix Archive folder prefix
#' @param dry_run If TRUE (default), only report what would be removed
#'
#' @return Tibble of removed (or would-be-removed) archive timestamps
#' @export
#' @concept archive
#'
#' @examples
#' \dontrun{
#' # preview what would be removed
#' cleanup_duplicate_archives("swfsc", "ichthyo")
#'
#' # actually remove duplicates
#' cleanup_duplicate_archives("swfsc", "ichthyo", dry_run = FALSE)
#' }
#' @importFrom purrr map map_chr
#' @importFrom dplyr group_by filter slice ungroup mutate n
#' @importFrom rlang hash
cleanup_duplicate_archives <- function(
    provider,
    dataset,
    gcs_bucket     = "calcofi-files-public",
    archive_prefix = "archive",
    dry_run        = TRUE) {

  # list all archive timestamps
  all_archives <- list_gcs_files(
    gcs_bucket, prefix = glue::glue("{archive_prefix}/"))
  timestamps <- all_archives$name |>
    stringr::str_extract(
      glue::glue("{archive_prefix}/([^/]+)/"), group = 1) |>
    unique() |>
    stats::na.omit() |>
    sort()

  if (length(timestamps) < 2) {
    message("Fewer than 2 archives, nothing to deduplicate")
    return(tibble::tibble(
      timestamp = character(), action = character()))
  }

  # get manifest (with md5) for each timestamp
  manifests <- purrr::map(timestamps, function(ts) {
    get_archive_manifest(
      archive_timestamp = ts,
      provider          = provider,
      dataset           = dataset,
      gcs_bucket        = gcs_bucket,
      archive_prefix    = archive_prefix) |>
      dplyr::mutate(timestamp = ts)
  })

  # create content fingerprint per timestamp (hash of sorted name+md5 pairs)
  fingerprints <- purrr::map_chr(manifests, function(m) {
    if (nrow(m) == 0 || all(is.na(m$md5))) return(NA_character_)
    rlang::hash(sort(paste(m$name, m$md5)))
  })

  # find duplicates: keep first occurrence of each fingerprint
  fp_df <- tibble::tibble(
    timestamp   = timestamps,
    fingerprint = fingerprints)

  dupes <- fp_df |>
    dplyr::group_by(fingerprint) |>
    dplyr::filter(!is.na(fingerprint), dplyr::n() > 1) |>
    dplyr::slice(-1) |>
    dplyr::ungroup()

  if (nrow(dupes) == 0) {
    message("No duplicate archives found")
    return(tibble::tibble(
      timestamp = character(), action = character()))
  }

  message(glue::glue("Found {nrow(dupes)} duplicate archive(s)"))

  if (dry_run) {
    message("Dry run -- no files deleted. Set dry_run = FALSE to remove.")
    return(dupes |> dplyr::mutate(action = "would_remove"))
  }

  # delete duplicate archives
  for (ts in dupes$timestamp) {
    prefix <- glue::glue(
      "{archive_prefix}/{ts}/{provider}/{dataset}/")
    gcs_uri <- glue::glue("gs://{gcs_bucket}/{prefix}")
    cmd     <- glue::glue('gcloud storage rm -r "{gcs_uri}"')
    message(glue::glue("Removing: {gcs_uri}"))
    system(cmd, intern = TRUE)
  }

  dupes |> dplyr::mutate(action = "removed")
}
