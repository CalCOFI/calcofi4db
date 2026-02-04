# GCS archive operations for calcofi data workflow
# Manages immutable archives of source data files

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

  # list archive directories
  archives <- tryCatch({
    list_gcs_files(gcs_bucket, prefix = glue::glue("{archive_prefix}/"))
  }, error = function(e) {
    return(NULL)
  })

  if (is.null(archives) || nrow(archives) == 0) {
    return(NULL)
  }

  # extract timestamps from paths like "archive/2026-02-02_121557/..."
  timestamps <- archives$name |>
    stringr::str_extract(glue::glue("{archive_prefix}/([^/]+)/"), group = 1) |>
    unique() |>
    stats::na.omit() |>
    sort(decreasing = TRUE)

  if (length(timestamps) == 0) {
    return(NULL)
  }

  timestamps[1]
}

#' Get archive manifest (file metadata)
#'
#' Retrieves metadata (size, path) for all files in a GCS archive.
#'
#' @param archive_timestamp Archive timestamp (e.g., "2026-02-02_121557")
#' @param provider Data provider (e.g., "swfsc.noaa.gov")
#' @param dataset Dataset name (e.g., "calcofi-db")
#' @param gcs_bucket GCS bucket name
#' @param archive_prefix Archive folder prefix
#'
#' @return Tibble with columns: name, size, gcs_path
#' @export
#' @concept archive
#'
#' @examples
#' \dontrun{
#' manifest <- get_archive_manifest(
#'   archive_timestamp = "2026-02-02_121557",
#'   provider = "swfsc.noaa.gov",
#'   dataset = "calcofi-db")
#' }
#' @importFrom tibble tibble
#' @importFrom dplyr filter mutate
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
    return(tibble::tibble(name = character(), size = numeric()))
  })

  if (nrow(files) == 0) {
    return(tibble::tibble(
      name     = character(),
      size     = numeric(),
      gcs_path = character()))
  }

  files |>
    dplyr::filter(stringr::str_detect(name, "\\.csv$")) |>
    dplyr::mutate(
      filename = basename(name),
      gcs_path = glue::glue("gs://{gcs_bucket}/{name}")) |>
    dplyr::select(name = filename, size, gcs_path)
}

#' Get local file manifest
#'
#' Retrieves metadata (size, path) for all CSV files in a local directory.
#'
#' @param dir_csv Local directory containing CSV files
#'
#' @return Tibble with columns: name, size, local_path
#' @export
#' @concept archive
#'
#' @examples
#' \dontrun{
#' manifest <- get_local_manifest("/path/to/csv/files")
#' }
#' @importFrom tibble tibble
#' @importFrom purrr map_dbl
get_local_manifest <- function(dir_csv) {
  csv_files <- list.files(dir_csv, pattern = "\\.csv$", full.names = TRUE)


  if (length(csv_files) == 0) {
    return(tibble::tibble(
      name       = character(),
      size       = numeric(),
      local_path = character()))
  }

  tibble::tibble(
    name       = basename(csv_files),
    size       = file.size(csv_files),
    local_path = csv_files)
}

#' Compare local files with GCS archive
#'
#' Compares local CSV files with a GCS archive to detect changes.
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
#'     \item \code{changed}: Files with different sizes
#'   }
#' @export
#' @concept archive
#'
#' @examples
#' \dontrun{
#' comparison <- compare_local_vs_archive(
#'   dir_csv = "/path/to/csv",
#'   archive_timestamp = "2026-02-02_121557",
#'   provider = "swfsc.noaa.gov",
#'   dataset = "calcofi-db")
#'
#' if (!comparison$matches) {
#'   message("Local files have changed since archive")
#' }
#' }
#' @importFrom dplyr anti_join inner_join filter
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
  added <- dplyr::anti_join(local_manifest, archive_manifest, by = "name")
  removed <- dplyr::anti_join(archive_manifest, local_manifest, by = "name")

  # find size changes in common files
  common <- dplyr::inner_join(
    local_manifest |> dplyr::select(name, local_size = size),
    archive_manifest |> dplyr::select(name, archive_size = size),
    by = "name")

  changed <- common |>
    dplyr::filter(local_size != archive_size)

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
#'   provider = "swfsc.noaa.gov",
#'   dataset  = "calcofi-db")
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
      message(glue::glue("Local files match archive {latest_timestamp}, no sync needed"))
      return(list(
        archive_timestamp = latest_timestamp,
        archive_path      = glue::glue("gs://{gcs_bucket}/{archive_prefix}/{latest_timestamp}/{provider}/{dataset}"),
        created_new       = FALSE,
        files_uploaded    = 0L))
    }

    # report changes
    if (nrow(comparison$added) > 0) {
      message(glue::glue("New files: {paste(comparison$added$name, collapse = ', ')}"))
    }
    if (nrow(comparison$removed) > 0) {
      message(glue::glue("Removed files: {paste(comparison$removed$name, collapse = ', ')}"))
    }
    if (nrow(comparison$changed) > 0) {
      message(glue::glue("Changed files: {paste(comparison$changed$name, collapse = ', ')}"))
    }
  }

  # create new archive
  new_timestamp <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
  archive_base  <- glue::glue("{archive_prefix}/{new_timestamp}/{provider}/{dataset}")

  message(glue::glue("Creating new archive: {new_timestamp}"))

  # get local files
  local_manifest <- get_local_manifest(dir_csv)

  if (nrow(local_manifest) == 0) {
    stop("No CSV files found in local directory")
  }

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
#'   provider = "swfsc.noaa.gov",
#'   dataset = "calcofi-db")
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
