# google cloud storage operations for calcofi data workflow

#' Get a file from Google Cloud Storage
#'
#' Downloads a file from GCS to a local path, using googleCloudStorageR
#' or gcloud CLI as fallback.
#'
#' @param gcs_path Full GCS path (gs://bucket/path/to/file) or relative path
#' @param bucket GCS bucket name (used if gcs_path is relative)
#' @param local_path Local path to save file. If NULL, returns a temp file path
#' @param overwrite Whether to overwrite existing local file (default: FALSE)
#'
#' @return Path to the downloaded local file
#' @export
#' @concept cloud
#'
#' @examples
#' \dontrun{
#' # using full gs:// path
#' local_file <- get_gcs_file("gs://calcofi-files/current/bottle.csv")
#'
#' # using bucket + relative path
#' local_file <- get_gcs_file(
#'   "current/calcofi.org/bottle-database/bottle.csv",
#'   bucket = "calcofi-files-public")
#' }
#' @importFrom glue glue
get_gcs_file <- function(
    gcs_path,
    bucket     = NULL,
    local_path = NULL,
    overwrite  = FALSE) {

  # parse gs:// path if provided
  if (grepl("^gs://", gcs_path)) {
    parsed    <- parse_gcs_path(gcs_path)
    bucket    <- parsed$bucket
    gcs_path  <- parsed$path
  }

  stopifnot(!is.null(bucket))

  # set local path if not provided
  if (is.null(local_path)) {
    local_path <- tempfile(fileext = paste0(".", tools::file_ext(gcs_path)))
  }

  # check if file exists and should not overwrite
  if (file.exists(local_path) && !overwrite) {
    message(glue::glue("File already exists: {local_path}"))
    return(local_path)
  }

  # ensure local directory exists
  local_dir <- dirname(local_path)
  if (!dir.exists(local_dir)) {
    dir.create(local_dir, recursive = TRUE)
  }

  # try googleCloudStorageR first, fall back to gcloud
  tryCatch({
    if (requireNamespace("googleCloudStorageR", quietly = TRUE)) {
      googleCloudStorageR::gcs_get_object(
        object_name  = gcs_path,
        bucket       = bucket,
        saveToDisk   = local_path,
        overwrite    = overwrite)
    } else {
      gcloud_download(bucket, gcs_path, local_path)
    }
  }, error = function(e) {
    # fallback to gcloud CLI
    gcloud_download(bucket, gcs_path, local_path)
  })

  stopifnot(file.exists(local_path))
  return(local_path)
}

#' Upload a file to Google Cloud Storage
#'
#' Uploads a local file to GCS.
#'
#' @param local_path Path to the local file
#' @param gcs_path Full GCS path (gs://bucket/path) or relative path
#' @param bucket GCS bucket name (used if gcs_path is relative)
#' @param content_type MIME content type (default: auto-detect)
#'
#' @return GCS URI of the uploaded file
#' @export
#' @concept cloud
#'
#' @examples
#' \dontrun{
#' put_gcs_file("local/bottle.csv", "gs://calcofi-files/current/bottle.csv")
#' }
#' @importFrom glue glue
put_gcs_file <- function(
    local_path,
    gcs_path,
    bucket       = NULL,
    content_type = NULL) {

  stopifnot(file.exists(local_path))

  # parse gs:// path if provided
  if (grepl("^gs://", gcs_path)) {
    parsed   <- parse_gcs_path(gcs_path)
    bucket   <- parsed$bucket
    gcs_path <- parsed$path
  }

  stopifnot(!is.null(bucket))

  # try googleCloudStorageR first, fall back to gcloud
  tryCatch({
    if (requireNamespace("googleCloudStorageR", quietly = TRUE)) {
      googleCloudStorageR::gcs_upload(
        file         = local_path,
        bucket       = bucket,
        name         = gcs_path,
        type         = content_type)
    } else {
      gcloud_upload(local_path, bucket, gcs_path)
    }
  }, error = function(e) {
    gcloud_upload(local_path, bucket, gcs_path)
  })

  return(glue::glue("gs://{bucket}/{gcs_path}"))
}

#' List files in a GCS bucket/prefix
#'
#' Lists objects in a GCS bucket with optional prefix filter.
#'
#' @param bucket GCS bucket name
#' @param prefix Path prefix to filter results
#' @param recursive Whether to list recursively (default: TRUE)
#'
#' @return Data frame with columns: name, size, updated, md5Hash
#' @export
#' @concept cloud
#'
#' @examples
#' \dontrun{
#' # list all files in current/
#' files <- list_gcs_files("calcofi-files", prefix = "current/")
#'
#' # list bottle-database files
#' files <- list_gcs_files(
#'   "calcofi-files",
#'   prefix = "current/calcofi.org/bottle-database/")
#' }
#' @importFrom tibble tibble
list_gcs_files <- function(bucket, prefix = NULL, recursive = TRUE) {
  # try googleCloudStorageR first
  tryCatch({
    if (requireNamespace("googleCloudStorageR", quietly = TRUE)) {
      objs <- googleCloudStorageR::gcs_list_objects(
        bucket    = bucket,
        prefix    = prefix,
        delimiter = if (!recursive) "/" else NULL)

      tibble::tibble(
        name    = objs$name,
        size    = as.numeric(objs$size),
        updated = lubridate::as_datetime(objs$updated),
        md5     = objs$md5Hash)
    } else {
      gcloud_list(bucket, prefix)
    }
  }, error = function(e) {
    gcloud_list(bucket, prefix)
  })
}

#' List versions of a file in GCS archive
#'
#' Finds all archived versions of a file in the calcofi-files bucket.
#'
#' @param path Relative path to the file (e.g., "calcofi.org/bottle-database/bottle.csv")
#' @param bucket GCS bucket name (default: "calcofi-files")
#'
#' @return Data frame with columns: version_date, gcs_path, size, updated
#' @export
#' @concept cloud
#'
#' @examples
#' \dontrun{
#' versions <- list_gcs_versions("calcofi.org/bottle-database/bottle.csv")
#' }
#' @importFrom tibble tibble
#' @importFrom stringr str_extract
list_gcs_versions <- function(path, bucket = "calcofi-files-public") {
  # list archive folder
  archive_files <- list_gcs_files(bucket, prefix = "archive/")

  # filter for matching path
  matching <- archive_files |>
    dplyr::filter(grepl(path, name, fixed = TRUE)) |>
    dplyr::mutate(
      version_date = stringr::str_extract(name, "\\d{4}-\\d{2}-\\d{2}_\\d{6}"),
      gcs_path     = glue::glue("gs://{bucket}/{name}")) |>
    dplyr::select(version_date, gcs_path, size, updated) |>
    dplyr::arrange(dplyr::desc(version_date))

  # add current version
  current <- list_gcs_files(bucket, prefix = glue::glue("current/{path}"))
  if (nrow(current) > 0) {
    current_row <- tibble::tibble(
      version_date = "current",
      gcs_path     = glue::glue("gs://{bucket}/current/{path}"),
      size         = current$size[1],
      updated      = current$updated[1])
    matching <- dplyr::bind_rows(current_row, matching)
  }

  matching
}

#' Get historical file from a specific date
#'
#' Retrieves a file as it existed on a specific date from the archive.
#'
#' @param path Relative path to the file
#' @param date Date to retrieve (character "YYYY-MM-DD" or Date object)
#' @param bucket GCS bucket name (default: "calcofi-files")
#' @param local_path Local path to save file (default: temp file)
#'
#' @return Path to the downloaded local file
#' @export
#' @concept cloud
#'
#' @examples
#' \dontrun{
#' # get bottle.csv as it was on 2026-01-15
#' file <- get_historical_file(
#'   "calcofi.org/bottle-database/bottle.csv",
#'   date = "2026-01-15")
#' }
get_historical_file <- function(
    path,
    date,
    bucket     = "calcofi-files",
    local_path = NULL) {

  date <- as.character(date)

  # list all versions
  versions <- list_gcs_versions(path, bucket)

  if (nrow(versions) == 0) {
    stop(glue::glue("No versions found for: {path}"))
  }

  # find version closest to but not after the date
  versions_dated <- versions |>
    dplyr::filter(version_date != "current") |>
    dplyr::mutate(
      date_only = as.Date(stringr::str_extract(version_date, "^\\d{4}-\\d{2}-\\d{2}")))

  # get closest version on or before the date
  target_date <- as.Date(date)
  valid_versions <- versions_dated |>
    dplyr::filter(date_only <= target_date)

  if (nrow(valid_versions) == 0) {
    # use current if no archive version is old enough
    message(glue::glue("No archived version found before {date}, using current"))
    gcs_path <- glue::glue("gs://{bucket}/current/{path}")
  } else {
    # use most recent version on or before the date
    gcs_path <- valid_versions$gcs_path[1]
  }

  get_gcs_file(gcs_path, local_path = local_path)
}

#' Create a manifest of current GCS files
#'
#' Generates a JSON manifest documenting the current state of files in GCS.
#'
#' @param bucket GCS bucket name (default: "calcofi-files")
#' @param prefix Path prefix to include (default: "current/")
#' @param output_path Path to save manifest JSON (default: NULL returns data)
#'
#' @return Data frame of file metadata, or path to saved JSON
#' @export
#' @concept cloud
#'
#' @examples
#' \dontrun{
#' manifest <- create_gcs_manifest()
#' create_gcs_manifest(output_path = "manifest_2026-01-31.json")
#' }
#' @importFrom jsonlite write_json
create_gcs_manifest <- function(
    bucket      = "calcofi-files",
    prefix      = "current/",
    output_path = NULL) {

  files <- list_gcs_files(bucket, prefix = prefix)

  manifest <- list(
    generated_at   = as.character(Sys.time()),
    sync_timestamp = format(Sys.time(), "%Y-%m-%d_%H%M%S"),
    bucket         = bucket,
    files          = files |>
      dplyr::mutate(
        path    = gsub("^current/", "", name),
        gcs_url = glue::glue("gs://{bucket}/{name}")) |>
      dplyr::select(path, size, mod_time = updated, md5, gcs_url))

  if (!is.null(output_path)) {
    jsonlite::write_json(manifest, output_path, pretty = TRUE, auto_unbox = TRUE)
    return(output_path)
  }

  manifest
}

# ─── immutable file api ──────────────────────────────────────────────────────

#' Get manifest for a specific date
#'
#' Retrieves the manifest JSON from GCS for a given sync timestamp.
#' The manifest contains metadata about all files in the archive snapshot.
#'
#' @param date Date string in format "YYYY-MM-DD_HHMMSS" or "latest" (default)
#' @param bucket Bucket type: "public" or "private" (default: "public")
#'
#' @return List containing manifest data (generated_at, sync_timestamp, archive_path, files)
#' @export
#' @concept cloud
#'
#' @examples
#' \dontrun{
#' manifest <- get_manifest()
#' manifest <- get_manifest("2026-02-01_143059", "public")
#' }
#' @importFrom jsonlite fromJSON
#' @importFrom glue glue
get_manifest <- function(date = "latest", bucket = "public") {
  bucket_name <- paste0("calcofi-files-", bucket)

  manifest_path <- if (date == "latest") {
    glue::glue("gs://{bucket_name}/manifests/manifest_latest.json")
  } else {
    glue::glue("gs://{bucket_name}/manifests/manifest_{date}.json")
  }

  local_file <- get_gcs_file(manifest_path)
  jsonlite::fromJSON(local_file)
}

#' List CalCOFI files from manifest
#'
#' Lists files available in a specific archive snapshot using the manifest.
#' This provides immutable, reproducible file references.
#'
#' @param date Date string in format "YYYY-MM-DD_HHMMSS" or "latest" (default)
#' @param bucket Bucket type: "public" or "private" (default: "public")
#' @param path Optional path filter (e.g., "swfsc.noaa.gov/calcofi-db")
#'
#' @return Data frame of files from the manifest
#' @export
#' @concept cloud
#'
#' @examples
#' \dontrun{
#' files <- list_calcofi_files()
#' files <- list_calcofi_files(path = "swfsc.noaa.gov/calcofi-db")
#' files <- list_calcofi_files("2026-02-01_143059", "public")
#' }
list_calcofi_files <- function(date = "latest", bucket = "public", path = NULL) {
  manifest <- get_manifest(date, bucket)
  files    <- manifest$files

  if (!is.null(path)) {
    # filter by path pattern
    path_col <- if ("Path" %in% names(files)) "Path" else "path"
    match_idx <- grepl(path, files[[path_col]], fixed = FALSE)
    files <- files[match_idx, ]
  }

  files
}

#' Get a CalCOFI file from the immutable archive
#'
#' Downloads a file from the immutable archive snapshot. This ensures
#' reproducible data access by referencing specific archive timestamps.
#'
#' @param path Relative path within archive (e.g., "swfsc.noaa.gov/calcofi-db/cruise.csv")
#' @param date Date string in format "YYYY-MM-DD_HHMMSS" or "latest" (default)
#' @param bucket Bucket type: "public" or "private" (default: "public")
#' @param local_path Local path to save file (default: temp file)
#'
#' @return Path to the downloaded local file
#' @export
#' @concept cloud
#'
#' @examples
#' \dontrun{
#' # get latest version
#' cruise_csv <- get_calcofi_file("swfsc.noaa.gov/calcofi-db/cruise.csv")
#'
#' # get specific version for reproducibility
#' cruise_csv <- get_calcofi_file(
#'   "swfsc.noaa.gov/calcofi-db/cruise.csv",
#'   date = "2026-02-01_143059")
#' }
#' @importFrom glue glue
get_calcofi_file <- function(
    path,
    date       = "latest",
    bucket     = "public",
    local_path = NULL) {

  manifest     <- get_manifest(date, bucket)
  archive_path <- manifest$archive_path
  bucket_name  <- paste0("calcofi-files-", bucket)

  gcs_path <- glue::glue("gs://{bucket_name}/{archive_path}/{path}")
  get_gcs_file(gcs_path, local_path = local_path)
}

# ─── helper functions (not exported) ──────────────────────────────────────────

#' Parse a gs:// path into bucket and object path
#' @noRd
parse_gcs_path <- function(gcs_uri) {
  if (!grepl("^gs://", gcs_uri)) {
    stop("Invalid GCS URI. Must start with gs://")
  }

  parts  <- sub("^gs://", "", gcs_uri)
  bucket <- sub("/.*", "", parts)
  path   <- sub("^[^/]+/?", "", parts)

  list(bucket = bucket, path = path)
}

#' Find gcloud CLI executable
#' @noRd
find_gcloud <- function() {
  # first check if gcloud is in PATH

  gcloud_path <- Sys.which("gcloud")
  if (nzchar(gcloud_path)) {
    return(gcloud_path)
  }

  # common installation locations
  common_paths <- c(
    "~/Downloads/google-cloud-sdk/bin/gcloud",
    "~/google-cloud-sdk/bin/gcloud",
    "/usr/local/google-cloud-sdk/bin/gcloud",
    "/opt/google-cloud-sdk/bin/gcloud",
    "/usr/local/bin/gcloud",
    "/opt/homebrew/bin/gcloud",
    "~/.local/bin/gcloud"
  )

  for (path in common_paths) {
    expanded_path <- path.expand(path)
    if (file.exists(expanded_path)) {
      return(expanded_path)
    }
  }

  stop("gcloud CLI not found. Please install it or add it to your PATH.
       See: https://cloud.google.com/sdk/docs/install")
}

#' Download file using gcloud CLI
#' @noRd
gcloud_download <- function(bucket, gcs_path, local_path) {
  gcloud  <- find_gcloud()
  gcs_uri <- glue::glue("gs://{bucket}/{gcs_path}")
  # redirect stderr to suppress gcloud ERROR messages
  cmd     <- glue::glue('"{gcloud}" storage cp "{gcs_uri}" "{local_path}" 2>/dev/null')
  # suppress warnings from system() when command fails
  result  <- suppressWarnings(system(cmd, intern = TRUE, ignore.stderr = TRUE))

  if (!file.exists(local_path)) {
    stop(glue::glue("Failed to download from GCS: {gcs_uri}"))
  }

  invisible(local_path)
}

#' Upload file using gcloud CLI
#' @noRd
gcloud_upload <- function(local_path, bucket, gcs_path) {
  gcloud  <- find_gcloud()
  gcs_uri <- glue::glue("gs://{bucket}/{gcs_path}")
  cmd     <- glue::glue('"{gcloud}" storage cp "{local_path}" "{gcs_uri}"')
  result  <- system(cmd, intern = TRUE)

  invisible(gcs_uri)
}

#' List files using gcloud CLI
#' @noRd
#' @importFrom tibble tibble
gcloud_list <- function(bucket, prefix = NULL) {
  gcloud  <- find_gcloud()
  gcs_uri <- if (is.null(prefix)) {
    glue::glue("gs://{bucket}/")
  } else {
    glue::glue("gs://{bucket}/{prefix}")
  }

  cmd    <- glue::glue('"{gcloud}" storage ls -l "{gcs_uri}" 2>/dev/null')
  result <- system(cmd, intern = TRUE)

  # parse gcloud ls output
  if (length(result) == 0) {
    return(tibble::tibble(
      name    = character(),
      size    = numeric(),
      updated = as.POSIXct(character()),
      md5     = character()))
  }

  # gcloud ls -l format: size  date  time  gs://bucket/path
  lines <- result[!grepl("^TOTAL:", result) & nchar(result) > 0]

  if (length(lines) == 0) {
    return(tibble::tibble(
      name    = character(),
      size    = numeric(),
      updated = as.POSIXct(character()),
      md5     = character()))
  }

  tibble::tibble(raw = lines) |>
    dplyr::mutate(
      size    = as.numeric(stringr::str_extract(raw, "^\\s*\\d+")),
      name    = stringr::str_extract(raw, "gs://[^\\s]+") |>
                  gsub(glue::glue("^gs://{bucket}/"), "", x = _),
      updated = NA_POSIXct_,
      md5     = NA_character_) |>
    dplyr::select(name, size, updated, md5)
}
