# the oceanmetrics gazetteer (https://storage.oceanmetrics.io/gazetteer/): pinned, verified reads

GAZETTEER_MANIFEST <- "https://storage.oceanmetrics.io/gazetteer/index/layers.json"
GAZETTEER_BASE     <- "https://storage.oceanmetrics.io/gazetteer/"
# the place-level columns every gazetteer layer is read with, in this order (NA when the parquet
# predates them: `status_upstream` and `status_history` arrived after v1.0.0)
GAZETTEER_COLS     <- c("place_id", "name", "status", "status_date", "status_source",
                        "status_upstream", "status_history")

#' Read one gazetteer layer, pinned and verified
#'
#' Downloads a collection's `places.parquet` from the oceanmetrics gazetteer and returns it as an `sf`.
#' The read is **pinned**: the manifest row (`index/layers.json`, matched on `slug`) names the
#' `version` and the parquet's `parquet_sha256`, and the downloaded bytes must hash to it, else an
#' error (a silently changed layer must never reach a release). The manifest's newer fields are
#' optional, so this works against older manifests:
#'
#' - no `parquet`: the URL is `<base><slug>/places.parquet` (`base` from the manifest, default
#'   `https://storage.oceanmetrics.io/gazetteer/`);
#' - no `parquet_sha256`, or a `version` other than the manifest's: the hash comes from
#'   `<base><slug>/versions.json` (`versions[]` entry for `version`, `assets["places.parquet"].sha256`,
#'   and its `href` when given).
#'
#' A verified copy is kept in `cache_dir/<slug>/<version>/places.parquet` and reused while its hash
#' still matches. `manifest`, `base`, `parquet` and `versions.json` may also be local paths.
#'
#' @param slug the gazetteer collection, e.g. `"noaa_aoa_socal"`.
#' @param version the collection version to read; `NULL` (default) = the manifest's `version`.
#' @param manifest URL or local path of the gazetteer's `index/layers.json`.
#' @param cache_dir directory for verified downloads (default: the user cache of calcofi4db).
#' @return An `sf` in EPSG:4326 with geometry column `geometry`, the columns `place_id`, `name`,
#'   `status`, `status_date`, `status_source`, `status_upstream`, `status_history` first (NA when the
#'   parquet lacks one), then the native attributes (the GeoParquet `bbox` covering column dropped).
#'   Attribute `gazetteer` = `list(slug, version, sha256, url)`.
#' @export
#' @concept spatial
#' @importFrom arrow read_parquet
#' @importFrom digest digest
#' @importFrom jsonlite fromJSON
#' @importFrom tools R_user_dir
#' @examples
#' \dontrun{
#' aoa <- read_gazetteer_layer("noaa_aoa_socal")
#' attr(aoa, "gazetteer")
#' }
read_gazetteer_layer <- function(slug, version = NULL, manifest = GAZETTEER_MANIFEST,
                                 cache_dir = tools::R_user_dir("calcofi4db", "cache")) {
  stopifnot(is.character(slug), length(slug) == 1, !is.na(slug), nzchar(slug))
  if (!requireNamespace("sf", quietly = TRUE)) stop("read_gazetteer_layer() needs the sf package", call. = FALSE)
  man  <- jsonlite::fromJSON(.gazetteer_text(manifest), simplifyVector = FALSE)
  base <- man$base %||% GAZETTEER_BASE
  if (!grepl("/$", base)) base <- paste0(base, "/")
  row <- Filter(function(g) identical(g$slug, slug), man$layers %||% list())
  if (!length(row)) stop("gazetteer manifest has no layer '", slug, "' (", manifest, ")", call. = FALSE)
  row <- row[[1]]
  if (is.null(version)) version <- row$version
  if (is.null(version)) stop("gazetteer layer '", slug, "': no version in the manifest; pass `version`", call. = FALSE)
  version <- as.character(version)

  # the pin: url + sha256 from the manifest row when it is the version asked for, else versions.json
  url <- row$parquet %||% paste0(base, slug, "/places.parquet")
  sha <- if (identical(as.character(row$version), version)) row$parquet_sha256 else NULL
  if (is.null(sha) || !identical(as.character(row$version), version)) {
    vj <- jsonlite::fromJSON(.gazetteer_text(paste0(base, slug, "/versions.json")), simplifyVector = FALSE)
    v  <- Filter(function(e) identical(as.character(e$version), version), vj$versions %||% list())
    if (!length(v)) stop("gazetteer layer '", slug, "' has no version ", version, " in versions.json", call. = FALSE)
    a  <- v[[1]]$assets[["places.parquet"]]
    if (is.null(a$sha256)) stop("gazetteer layer '", slug, "' v", version, ": versions.json lists no places.parquet sha256", call. = FALSE)
    sha <- a$sha256
    # an older version may live at its own href (relative to base)
    if (!identical(as.character(row$version), version) && !is.null(a$href)) url <- paste0(base, a$href)
  }
  sha <- tolower(as.character(sha))

  # the verified copy: reuse while it still hashes to the pin, else fetch and verify
  path <- file.path(cache_dir, slug, version, "places.parquet")
  if (!file.exists(path) || !identical(digest::digest(path, algo = "sha256", file = TRUE), sha)) {
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    tmp <- tempfile(fileext = ".parquet"); on.exit(unlink(tmp), add = TRUE)
    .gazetteer_fetch(url, tmp)
    got <- digest::digest(tmp, algo = "sha256", file = TRUE)
    if (!identical(got, sha))
      stop("gazetteer layer '", slug, "' v", version, ": sha256 mismatch for ", url,
           " (expected ", sha, ", got ", got, ")", call. = FALSE)
    file.copy(tmp, path, overwrite = TRUE)
  }

  x <- .read_geoparquet(path)
  for (cl in setdiff(GAZETTEER_COLS, names(x))) x[[cl]] <- NA_character_
  x <- x[, c(GAZETTEER_COLS, setdiff(names(x), GAZETTEER_COLS))]
  attr(x, "gazetteer") <- list(slug = slug, version = version, sha256 = sha, url = url)
  x
}

# a GeoParquet file as an sf in EPSG:4326 (geometry column `geometry`), read through arrow (no GDAL
# Parquet driver needed); the GeoParquet 1.1 `bbox` covering column is dropped
.read_geoparquet <- function(path) {
  tb  <- arrow::read_parquet(path, as_data_frame = FALSE)
  geo <- if (!is.null(tb$metadata$geo)) jsonlite::fromJSON(tb$metadata$geo, simplifyVector = FALSE) else list()
  gcol <- geo$primary_column %||% "geometry"
  if (!gcol %in% names(tb)) stop("GeoParquet ", path, " has no geometry column '", gcol, "'", call. = FALSE)
  cov  <- unlist(lapply(geo$columns[[gcol]]$covering$bbox %||% list(), function(p) p[[1]]))
  df   <- as.data.frame(tb)
  wkb  <- structure(lapply(df[[gcol]], as.raw), class = "WKB")
  df   <- df[, setdiff(names(df), unique(c(cov, gcol))), drop = FALSE]
  sfc  <- sf::st_as_sfc(wkb, EWKB = FALSE)
  crs  <- .geoparquet_crs(geo$columns[[gcol]]$crs)
  sfc  <- if (is.na(crs)) sf::st_set_crs(sfc, 4326) else sf::st_transform(sf::st_set_crs(sfc, crs), 4326)
  sf::st_sf(df, geometry = sfc)
}

# the column's crs: NA (= EPSG:4326, also the GeoParquet default OGC:CRS84) when absent or WGS 84
# geographic, else the projjson as an sf crs to transform from
.geoparquet_crs <- function(crs) {
  if (is.null(crs)) return(NA)
  id <- crs$id
  if (!is.null(id) && ((identical(id$authority, "EPSG") && identical(as.integer(id$code), 4326L)) ||
                       (identical(id$authority, "OGC") && identical(as.character(id$code), "CRS84"))))
    return(NA)
  if (identical(crs$type, "GeographicCRS") && grepl("WGS ?84", crs$name %||% "")) return(NA)
  sf::st_crs(as.character(jsonlite::toJSON(crs, auto_unbox = TRUE, digits = NA)))
}

# text of a local file or a URL (redirects followed); an error when it cannot be read
.gazetteer_text <- function(x, timeout = 30) {
  if (file.exists(x)) return(paste(readLines(x, warn = FALSE, encoding = "UTF-8"), collapse = "\n"))
  if (!requireNamespace("curl", quietly = TRUE)) stop("reading ", x, " needs the curl package", call. = FALSE)
  r <- curl::curl_fetch_memory(x, handle = curl::new_handle(timeout = timeout, followlocation = TRUE))
  if (r$status_code != 200) stop("HTTP ", r$status_code, " for ", x, call. = FALSE)
  rawToChar(r$content)
}

# copy a local file or download a URL to `dest`; an error when it cannot be fetched
.gazetteer_fetch <- function(x, dest, timeout = 300) {
  src <- sub("^file://", "", x)
  if (file.exists(src)) return(invisible(file.copy(src, dest, overwrite = TRUE)))
  if (!requireNamespace("curl", quietly = TRUE)) stop("downloading ", x, " needs the curl package", call. = FALSE)
  curl::curl_download(x, dest, quiet = TRUE, handle = curl::new_handle(timeout = timeout, followlocation = TRUE))
  invisible(TRUE)
}
