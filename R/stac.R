# the static STAC catalog ---------------------------------------------------------------
#
# A SpatioTemporal Asset Catalog (STAC 1.0.0) of the release, written as plain
# JSON files at release time and served from `gs://calcofi-db/stac/` — a root
# Catalog, one Collection per public dataset and per spatial layer, and one Item
# per release under each dataset Collection whose assets are the parquet objects,
# the CF netCDF, the ERDDAP pages and the ISO 19115 record the record already
# names. Nothing here measures anything: every field comes from `datasets.json`
# (the record, WS-R0), `metadata.json` (the column descriptions) and
# `spatial_layers.json`. STAC is a machine surface, never the UI the catalog
# depends on — the pages read `datasets.json` directly (plan § D-5.3).

#' @keywords internal
CC_STAC_VERSION <- "1.0.0"

#' @keywords internal
CC_STAC_HTTPS <- "https://storage.googleapis.com/calcofi-db/stac"

# the extensions the documents declare, pinned
STAC_EXT <- c(
  table      = "https://stac-extensions.github.io/table/v1.2.0/schema.json",
  scientific = "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
  file       = "https://stac-extensions.github.io/file/v2.1.0/schema.json")

# sha256 hex -> a multihash (`file:checksum`): 0x12 = sha2-256, 0x20 = 32 bytes
.stac_multihash <- function(sha256) {
  s <- .s(sha256)
  if (!nzchar(s) || !grepl("^[0-9a-fA-F]{64}$", s)) return(NULL)
  paste0("1220", tolower(s))
}

# a media type from a URL's extension — the assets we publish, nothing generic
.stac_media_type <- function(url) {
  u <- tolower(sub("[?#].*$", "", .s(url)))
  if (grepl("\\.parquet$", u))            return("application/x-parquet")
  if (grepl("\\.nc$", u))                 return("application/x-netcdf")
  if (grepl("\\.xml$", u))                return("application/xml")
  if (grepl("\\.json$|\\.geojson$", u))   return("application/json")
  if (grepl("\\.pmtiles$", u))            return("application/vnd.pmtiles")
  if (grepl("\\.csv$", u))                return("text/csv")
  if (grepl("\\.tif$|\\.tiff$", u))       return("image/tiff; application=geotiff")
  "text/html"
}

# an id that is safe as a directory name and stable across releases
.stac_id <- function(x) gsub("[^A-Za-z0-9_.-]+", "-", .s(x))

# ISO 8601 UTC instants from a record's year span; NULL when there is no span
.stac_interval <- function(cov) {
  y0 <- .int_or_null(cov[["year_min"]]); y1 <- .int_or_null(cov[["year_max"]])
  if (is.null(y0) && is.null(y1)) return(list(NULL, NULL))
  list(if (is.null(y0)) NULL else sprintf("%04d-01-01T00:00:00Z", y0),
       if (is.null(y1)) NULL else sprintf("%04d-12-31T23:59:59Z", y1))
}

# a record bbox -> the STAC [west, south, east, north]
.stac_bbox <- function(bb) {
  if (is.null(bb)) return(NULL)
  v <- c(.num_or_null(bb[["lon_min"]]), .num_or_null(bb[["lat_min"]]),
         .num_or_null(bb[["lon_max"]]), .num_or_null(bb[["lat_max"]]))
  if (length(v) != 4 || anyNA(v)) return(NULL)
  unname(v)
}

# [w, s, e, n] -> a closed GeoJSON polygon ring
.stac_geometry <- function(bbox) {
  if (is.null(bbox)) return(NULL)
  w <- bbox[1]; s <- bbox[2]; e <- bbox[3]; n <- bbox[4]
  ring <- list(c(w, s), c(e, s), c(e, n), c(w, n), c(w, s))
  list(type = "Polygon", coordinates = list(lapply(ring, function(p) unname(p))))
}

.stac_link <- function(rel, href, type = "application/json", title = NULL) {
  x <- list(rel = rel, href = href, type = type, title = title)
  x[!vapply(x, is.null, logical(1))]
}

# `table:columns` for one table, from metadata.json's flat `columns` map
# (`{table}.{column}` -> {name_long, description_md, data_type})
.stac_table_columns <- function(metadata, table) {
  cols <- metadata[["columns"]]
  if (is.null(cols) || !length(cols)) return(NULL)
  pre <- paste0(table, ".")
  nm  <- names(cols)
  hit <- nm[startsWith(nm, pre)]
  if (!length(hit)) return(NULL)
  lapply(hit, function(k) {
    c_i <- cols[[k]]
    x <- list(name = substring(k, nchar(pre) + 1),
              description = .chr_or_null(c_i[["description_md"]]),
              type = .chr_or_null(c_i[["data_type"]]))
    x[!vapply(x, is.null, logical(1))]
  })
}

# `table:tables` for a Collection: the dataset's tables with their descriptions
.stac_tables <- function(metadata, tables) {
  tb <- metadata[["tables"]]
  out <- lapply(tables, function(t) {
    x <- list(name = t, description = .chr_or_null(tb[[t]][["description_md"]]))
    x[!vapply(x, is.null, logical(1))]
  })
  if (!length(out)) NULL else out
}

.stac_providers <- function(rec) {
  p <- rec[["provider"]]
  out <- list()
  if (!is.null(p) && nzchar(.s(p[["name"]])))
    out <- c(out, list(.drop_null(list(
      name = .s(p[["name"]]), roles = I(c("producer", "licensor")),
      url = .chr_or_null(p[["url"]])))))
  c(out, list(list(
    name = "CalCOFI", roles = I(c("processor", "host")), url = "https://calcofi.io")))
}

.drop_null <- function(x) x[!vapply(x, is.null, logical(1))]

# a dataset's assets, from the record's own objects and distributions
.stac_assets <- function(rec, metadata) {
  assets <- list()
  seen <- character()
  add <- function(key, a) {
    key <- .stac_id(key)
    i <- 1L; k <- key
    while (k %in% seen) { i <- i + 1L; k <- paste0(key, "_", i) }
    seen <<- c(seen, k)
    assets[[k]] <<- .drop_null(a)
  }
  # the parquet objects: one asset per object, with its table's columns
  for (o in .rows(rec[["objects"]])) {
    tbl <- .s(o[["table"]])
    key <- if (identical(.s(o[["scope"]]), "partition")) paste0(tbl, "_partition") else tbl
    a <- list(
      href  = .s(o[["url"]]),
      type  = "application/x-parquet",
      title = sprintf("%s (parquet%s)", tbl,
                      if (identical(.s(o[["scope"]]), "partition")) ", this dataset's partition" else ""),
      roles = I("data"),
      `file:size` = .int_or_null(o[["bytes"]]),
      `file:checksum` = .stac_multihash(o[["sha256"]]),
      `table:columns` = .stac_table_columns(metadata, tbl))
    add(key, a)
  }
  # every non-parquet distribution the record carries, by kind
  for (d in .rows(rec[["distributions"]])) {
    url <- .s(d[["url"]])
    if (!nzchar(url) || grepl("\\.parquet$", url)) next
    if (!.s(d[["status"]]) %in% c("current", "external", "")) next
    kind <- .s(d[["kind"]])
    type <- .stac_media_type(url)
    role <- switch(kind,
      download = "data",
      service  = if (identical(type, "application/xml")) "metadata" else "overview",
      notebook = "overview",
      page     = "overview",
      "overview")
    key <- if (identical(type, "application/x-netcdf")) "netcdf"
      else if (identical(type, "application/xml")) "iso19115"
      else if (nzchar(.s(d[["id"]]))) .s(d[["id"]])
      else kind
    add(key, list(
      href = url, type = type, roles = I(role),
      title = .chr_or_null(d[["title"]]) %||% sprintf("%s (%s)", kind, .s(d[["portal"]]) )))
  }
  assets
}

#' A STAC Collection for one dataset record
#'
#' @param rec one element of `datasets.json`'s `datasets[]`
#' @param metadata the parsed `metadata.json` (for `table:tables`)
#' @param base_url the HTTPS root the catalog is served from
#' @return A named list, the Collection document.
#' @export
#' @concept catalog
stac_collection <- function(rec, metadata = list(), base_url = CC_STAC_HTTPS) {
  key <- .s(rec[["dataset_key"]])
  id  <- .stac_id(key)
  cov <- rec[["coverage"]] %||% list()
  bb  <- .stac_bbox(cov[["bbox"]])
  iv  <- .stac_interval(cov)
  att <- rec[["attribution"]] %||% list()
  lic <- .chr_or_null(att[["license"]])
  self <- sprintf("%s/collections/%s/collection.json", base_url, id)
  links <- list(
    .stac_link("root", paste0(base_url, "/catalog.json")),
    .stac_link("parent", paste0(base_url, "/catalog.json")),
    .stac_link("self", self))
  page <- .chr_or_null((rec[["links"]] %||% list())[["page"]])
  if (!is.null(page)) links <- c(links, list(.stac_link("about", page, "text/html", "Dataset page")))
  if (!is.null(.chr_or_null(att[["license_url"]])))
    links <- c(links, list(.stac_link("license", .s(att[["license_url"]]), "text/html",
                                      .chr_or_null(att[["license_name"]]))))
  if (!is.null(.chr_or_null(att[["doi"]])))
    links <- c(links, list(.stac_link("cite-as", paste0("https://doi.org/", .s(att[["doi"]])), "text/html")))
  tables <- .stac_tables(metadata, unlist(rec[["tables"]]))
  doi <- .chr_or_null(att[["doi"]]); cite <- .chr_or_null(att[["citation_main"]])
  # an extension is DECLARED only when the document uses it: the scientific schema
  # requires one of sci:doi / sci:citation / sci:publications, so declaring it on a
  # dataset with neither (5 of 16 at v2026.09.05, all on an open citation question)
  # makes the document invalid — measured against stac-validator 2026-09-05
  exts <- c(if (!is.null(tables)) STAC_EXT[["table"]],
            if (!is.null(doi) || !is.null(cite)) STAC_EXT[["scientific"]])
  .drop_null(list(
    type = "Collection",
    stac_version = CC_STAC_VERSION,
    stac_extensions = if (length(exts)) I(unname(exts)) else I(character()),
    id = id,
    title = .chr_or_null(rec[["dataset_name"]]) %||% id,
    description = .chr_or_null(rec[["description_md"]]) %||%
      sprintf("CalCOFI dataset %s.", key),
    keywords = if (length(.rows(rec[["keywords"]]))) .arr(unlist(rec[["keywords"]])) else NULL,
    license = lic %||% "other",
    providers = .stac_providers(rec),
    extent = list(
      spatial  = list(bbox = list(bb %||% c(-180, -90, 180, 90))),
      temporal = list(interval = list(list(iv[[1]], iv[[2]])))),
    `sci:doi` = doi,
    `sci:citation` = cite,
    `table:tables` = tables,
    # a summary value is an ARRAY (or a Range/JSON Schema object), never a scalar
    summaries = .drop_null(list(
      `calcofi:category` = if (nzchar(.s((rec[["category"]] %||% list())[["name"]])))
        .arr(.s((rec[["category"]])[["name"]])) else NULL,
      `calcofi:variables` = if (length(.rows(cov[["variables"]])))
        .arr(unlist(cov[["variables"]])) else NULL)),
    links = links))
}

#' A STAC Item for one dataset at one release
#'
#' @inheritParams stac_collection
#' @param version the release version (the Item id)
#' @param release_date the release date (`datetime`/`created`)
#' @return A named list, the Item document.
#' @export
#' @concept catalog
stac_item <- function(rec, metadata = list(), version = NULL, release_date = NULL,
                      base_url = CC_STAC_HTTPS) {
  key <- .stac_id(rec[["dataset_key"]])
  ver <- .stac_id(version)
  cov <- rec[["coverage"]] %||% list()
  bb  <- .stac_bbox(cov[["bbox"]])
  iv  <- .stac_interval(cov)
  att <- rec[["attribution"]] %||% list()
  created <- if (!is.null(release_date) && nzchar(.s(release_date)))
    paste0(.s(release_date), "T00:00:00Z") else NULL
  # STAC requires `datetime`; it may be null ONLY when start_ and end_datetime are both set
  dt <- if (!is.null(iv[[1]]) && !is.null(iv[[2]])) NA else (created %||% NA)
  props <- c(
    .drop_null(list(
      title = sprintf("%s \u2014 %s", .s(rec[["dataset_name"]]), version),
      description = .chr_or_null(rec[["description_md"]]))),
    list(datetime = dt),
    .drop_null(list(
      start_datetime = iv[[1]], end_datetime = iv[[2]], created = created,
      license = .chr_or_null(att[["license"]]),
      `sci:doi` = .chr_or_null(att[["doi"]]),
      `sci:citation` = .chr_or_null(att[["citation_main"]]))))
  assets <- .stac_assets(rec, metadata)
  uses <- function(field) any(vapply(assets, function(a) field %in% names(a), logical(1)))
  exts <- c(if (uses("table:columns")) STAC_EXT[["table"]],
            if (!is.null(props[["sci:doi"]]) || !is.null(props[["sci:citation"]])) STAC_EXT[["scientific"]],
            if (uses("file:size") || uses("file:checksum")) STAC_EXT[["file"]])
  list(
    type = "Feature",
    stac_version = CC_STAC_VERSION,
    stac_extensions = if (length(exts)) I(unname(exts)) else I(character()),
    id = ver,
    collection = key,
    geometry = .stac_geometry(bb) %||% NA,
    bbox = if (is.null(bb)) NA else I(bb),
    properties = props,
    assets = assets,
    links = list(
      .stac_link("root", paste0(base_url, "/catalog.json")),
      .stac_link("parent", sprintf("%s/collections/%s/collection.json", base_url, key)),
      .stac_link("collection", sprintf("%s/collections/%s/collection.json", base_url, key)),
      .stac_link("self", sprintf("%s/collections/%s/items/%s.json", base_url, key, ver))))
}

# the spatial layers as plain rows: the record's own reference[] rows when it has
# them (their `url` is already absolute), else spatial_layers.json, whose layers
# name a `source` archive under `pmtiles_base` rather than a URL
.stac_layer_rows <- function(record, layers) {
  ref <- Filter(function(r) identical(.s(r[["kind"]]), "layer"), .rows(record[["reference"]]))
  if (length(ref)) return(ref)
  if (is.null(layers)) return(list())
  base <- .s(layers[["pmtiles_base"]])
  lapply(.rows(layers[["layers"]] %||% layers), function(l) {
    src <- .s(l[["source"]])
    l[["key"]] <- .s(l[["id"]] %||% l[["key"]])
    l[["url"]] <- if (nzchar(base) && nzchar(src)) paste0(base, src, ".pmtiles") else .s(l[["url"]])
    l
  })
}

# one spatial layer -> a Collection with its PMTiles asset
.stac_layer_collection <- function(lyr, version = NULL, built = NULL, base_url = CC_STAC_HTTPS) {
  key <- .s(lyr[["key"]] %||% lyr[["id"]])
  id  <- paste0("layer_", .stac_id(key))
  bb  <- unlist(lyr[["bbox"]])
  bb  <- if (length(bb) == 4) unname(as.numeric(bb)) else c(-180, -90, 180, 90)
  url <- .s(lyr[["url"]])
  .drop_null(list(
    type = "Collection",
    stac_version = CC_STAC_VERSION,
    id = id,
    title = .chr_or_null(lyr[["name"]]) %||% id,
    description = .chr_or_null(lyr[["description_md"]]) %||% .chr_or_null(lyr[["description"]]) %||%
      sprintf("CalCOFI spatial reference layer %s.", key),
    keywords = .arr(c("spatial layer", .s(lyr[["group"]]))),
    license = "other",
    providers = list(.drop_null(list(
      name = .chr_or_null(lyr[["attribution"]]) %||% "CalCOFI", roles = I(c("producer", "licensor")))),
      list(name = "CalCOFI", roles = I(c("processor", "host")), url = "https://calcofi.io")),
    extent = list(
      spatial  = list(bbox = list(bb)),
      temporal = list(interval = list(list(NULL, NULL)))),
    summaries = .drop_null(list(
      `calcofi:n_features` = if (.has_value(lyr[["n_features"]])) I(as.integer(lyr[["n_features"]])) else NULL,
      `calcofi:group` = if (nzchar(.s(lyr[["group"]]))) .arr(.s(lyr[["group"]])) else NULL)),
    assets = if (nzchar(url)) list(pmtiles = .drop_null(list(
      href = url, type = .stac_media_type(url), roles = I(c("data", "visual")),
      title = sprintf("%s (PMTiles)", .s(lyr[["name"]]))))) else NULL,
    links = list(
      .stac_link("root", paste0(base_url, "/catalog.json")),
      .stac_link("parent", paste0(base_url, "/catalog.json")),
      .stac_link("self", sprintf("%s/collections/%s/collection.json", base_url, id)))))
}

#' Build the static STAC catalog of a release
#'
#' Writes STAC 1.0.0 into `dir`: a root `catalog.json`, one
#' `collections/{dataset_key}/collection.json` per **public** dataset with its
#' extent, licence, providers, keywords, `table:tables` and `sci:doi` /
#' `sci:citation`, one Item per release at
#' `collections/{dataset_key}/items/{version}.json` whose assets are the
#' dataset's parquet objects (`application/x-parquet`, `roles: [data]`, with
#' `table:columns` from `metadata.json` and `file:size` / `file:checksum`), its
#' CF netCDF, the ERDDAP pages (`roles: [overview]`) and the ISO 19115 record
#' (`roles: [metadata]`); and one `collections/layer_{key}/collection.json` per
#' spatial layer with its PMTiles asset. Everything is read from the record —
#' no service is asked anything here.
#'
#' @param record `datasets.json`: a path/URL or the parsed list
#' @param catalog the release `catalog.json` (path/URL or list), or NULL — used
#'   only for the release version/date when `record$release` lacks them
#' @param spatial_layers `spatial_layers.json` (path/URL or list), or NULL
#' @param dir the directory to write into (created)
#' @param base_url the HTTPS root the catalog will be served from; the `self`,
#'   `root`, `parent`, `child` and `item` links are absolute against it. Pass the
#'   staging root for a staging run.
#' @param metadata `metadata.json` (path/URL or list), or NULL — the source of
#'   the column and table descriptions
#' @param include_layers write the spatial-layer collections (default TRUE)
#' @return The paths written, invisibly, `catalog.json` first.
#' @export
#' @concept catalog
build_stac <- function(record, catalog = NULL, spatial_layers = NULL, dir,
                       base_url = CC_STAC_HTTPS, metadata = NULL, include_layers = TRUE) {
  record   <- .read_json(record)
  catalog  <- if (is.null(catalog)) NULL else .read_json(catalog)
  metadata <- if (is.null(metadata)) list() else .read_json(metadata)
  layers   <- if (is.null(spatial_layers)) NULL else .read_json(spatial_layers)
  base_url <- sub("/+$", "", .s(base_url))
  rel <- record[["release"]] %||% list()
  version <- .s(rel[["version"]] %||% catalog[["version"]])
  rdate   <- .s(rel[["release_date"]] %||% catalog[["release_date"]])
  if (!nzchar(version)) stop("build_stac(): the record carries no release version", call. = FALSE)

  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  paths <- character()
  write_doc <- function(x, ...) {
    p <- file.path(dir, ...)
    dir.create(dirname(p), recursive = TRUE, showWarnings = FALSE)
    jsonlite::write_json(x, p, auto_unbox = TRUE, pretty = TRUE, digits = NA, null = "null", na = "null")
    paths <<- c(paths, p)
    p
  }

  children <- list()
  # one Collection + one Item per public dataset
  for (rec in .rows(record[["datasets"]])) {
    if (!identical(.s(rec[["visibility"]] %||% "public"), "public")) next
    key <- .stac_id(rec[["dataset_key"]])
    rec[["release"]] <- rel
    write_doc(stac_collection(rec, metadata, base_url), "collections", key, "collection.json")
    write_doc(stac_item(rec, metadata, version, rdate, base_url),
              "collections", key, "items", paste0(version, ".json"))
    children[[length(children) + 1]] <- .stac_link(
      "child", sprintf("%s/collections/%s/collection.json", base_url, key),
      title = .s(rec[["dataset_name"]]))
  }
  # the spatial layers, a second set of Collections
  if (include_layers) {
    for (lyr in .stac_layer_rows(record, layers)) {
      col <- .stac_layer_collection(lyr, version, layers[["built"]] %||% NULL, base_url)
      write_doc(col, "collections", col$id, "collection.json")
      children[[length(children) + 1]] <- .stac_link(
        "child", sprintf("%s/collections/%s/collection.json", base_url, col$id), title = col$title)
    }
  }

  root <- .drop_null(list(
    type = "Catalog",
    stac_version = CC_STAC_VERSION,
    id = "calcofi",
    title = "CalCOFI Integrated Database",
    description = paste0(
      "The CalCOFI integrated database as a static SpatioTemporal Asset Catalog: one Collection ",
      "per dataset with an Item per release (parquet, CF netCDF, ERDDAP and ISO 19115 assets), ",
      "plus the spatial reference layers. Generated from the release record ",
      "(https://calcofi.io/datasets/). Release ", version, "."),
    links = c(list(
      .stac_link("root", paste0(base_url, "/catalog.json")),
      .stac_link("self", paste0(base_url, "/catalog.json")),
      .stac_link("about", "https://calcofi.io/datasets/", "text/html", "CalCOFI datasets"),
      .stac_link("describedby", .s(rel[["url"]] %||% "https://calcofi.io/datasets/"), "application/json",
                 "datasets.json (the record)")),
      children)))
  p_root <- write_doc(root, "catalog.json")
  invisible(c(p_root, setdiff(paths, p_root)))
}

# check ---------------------------------------------------------------------------------

# Run the `stac-validator` CLI over the written documents. Two CLI shapes exist:
# >= 3.4 takes a `validate` / `batch` subcommand, earlier versions take the file as
# the first argument. Each document is validated on its own (never `--recursive`),
# because the child/item links are absolute GCS URLs that do not exist until the
# catalog is uploaded — the structural half is what checks the links locally.
.stac_validate_files <- function(bin, files) {
  has_sub <- identical(as.integer(suppressWarnings(system2(
    bin, c("validate", "--help"), stdout = FALSE, stderr = FALSE)) %||% 0L), 0L)
  invalid <- character(); detail <- character()
  for (f in files) {
    out <- suppressWarnings(system2(bin, if (has_sub) c("validate", f) else f,
                                    stdout = TRUE, stderr = TRUE))
    st <- as.integer(attr(out, "status") %||% 0L)
    txt <- paste(out, collapse = " ")
    if (!identical(st, 0L) || grepl('"valid_stac"\\s*:\\s*false', txt)) {
      invalid <- c(invalid, f)
      msg <- sub('.*"error_message"\\s*:\\s*"([^"]*)".*', "\\1", txt)
      detail <- c(detail, substr(if (identical(msg, txt)) txt else msg, 1, 300))
    }
  }
  list(invalid = invalid, detail = detail)
}

#' The findings [check_stac()] can report
#' @return A named character vector, finding -> level (`error` | `warn`).
#' @export
#' @concept catalog
stac_findings <- function() c(
  missing_field   = "error",  # a required STAC field is absent
  bad_link        = "error",  # a child/item link points at a file that was not written
  bad_asset       = "error",  # an asset has no href or no type
  invalid_json    = "error",  # a document does not parse
  validator_error = "error",  # stac-validator said no
  no_validator    = "warn",   # stac-validator is not installed; structure only
  no_asset        = "warn",   # an Item with no assets
  ok              = "ok")

#' Validate a written STAC catalog
#'
#' Runs `stac-validator` (pip, `stac_validator`) over every written document when
#' it is on `PATH`, and always runs a structural check: that every document parses, carries `type`/`stac_version`/`id`, that
#' every `child`/`item` link resolves to a file that was written, and that every
#' asset has an `href` and a `type`. The structural half always runs, so a
#' machine without the validator still fails on a broken catalog.
#'
#' @param dir the directory [build_stac()] wrote
#' @param network run the external validator (it fetches the STAC JSON schemas);
#'   defaults to off when `CALCOFI_SKIP_LINK_CHECK` is set
#' @return A [tibble][tibble::tibble] `document`, `finding`, `level`, `detail` —
#'   one `ok` row per document that passed.
#' @export
#' @concept catalog
check_stac <- function(dir, network = !nzchar(Sys.getenv("CALCOFI_SKIP_LINK_CHECK"))) {
  stopifnot(dir.exists(dir))
  files <- sort(list.files(dir, pattern = "[.]json$", recursive = TRUE, full.names = TRUE))
  rel   <- substring(files, nchar(dir) + 2)
  out <- list()
  add <- function(doc, finding, detail = NA_character_) out[[length(out) + 1]] <<- tibble::tibble(
    document = doc, finding = finding, level = unname(stac_findings()[finding] %||% "error"),
    detail = detail)

  docs <- list()
  for (i in seq_along(files)) {
    j <- tryCatch(jsonlite::fromJSON(files[i], simplifyVector = FALSE), error = function(e) NULL)
    if (is.null(j)) { add(rel[i], "invalid_json", files[i]); next }
    docs[[rel[i]]] <- j
    for (f in c("type", "stac_version", "id"))
      if (!nzchar(.s(j[[f]]))) add(rel[i], "missing_field", f)
    if (identical(.s(j[["type"]]), "Collection")) {
      if (!nzchar(.s(j[["license"]]))) add(rel[i], "missing_field", "license")
      if (is.null(j[["extent"]]))      add(rel[i], "missing_field", "extent")
    }
    if (identical(.s(j[["type"]]), "Feature")) {
      if (is.null(j[["properties"]])) add(rel[i], "missing_field", "properties")
      else if (!"datetime" %in% names(j[["properties"]])) add(rel[i], "missing_field", "properties.datetime")
      if (!length(j[["assets"]])) add(rel[i], "no_asset")
      for (nm in names(j[["assets"]])) {
        a <- j[["assets"]][[nm]]
        if (!nzchar(.s(a[["href"]])) || !nzchar(.s(a[["type"]]))) add(rel[i], "bad_asset", nm)
      }
    }
  }
  # every child/item link must resolve to a document that was written
  written <- names(docs)
  for (d in names(docs)) {
    for (lk in .rows(docs[[d]][["links"]])) {
      if (!.s(lk[["rel"]]) %in% c("child", "item")) next
      href <- .s(lk[["href"]])
      tail <- sub("^.*/(collections/.*)$", "\\1", href)
      if (!tail %in% written) add(d, "bad_link", href)
    }
  }

  if (network) {
    bin <- Sys.which("stac-validator")
    if (nzchar(bin)) {
      res <- .stac_validate_files(bin, files)
      for (i in seq_along(res$invalid))
        add(rel[match(res$invalid[i], files)] %||% res$invalid[i], "validator_error", res$detail[i])
    } else {
      add("catalog.json", "no_validator", "install with `pip install stac-validator`")
    }
  } else {
    add("catalog.json", "no_validator", "network checks skipped (CALCOFI_SKIP_LINK_CHECK)")
  }

  d <- if (length(out)) dplyr::bind_rows(out) else
    tibble::tibble(document = character(), finding = character(), level = character(), detail = character())
  ok <- setdiff(names(docs), d$document[d$level == "error"])
  d <- dplyr::bind_rows(d, tibble::tibble(document = ok, finding = "ok", level = "ok", detail = NA_character_))
  d[order(d$level != "error", d$document), , drop = FALSE]
}

#' Stop when [check_stac()] found an error
#'
#' @param d the tibble from [check_stac()]
#' @param quiet suppress the summary line
#' @return `d`, invisibly.
#' @export
#' @concept catalog
assert_stac <- function(d, quiet = FALSE) {
  bad <- d[d$level == "error", , drop = FALSE]
  if (!quiet)
    message(sprintf("STAC: %d document(s) ok · %d error · %d warn",
                    sum(d$finding == "ok"), nrow(bad), sum(d$level == "warn")))
  if (nrow(bad))
    stop("STAC catalog is invalid:\n",
         paste(sprintf("  %s: %s%s", bad$document, bad$finding,
                       ifelse(is.na(bad$detail), "", paste0(" (", bad$detail, ")"))),
               collapse = "\n"), call. = FALSE)
  invisible(d)
}
