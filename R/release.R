# content-addressed releases ------------------------------------------------------
#
# Measured on v2026.08.14 -> v2026.08.25 (2026-08-25): 52 MB of 2.09 GB was
# byte-identical between two releases, and tables whose ROW COUNT had not changed
# (obs_mets_full, taxon, cruise, measurement_type) still differed byte-for-byte,
# because the release COPYs carried no total order and ran multi-threaded. A
# total ORDER BY alone is not enough: at default threads `sample.parquet` came out
# different on two consecutive runs; with `SET threads = 1` it is byte-identical
# (cost ~3x on the write: obs, 26M rows, 18.6 s vs 6.4 s).
#
# Two identities, deliberately: `content_hash` (the order-independent row
# signature already used at ingest, .table_content_hash()) says whether the DATA
# changed and names the canonical object; `sha256` of the bytes is recorded for
# verification. An unchanged content_hash reuses the previous release's object —
# even if a DuckDB upgrade would have produced different bytes.
#
# Layouts:
#   compat    releases/{version}/parquet/{table}.parquet  (+ Hive dirs)  — today's
#             URLs; unchanged objects are GCS server-side COPIED from the previous
#             release instead of uploaded (no upload bytes; storage per version).
#   canonical tables/{table}/{content_hash}/{table}.parquet and
#             tables/{table}/{col}={value}/{content_hash}/data_0.parquet — one
#             object per distinct content, shared by every release that carries
#             it; the release's catalog.json points at them, and compat copies are
#             made for the promoted/consolidated versions so legacy URLs resolve.

#' Pinned parquet writer options for released tables
#'
#' A change to any of them changes every table's bytes; record it in RELEASES.md.
#' @export
#' @concept release
CC_PARQUET_WRITER <- list(compression = "zstd", row_group_size = 122880L,
                          parquet_version = "V1")

#' Release layout prefixes (bucket-relative)
#' @export
#' @concept release
CC_RELEASE_PREFIX <- "ducklake/releases"
#' @rdname CC_RELEASE_PREFIX
#' @export
CC_TABLES_PREFIX <- "ducklake/tables"

.is_provenance_col <- function(x) grepl("^_", x)

#' Sort keys (and partition column) for every released table
#'
#' The ORDER BY that makes an export deterministic must be a unique total order:
#' partition column first, then the clustering sort, then the primary key as the
#' tiebreak. A released table missing from this registry — and without a primary
#' key in [core_relationships()] — makes [export_release_parquet()] refuse to
#' write, rather than write non-deterministically.
#'
#' @param core_sort clustering sort for the long observation tables.
#' @return Named list: `table -> list(partition_by = <col or NULL>, order_by = <chr>)`.
#' @export
#' @concept release
release_sort_keys <- function(core_sort = c("grid_key NULLS LAST", "depth_min_m NULLS LAST",
                                            "measurement_type", "datetime")) {
  # the core PKs, as core_relationships() declares them (it filters by the tables
  # you pass, so ask for all of them explicitly)
  core <- c("sample", "obs", "obs_attribute", "sample_measurement", "taxon", "dataset_taxon",
            "grid", "cruise", "ship", "measurement_type", "region")
  pk <- core_relationships(core)$primary_keys
  keyed <- lapply(pk, function(k) list(partition_by = NULL, order_by = k))
  utils::modifyList(keyed, list(
    obs                = list(partition_by = "dataset_key",
                              order_by = c("dataset_key", core_sort, "obs_id")),
    obs_ctd_full       = list(partition_by = "cruise_key",
                              order_by = c("cruise_key", core_sort, "obs_id")),
    obs_mets_full      = list(partition_by = "cruise_key",
                              order_by = c("cruise_key", core_sort, "obs_id")),
    dataset            = list(partition_by = NULL, order_by = "dataset_key"),
    spatial            = list(partition_by = NULL, order_by = "spatial_key"),
    spatial_attribute  = list(partition_by = NULL, order_by = c("spatial_key", "fld")),
    lookup             = list(partition_by = NULL, order_by = "lookup_id"),
    taxon_group        = list(partition_by = NULL, order_by = c("taxon_group_key", "taxon_key"))))
}

.order_cols <- function(order_by) trimws(sub("\\s+NULLS\\s+(FIRST|LAST)$", "", order_by, ignore.case = TRUE))

#' Deterministic parquet export of one released table
#'
#' Writes `SELECT <non-provenance cols> FROM table ORDER BY <order_by>` with the
#' pinned writer options ([CC_PARQUET_WRITER]) and a single writer thread, so the
#' same rows always produce the same bytes. Refuses to write if `order_by` is not
#' a unique key of the table.
#'
#' @param con DuckDB connection.
#' @param table table (or view) name.
#' @param path output file (single) or directory (partitioned).
#' @param order_by character vector of ORDER BY terms (may carry `NULLS LAST`).
#' @param partition_by optional partition column (Hive layout, one file per value).
#' @param writer list of writer options; see [CC_PARQUET_WRITER].
#' @param strip_provenance drop `_source_*` / `_ingested_at` style columns.
#' @return Invisibly, a tibble of files written: `rel_path`, `bytes`.
#' @export
#' @concept release
#' @importFrom DBI dbExecute dbGetQuery dbListFields
#' @importFrom glue glue
export_release_parquet <- function(con, table, path, order_by, partition_by = NULL,
                                   writer = CC_PARQUET_WRITER, strip_provenance = TRUE) {
  stopifnot(length(order_by) >= 1)
  flds <- DBI::dbListFields(con, table)
  keep <- if (strip_provenance) flds[!.is_provenance_col(flds)] else flds
  keycols <- .order_cols(order_by)
  miss <- setdiff(c(keycols, partition_by), flds)
  if (length(miss))
    stop("`", table, "` has no column(s) ", paste(miss, collapse = ", "),
         " named in its sort key", call. = FALSE)
  qk <- paste0('"', keycols, '"', collapse = ", ")
  dup <- DBI::dbGetQuery(con, glue::glue(
    'SELECT COUNT(*) - COUNT(DISTINCT ({qk})) AS n_dup FROM "{table}"'))$n_dup
  if (dup > 0)
    stop("`", table, "`: ORDER BY (", paste(keycols, collapse = ", "),
         ") is not a unique key (", dup, " duplicate rows) — the export would ",
         "not be deterministic. Add the primary key to release_sort_keys().",
         call. = FALSE)
  sel <- paste0('"', keep, '"', collapse = ", ")
  ord <- paste(vapply(order_by, function(o) {
    col <- .order_cols(o); dec <- sub(paste0("^", col), "", o)
    paste0('"', col, '"', dec) }, ""), collapse = ", ")
  opts <- glue::glue(
    "FORMAT PARQUET, COMPRESSION '{writer$compression}', ",
    "ROW_GROUP_SIZE {as.integer(writer$row_group_size)}, ",
    "PARQUET_VERSION '{writer$parquet_version}'")
  if (!is.null(partition_by)) {
    if (dir.exists(path)) unlink(path, recursive = TRUE)
    opts <- glue::glue('{opts}, PARTITION_BY ("{partition_by}"), OVERWRITE_OR_IGNORE')
  }
  threads <- DBI::dbGetQuery(con, "SELECT current_setting('threads') AS t")$t
  DBI::dbExecute(con, "SET threads TO 1")
  on.exit(DBI::dbExecute(con, glue::glue("SET threads TO {threads}")), add = TRUE)
  DBI::dbExecute(con, glue::glue(
    'COPY (SELECT {sel} FROM "{table}" ORDER BY {ord}) TO \'{path}\' ({opts})'))
  files <- if (is.null(partition_by)) path else
    list.files(path, pattern = "[.]parquet$", recursive = TRUE, full.names = TRUE)
  base <- if (is.null(partition_by)) dirname(path) else dirname(path)
  invisible(tibble::tibble(
    rel_path = sub(paste0("^", gsub("([.|()\\\\^${}+?*\\[\\]])", "\\\\\\1", base), "/"), "", files),
    bytes    = file.size(files)))
}

#' Canonical (content-addressed) object path for a table or partition
#' @param table table name.
#' @param content_hash the object's content signature (see [release_objects()]).
#' @param partition_by,partition_value partition column and value, or NULL.
#' @param prefix bucket-relative prefix.
#' @return A bucket-relative path.
#' @export
#' @concept release
canonical_path <- function(table, content_hash, partition_by = NULL,
                           partition_value = NULL, prefix = CC_TABLES_PREFIX) {
  h <- substr(content_hash, 1, 24)
  if (is.null(partition_by))
    sprintf("%s/%s/%s/%s.parquet", prefix, table, h, table)
  else
    sprintf("%s/%s/%s=%s/%s/data_0.parquet", prefix, table, partition_by, partition_value, h)
}

# a short, filesystem-safe digest of the (long) row-signature string
.short_sig <- function(sig) substr(digest::digest(sig, algo = "sha256", serialize = FALSE), 1, 32)

#' Describe the objects an exported table consists of
#'
#' One row per parquet object with its bytes, `sha256`, `content_hash` (row
#' signature of the table or of that partition's rows, provenance columns
#' excluded) and `since` — the first release that carried an object with this
#' content, looked up in the previous release's catalog.
#'
#' @param con DuckDB connection holding `table`.
#' @param table table name.
#' @param dir_out the export root (`path`'s parent for single files, or the dir).
#' @param files the tibble returned by [export_release_parquet()].
#' @param version the release being cut.
#' @param partition_by partition column or NULL.
#' @param prev_catalog the previous release's parsed `catalog.json` (list) or NULL.
#' @return A tibble: `table, partition_by, partition_value, rel_path, bytes,
#'   sha256, content_hash, since`.
#' @export
#' @concept release
release_objects <- function(con, table, dir_out, files, version, partition_by = NULL,
                            prev_catalog = NULL) {
  flds <- DBI::dbListFields(con, table)
  excl <- flds[.is_provenance_col(flds)]
  # rel_path is the object's place under a release's parquet/ prefix, derived
  # from the TABLE (not from whatever the local export directory was called);
  # local_path is where the bytes are on disk for the upload
  if (is.null(partition_by)) {
    sig <- .short_sig(.table_content_hash(con, table, excl))
    d <- tibble::tibble(table = table, partition_by = NA_character_,
                        partition_value = NA_character_,
                        rel_path = sprintf("%s.parquet", table),
                        local_path = file.path(dir_out, files$rel_path),
                        bytes = files$bytes, content_hash = sig)
  } else {
    sigs <- .partition_content_hashes(con, table, partition_by, excl)
    pv <- utils::URLdecode(sub(paste0("^.*", partition_by, "=([^/]+)/.*$"), "\\1", files$rel_path))
    d <- tibble::tibble(table = table, partition_by = partition_by, partition_value = pv,
                        rel_path = sprintf("%s/%s=%s/data_0.parquet", table, partition_by, pv),
                        local_path = file.path(dir_out, files$rel_path),
                        bytes = files$bytes,
                        content_hash = vapply(pv, function(v) .short_sig(sigs[[v]]), "",
                                              USE.NAMES = FALSE))
    if (anyNA(d$content_hash))
      stop("partition value(s) in files but not in the table: ",
           paste(pv[is.na(d$content_hash)], collapse = ", "), call. = FALSE)
  }
  d$sha256 <- vapply(d$local_path, function(f)
    digest::digest(f, algo = "sha256", file = TRUE), "", USE.NAMES = FALSE)
  d$since <- version
  prev <- .catalog_objects(prev_catalog)
  if (nrow(prev)) {
    key_new <- paste(d$table, d$partition_value, d$content_hash, sep = "|")
    key_old <- paste(prev$table, prev$partition_value, prev$content_hash, sep = "|")
    m <- match(key_new, key_old)
    d$since[!is.na(m)] <- prev$since[m[!is.na(m)]]
  }
  d
}

# flatten a catalog's tables[].objects[] into one tibble (empty if none)
.catalog_objects <- function(catalog) {
  empty <- tibble::tibble(table = character(), partition_value = character(),
                          content_hash = character(), sha256 = character(),
                          path = character(), since = character(), bytes = numeric())
  if (is.null(catalog) || is.null(catalog$tables)) return(empty)
  tbls <- catalog$tables
  rows <- list()
  n <- if (is.data.frame(tbls)) nrow(tbls) else length(tbls)
  for (i in seq_len(n)) {
    t <- if (is.data.frame(tbls)) as.list(tbls[i, ]) else tbls[[i]]
    objs <- t$objects
    if (is.null(objs)) next
    if (is.data.frame(objs[[1]] %||% NULL)) objs <- objs[[1]]
    if (is.data.frame(objs)) objs <- lapply(seq_len(nrow(objs)), function(j) as.list(objs[j, ]))
    for (o in objs) {
      pv <- o$partition_value
      if (is.null(pv) || (is.list(pv) && !length(pv))) pv <- NA_character_
      rows[[length(rows) + 1]] <- tibble::tibble(
        table = as.character(t$name), partition_value = as.character(pv),
        content_hash = as.character(o$content_hash), sha256 = as.character(o$sha256 %||% NA),
        path = as.character(o$path), since = as.character(o$since %||% NA),
        bytes = as.numeric(o$bytes %||% NA))
    }
  }
  if (!length(rows)) return(empty)
  do.call(rbind, rows)
}

#' Decide, per object, whether to upload or reuse
#'
#' @param objects tibble from [release_objects()] (all tables, row-bound).
#' @param prev_catalog previous release's catalog (list) or NULL.
#' @param layout `"compat"` (objects live under the release prefix; unchanged
#'   ones are server-side copied from the previous release) or `"canonical"`
#'   (objects live under [CC_TABLES_PREFIX]; unchanged ones already exist there).
#' @param version,release_prefix the release and its prefix.
#' @return `objects` plus `path` (bucket-relative destination), `action`
#'   (`upload` | `copy` | `exists`) and `source` (bucket-relative path copied from,
#'   or NA).
#' @export
#' @concept release
freeze_plan <- function(objects, prev_catalog, version, layout = c("compat", "canonical"),
                        release_prefix = CC_RELEASE_PREFIX) {
  layout <- match.arg(layout)
  prev <- .catalog_objects(prev_catalog)
  key_new <- paste(objects$table, objects$partition_value, objects$content_hash, sep = "|")
  key_old <- paste(prev$table, prev$partition_value, prev$content_hash, sep = "|")
  m <- match(key_new, key_old)
  objects$compat_path <- sprintf("%s/%s/parquet/%s", release_prefix, version, objects$rel_path)
  if (layout == "compat") {
    objects$path   <- objects$compat_path
    objects$action <- ifelse(is.na(m), "upload", "copy")
    objects$source <- ifelse(is.na(m), NA_character_, prev$path[m])
  } else {
    objects$path <- mapply(function(t, h, pb, pv) canonical_path(
      t, h, if (is.na(pb)) NULL else pb, if (is.na(pv)) NULL else pv),
      objects$table, objects$content_hash, objects$partition_by, objects$partition_value,
      USE.NAMES = FALSE)
    # an object exists already if the previous release pointed at the same
    # canonical path (a compat-layout previous release never does)
    objects$action <- ifelse(!is.na(m) & prev$path[pmax(m, 1)] == objects$path & !is.na(m),
                             "exists", "upload")
    objects$source <- NA_character_
  }
  objects
}

#' Build the release catalog with per-table hashes and objects
#'
#' Keeps every field consumers read today (`name`, `rows`, `partitioned`,
#' `supplemental`) and adds `content_hash`, `bytes`, `objects[]`
#' (`path`, `bytes`, `sha256`, `content_hash`, `since`, `partition_by`,
#' `partition_value`) and, for the canonical layout, `compat_path`.
#'
#' @param version release version.
#' @param tables_df data.frame with `name, rows, partitioned, supplemental`.
#' @param plan tibble from [freeze_plan()].
#' @param layout as in [freeze_plan()].
#' @param release_date character date.
#' @return A list ready for `jsonlite::write_json(auto_unbox = TRUE)`.
#' @export
#' @concept release
build_release_catalog <- function(version, tables_df, plan, layout = "compat",
                                  release_date = as.character(Sys.Date())) {
  tbls <- lapply(seq_len(nrow(tables_df)), function(i) {
    nm <- tables_df$name[i]
    o <- plan[plan$table == nm, , drop = FALSE]
    objs <- lapply(seq_len(nrow(o)), function(j) {
      x <- list(path = o$path[j], bytes = o$bytes[j], sha256 = o$sha256[j],
                content_hash = o$content_hash[j], since = o$since[j])
      if (!is.na(o$partition_by[j])) {
        x$partition_by <- o$partition_by[j]; x$partition_value <- o$partition_value[j] }
      x
    })
    tbl_hash <- if (nrow(o) == 1) o$content_hash[1] else
      .short_sig(paste(sort(o$content_hash), collapse = "|"))
    t <- list(name = nm, rows = tables_df$rows[i],
              partitioned = isTRUE(tables_df$partitioned[i]),
              supplemental = isTRUE(tables_df$supplemental[i]),
              content_hash = tbl_hash, bytes = sum(o$bytes), objects = objs)
    if (layout == "canonical")
      t$compat_path <- if (nrow(o) == 1) o$compat_path[1] else
        sub("/[^/]+$", "/", o$compat_path[1])
    t
  })
  list(version = version, release_date = release_date, layout = layout,
       writer = CC_PARQUET_WRITER,
       total_rows = sum(tables_df$rows, na.rm = TRUE),
       total_size = sum(plan$bytes), tables = tbls)
}

#' Execute a freeze plan against GCS
#'
#' Uploads `upload` objects from `dir_out`, server-side copies `copy` objects
#' from their `source`, and — for the canonical layout — makes a compat copy of
#' every object at `compat_path` when `compat = TRUE`.
#'
#' @param plan tibble from [freeze_plan()].
#' @param dir_out unused (kept for symmetry); uploads use `plan$local_path`.
#' @param bucket GCS bucket.
#' @param compat also write compat copies (canonical layout only).
#' @param dry_run print the plan, touch nothing.
#' @return Invisibly, a summary tibble of bytes by action.
#' @export
#' @concept release
upload_release_objects <- function(plan, dir_out, bucket, compat = TRUE, dry_run = FALSE) {
  gcloud <- find_gcloud()
  gs <- function(p) sprintf("gs://%s/%s", bucket, p)
  run <- function(...) {
    if (dry_run) { message("  [dry-run] gcloud ", paste(c(...), collapse = " ")); return(0L) }
    res <- system2(gcloud, c(...), stdout = TRUE, stderr = TRUE)
    st <- attr(res, "status") %||% 0L
    if (st != 0) stop("gcloud failed: ", paste(utils::tail(res, 5), collapse = "; "), call. = FALSE)
    st
  }
  for (i in seq_len(nrow(plan))) {
    a <- plan$action[i]
    if (a == "upload") {
      run("storage", "cp", plan$local_path[i], gs(plan$path[i]))
    } else if (a == "copy") {
      run("storage", "cp", gs(plan$source[i]), gs(plan$path[i]))
    }
    if (compat && !identical(plan$path[i], plan$compat_path[i]))
      run("storage", "cp", gs(plan$path[i]), gs(plan$compat_path[i]))
  }
  s <- stats::aggregate(bytes ~ action, data = plan, FUN = sum)
  s$n <- as.vector(table(plan$action)[s$action])
  message(sprintf("release objects: %s",
    paste(sprintf("%s %d (%.1f MB)", s$action, s$n, s$bytes / 1e6), collapse = ", ")))
  invisible(tibble::as_tibble(s))
}
