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
  core <- c("sample", "obs", "obs_attribute", "sample_measurement", "sample_track", "taxon", "dataset_taxon",
            "grid", "cruise", "ship", "measurement_type", "region", "fish", "ichthyo_net")
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
    taxon_group        = list(partition_by = NULL, order_by = c("taxon_group_key", "taxon_key")),
    # browser-shaped objects (R/explore.R); obs_env is one object per variable
    sample_root        = list(partition_by = NULL, order_by = "root_id"),
    obs_bio            = list(partition_by = NULL,
                              order_by = c("dataset_key", "taxon_key NULLS LAST", "datetime NULLS LAST", "obs_id")),
    obs_env            = list(partition_by = "measurement_type",
                              order_by = c("measurement_type", "root_id NULLS LAST", "depth_min_m NULLS LAST", "obs_id")),
    sample_spatial     = list(partition_by = NULL, order_by = c("layer", "spatial_key", "root_id")),
    # the previous grid's cells against this release's (R/grid.R)
    grid_crosswalk     = list(partition_by = NULL, order_by = c("prev_grid_key", "grid_key")),
    # the one baseline every anomaly subtracts (R/climatology.R); one object per variable, like obs_env
    climatology        = list(partition_by = "measurement_type",
                              order_by = c("measurement_type", "dataset_key", "site_key", "month", "depth_bin",
                                           "sigma_theta")),
    # conditions on density surfaces (R/isopycnal.R), one row per cast x surface x type
    isopycnal          = list(partition_by = NULL,
                              order_by = c("sample_key", "sigma_theta", "measurement_type"))))
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
#' @param partition_by optional partition column (Hive layout, one file per value:
#'   the write raises DuckDB's `partitioned_write_max_open_files` above the number
#'   of partition values for its duration, and stops, naming them, if any
#'   partition directory holds more than one parquet file, 4.38.0).
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
    # DuckDB (>= 2.0.0) starts another file for a partition once more than
    # partitioned_write_max_open_files (default 100) are open; keep every
    # partition open for the whole write so each gets exactly one file
    n_part <- DBI::dbGetQuery(con, glue::glue(
      'SELECT COUNT(DISTINCT "{partition_by}") + MAX(("{partition_by}" IS NULL)::INT) AS n
       FROM "{table}"'))$n
    max_open <- DBI::dbGetQuery(con,
      "SELECT current_setting('partitioned_write_max_open_files') AS v")$v
    DBI::dbExecute(con, glue::glue(
      "SET partitioned_write_max_open_files = {as.integer(max(as.numeric(max_open), n_part + 1))}"))
    on.exit(DBI::dbExecute(con, glue::glue(
      "SET partitioned_write_max_open_files = {max_open}")), add = TRUE)
  }
  threads <- DBI::dbGetQuery(con, "SELECT current_setting('threads') AS t")$t
  DBI::dbExecute(con, "SET threads TO 1")
  on.exit(DBI::dbExecute(con, glue::glue("SET threads TO {threads}")), add = TRUE)
  DBI::dbExecute(con, glue::glue(
    'COPY (SELECT {sel} FROM "{table}" ORDER BY {ord}) TO \'{path}\' ({opts})'))
  files <- if (is.null(partition_by)) path else
    list.files(path, pattern = "[.]parquet$", recursive = TRUE, full.names = TRUE)
  base <- if (is.null(partition_by)) dirname(path) else dirname(path)
  rel <- sub(paste0("^", gsub("([.|()\\\\^${}+?*\\[\\]])", "\\\\\\1", base), "/"), "", files)
  if (!is.null(partition_by)) {
    .check_one_file_per_partition(rel, table, partition_by)
    if (length(rel) != n_part)
      stop("`", table, "`: ", length(rel), " partition files for ", n_part,
           " values of ", partition_by, call. = FALSE)
  }
  invisible(tibble::tibble(rel_path = rel, bytes = file.size(files)))
}

# stop, naming them, when a partition directory holds more than one parquet
# file: every release path and canonical path is one object per partition value
.check_one_file_per_partition <- function(rel_path, table, partition_by) {
  pv <- utils::URLdecode(sub(paste0("^.*", partition_by, "=([^/]+)/.*$"), "\\1", rel_path))
  n  <- vapply(split(pv, pv), length, 1L)
  bad <- n[n > 1]
  if (length(bad))
    stop(length(bad), " partition(s) of `", table, "` hold more than one file (",
         "one object per partition value is what the release publishes): ",
         paste(sprintf("%s (%d files)", names(bad), as.integer(bad)), collapse = ", "),
         call. = FALSE)
  invisible(TRUE)
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
    dup <- unique(pv[duplicated(pv)])
    if (length(dup))
      stop("`", table, "`: more than one file for partition value(s) ",
           paste(dup, collapse = ", "), " — each would publish to the same path and ",
           "overwrite the others (4.38.0)", call. = FALSE)
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
        content_hash = as.character(o$content_hash %||% NA), sha256 = as.character(o$sha256 %||% NA),
        path = as.character(o$path %||% NA), since = as.character(o$since %||% NA),
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
#'   or NA). For a `copy`/`exists` object `bytes` and `sha256` describe the object
#'   that will actually sit at `path` — the previous release's — and the local
#'   export's values move to `bytes_local`/`sha256_local` (4.0.3): identity is the
#'   row signature, and a row-identical re-export is not byte-identical for every
#'   table (v2026.09.04 shipped 173 catalog entries whose size/sha256 described
#'   the local file while the path held the copied object).
#' @export
#' @concept release
freeze_plan <- function(objects, prev_catalog, version, layout = c("compat", "canonical"),
                        release_prefix = CC_RELEASE_PREFIX) {
  layout <- match.arg(layout)
  prev <- .catalog_objects(prev_catalog)
  key_new <- paste(objects$table, objects$partition_value, objects$content_hash, sep = "|")
  key_old <- paste(prev$table, prev$partition_value, prev$content_hash, sep = "|")
  m <- match(key_new, key_old)
  objects$bytes_local  <- objects$bytes
  objects$sha256_local <- objects$sha256
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
  dp <- unique(objects$path[duplicated(objects$path)])
  if (length(dp))
    stop("freeze_plan(): duplicated path(s) — two objects would be uploaded to one ",
         "place and the last would win: ", paste(dp, collapse = ", "), call. = FALSE)
  # a reused object keeps the previous release's bytes/sha256 (a legacy catalog
  # without them falls back to the local export's)
  reuse <- objects$action %in% c("copy", "exists") & !is.na(m)
  if (any(reuse)) {
    pb <- prev$bytes[m[reuse]];  ps <- prev$sha256[m[reuse]]
    objects$bytes[reuse]  <- ifelse(is.na(pb), objects$bytes[reuse],  pb)
    objects$sha256[reuse] <- ifelse(is.na(ps), objects$sha256[reuse], ps)
  }
  objects
}

#' Views a release carries beside its tables
#'
#' The registry behind `catalog.json`'s top-level `views` map (3.31.0, pre-release plan D-S1):
#' one entry per view, with the SQL over `{{table}}` tokens ([obs_view_sql()]), the source
#' `tables` it needs, the physical table it `replaces` (marked `deprecated` in the catalog while
#' it still ships) and `removed_in`, the release its objects disappear in. [build_release_catalog()]
#' consults it by default and includes a view only when every source table is in the release.
#'
#' `"next"` is deliberate for `removed_in`: release versions are dates and the next one is not known
#' at freeze time. A consumer treats any non-NULL `removed_in` as "migrate now".
#'
#' @param removed_in the release `obs`'s objects are dropped in (default `"next"`).
#' @return Named list of `list(sql, tables, replaces, removed_in)`.
#' @export
#' @concept release
release_views <- function(removed_in = "next") {
  list(obs = list(
    sql        = obs_view_sql(),
    tables     = c("obs_bio", "obs_env"),
    replaces   = "obs",
    removed_in = removed_in))
}

#' Build the release catalog with per-table hashes and objects
#'
#' Keeps every field consumers read today (`name`, `rows`, `partitioned`,
#' `supplemental`) and adds `content_hash`, `bytes`, `objects[]`
#' (`path`, `bytes`, `sha256`, `content_hash`, `since`, `partition_by`,
#' `partition_value`) and, for the canonical layout, `compat_path`.
#'
#' Since 3.31.0 the catalog also carries a top-level **`views`** map — view name → SQL over
#' `{{table}}` tokens — for every entry of `views` whose source tables are all in `tables_df`, and
#' the table a view `replaces` gains `deprecated: true`, `replaced_by: [...]` and `removed_in` while
#' it still ships. A resolver (`calcofi4r::cc_get_db()`, `calcofi4py.cc_get_db()`, db-query) creates
#' the views after the tables; a deprecated table's objects are read only when the view's sources
#' were not loaded.
#'
#' @param version release version.
#' @param tables_df data.frame with `name, rows, partitioned, supplemental`.
#' @param plan tibble from [freeze_plan()].
#' @param layout as in [freeze_plan()].
#' @param release_date character date.
#' @param views the view registry, see [release_views()]; `list()` for none.
#' @return A list ready for `jsonlite::write_json(auto_unbox = TRUE)`.
#' @export
#' @concept release
build_release_catalog <- function(version, tables_df, plan, layout = "compat",
                                  release_date = as.character(Sys.Date()),
                                  views = release_views()) {
  tbls <- lapply(seq_len(nrow(tables_df)), function(i) {
    nm <- tables_df$name[i]
    o <- plan[plan$table == nm, , drop = FALSE]
    objs <- lapply(seq_len(nrow(o)), function(j) {
      x <- list(path = o$path[j], bytes = o$bytes[j], sha256 = o$sha256[j],
                content_hash = o$content_hash[j], since = o$since[j])
      if (!is.na(o$partition_by[j])) {
        x$partition_by <- o$partition_by[j]; x$partition_value <- o$partition_value[j] }
      # the legacy per-release path of THIS object (a partition file, or the
      # single-file twin a partitioned table may also publish), so redirects and
      # verification never have to reconstruct it
      if (layout == "canonical" && !is.na(o$compat_path[j])) x$compat_path <- o$compat_path[j]
      x
    })
    tbl_hash <- if (nrow(o) == 1) o$content_hash[1] else
      .short_sig(paste(sort(o$content_hash), collapse = "|"))
    t <- list(name = nm, rows = tables_df$rows[i],
              partitioned = isTRUE(tables_df$partitioned[i]),
              supplemental = isTRUE(tables_df$supplemental[i]),
              content_hash = tbl_hash, bytes = sum(o$bytes), objects = objs)
    if (layout == "canonical") {
      # a partitioned table's compat_path is its hive directory (…/{table}/),
      # taken from a PARTITION row: a single-file twin (obs.parquet) sits beside
      # the directory, and dropping one segment from a partition file gives the
      # partition's own directory, not the table's
      pr <- o[!is.na(o$partition_by), , drop = FALSE]
      t$compat_path <- if (nrow(pr)) sub("/[^/]+/[^/]+$", "/", pr$compat_path[1]) else
        o$compat_path[1]
    }
    t
  })
  # views over the tables above (D-S1): included only when every source table
  # ships; the table a view replaces is marked deprecated while it still ships
  vw <- list()
  for (nm in names(views)) {
    v   <- views[[nm]]
    src <- v$tables %||% release_view_tables(v$sql)
    if (!length(src) || !all(src %in% tables_df$name)) next
    vw[[nm]] <- v$sql
    rep <- v$replaces
    if (!is.null(rep) && rep %in% tables_df$name) {
      i <- match(rep, tables_df$name)
      tbls[[i]]$deprecated  <- TRUE
      tbls[[i]]$replaced_by <- I(src)          # always a JSON array, even of one
      tbls[[i]]$removed_in  <- v$removed_in %||% "next"
    }
  }
  out <- list(version = version, release_date = release_date, layout = layout,
              writer = CC_PARQUET_WRITER,
              total_rows = sum(tables_df$rows, na.rm = TRUE),
              total_size = sum(plan$bytes), tables = tbls)
  if (length(vw)) out$views <- vw
  out
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

# versions.json ---------------------------------------------------------------

#' Build the `versions.json` register of every release under a prefix
#'
#' Discovers each `{prefix}/{version}/catalog.json` in the bucket and returns
#' the list `release_database.qmd` writes as `versions.json` — one record per
#' release with `version`, `release_date`, `tables`, `total_rows`, `size_mb`,
#' plus the archive-policy fields: `consolidated` (parquet kept indefinitely,
#' from `metadata/release_policy.yml`) and, when the version's parquet has been
#' removed by `scripts/thin_releases.R`, `retired` (`{retired_utc, to, reason}`
#' read from its `retired.json`). Both the release notebook and the thinning
#' script call this, so a re-run of the release cannot drop the policy fields.
#'
#' @param bucket GCS bucket
#' @param prefix release prefix (`ducklake/releases`, or a staging prefix)
#' @param consolidated character vector of consolidated versions
#' @param current optional record for the release being written right now
#'   (takes precedence over what the bucket holds for that version)
#' @return list of release records, newest first
#' @concept release
#' @export
build_versions_json <- function(bucket, prefix = CC_RELEASE_PREFIX,
                                consolidated = character(), current = NULL) {
  https <- function(p) sprintf("https://storage.googleapis.com/%s/%s", bucket, p)
  gcloud <- find_gcloud()
  dirs <- tryCatch(system2(gcloud, c("storage", "ls", sprintf("gs://%s/%s/", bucket, prefix)),
                           stdout = TRUE, stderr = FALSE), error = function(e) character())
  vers <- basename(dirs[grepl("/v[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}/$", dirs)])
  recs <- lapply(vers, function(v) {
    cat_ <- tryCatch(jsonlite::fromJSON(https(sprintf("%s/%s/catalog.json", prefix, v)),
                                        simplifyVector = FALSE), error = function(e) NULL)
    if (is.null(cat_)) return(NULL)
    rec <- versions_record(cat_)
    ret <- tryCatch(jsonlite::fromJSON(https(sprintf("%s/%s/retired.json", prefix, v)),
                                       simplifyVector = FALSE), error = function(e) NULL)
    if (!is.null(ret)) rec$retired <- ret
    rec
  })
  recs <- Filter(Negate(is.null), recs)
  if (!is.null(current)) {
    recs <- Filter(function(r) !identical(r$version, current$version), recs)
    recs <- c(list(current), recs)
  }
  recs <- lapply(recs, function(r) { r$consolidated <- r$version %in% consolidated; r })
  recs[order(vapply(recs, `[[`, "", "version"), decreasing = TRUE)]
}

#' One release's record in `versions.json`, from its `catalog.json`
#'
#' @param catalog a parsed `catalog.json` (list)
#' @return list: `version`, `release_date`, `tables`, `total_rows`, `size_mb`, and
#'   `doi` once [publish_release_notes()] has written it into the catalog
#' @concept release
#' @export
versions_record <- function(catalog) {
  rec <- list(
    version      = catalog$version,
    release_date = catalog$release_date,
    tables       = length(catalog$tables),
    total_rows   = as.numeric(catalog$total_rows %||% 0),
    size_mb      = round((catalog$total_size %||% 0) / 1024 / 1024, 1))
  if (!is.null(catalog$doi) && nzchar(catalog$doi)) rec$doi <- catalog$doi
  rec
}

#' Archive-thinning plan: which versions lose their parquet
#'
#' Pure policy over a `versions.json` list: keep the consolidated versions, the
#' promoted version and its `keep_latest - 1` predecessors; everything else not
#' already retired is a candidate. Each candidate is retired *to* the nearest
#' consolidated (or kept) version at or after it — the closest data a reader
#' can substitute.
#'
#' @param versions list as returned by [build_versions_json()] (any order)
#' @param latest the promoted version string
#' @param consolidated character vector of consolidated versions
#' @param keep_latest how many of the newest versions to keep (default 2)
#' @return tibble `version`, `keep` (logical), `reason`, `to` (replacement for
#'   candidates, `NA` for kept)
#' @concept release
#' @export
thin_plan <- function(versions, latest, consolidated, keep_latest = 2) {
  vs <- sort(vapply(versions, `[[`, "", "version"), decreasing = TRUE)
  retired <- vapply(versions, function(r) !is.null(r$retired), TRUE)[match(vs, vapply(versions, `[[`, "", "version"))]
  newest <- vs[vs <= latest][seq_len(min(keep_latest, sum(vs <= latest)))]
  keep <- vs %in% consolidated | vs %in% newest | vs > latest
  reason <- ifelse(vs %in% consolidated, "consolidated",
            ifelse(vs %in% newest, "promoted or predecessor",
            ifelse(vs > latest, "newer than promoted (unpromoted candidate)",
            ifelse(retired, "already retired", "thin"))))
  keep <- keep | retired
  to <- vapply(vs, function(v) {
    later <- vs[keep & !retired & vs >= v]
    if (!length(later)) NA_character_ else min(later)
  }, "")
  to[keep] <- NA_character_
  tibble::tibble(version = vs, keep = keep, reason = reason, to = unname(to))
}

# published objects ------------------------------------------------------------

#' Check a release catalog's objects as published
#'
#' Three checks per table of a `catalog.json`, supplemental tables included (4.38.0):
#'
#' - `unique_paths`: no object path is listed twice;
#' - `one_object_per_partition`: a partitioned table has one object per partition
#'   value (and at most one single-file twin, an object without a partition value
#'   holding every row, as `obs` publishes), a single-file table exactly one object;
#' - `published_rows` (only with `con`): the rows in the parquet footers of the
#'   table's distinct partition objects, read where they are published (no scan),
#'   sum to the catalog's `rows`, and so do a twin's.
#'
#' v2026.11.05 and v2026.11.09 failed all three for `obs_ctd_full`: 169 objects for
#' 143 cruises, 24 paths listed 2 or 3 times, and 240,092,665 published rows against
#' a catalog `rows` of 287,281,956, because the export wrote some partitions as two or
#' three files and the uploads overwrote each other (see [export_release_parquet()]).
#'
#' @param catalog a parsed `catalog.json` (list, as `jsonlite::fromJSON(...,
#'   simplifyVector = FALSE)` returns it, or a data-frame form).
#' @param con optional DuckDB connection able to read `base_url` (httpfs loaded for
#'   https); without it `published_rows` is not checked.
#' @param base_url where the catalog's bucket-relative `path`s resolve.
#' @param tables optional subset of table names.
#' @return A tibble: `table`, `check`, `ok`, `n` (objects, or published rows),
#'   `expected`, `detail`.
#' @export
#' @concept release
#' @importFrom DBI dbGetQuery
check_release_objects <- function(catalog, con = NULL,
                                  base_url = "https://storage.googleapis.com/calcofi-db",
                                  tables = NULL) {
  objs <- .catalog_objects(catalog)
  tbls <- catalog$tables
  n <- if (is.data.frame(tbls)) nrow(tbls) else length(tbls)
  meta <- do.call(rbind, lapply(seq_len(n), function(i) {
    t <- if (is.data.frame(tbls)) as.list(tbls[i, ]) else tbls[[i]]
    tibble::tibble(table = as.character(t[["name"]]), rows = as.numeric(t[["rows"]] %||% NA),
                   partitioned = isTRUE(as.logical(t[["partitioned"]] %||% FALSE)))
  }))
  if (!is.null(tables)) meta <- meta[meta$table %in% tables, ]
  rows_of <- NULL
  if (!is.null(con)) {
    paths <- unique(objs$path[objs$table %in% meta$table])
    urls  <- paste0(sub("/$", "", base_url), "/", paths)
    got   <- list()
    for (ch in split(seq_along(urls), ceiling(seq_along(urls) / 200))) {
      lst <- paste0("'", gsub("'", "''", urls[ch]), "'", collapse = ", ")
      got[[length(got) + 1]] <- DBI::dbGetQuery(con, sprintf(
        "SELECT file_name, num_rows FROM parquet_file_metadata([%s])", lst))
    }
    got <- do.call(rbind, got)
    rows_of <- stats::setNames(as.numeric(got$num_rows), got$file_name)
    rows_of <- stats::setNames(rows_of[urls], paths)
  }
  out <- list()
  for (i in seq_len(nrow(meta))) {
    tb <- meta$table[i]
    o  <- objs[objs$table == tb, ]
    dp <- unique(o$path[duplicated(o$path)])
    out[[length(out) + 1]] <- tibble::tibble(
      table = tb, check = "unique_paths", ok = !length(dp), n = nrow(o),
      expected = length(unique(o$path)),
      detail = if (length(dp)) sprintf("%d path(s) listed more than once: %s", length(dp),
                                       .head_list(dp)) else NA_character_)
    # a partitioned table may also list one single-file twin (`obs` does): an object
    # without a partition value that holds every row; it is counted on its own
    part <- meta$partitioned[i] || any(!is.na(o$partition_value))
    is_p <- part & !is.na(o$partition_value)
    twin <- if (part) o$path[!is_p] else character()
    pv   <- o$partition_value[is_p]
    n_obj <- if (part) sum(is_p) else nrow(o)
    n_exp <- if (part) length(unique(pv)) else 1L
    ok_part <- n_obj == n_exp && length(twin) <= 1 && (!part || n_exp > 0)
    out[[length(out) + 1]] <- tibble::tibble(
      table = tb, check = "one_object_per_partition", ok = ok_part, n = n_obj,
      expected = n_exp,
      detail = if (ok_part) NA_character_ else if (part)
        sprintf("%d object(s) for %d partition value(s)%s%s", n_obj, n_exp,
                if (n_obj > n_exp) paste0("; repeated: ", .head_list(unique(pv[duplicated(pv)]))) else "",
                if (length(twin) > 1) sprintf("; %d single-file twins", length(twin)) else "")
        else sprintf("%d objects for a single-file table", nrow(o)))
    if (!is.null(rows_of)) {
      sets <- if (part) list(partitions = unique(o$path[is_p]), twin = unique(twin)) else
        list(object = unique(o$path))
      sets <- sets[lengths(sets) > 0]
      msg <- character(); got_rows <- NA_real_
      for (nm in names(sets)) {
        r <- rows_of[sets[[nm]]]
        g <- sum(r, na.rm = TRUE)
        if (nm != "twin") got_rows <- g
        if (anyNA(r)) msg <- c(msg, sprintf("%s: %d object(s) unreadable", nm, sum(is.na(r))))
        else if (!isTRUE(all.equal(g, meta$rows[i])))
          msg <- c(msg, sprintf("%spublished %s rows, catalog says %s (%s)",
                                if (nm == "twin") "single-file twin: " else "",
                                format(g, big.mark = ","), format(meta$rows[i], big.mark = ","),
                                format(g - meta$rows[i], big.mark = ",")))
      }
      out[[length(out) + 1]] <- tibble::tibble(
        table = tb, check = "published_rows", ok = !length(msg) && length(sets) > 0,
        n = got_rows, expected = meta$rows[i],
        detail = if (length(msg)) paste(msg, collapse = "; ") else
          if (!length(sets)) "no objects" else NA_character_)
    }
  }
  do.call(rbind, out)
}

.head_list <- function(x, k = 5) paste0(paste(utils::head(x, k), collapse = ", "),
                                        if (length(x) > k) sprintf(", … (%d more)", length(x) - k) else "")
