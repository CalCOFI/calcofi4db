# assemble the consolidated core from per-dataset ingest shards ----------------
# Each ingest now writes its own slice of sample / obs / obs_attribute /
# sample_measurement / taxon / dataset_taxon / taxon_group. The release
# concatenates those shards rather than re-deriving the core from per-dataset
# tables — the re-derivation is what let the two projections drift apart.
#
# NOTE the release table registry marks the FIRST ingest that supplies a table
# name as canonical, which is right for a genuinely shared reference (grid) and
# wrong for a shard (every ingest supplies `obs`). These helpers bypass the
# registry and read every matching path.

#' Find the per-dataset parquet shards for a core table
#'
#' @param root workflows repo root (contains `data/parquet/`)
#' @param table core table name (e.g. `"obs"`)
#' @param parquet_dir directory holding the per-dataset output dirs. Defaults
#'   to the local staging root (see [cc_stage_dir()]), where the bulk parquet
#'   lives; an absolute path is used as-is, a relative one is resolved against
#'   `root`. The JSON sidecars stay in the repo and are found separately.
#' @param exclude dataset dir names to skip; defaults to the ingests that
#'   declare `calcofi.in_release: false` (see [release_excluded_datasets()]), so
#'   an in-progress ingest's shards stay out of the release even though its
#'   parquet is on disk
#' @return character vector of readable parquet paths/globs, one per dataset
#' @export
#' @concept shards
core_shard_paths <- function(table, root = ".", parquet_dir = cc_stage_path("parquet"),
                             exclude = release_excluded_datasets(root)) {
  base <- .shard_base(root, parquet_dir)
  dirs <- list.dirs(base, recursive = FALSE)
  dirs <- dirs[!basename(dirs) %in% exclude]
  out <- character()
  for (d in dirs) {
    f <- file.path(d, paste0(table, ".parquet"))
    p <- file.path(d, table)                     # hive-partitioned directory
    if (file.exists(f)) out <- c(out, f)
    else if (dir.exists(p)) out <- c(out, file.path(p, "**", "*.parquet"))
  }
  out
}

# Resolve where the per-dataset shard dirs live. The bulk parquet moved out of
# the repo to the staging root (an absolute path), while `root` still locates
# the repo for the sidecars and the in_release YAML — so an absolute
# parquet_dir must NOT be pasted onto root, or the shards resolve to a path
# that cannot exist and every core table silently assembles from zero shards.
.shard_base <- function(root, parquet_dir) {
  is_abs <- grepl("^(/|~|[A-Za-z]:[\\\\/])", parquet_dir)
  if (is_abs) path.expand(parquet_dir) else file.path(root, parquet_dir)
}

# SQL reading one shard path (hive partitioning for the directory form)
.shard_read <- function(p) {
  if (grepl("\\*\\*", p)) {
    glue::glue("read_parquet('{p}', hive_partitioning = true, union_by_name = true)")
  } else {
    glue::glue("read_parquet('{p}', union_by_name = true)")
  }
}

#' Assemble one core table from its per-dataset shards
#'
#' UNIONs every dataset's shard into a single table. Surrogate ids are renumbered
#' globally after the union (each ingest numbers from 1 within its own shard, so
#' the raw ids collide across datasets).
#'
#' @param con a DuckDB connection
#' @param table core table name
#' @param root workflows repo root
#' @param id_col surrogate id column to renumber globally (NULL to keep as-is)
#' @param order_by optional ORDER BY used when renumbering, so ids are stable
#'   across re-runs of unchanged data
#' @param parquet_dir directory holding the per-dataset output dirs. Defaults
#'   to the local staging root (see [cc_stage_dir()]), where the bulk parquet
#'   lives; an absolute path is used as-is, a relative one is resolved against
#'   `root`. The JSON sidecars stay in the repo and are found separately.
#' @param exclude dataset dir names to skip (see [core_shard_paths()])
#' @return (invisibly) the row count written, or 0 when no shard exists
#' @export
#' @concept shards
assemble_core_table <- function(con, table, root = ".", id_col = NULL,
                                order_by = NULL, parquet_dir = cc_stage_path("parquet"),
                                exclude = release_excluded_datasets(root)) {
  paths <- core_shard_paths(table, root, parquet_dir, exclude = exclude)
  if (!length(paths)) {
    message("assemble_core_table(): no shards found for ", table)
    return(invisible(0L))
  }
  sel <- paste(vapply(paths, function(p)
    glue::glue("SELECT * FROM {.shard_read(p)}"), ""), collapse = "\nUNION ALL BY NAME\n")

  DBI::dbExecute(con, glue::glue("DROP VIEW IF EXISTS {table}"))
  DBI::dbExecute(con, glue::glue("DROP TABLE IF EXISTS {table}"))
  if (is.null(id_col)) {
    DBI::dbExecute(con, glue::glue("CREATE TABLE {table} AS {sel}"))
  } else {
    ord <- if (is.null(order_by)) "" else glue::glue("ORDER BY {order_by}")
    # renumber the surrogate id across the reassembled shards
    DBI::dbExecute(con, glue::glue(
      "CREATE TABLE {table} AS
       SELECT * REPLACE (ROW_NUMBER() OVER ({ord}) AS {id_col})
       FROM ( {sel} )"))
  }
  n <- DBI::dbGetQuery(con, glue::glue("SELECT COUNT(*) AS n FROM {table}"))$n
  message(glue::glue("{table}: {format(n, big.mark = ',')} rows from {length(paths)} shard(s)"))
  invisible(n)
}

#' Merge the per-dataset `taxon` shards into one authoritative reference
#'
#' Each ingest emits the taxon rows its own vocabulary reaches. The same taxon
#' can appear in several shards (Appendicularia is in both zoodb and zooscan), so
#' rows are collapsed on `taxon_key` and each field takes the first non-NULL
#' value in **dataset directory order**. There is no priority list (taxon plan
#' D5): every shard's `scientific_name` / `rank` / classification comes from the
#' same cached authority lineage, so shards agree wherever both have a value and
#' the order only settles which shard fills a gap. `common_name` is not decided
#' here at all — the release applies the written precedence with
#' [apply_taxon_common()] — and `notes` is unioned, never picked.
#'
#' @param con a DuckDB connection
#' @param root workflows repo root
#' @param parquet_dir directory holding the per-dataset output dirs. Defaults
#'   to the local staging root (see [cc_stage_dir()]), where the bulk parquet
#'   lives; an absolute path is used as-is, a relative one is resolved against
#'   `root`. The JSON sidecars stay in the repo and are found separately.
#' @param exclude dataset dir names to skip (see [core_shard_paths()])
#' @return (invisibly) the row count of the merged `taxon`
#' @export
#' @concept shards
merge_taxon_shards <- function(con, root = ".",
                               parquet_dir = cc_stage_path("parquet"),
                               exclude = release_excluded_datasets(root)) {
  base  <- .shard_base(root, parquet_dir)
  dirs  <- list.dirs(base, recursive = FALSE)
  dirs  <- dirs[!basename(dirs) %in% exclude]
  paths <- Filter(file.exists, file.path(dirs, "taxon.parquet"))
  if (!length(paths)) {
    message("merge_taxon_shards(): no taxon shards found")
    return(invisible(0L))
  }
  # the shard's dataset directory name is the tie-break key: deterministic,
  # and nothing to append to when a dataset is added
  arms <- vapply(paths, function(p) glue::glue(
    "SELECT '{basename(dirname(p))}' AS \"_src\", * FROM read_parquet('{p}', union_by_name = true)"), "")
  sel <- paste(arms, collapse = "\nUNION ALL BY NAME\n")

  cols <- DBI::dbGetQuery(con, glue::glue(
    "SELECT * FROM ({sel}) LIMIT 0"))
  val_cols <- setdiff(names(cols), c("_src", "taxon_key"))
  # first non-NULL in dataset order — arg_min over _src ignores NULLs, which is
  # exactly the coalesce we want
  # quote every identifier: `rank` and `order` are reserved words in DuckDB
  #
  # `notes` is the exception: it is append-only provenance, so a taxon two
  # datasets both reach must keep BOTH lines rather than let the first shard's
  # note silently win. Identical blocks (the common case — the xref cache is
  # global) collapse via DISTINCT.
  aggs <- paste(vapply(val_cols, function(cl) if (cl == "notes") glue::glue(
    'string_agg(DISTINCT "{cl}", chr(10)) AS "{cl}"') else glue::glue(
    'arg_min("{cl}", "_src") FILTER (WHERE "{cl}" IS NOT NULL) AS "{cl}"'), ""),
    collapse = ",\n         ")

  DBI::dbExecute(con, "DROP VIEW IF EXISTS taxon")
  DBI::dbExecute(con, "DROP TABLE IF EXISTS taxon")
  DBI::dbExecute(con, glue::glue(
    "CREATE TABLE taxon AS
     SELECT taxon_key,
            {aggs}
     FROM ( {sel} )
     GROUP BY taxon_key
     ORDER BY taxon_key"))
  n <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM taxon")$n
  message(glue::glue("taxon: {format(n, big.mark = ',')} rows merged from {length(paths)} shard(s)"))
  invisible(n)
}

#' Supplemental full-resolution tables declared by the ingests
#'
#' Reads every ingest's `calcofi.tables_owned` and returns the tables flagged
#' `supplemental: true` — the full-resolution products that ship alongside the
#' thinned core (`obs_ctd_full`, `obs_mets_full`), hosted and tagged to the
#' release but hidden from the default table list and the ERD.
#'
#' @param root workflows repo root.
#' @param which `TRUE` for all declared, `FALSE` for none, or an explicit
#'   character vector (returned as given, so a caller can override).
#'
#' @return Character vector of table names, possibly empty.
#'
#' @details
#' Only `obs`-shaped tables (named `obs_*`) are returned. A supplemental table
#' that is not a slice of the core cannot be assembled by [assemble_core()],
#' which renumbers `obs_id` and orders by the core's columns — `calcofi_mets`
#' previously declared the raw `mets_measurement` here, which carried neither an
#' `obs_id` nor any coordinate and could not be published usefully.
#'
#' @export
#' @concept shards
#' @examples
#' \dontrun{
#' supplemental_core_tables("workflows")   # "obs_ctd_full" "obs_mets_full"
#' }
supplemental_core_tables <- function(root = ".", which = TRUE) {
  if (isFALSE(which)) return(character())
  if (!isTRUE(which)) return(as.character(which))
  iy <- tryCatch(read_ingest_yaml(root), error = function(e) list())
  tbls <- unlist(lapply(iy, function(cc) {
    to <- cc$tables_owned
    if (is.null(to)) return(NULL)
    hit <- Filter(function(t) isTRUE(t$supplemental), to)
    vapply(hit, function(t) as.character(t$table), character(1))
  }), use.names = FALSE)
  tbls <- unique(tbls %||% character())
  # obs-shaped only: assemble_core() renumbers obs_id and orders by core columns
  sort(grep("^obs_", tbls, value = TRUE))
}

#' Assemble the whole consolidated core from the ingest shards
#'
#' Convenience wrapper: `sample`, `obs`, `obs_attribute`, `sample_measurement`
#' (surrogate ids renumbered globally), the supplemental `obs_ctd_full`, and the
#' taxa references (`taxon` merged in dataset order; `dataset_taxon` /`taxon_group`
#' deduplicated). Errors if `sample_key` is not globally unique — the namespacing
#' guarantee the whole model rests on.
#'
#' @param con a DuckDB connection
#' @param root workflows repo root
#' @param supplemental `TRUE` (default) to include every supplemental
#'   full-resolution table the ingests declare, `FALSE` for none, or an explicit
#'   character vector of table names. See [supplemental_core_tables()].
#' @param parquet_dir directory holding the per-dataset output dirs. Defaults
#'   to the local staging root (see [cc_stage_dir()]), where the bulk parquet
#'   lives; an absolute path is used as-is, a relative one is resolved against
#'   `root`. The JSON sidecars stay in the repo and are found separately.
#' @param exclude dataset dir names to skip; defaults to the ingests declaring
#'   `calcofi.in_release: false` (see [release_excluded_datasets()]). Resolved
#'   once here and threaded to every shard read.
#' @return (invisibly) a named list of row counts
#' @export
#' @concept shards
assemble_core <- function(con, root = ".", supplemental = TRUE,
                          parquet_dir = cc_stage_path("parquet"),
                          exclude = release_excluded_datasets(root)) {
  if (length(exclude))
    message(glue::glue(
      "assemble_core(): excluding {length(exclude)} dataset(s) flagged ",
      "in_release: false — {paste(exclude, collapse = ', ')}"))
  out <- list()
  out$sample <- assemble_core_table(
    con, "sample", root, parquet_dir = parquet_dir, exclude = exclude)

  dup <- DBI::dbGetQuery(con,
    "SELECT sample_key, COUNT(*) n FROM sample GROUP BY 1 HAVING COUNT(*) > 1
     ORDER BY n DESC LIMIT 5")
  if (nrow(dup))
    stop("assemble_core(): duplicate sample_key across shards, e.g. ",
         paste(sprintf("%s (x%d)", dup$sample_key, dup$n), collapse = "; "))

  out$obs <- assemble_core_table(
    con, "obs", root, id_col = "obs_id",
    order_by = "dataset_key, grid_key NULLS LAST, depth_min_m NULLS LAST, measurement_type",
    parquet_dir = parquet_dir, exclude = exclude)
  out$obs_attribute <- assemble_core_table(
    con, "obs_attribute", root, id_col = "obs_attribute_id",
    order_by = "dataset_key, sample_key, measurement_type, bin_value NULLS LAST",
    parquet_dir = parquet_dir, exclude = exclude)
  out$sample_measurement <- assemble_core_table(
    con, "sample_measurement", root, id_col = "sample_measurement_id",
    order_by = "dataset_key, sample_key, measurement_type",
    parquet_dir = parquet_dir, exclude = exclude)
  # Supplemental full-resolution tables are DISCOVERED from the ingests, not
  # hardcoded: `obs_ctd_full` was the only one until `calcofi_mets` added
  # `obs_mets_full`, and a hardcoded name silently drops any new one from the
  # release while the ingest still writes it.
  for (t in supplemental_core_tables(root, supplemental))
    out[[t]] <- assemble_core_table(
      con, t, root, id_col = "obs_id",
      order_by = "cruise_key, grid_key NULLS LAST, depth_min_m NULLS LAST, measurement_type",
      parquet_dir = parquet_dir, exclude = exclude)

  out$taxon <- merge_taxon_shards(
    con, root, parquet_dir = parquet_dir, exclude = exclude)

  for (t in c("dataset_taxon", "taxon_group")) {
    n <- assemble_core_table(
      con, t, root, parquet_dir = parquet_dir, exclude = exclude)
    if (n > 0) {
      key <- if (t == "dataset_taxon") "ds_taxon_key" else "taxon_group_key, taxon_key"
      DBI::dbExecute(con, glue::glue(
        "CREATE OR REPLACE TABLE {t} AS
         SELECT * FROM {t} QUALIFY row_number() OVER (PARTITION BY {key}) = 1"))
      n <- DBI::dbGetQuery(con, glue::glue("SELECT COUNT(*) AS n FROM {t}"))$n
    }
    out[[t]] <- n
  }
  invisible(out)
}
