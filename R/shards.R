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
#' @param parquet_dir directory holding the per-dataset output dirs
#' @param exclude dataset dir names to skip; defaults to the ingests that
#'   declare `calcofi.in_release: false` (see [release_excluded_datasets()]), so
#'   an in-progress ingest's shards stay out of the release even though its
#'   parquet is on disk
#' @return character vector of readable parquet paths/globs, one per dataset
#' @export
#' @concept shards
core_shard_paths <- function(table, root = ".", parquet_dir = "data/parquet",
                             exclude = release_excluded_datasets(root)) {
  base <- file.path(root, parquet_dir)
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
#' @param parquet_dir directory holding the per-dataset output dirs
#' @param exclude dataset dir names to skip (see [core_shard_paths()])
#' @return (invisibly) the row count written, or 0 when no shard exists
#' @export
#' @concept shards
assemble_core_table <- function(con, table, root = ".", id_col = NULL,
                                order_by = NULL, parquet_dir = "data/parquet",
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
#' value by source priority — the WoRMS-lineage-bearing shard (`swfsc_ichthyo`,
#' which carries the hierarchy) first, then the curated seabird/mammal and
#' plankton vocabularies, then everything else. This reproduces the coalescing
#' `build_taxon_reference()` did when it saw every source at once.
#'
#' @param con a DuckDB connection
#' @param root workflows repo root
#' @param priority dataset dirs in descending priority
#' @param parquet_dir directory holding the per-dataset output dirs
#' @param exclude dataset dir names to skip (see [core_shard_paths()])
#' @return (invisibly) the row count of the merged `taxon`
#' @export
#' @concept shards
merge_taxon_shards <- function(con, root = ".",
                               priority = c("swfsc_ichthyo",
                                            "calcofi_bird_mammal_census",
                                            "cce-lter_zoodb", "cce-lter_zooscan",
                                            "calcofi_phytoplankton"),
                               parquet_dir = "data/parquet",
                               exclude = release_excluded_datasets(root)) {
  base  <- file.path(root, parquet_dir)
  dirs  <- list.dirs(base, recursive = FALSE)
  dirs  <- dirs[!basename(dirs) %in% exclude]
  paths <- Filter(file.exists, file.path(dirs, "taxon.parquet"))
  if (!length(paths)) {
    message("merge_taxon_shards(): no taxon shards found")
    return(invisible(0L))
  }
  prio <- function(p) {
    d <- basename(dirname(p))
    m <- match(d, priority)
    if (is.na(m)) length(priority) + 1L else m
  }
  arms <- vapply(paths, function(p) glue::glue(
    "SELECT {prio(p)} AS \"_prio\", * FROM read_parquet('{p}', union_by_name = true)"), "")
  sel <- paste(arms, collapse = "\nUNION ALL BY NAME\n")

  cols <- DBI::dbGetQuery(con, glue::glue(
    "SELECT * FROM ({sel}) LIMIT 0"))
  val_cols <- setdiff(names(cols), c("_prio", "taxon_key"))
  # first non-NULL by priority, then by shard order — arg_min over _prio ignores
  # NULLs, which is exactly the coalesce-with-priority we want
  # quote every identifier: `rank` and `order` are reserved words in DuckDB
  aggs <- paste(vapply(val_cols, function(cl) glue::glue(
    'arg_min("{cl}", "_prio") FILTER (WHERE "{cl}" IS NOT NULL) AS "{cl}"'), ""),
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

#' Assemble the whole consolidated core from the ingest shards
#'
#' Convenience wrapper: `sample`, `obs`, `obs_attribute`, `sample_measurement`
#' (surrogate ids renumbered globally), the supplemental `obs_ctd_full`, and the
#' taxa references (`taxon` merged with priority; `dataset_taxon` /`taxon_group`
#' deduplicated). Errors if `sample_key` is not globally unique — the namespacing
#' guarantee the whole model rests on.
#'
#' @param con a DuckDB connection
#' @param root workflows repo root
#' @param supplemental include `obs_ctd_full` (default TRUE)
#' @param parquet_dir directory holding the per-dataset output dirs
#' @param exclude dataset dir names to skip; defaults to the ingests declaring
#'   `calcofi.in_release: false` (see [release_excluded_datasets()]). Resolved
#'   once here and threaded to every shard read.
#' @return (invisibly) a named list of row counts
#' @export
#' @concept shards
assemble_core <- function(con, root = ".", supplemental = TRUE,
                          parquet_dir = "data/parquet",
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
  if (isTRUE(supplemental))
    out$obs_ctd_full <- assemble_core_table(
      con, "obs_ctd_full", root, id_col = "obs_id",
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
