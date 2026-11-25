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

#' Rows of id space each dataset owns in a surrogate id column
#'
#' A surrogate id (`obs_id`, `obs_attribute_id`, `sample_measurement_id`) is
#' `id_block * CC_ID_BLOCK_SIZE + n`, where `id_block` is the dataset's number in
#' `metadata/id_block.csv` and `n` counts the dataset's own rows from 1. Ten
#' billion rows per dataset per table leaves every id below 2^53 (a JavaScript
#' safe integer) for any block up to 900,000.
#' @export
#' @concept shards
CC_ID_BLOCK_SIZE <- 1e10

#' Read the id-block registry
#'
#' `metadata/id_block.csv` gives every dataset a number that is never changed
#' and never reused: `dataset_key, id_block, added`. It is what lets a
#' surrogate id say which dataset it belongs to, so one dataset growing cannot
#' renumber another (through v2026.11.25 the ids were one row number over the
#' whole table, and 17 new CTD casts re-identified every row of fifteen other
#' datasets).
#'
#' @param root workflows repo root (holds `metadata/id_block.csv`)
#' @param path the registry file
#' @return A data frame `dataset_key` (chr), `id_block` (dbl, whole, unique,
#'   1..900000), sorted by `id_block`.
#' @export
#' @concept shards
read_id_blocks <- function(root = ".", path = file.path(root, "metadata", "id_block.csv")) {
  if (!file.exists(path))
    stop("no id-block registry at ", path, ": a surrogate id is numbered inside its ",
         "dataset's block (`dataset_key,id_block,added`), so the release cannot ",
         "assemble without it", call. = FALSE)
  d <- utils::read.csv(path, colClasses = "character", na.strings = character(),
                       strip.white = TRUE)
  miss <- setdiff(c("dataset_key", "id_block"), names(d))
  if (length(miss))
    stop(path, " lacks column(s): ", paste(miss, collapse = ", "), call. = FALSE)
  b <- suppressWarnings(as.numeric(d$id_block))
  bad <- is.na(b) | b != floor(b) | b < 1 | b > 900000 | !nzchar(d$dataset_key)
  if (any(bad))
    stop(path, ": id_block must be a whole number from 1 to 900000 and dataset_key ",
         "non-empty (row(s) ", paste(which(bad), collapse = ", "), ")", call. = FALSE)
  if (anyDuplicated(b) || anyDuplicated(d$dataset_key))
    stop(path, ": a dataset_key or an id_block appears twice (",
         paste(unique(c(d$dataset_key[duplicated(d$dataset_key)],
                        d$id_block[duplicated(b)])), collapse = ", "),
         "); a block is never shared and never reused", call. = FALSE)
  out <- data.frame(dataset_key = d$dataset_key, id_block = b, stringsAsFactors = FALSE)
  out[order(out$id_block), , drop = FALSE]
}

# the ORDER BY that numbers one dataset's rows: the caller's leading terms (those
# whose column the table has), then every other column, so the order is total up
# to rows identical in every column — and which of two identical rows gets which
# number changes nothing. A column that cannot be compared directly is ordered
# by its text or WKB form.
.id_order_sql <- function(cols, types, id_col, order_by = NULL) {
  lead <- trimws(unlist(strsplit(order_by %||% "", ",", fixed = TRUE)))
  lead <- lead[nzchar(lead)]
  lead_col <- gsub('"', "", .order_cols(lead), fixed = TRUE)
  keep <- lead_col %in% setdiff(cols, c(id_col, "dataset_key"))
  lead <- lead[keep]; lead_col <- lead_col[keep]
  rest <- setdiff(cols, c(id_col, "dataset_key", lead_col))
  expr <- vapply(rest, function(cn) {
    dt <- toupper(types[[cn]])
    q <- paste0('"', cn, '"')
    if (grepl("GEOMETRY", dt)) paste0("ST_AsWKB(", q, ")")
    else if (grepl("STRUCT|MAP|LIST|UNION|\\[\\]", dt)) paste0("CAST(", q, " AS VARCHAR)")
    else q
  }, character(1))
  paste(c(lead, if (length(expr)) paste(expr, "NULLS LAST")), collapse = ", ")
}

#' Assemble one core table from its per-dataset shards
#'
#' UNIONs every dataset's shard into a single table. A surrogate id is numbered
#' **inside its dataset's block**: `id_block * CC_ID_BLOCK_SIZE + n`, `n` being
#' the row's rank among its own dataset's rows under a total order of the
#' columns. So an id changes only when its own dataset's rows change, never
#' because another dataset gained or lost rows, and re-assembling unchanged
#' shards gives the same ids whatever order the shards or their rows arrive in.
#'
#' @param con a DuckDB connection
#' @param table core table name
#' @param root workflows repo root
#' @param id_col surrogate id column to number (NULL to keep the table as-is)
#' @param order_by the leading terms of the order a dataset's rows are numbered
#'   in (a term naming a column the table lacks, or `dataset_key`, is skipped);
#'   every remaining column follows as the tiebreak
#' @param parquet_dir directory holding the per-dataset output dirs. Defaults
#'   to the local staging root (see [cc_stage_dir()]), where the bulk parquet
#'   lives; an absolute path is used as-is, a relative one is resolved against
#'   `root`. The JSON sidecars stay in the repo and are found separately.
#' @param exclude dataset dir names to skip (see [core_shard_paths()])
#' @param id_blocks the registry from [read_id_blocks()]; read only when
#'   `id_col` is given
#' @return (invisibly) the row count written, or 0 when no shard exists
#' @export
#' @concept shards
assemble_core_table <- function(con, table, root = ".", id_col = NULL,
                                order_by = NULL, parquet_dir = cc_stage_path("parquet"),
                                exclude = release_excluded_datasets(root),
                                id_blocks = NULL) {
  paths <- core_shard_paths(table, root, parquet_dir, exclude = exclude)
  if (!length(paths)) {
    message("assemble_core_table(): no shards found for ", table)
    return(invisible(0L))
  }
  sel <- paste(vapply(paths, function(p)
    glue::glue("SELECT * FROM {.shard_read(p)}"), ""), collapse = "\nUNION ALL BY NAME\n")

  # DROP VIEW on a table (or the reverse) is an error even with IF EXISTS, so ask what is there
  kind <- DBI::dbGetQuery(con, glue::glue(
    "SELECT table_type FROM information_schema.tables WHERE table_name = '{table}' AND table_schema = 'main'"))$table_type
  if (length(kind))
    DBI::dbExecute(con, glue::glue("DROP {if (identical(kind[1], 'VIEW')) 'VIEW' else 'TABLE'} {table}"))
  if (is.null(id_col)) {
    DBI::dbExecute(con, glue::glue("CREATE TABLE {table} AS {sel}"))
  } else {
    if (is.null(id_blocks)) id_blocks <- read_id_blocks(root)
    d <- DBI::dbGetQuery(con, glue::glue("DESCRIBE SELECT * FROM ( {sel} ) LIMIT 0"))
    cols <- d$column_name; types <- stats::setNames(d$column_type, cols)
    if (!"dataset_key" %in% cols)
      stop("assemble_core_table(): `", table, "` has no dataset_key column, so `", id_col,
           "` cannot be numbered inside a dataset's block", call. = FALSE)
    keys <- DBI::dbGetQuery(con, glue::glue(
      "SELECT DISTINCT dataset_key FROM ( {sel} )"))$dataset_key
    if (anyNA(keys))
      stop("assemble_core_table(): `", table, "` has rows with no dataset_key", call. = FALSE)
    new <- setdiff(keys, id_blocks$dataset_key)
    if (length(new))
      stop("assemble_core_table(): no id block for ", paste(sort(new), collapse = ", "),
           ". Append each to metadata/id_block.csv with the next unused number (",
           max(c(0, id_blocks$id_block)) + 1, " onward); never reuse or change one",
           call. = FALSE)
    b <- id_blocks[id_blocks$dataset_key %in% keys, , drop = FALSE]
    block <- paste0("CASE dataset_key ", paste(sprintf(
      "WHEN '%s' THEN %.0f", gsub("'", "''", b$dataset_key), b$id_block * CC_ID_BLOCK_SIZE),
      collapse = " "), " END")
    ord <- .id_order_sql(cols, types, id_col, order_by)
    DBI::dbExecute(con, glue::glue(
      "CREATE TABLE {table} AS
       SELECT * REPLACE (CAST({block} + ROW_NUMBER() OVER (
                 PARTITION BY dataset_key ORDER BY {ord}) AS BIGINT) AS {id_col})
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
#' which numbers `obs_id` and orders by the core's columns — `calcofi_mets`
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
  # obs-shaped only: assemble_core() numbers obs_id and orders by core columns
  sort(grep("^obs_", tbls, value = TRUE))
}

#' Assemble the whole consolidated core from the ingest shards
#'
#' Convenience wrapper: `sample`, `obs`, `obs_attribute`, `sample_measurement`
#' (each surrogate id numbered inside its dataset's block, see
#' [assemble_core_table()]), `sample_track` (when any shard has one), the supplemental `obs_ctd_full`, and the
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
  # read once: every surrogate id below is numbered inside its dataset's block. A root with
  # no registry is only an error for a table that has ids to number, and
  # assemble_core_table() raises it there
  blocks <- tryCatch(read_id_blocks(root), error = function(e) NULL)
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
    # time first, so a dataset that gains a later cruise keeps the ids of its earlier rows
    order_by = paste("datetime NULLS LAST, sample_key NULLS LAST, depth_min_m NULLS LAST,",
                     "depth_max_m NULLS LAST, measurement_type, taxon_key NULLS LAST, life_stage NULLS LAST"),
    parquet_dir = parquet_dir, exclude = exclude, id_blocks = blocks)
  out$obs_attribute <- assemble_core_table(
    con, "obs_attribute", root, id_col = "obs_attribute_id",
    order_by = paste("sample_key NULLS LAST, taxon_key NULLS LAST, life_stage NULLS LAST,",
                     "measurement_type, bin_value NULLS LAST, bin_label NULLS LAST"),
    parquet_dir = parquet_dir, exclude = exclude, id_blocks = blocks)
  out$sample_measurement <- assemble_core_table(
    con, "sample_measurement", root, id_col = "sample_measurement_id",
    order_by = "sample_key NULLS LAST, measurement_type",
    parquet_dir = parquet_dir, exclude = exclude, id_blocks = blocks)
  # a path per sample (4.35.0): only the datasets that have one write the shard
  out$sample_track <- assemble_core_table(
    con, "sample_track", root, order_by = "dataset_key, sample_key",
    parquet_dir = parquet_dir, exclude = exclude)
  if (out$sample_track > 0) {
    dup <- DBI::dbGetQuery(con,
      "SELECT sample_key FROM sample_track GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 5")$sample_key
    if (length(dup))
      stop("assemble_core(): a sample has more than one sample_track row, e.g. ",
           paste(dup, collapse = "; "))
  }
  # Supplemental full-resolution tables are DISCOVERED from the ingests, not
  # hardcoded: `obs_ctd_full` was the only one until `calcofi_mets` added
  # `obs_mets_full`, and a hardcoded name silently drops any new one from the
  # release while the ingest still writes it.
  for (t in supplemental_core_tables(root, supplemental))
    out[[t]] <- assemble_core_table(
      con, t, root, id_col = "obs_id",
      # cruise first: these tables publish one object per cruise, and a new latest cruise
      # then leaves every earlier cruise's ids, and so its object, as they were
      order_by = paste("cruise_key NULLS LAST, datetime NULLS LAST, sample_key NULLS LAST,",
                       "depth_min_m NULLS LAST, depth_max_m NULLS LAST, measurement_type"),
      parquet_dir = parquet_dir, exclude = exclude, id_blocks = blocks)

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


#' Check that every surrogate id sits in its dataset's block
#'
#' The invariant behind "an id changes only when its own dataset changes": in
#' each id-bearing table, a row's id is `id_block * CC_ID_BLOCK_SIZE + n` for
#' its own `dataset_key`, ids are unique, and a dataset's `n` run from 1 to its
#' row count with no gap. A table numbered any other way (one row number over
#' the whole table, as through v2026.11.25) fails here.
#'
#' @param con a DuckDB connection holding the tables
#' @param id_blocks the registry from [read_id_blocks()]
#' @param tables named character, table -> id column; a table absent from the
#'   connection, or without both `dataset_key` and its id column, is skipped.
#'   `sample_root` is checked against the smaller [CC_ROOT_ID_BLOCK_SIZE].
#' @return A data frame, one row per table x dataset: `table, dataset_key,
#'   id_block, n, id_min, id_max, n_distinct, ok, why` (`why` empty when `ok`).
#' @export
#' @concept shards
check_id_blocks <- function(con, id_blocks,
                            tables = c(obs = "obs_id", obs_attribute = "obs_attribute_id",
                                       sample_measurement = "sample_measurement_id",
                                       obs_ctd_full = "obs_id", obs_mets_full = "obs_id",
                                       sample_root = "root_id")) {
  have <- DBI::dbGetQuery(con, "SELECT table_name, column_name FROM information_schema.columns")
  out <- list()
  for (tb in names(tables)) {
    id <- tables[[tb]]
    cols <- have$column_name[have$table_name == tb]
    if (!all(c("dataset_key", id) %in% cols)) next
    d <- DBI::dbGetQuery(con, glue::glue(
      'SELECT dataset_key, count(*)::DOUBLE AS n, min("{id}")::DOUBLE AS id_min,
              max("{id}")::DOUBLE AS id_max, count(DISTINCT "{id}")::DOUBLE AS n_distinct
       FROM "{tb}" GROUP BY 1 ORDER BY 1'))
    if (!nrow(d)) next
    d$table <- tb
    d$id_block <- id_blocks$id_block[match(d$dataset_key, id_blocks$dataset_key)]
    base <- d$id_block * (if (identical(tb, "sample_root")) CC_ROOT_ID_BLOCK_SIZE else CC_ID_BLOCK_SIZE)
    d$why <- ifelse(is.na(d$dataset_key), "rows with no dataset_key",
             ifelse(is.na(d$id_block), "no block in metadata/id_block.csv",
             ifelse(d$n_distinct != d$n, "ids repeat",
             ifelse(d$id_min != base + 1, "ids do not start at the block's first number",
             ifelse(d$id_max != base + d$n, "ids are not the block's first n numbers", "")))))
    d$ok <- !nzchar(d$why)
    out[[tb]] <- d[, c("table", "dataset_key", "id_block", "n", "id_min", "id_max", "n_distinct", "ok", "why")]
  }
  res <- if (length(out)) do.call(rbind, out) else data.frame(
    table = character(), dataset_key = character(), id_block = numeric(), n = numeric(),
    id_min = numeric(), id_max = numeric(), n_distinct = numeric(), ok = logical(), why = character())
  rownames(res) <- NULL
  res
}
