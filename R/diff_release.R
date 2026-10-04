# diff an ingest's staged output against a published release, per measurement type ----
#
# the measurement-bounds rule "a data fix is diffed against the release before it
# is re-staged: per measurement_type, rows changed / filled / removed and the
# largest change, for every series the fix can touch". the per-row key of each
# measurement-bearing table is in DIFF_STAGE_KEYS; everything else is generic.

# the columns that identify one value in each measurement-bearing table. obs_id
# and sample_measurement_id are sequences reassigned at every staging run, so
# they are never part of the key; cruise_key / grid_key / hex_id are
# denormalized attributes of the sample, not identity.
DIFF_STAGE_KEYS <- list(
  obs                = c("dataset_key", "sample_key", "depth_min_m", "depth_max_m",
                         "taxon_key", "life_stage", "measurement_type"),
  obs_ctd_full       = c("dataset_key", "sample_key", "depth_min_m", "depth_max_m",
                         "taxon_key", "life_stage", "measurement_type"),
  sample_measurement = c("dataset_key", "sample_key", "measurement_type"))

# sql type of every key column (for the empty side of a table one side lacks)
DIFF_STAGE_KEY_TYPES <- c(
  dataset_key = "VARCHAR", sample_key = "VARCHAR", depth_min_m = "DOUBLE",
  depth_max_m = "DOUBLE", taxon_key = "VARCHAR", life_stage = "VARCHAR",
  measurement_type = "VARCHAR")

#' Diff an ingest's staged output against a published release
#'
#' The dry run behind the rule "a data fix is diffed against the release before it is
#' re-staged" (workflows `measurement-bounds` skill): for every measurement-bearing table an
#' ingest stages (`obs`, `obs_ctd_full`, `sample_measurement`), compares each value with the
#' same value in a release and counts, **per table x `measurement_type`**, the rows added,
#' removed, filled, blanked, changed and unchanged, the rows whose `measurement_qual` changed,
#' the largest absolute change and the duplicate keys on each side. Every measurement type
#' present on either side gets a row, and so does every type named in `measurement_type` that
#' is present on neither (all zeros), so a series cannot drop out of the breakdown unseen.
#'
#' **Key.** A value is identified by the columns in `DIFF_STAGE_KEYS`: for `obs` and
#' `obs_ctd_full` `dataset_key`, `sample_key`, `depth_min_m`, `depth_max_m`, `taxon_key`,
#' `life_stage` and `measurement_type` (compared with `IS NOT DISTINCT FROM`, so a NULL
#' `taxon_key` matches a NULL); for `sample_measurement` `dataset_key`, `sample_key` and
#' `measurement_type`. `obs_id` / `sample_measurement_id` are reassigned at every staging run
#' and are never compared.
#'
#' **Duplicates.** A key may hold more than one row (CalCOFI/workflows#131). The rows of one
#' key are numbered on each side in order of value then qual and paired by that number, so a
#' key with two rows in the release and one in the stage counts one pair plus one `removed`.
#' `n_dup_keys_release` / `n_dup_keys_stage` count the keys holding more than one row.
#'
#' **Value status** of a paired row (exactly one): `same` (both NULL, both NaN, equal, or
#' within `tolerance`), `changed` (both numbers, `|stage - release| > tolerance`), `filled`
#' (release NULL or NaN, stage a number), `blanked` (release a number, stage NULL or NaN) or
#' `nan_null` (NaN on one side, NULL on the other: `NaN` is not `NULL`). An unpaired row is
#' `added` (stage only) or `removed` (release only). `n_qual_changed` counts paired rows whose
#' `measurement_qual` differs (NULL-safe), independently of the value; `n_unchanged` is a
#' paired row with value `same` **and** the same qual. So `n_release = n_removed + paired` and
#' `n_stage = n_added + paired`, where `paired = n_same + n_changed + n_filled + n_blanked +
#' n_nan_null`.
#'
#' **Sources.** The stage side is `{stage_dir}/{table}.parquet` or the hive-partitioned
#' `{stage_dir}/{table}/` (default `cc_stage_path("parquet", dataset_key)`). The release side
#' is a local directory holding a release's parquet (same layout), a catalog list from
#' `calcofi4r::cc_catalog()`, or a version string (`"v2026.10.01"`, `"latest"`) resolved
#' through `calcofi4r::cc_catalog()` + `calcofi4r::cc_release_sources()`, never a
#' `releases/{v}/parquet/` path built by hand. The release's `obs` is its `obs_bio` +
#' `obs_env` pair (`value` read as `measurement_value`), falling back to a table named `obs`
#' only for a release without the pair; the release side is always filtered to
#' `dataset_key`.
#'
#' **Scale.** A table whose larger side exceeds `chunk_rows` is diffed in batches of whole
#' `cruise_key`s (partition files outside a batch are never opened), so a
#' 284-million-row `obs_ctd_full` runs inside a 3 GB DuckDB. A sample whose `cruise_key`
#' itself changed then shows as `removed` in one batch and `added` in another.
#'
#' @param dataset_key the dataset, e.g. `"calcofi_ctd-cast"`
#' @param release a local directory of release parquet, a catalog list (`calcofi4r::cc_catalog()`)
#'   or a version string resolved through `calcofi4r` (default `"latest"`)
#' @param stage_dir the ingest's stage directory (default `cc_stage_path("parquet", dataset_key)`)
#' @param tables measurement-bearing tables to diff (default all of `names(DIFF_STAGE_KEYS)`);
#'   a table on neither side is skipped, one on one side only is diffed against nothing
#' @param cruise_key optional character: restrict both sides to these cruises
#'   (`sample_measurement`, which has no `cruise_key`, through its samples' cruise in the
#'   stage `sample` / `obs` and the release `sample`)
#' @param measurement_type optional character: restrict both sides to these types
#' @param tolerance absolute tolerance below which two numbers count as `same` (default `1e-9`)
#' @param max_rows how many differing rows to keep in `attr(, "rows")`, largest change first
#'   (default 10000; 0 keeps none)
#' @param chunk_rows a table larger than this is diffed in batches of cruises of about this many
#'   rows (default 2e7)
#' @param con optional DuckDB connection; by default one from [get_duckdb_con()] with
#'   `memory_limit = "3GB"`, `threads = 2`, closed on exit
#' @param verbose print one line per table / batch (default `FALSE`)
#'
#' @return a tibble, one row per table x `measurement_type`, ordered by both: `table`,
#'   `measurement_type`, `n_release`, `n_stage`, `n_unchanged`, `n_added`, `n_removed`,
#'   `n_filled`, `n_blanked`, `n_nan_null`, `n_changed`, `n_qual_changed`, `max_abs_change`
#'   (largest `|stage - release|` among `changed`, `NA` when none), `n_dup_keys_release`,
#'   `n_dup_keys_stage`. Attributes: `rows`, a tibble of up to `max_rows` differing rows
#'   (`table`, the key columns, `cruise_key`, `status`, `value_release`, `value_stage`,
#'   `abs_change`, `qual_release`, `qual_stage`, `qual_changed`), and `elapsed` (seconds).
#'   [diff_stage_vs_release_rows()] returns the `rows` attribute.
#' @export
#' @concept release
#' @examples
#' \dontrun{
#' d <- diff_stage_vs_release(
#'   "calcofi_ctd-cast",
#'   release    = "~/_big/calcofi/releases/v2026.10.01/parquet",
#'   cruise_key = "2026-07-3322")
#' d
#' diff_stage_vs_release_rows(d)
#' }
#' @importFrom DBI dbGetQuery dbExecute
#' @importFrom glue glue
#' @importFrom tibble as_tibble tibble
#' @importFrom dplyr arrange bind_rows group_by summarise across all_of
#' @importFrom utils head URLdecode
#' @importFrom rlang .data
diff_stage_vs_release <- function(
    dataset_key,
    release          = "latest",
    stage_dir        = cc_stage_path("parquet", dataset_key),
    tables           = names(DIFF_STAGE_KEYS),
    cruise_key       = NULL,
    measurement_type = NULL,
    tolerance        = 1e-9,
    max_rows         = 10000,
    chunk_rows       = 2e7,
    con              = NULL,
    verbose          = FALSE) {

  stopifnot(
    is.character(dataset_key), length(dataset_key) == 1, nzchar(dataset_key),
    dir.exists(stage_dir),
    all(tables %in% names(DIFF_STAGE_KEYS)),
    is.null(cruise_key) || is.character(cruise_key),
    is.null(measurement_type) || is.character(measurement_type),
    is.numeric(tolerance), length(tolerance) == 1, tolerance >= 0,
    is.numeric(max_rows), max_rows >= 0,
    is.numeric(chunk_rows), chunk_rows > 0)
  t0 <- Sys.time()

  if (is.null(con)) {
    con <- get_duckdb_con(config = list(memory_limit = "3GB", threads = 2))
    on.exit(close_duckdb(con), add = TRUE)
  }

  rel <- .diff_release_resolver(release)
  out <- list(); rows <- list()

  for (tbl in tables) {
    s_src <- .diff_stage_sources(stage_dir, tbl)
    r_src <- rel(tbl)
    if (is.null(s_src) && is.null(r_src)) {
      if (verbose) cat(glue("{tbl}: on neither side, skipped"), "\n")
      next
    }
    res <- .diff_table(
      con, tbl, s_src, r_src, dataset_key, stage_dir, rel,
      cruise_key, measurement_type, tolerance, max_rows, chunk_rows, verbose)
    out[[tbl]]  <- res$summary
    rows[[tbl]] <- res$rows
  }

  d <- dplyr::bind_rows(out)
  if (!nrow(d)) d <- .diff_empty_summary()

  # a type the caller named but neither side holds still gets its (all-zero) row
  if (length(measurement_type) && length(out)) {
    for (tbl in names(out)) {
      miss <- setdiff(measurement_type, d$measurement_type[d$table == tbl])
      if (length(miss))
        d <- dplyr::bind_rows(d, .diff_zero_rows(tbl, miss))
    }
  }
  d <- dplyr::arrange(d, .data$table, .data$measurement_type)

  r <- dplyr::bind_rows(rows)
  if (nrow(r)) {
    r <- r[order(r$table, -ifelse(is.na(r$abs_change), -Inf, r$abs_change), r$measurement_type,
                 r$sample_key, r$depth_min_m, r$status, method = "radix"), , drop = FALSE]
    r <- utils::head(r, max_rows)
  }
  attr(d, "rows")    <- tibble::as_tibble(r)
  attr(d, "elapsed") <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  d
}

#' @rdname diff_stage_vs_release
#' @param d the result of [diff_stage_vs_release()]
#' @export
#' @concept release
diff_stage_vs_release_rows <- function(d) {
  r <- attr(d, "rows")
  if (is.null(r)) stop("not a diff_stage_vs_release() result: no `rows` attribute")
  r
}

# sources ----

# a source is list(files = <character>, hive = <logical>, value_col = <chr>, kind = <chr>)
# or NULL. obs on the release side is a list of sources (obs_bio + obs_env).

.diff_dir_source <- function(dir, tbl, value_col = "measurement_value") {
  f <- file.path(dir, paste0(tbl, ".parquet"))
  d <- file.path(dir, tbl)
  if (file.exists(f) && !dir.exists(f))
    return(list(files = normalizePath(f), hive = FALSE, value_col = value_col))
  if (dir.exists(d)) {
    files <- list.files(d, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)
    if (length(files))
      return(list(files = normalizePath(sort(files)), hive = TRUE, value_col = value_col))
  }
  NULL
}

.diff_stage_sources <- function(stage_dir, tbl) {
  s <- .diff_dir_source(stage_dir, tbl)
  if (is.null(s)) NULL else list(s)
}

# returns function(tbl) -> list of sources (or NULL); "sample" resolves too
.diff_release_resolver <- function(release) {
  if (is.character(release) && length(release) == 1 && dir.exists(release)) {
    dir <- release
    return(function(tbl) {
      if (tbl == "obs") {
        pair <- Filter(Negate(is.null), list(
          .diff_dir_source(dir, "obs_bio", "value"),
          .diff_dir_source(dir, "obs_env", "value")))
        if (length(pair)) return(pair)
      }
      s <- .diff_dir_source(dir, tbl)
      if (is.null(s)) NULL else list(s)
    })
  }

  # a catalog, or a version to read one for, through calcofi4r's resolvers
  if (!requireNamespace("calcofi4r", quietly = TRUE))
    stop("`release` is not a local directory; resolving a release catalog needs calcofi4r ",
         "(cc_catalog() + cc_release_sources())")
  catalog <- if (is.list(release)) release else calcofi4r::cc_catalog(release)
  has <- function(tbl) tryCatch({
    calcofi4r::cc_release_sources(catalog, tbl); TRUE }, error = function(e) FALSE)
  src <- function(tbl, value_col = "measurement_value") {
    s <- calcofi4r::cc_release_sources(catalog, tbl)
    list(files = as.character(s$urls), hive = isTRUE(s$hive), value_col = value_col)
  }
  function(tbl) {
    if (tbl == "obs") {
      pair <- list()
      for (t in c("obs_bio", "obs_env")) if (has(t)) pair[[t]] <- src(t, "value")
      if (length(pair)) return(unname(pair))
    }
    if (has(tbl)) list(src(tbl)) else NULL
  }
}

# keep only the files whose hive segment for `col` is in `vals` (a file without that
# segment is kept: the WHERE clause filters its rows)
.diff_prune <- function(files, col, vals) {
  if (is.null(vals)) return(files)
  seg <- regmatches(files, regexpr(paste0("(?<=[/]", col, "=)[^/]+"), files, perl = TRUE))
  has <- grepl(paste0("/", col, "="), files, fixed = TRUE)
  val <- rep(NA_character_, length(files))
  val[has] <- utils::URLdecode(seg)
  files[!has | val %in% vals]
}

.diff_read_sql <- function(src, dataset_key, cruise_key, measurement_type) {
  files <- src$files
  files <- .diff_prune(files, "dataset_key", dataset_key)
  files <- .diff_prune(files, "cruise_key", cruise_key)
  files <- .diff_prune(files, "measurement_type", measurement_type)
  if (!length(files)) return(NULL)
  lst <- paste0("[", paste0("'", gsub("'", "''", files), "'", collapse = ", "), "]")
  # hive values stay VARCHAR: a cruise_key or measurement_type is never a number or a date
  if (isTRUE(src$hive))
    glue("read_parquet({lst}, hive_partitioning = true, hive_types_autocast = false, union_by_name = true)")
  else
    glue("read_parquet({lst}, union_by_name = true)")
}

.diff_sql_in <- function(x) paste0("(", paste0("'", gsub("'", "''", x), "'", collapse = ", "), ")")

# the normalized side of one table: key columns, cruise_key, v, q
.diff_side_sql <- function(con, srcs, tbl, dataset_key, cruise_key, measurement_type,
                           sample_cruise_sql = NULL) {
  keys <- DIFF_STAGE_KEYS[[tbl]]
  empty <- paste0(
    "SELECT ", paste0("NULL::", DIFF_STAGE_KEY_TYPES[keys], " AS ", keys, collapse = ", "),
    ", NULL::VARCHAR AS cruise_key, NULL::DOUBLE AS v, NULL::VARCHAR AS q WHERE false")
  parts <- character()
  for (s in srcs) {
    rd <- .diff_read_sql(s, dataset_key, cruise_key, measurement_type)
    if (is.null(rd)) next
    cols <- DBI::dbGetQuery(con, glue("SELECT column_name FROM (DESCRIBE SELECT * FROM {rd})"))$column_name
    sel <- vapply(keys, function(k)
      if (k %in% cols) glue('"{k}"::{DIFF_STAGE_KEY_TYPES[[k]]} AS "{k}"')
      else glue('NULL::{DIFF_STAGE_KEY_TYPES[[k]]} AS "{k}"'), "")
    has_cruise <- "cruise_key" %in% cols
    sel <- c(sel,
      if (has_cruise) "cruise_key::VARCHAR AS cruise_key" else "NULL::VARCHAR AS cruise_key",
      glue('"{s$value_col}"::DOUBLE AS v'),
      if ("measurement_qual" %in% cols) "measurement_qual::VARCHAR AS q" else "NULL::VARCHAR AS q")
    wh <- glue("dataset_key = '{gsub(\"'\", \"''\", dataset_key)}'")
    if (length(measurement_type))
      wh <- c(wh, glue("measurement_type IN {.diff_sql_in(measurement_type)}"))
    if (length(cruise_key)) {
      if (has_cruise) {
        wh <- c(wh, glue("cruise_key IN {.diff_sql_in(cruise_key)}"))
      } else if (!is.null(sample_cruise_sql)) {
        wh <- c(wh, glue(
          "sample_key IN (SELECT sample_key FROM ({sample_cruise_sql}) ",
          "WHERE cruise_key IN {.diff_sql_in(cruise_key)})"))
      } else {
        wh <- c(wh, "false")
      }
    }
    parts <- c(parts, glue(
      "SELECT {paste(sel, collapse = ', ')} FROM {rd} WHERE {paste(wh, collapse = ' AND ')}"))
  }
  if (!length(parts)) return(empty)
  paste(parts, collapse = "\nUNION ALL\n")
}

# sample_key -> cruise_key from every side that knows it (for sample_measurement's cruise filter)
.diff_sample_cruise_sql <- function(stage_dir, rel, cruise_key) {
  srcs <- c(
    .diff_stage_sources(stage_dir, "sample"),
    .diff_stage_sources(stage_dir, "obs"),
    rel("sample"))
  parts <- character()
  for (s in srcs) {
    rd <- .diff_read_sql(s, NULL, cruise_key, NULL)
    if (!is.null(rd))
      parts <- c(parts, glue("SELECT sample_key::VARCHAR AS sample_key, cruise_key::VARCHAR AS cruise_key FROM {rd}"))
  }
  if (!length(parts)) return(NULL)
  paste0("SELECT DISTINCT sample_key, cruise_key FROM (", paste(parts, collapse = " UNION ALL "), ")")
}

# one table ----

.diff_table <- function(con, tbl, s_src, r_src, dataset_key, stage_dir, rel,
                        cruise_key, measurement_type, tolerance, max_rows, chunk_rows, verbose) {
  keys <- DIFF_STAGE_KEYS[[tbl]]
  sc_sql <- if (length(cruise_key) && tbl == "sample_measurement")
    .diff_sample_cruise_sql(stage_dir, rel, cruise_key) else NULL

  side <- function(srcs, ck) .diff_side_sql(
    con, srcs, tbl, dataset_key, ck, measurement_type, sc_sql)

  # plan batches of whole cruises when the larger side is big
  batches <- list(cruise_key)
  if (tbl != "sample_measurement") {
    cnt <- DBI::dbGetQuery(con, glue(
      "SELECT cruise_key, max(n) AS n FROM (
         SELECT 's' AS side, cruise_key, count(*) AS n FROM ({side(s_src, cruise_key)}) GROUP BY ALL
         UNION ALL
         SELECT 'r' AS side, cruise_key, count(*) AS n FROM ({side(r_src, cruise_key)}) GROUP BY ALL)
       GROUP BY cruise_key ORDER BY cruise_key NULLS LAST"))
    if (sum(cnt$n) > chunk_rows) {
      batches <- list(); cur <- character(); acc <- 0
      for (i in seq_len(nrow(cnt))) {
        if (length(cur) && acc + cnt$n[i] > chunk_rows) {
          batches[[length(batches) + 1]] <- cur; cur <- character(); acc <- 0 }
        cur <- c(cur, if (is.na(cnt$cruise_key[i])) NA_character_ else cnt$cruise_key[i])
        acc <- acc + cnt$n[i]
      }
      if (length(cur)) batches[[length(batches) + 1]] <- cur
    }
  }

  kj   <- paste0('r."', keys, '" IS NOT DISTINCT FROM s."', keys, '"', collapse = " AND ")
  part <- paste0('"', keys, '"', collapse = ", ")
  kcol <- paste0('coalesce(r."', keys, '", s."', keys, '") AS "', keys, '"', collapse = ", ")

  sums <- list(); rows <- list()
  for (b in seq_along(batches)) {
    ck <- batches[[b]]
    # a batch is whole cruises: NA in a batch means the NULL-cruise rows
    if (!is.null(ck) && anyNA(ck)) {
      ck_ok <- ck[!is.na(ck)]
      s_sql <- .diff_or_null_cruise(side(s_src, NULL), ck_ok)
      r_sql <- .diff_or_null_cruise(side(r_src, NULL), ck_ok)
    } else {
      s_sql <- side(s_src, ck)
      r_sql <- side(r_src, ck)
    }
    t0 <- Sys.time()
    DBI::dbExecute(con, glue(
      "CREATE OR REPLACE TEMP TABLE _diff_chunk AS
       WITH
       r AS (SELECT *, row_number() OVER (PARTITION BY {part} ORDER BY v NULLS LAST, q NULLS LAST) AS rn,
                       count(*) OVER (PARTITION BY {part}) AS nk FROM ({r_sql})),
       s AS (SELECT *, row_number() OVER (PARTITION BY {part} ORDER BY v NULLS LAST, q NULLS LAST) AS rn,
                       count(*) OVER (PARTITION BY {part}) AS nk FROM ({s_sql})),
       j AS (
         SELECT {kcol}, coalesce(r.cruise_key, s.cruise_key) AS cruise_key,
           r.rn IS NOT NULL AS in_r, s.rn IS NOT NULL AS in_s,
           r.v AS value_release, s.v AS value_stage, r.q AS qual_release, s.q AS qual_stage,
           coalesce(r.rn = 1 AND r.nk > 1, false) AS dup_r,
           coalesce(s.rn = 1 AND s.nk > 1, false) AS dup_s
         FROM r FULL OUTER JOIN s ON {kj} AND r.rn = s.rn)
       SELECT *,
         CASE
           WHEN NOT in_r THEN 'added'
           WHEN NOT in_s THEN 'removed'
           WHEN value_release IS NULL AND value_stage IS NULL THEN 'same'
           WHEN value_release IS NULL AND isnan(value_stage) THEN 'nan_null'
           WHEN value_stage IS NULL AND isnan(value_release) THEN 'nan_null'
           WHEN isnan(value_release) AND isnan(value_stage) THEN 'same'
           WHEN value_release IS NULL OR isnan(value_release) THEN 'filled'
           WHEN value_stage IS NULL OR isnan(value_stage) THEN 'blanked'
           WHEN value_release = value_stage THEN 'same'
           WHEN abs(value_stage - value_release) <= {tolerance} THEN 'same'
           ELSE 'changed' END AS status,
         in_r AND in_s AND qual_release IS DISTINCT FROM qual_stage AS qual_changed,
         CASE WHEN in_r AND in_s AND NOT isnan(value_release) AND NOT isnan(value_stage)
              THEN abs(value_stage - value_release) END AS abs_change
       FROM j"))
    sums[[b]] <- DBI::dbGetQuery(con,
      "SELECT measurement_type,
         count(*) FILTER (WHERE in_r)                           AS n_release,
         count(*) FILTER (WHERE in_s)                           AS n_stage,
         count(*) FILTER (WHERE status = 'same' AND NOT qual_changed) AS n_unchanged,
         count(*) FILTER (WHERE status = 'added')               AS n_added,
         count(*) FILTER (WHERE status = 'removed')             AS n_removed,
         count(*) FILTER (WHERE status = 'filled')              AS n_filled,
         count(*) FILTER (WHERE status = 'blanked')             AS n_blanked,
         count(*) FILTER (WHERE status = 'nan_null')            AS n_nan_null,
         count(*) FILTER (WHERE status = 'changed')             AS n_changed,
         count(*) FILTER (WHERE qual_changed)                   AS n_qual_changed,
         max(abs_change) FILTER (WHERE status = 'changed')      AS max_abs_change,
         count(*) FILTER (WHERE dup_r)                          AS n_dup_keys_release,
         count(*) FILTER (WHERE dup_s)                          AS n_dup_keys_stage
       FROM _diff_chunk GROUP BY measurement_type ORDER BY measurement_type")
    if (max_rows > 0) {
      sel <- c(keys, "cruise_key", "status", "value_release", "value_stage", "abs_change",
               "qual_release", "qual_stage", "qual_changed")
      rows[[b]] <- DBI::dbGetQuery(con, glue(
        "SELECT {paste0('\"', sel, '\"', collapse = ', ')} FROM _diff_chunk
         WHERE NOT (status = 'same' AND NOT qual_changed)
         ORDER BY abs_change DESC NULLS LAST, {part}, status, value_release NULLS LAST,
                  value_stage NULLS LAST, qual_release NULLS LAST, qual_stage NULLS LAST
         LIMIT {as.integer(max_rows)}"))
    }
    DBI::dbExecute(con, "DROP TABLE IF EXISTS _diff_chunk")
    if (verbose) cat(glue(
      "{tbl}: batch {b}/{length(batches)} ({length(ck)} cruise(s)) in ",
      "{round(as.numeric(difftime(Sys.time(), t0, units = 'secs')), 1)} s"), "\n")
  }

  s <- dplyr::bind_rows(sums)
  cnt_cols <- setdiff(names(s), c("measurement_type", "max_abs_change"))
  s <- s |>
    dplyr::group_by(.data$measurement_type) |>
    dplyr::summarise(
      dplyr::across(dplyr::all_of(cnt_cols), ~ as.numeric(sum(.x))),
      max_abs_change = suppressWarnings(max(.data$max_abs_change, na.rm = TRUE)),
      .groups = "drop")
  s$max_abs_change[!is.finite(s$max_abs_change)] <- NA_real_
  s$table <- rep(tbl, nrow(s))
  s <- tibble::as_tibble(s)[, names(.diff_empty_summary())]

  r <- dplyr::bind_rows(rows)
  if (nrow(r)) {
    for (k in setdiff(c("depth_min_m", "depth_max_m"), names(r))) r[[k]] <- NA_real_
    for (k in setdiff(c("taxon_key", "life_stage"), names(r))) r[[k]] <- NA_character_
    r <- cbind(table = tbl, r)
  }
  list(summary = s, rows = r)
}

.diff_or_null_cruise <- function(side_sql, ck_ok) {
  cond <- if (length(ck_ok)) glue("cruise_key IN {.diff_sql_in(ck_ok)} OR cruise_key IS NULL") else
    "cruise_key IS NULL"
  glue("SELECT * FROM ({side_sql}) WHERE {cond}")
}

.diff_empty_summary <- function() {
  tibble::tibble(
    table = character(), measurement_type = character(),
    n_release = numeric(), n_stage = numeric(), n_unchanged = numeric(),
    n_added = numeric(), n_removed = numeric(), n_filled = numeric(),
    n_blanked = numeric(), n_nan_null = numeric(), n_changed = numeric(),
    n_qual_changed = numeric(), max_abs_change = numeric(),
    n_dup_keys_release = numeric(), n_dup_keys_stage = numeric())
}

.diff_zero_rows <- function(tbl, types) {
  z <- .diff_empty_summary()[rep(NA_integer_, length(types)), ]
  z$table <- tbl
  z$measurement_type <- types
  num <- setdiff(names(z), c("table", "measurement_type", "max_abs_change"))
  for (k in num) z[[k]] <- 0
  z$max_abs_change <- NA_real_
  z
}
