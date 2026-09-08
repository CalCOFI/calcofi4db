# release relationship integrity ----------------------------------------------

#' Measure every declared primary and foreign key on the frozen tables
#'
#' The release has always *declared* its keys (`relationships.json`, from
#' [core_relationships()] and `metadata/relationships_cross.csv`) and gated a
#' few of them — [check_core_pk_unique()] on the core primary keys,
#' [check_cruise_key_integrity()] on the `cruise_key` edges — while the other
#' ~50 declared foreign keys were true by construction and never measured. This
#' walks the whole declaration on the assembled tables and writes the result
#' as the `integrity.json` sidecar beside `catalog.json`, so a consumer (the
#' schema browser, the docs) can show "unique, measured" and "0 orphans" per
#' key instead of trusting the diagram.
#'
#' For each primary key: rows, distinct key values, duplicates, rows with a
#' `NULL` in any key column. For each foreign key: rows, rows whose key is
#' `NULL` (permitted — a nullable edge is one where the source has nothing to
#' point at, e.g. `obs.taxon_key` on an env row), and **orphans**: non-`NULL`
#' values with no match in the referenced column. A key whose table or column
#' is not in `con` is reported as `skipped`, never silently dropped.
#'
#' @param con DBI connection holding the frozen tables.
#' @param rels Path to a `relationships.json`, or the list it parses to
#'   (`primary_keys`, `foreign_keys`).
#' @param path Optional path to write `integrity.json` to.
#' @param version Release version stamped into the sidecar (no wall clock: the
#'   file is deterministic for unchanged inputs, like the release stamp).
#' @param halt `stop()` on any duplicate or `NULL` primary key, or any foreign-key
#'   orphan (default `TRUE`). A skipped key never halts.
#' @return Invisibly, a list: `primary_keys` and `foreign_keys` data frames,
#'   `ok`, and `n_skipped`.
#' @export
#' @concept validation
#' @importFrom DBI dbListTables dbListFields dbGetQuery
#' @importFrom glue glue
check_release_relationships <- function(con, rels, path = NULL, version = NULL, halt = TRUE) {
  if (is.character(rels)) rels <- jsonlite::fromJSON(rels, simplifyVector = FALSE)
  stopifnot(is.list(rels), !is.null(rels$primary_keys) || !is.null(rels$foreign_keys))
  tbls <- DBI::dbListTables(con)
  q <- function(x) paste0('"', x, '"')

  # primary keys ----
  pk_rows <- lapply(names(rels$primary_keys), function(tb) {
    cols <- unlist(rels$primary_keys[[tb]])
    base <- data.frame(table = tb, columns = paste(cols, collapse = ","),
                       n_rows = NA_real_, n_distinct = NA_real_, n_dup = NA_real_, n_null = NA_real_,
                       status = "skipped", stringsAsFactors = FALSE)
    if (!tb %in% tbls) return(base)
    have <- DBI::dbListFields(con, tb)
    if (!all(cols %in% have)) return(base)
    # a NULL anywhere in the key is counted once, as n_null, and kept out of the
    # duplicate count (COUNT(DISTINCT) drops NULLs, which would make every NULL
    # row look like a duplicate); a composite key is compared as a tuple
    any_null <- paste(paste(q(cols), "IS NULL"), collapse = " OR ")
    tuple    <- paste(q(cols), collapse = ", ")
    r <- DBI::dbGetQuery(con, glue::glue(
      'SELECT COUNT(*) AS n_rows,
              COUNT(DISTINCT ({tuple})) FILTER (WHERE NOT ({any_null})) AS n_distinct,
              COUNT(*) FILTER (WHERE NOT ({any_null})) - COUNT(DISTINCT ({tuple})) FILTER (WHERE NOT ({any_null})) AS n_dup,
              COUNT(*) FILTER (WHERE {any_null}) AS n_null
       FROM {q(tb)}'))
    base$n_rows <- as.numeric(r$n_rows); base$n_distinct <- as.numeric(r$n_distinct)
    base$n_dup  <- as.numeric(r$n_dup);  base$n_null <- as.numeric(ifelse(is.na(r$n_null), 0, r$n_null))
    base$status <- if (base$n_dup == 0 && base$n_null == 0) "ok" else "fail"
    base
  })
  pk <- do.call(rbind, c(pk_rows, list(stringsAsFactors = FALSE)))
  if (is.null(pk)) pk <- data.frame(table = character(), columns = character(), n_rows = numeric(),
                                    n_distinct = numeric(), n_dup = numeric(), n_null = numeric(),
                                    status = character(), stringsAsFactors = FALSE)

  # foreign keys ----
  fk_rows <- lapply(rels$foreign_keys, function(fk) {
    base <- data.frame(table = fk$table, column = fk$column,
                       ref_table = fk$ref_table, ref_column = fk$ref_column,
                       n_rows = NA_real_, n_null = NA_real_, n_orphan = NA_real_,
                       status = "skipped", stringsAsFactors = FALSE)
    if (!(fk$table %in% tbls) || !(fk$ref_table %in% tbls)) return(base)
    if (!(fk$column %in% DBI::dbListFields(con, fk$table)) ||
        !(fk$ref_column %in% DBI::dbListFields(con, fk$ref_table))) return(base)
    r <- DBI::dbGetQuery(con, glue::glue(
      'WITH ref AS (SELECT DISTINCT {q(fk$ref_column)} AS k FROM {q(fk$ref_table)} WHERE {q(fk$ref_column)} IS NOT NULL)
       SELECT COUNT(*) AS n_rows,
              SUM(CASE WHEN t.{q(fk$column)} IS NULL THEN 1 ELSE 0 END) AS n_null,
              SUM(CASE WHEN t.{q(fk$column)} IS NOT NULL AND ref.k IS NULL THEN 1 ELSE 0 END) AS n_orphan
       FROM {q(fk$table)} t LEFT JOIN ref ON t.{q(fk$column)} = ref.k'))
    base$n_rows   <- as.numeric(r$n_rows)
    base$n_null   <- as.numeric(ifelse(is.na(r$n_null), 0, r$n_null))
    base$n_orphan <- as.numeric(ifelse(is.na(r$n_orphan), 0, r$n_orphan))
    base$status   <- if (base$n_orphan == 0) "ok" else "fail"
    base
  })
  fk <- do.call(rbind, c(fk_rows, list(stringsAsFactors = FALSE)))
  if (is.null(fk)) fk <- data.frame(table = character(), column = character(), ref_table = character(),
                                    ref_column = character(), n_rows = numeric(), n_null = numeric(),
                                    n_orphan = numeric(), status = character(), stringsAsFactors = FALSE)

  n_skipped <- sum(pk$status == "skipped") + sum(fk$status == "skipped")
  ok <- !any(pk$status == "fail") && !any(fk$status == "fail")
  out <- list(primary_keys = pk, foreign_keys = fk, ok = ok, n_skipped = n_skipped)

  if (!is.null(path)) {
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    doc <- list(
      schema_version = "1.0",
      version        = version,
      ok             = ok,
      n_primary_keys = sum(pk$status != "skipped"),
      n_foreign_keys = sum(fk$status != "skipped"),
      n_skipped      = n_skipped,
      primary_keys   = pk,
      foreign_keys   = fk)
    jsonlite::write_json(doc, path, auto_unbox = TRUE, pretty = TRUE, digits = NA, null = "null", na = "null")
  }

  if (halt && !ok) {
    bad_pk <- pk[pk$status == "fail", , drop = FALSE]
    bad_fk <- fk[fk$status == "fail", , drop = FALSE]
    stop("release relationships failed: ",
         paste(c(
           sprintf("%s(%s): %g duplicate, %g NULL", bad_pk$table, bad_pk$columns, bad_pk$n_dup, bad_pk$n_null),
           sprintf("%s.%s -> %s.%s: %g orphan(s)", bad_fk$table, bad_fk$column, bad_fk$ref_table,
                   bad_fk$ref_column, bad_fk$n_orphan)),
           collapse = "; "),
         call. = FALSE)
  }
  invisible(out)
}
