# cruise_key resolution --------------------------------------------------------
#
# `cruise_key` is `YYYY-MM-NODC`, and the YYYY-MM is the cruise's DESIGNATED
# month — the one SWFSC assigns to the whole cruise (`cruise.date_ym` in the
# ichthyo source, `Cruise` = YYYYMM in the bottle database) — not the month a
# given cast or tow happened to fall in. Cruises routinely straddle a calendar
# boundary (5508BD ran 7 Aug – 25 Sep 1955; 184 of the 664 bottle cruises span
# two months), so a key built from each event's own timestamp shears one cruise
# into two, and when the neighbouring month is itself a real cruise of the same
# ship the shorn-off casts land on it without any FK ever failing. That is how
# 664 source cruises became 799 keys in v2026.08.14 and 5,941 casts carried a
# key that disagreed with their own source.
#
# Two sources can also disagree about the designation: the ichthyo reference
# calls the 9 Feb – 29 Mar 1984 Jordan cruise 8403, the bottle database calls it
# 8402. The reference wins, because every dataset joins to it. So resolution
# goes, in order:
#   1. the reference cruise of the same ship whose observed DATE SPAN (± a few
#      days) contains the event — unambiguous, because no two cruises of one
#      ship overlap in time (0 overlapping pairs across 691 reference cruises);
#   2. the source's own designation (`cruise_ym_col`, YYYYMM) when it carries
#      one — covers cruises the reference has no sites for;
#   3. the event's month — the old rule, kept only as the last resort.
# [add_cruise_date_span()] puts the spans on the reference table in the ingest
# that builds it; [resolve_cruise_key()] applies the ladder anywhere else.

#' Add the observed date span of each cruise to the cruise reference
#'
#' Computes `date_min` / `date_max` (DATE) per `cruise_key` from an event query
#' and writes them onto `cruise_tbl`, so downstream ingests can match events to
#' cruises by containment rather than by calendar month (see
#' [resolve_cruise_key()]). Run this in the ingest that owns the `cruise`
#' reference (`ingest_swfsc_ichthyo.qmd`), after `cruise_key` has been propagated
#' to the event tables; every other ingest loads that shard read-only.
#'
#' @param con DBI connection to DuckDB.
#' @param event_sql A `SELECT` returning two columns named `cruise_key` and
#'   `datetime` — one row per event (site, tow, cast, ...). Rows with a NULL in
#'   either are ignored.
#' @param cruise_tbl Name of the cruise reference table (default `"cruise"`).
#' @return Invisibly, a tibble with one row per cruise: `cruise_key`,
#'   `date_ym` (if present), `date_min`, `date_max`, `n_events`, `spills_month`
#'   (span extends outside the designated month) and `overlaps` (the span
#'   intersects another cruise of the same ship — a reference-data error that
#'   would make span matching ambiguous; assert `sum(overlaps) == 0`).
#' @export
#' @concept ship
#' @importFrom DBI dbExecute dbGetQuery dbListFields dbListTables
#' @importFrom glue glue
add_cruise_date_span <- function(con, event_sql, cruise_tbl = "cruise") {
  stopifnot(
    "cruise table required" = cruise_tbl %in% DBI::dbListTables(con),
    "event_sql must be a single SQL string" =
      is.character(event_sql) && length(event_sql) == 1)
  ct <- DBI::dbQuoteIdentifier(con, cruise_tbl)
  cf <- DBI::dbListFields(con, cruise_tbl)
  stopifnot("cruise table needs cruise_key" = "cruise_key" %in% cf)

  DBI::dbExecute(con, "DROP TABLE IF EXISTS _cruise_span")
  DBI::dbExecute(con, glue::glue("
    CREATE TEMP TABLE _cruise_span AS
    SELECT cruise_key,
           MIN(CAST(datetime AS DATE)) AS date_min,
           MAX(CAST(datetime AS DATE)) AS date_max,
           COUNT(*)                    AS n_events
    FROM ({event_sql}) e
    WHERE cruise_key IS NOT NULL AND datetime IS NOT NULL
    GROUP BY 1"))

  for (cl in c("date_min", "date_max"))
    DBI::dbExecute(con, glue::glue(
      "ALTER TABLE {ct} ADD COLUMN IF NOT EXISTS {cl} DATE"))
  DBI::dbExecute(con, glue::glue("
    UPDATE {ct} SET date_min = s.date_min, date_max = s.date_max
    FROM _cruise_span s WHERE {ct}.cruise_key = s.cruise_key"))

  ym_expr <- if ("date_ym" %in% cf) "c.date_ym" else "NULL::DATE"
  ship_join <- if ("ship_key" %in% cf) "c.ship_key" else "NULL"
  out <- DBI::dbGetQuery(con, glue::glue("
    WITH c AS (
      SELECT c.cruise_key, {ym_expr} AS date_ym, {ship_join} AS ship_key,
             c.date_min, c.date_max, COALESCE(s.n_events, 0) AS n_events
      FROM {ct} c LEFT JOIN _cruise_span s USING (cruise_key))
    SELECT a.cruise_key, a.date_ym, a.date_min, a.date_max, a.n_events,
           a.date_min IS NOT NULL AND a.date_ym IS NOT NULL AND
             (strftime(a.date_min, '%Y-%m') <> strftime(a.date_ym, '%Y-%m') OR
              strftime(a.date_max, '%Y-%m') <> strftime(a.date_ym, '%Y-%m'))
             AS spills_month,
           EXISTS (SELECT 1 FROM c b
                   WHERE b.ship_key = a.ship_key AND b.cruise_key <> a.cruise_key
                     AND a.date_min IS NOT NULL AND b.date_min IS NOT NULL
                     AND a.date_max >= b.date_min AND b.date_max >= a.date_min)
             AS overlaps
    FROM c a ORDER BY a.cruise_key"))
  DBI::dbExecute(con, "DROP TABLE IF EXISTS _cruise_span")

  message(glue::glue(
    "cruise date span: {sum(!is.na(out$date_min))}/{nrow(out)} cruises spanned, ",
    "{sum(out$spills_month, na.rm = TRUE)} extend outside their designated month, ",
    "{sum(out$overlaps, na.rm = TRUE)} overlap another cruise of the same ship"))
  invisible(tibble::as_tibble(out))
}

#' Resolve `cruise_key` on an event table by span, designation, then month
#'
#' Writes `cruise_key` (and `cruise_key_method`) onto `table_name` for every row
#' with a matched `ship_key`, trying in order:
#' \enumerate{
#'   \item \strong{span} — the reference cruise of the same ship whose
#'     `date_min - tolerance_days .. date_max + tolerance_days` contains the
#'     event (nearest span centre on the rare tie);
#'   \item \strong{source} — `cruise_ym_col`, the source's own YYYYMM
#'     designation, when supplied and well-formed;
#'   \item \strong{month} — the event's own year-month (the legacy rule).
#' }
#' Every key is `YYYY-MM-` + the ship's NODC code from `ship_tbl`. Steps 2–3
#' (which mint a key rather than copy one from `cruise_tbl`) require
#' `ship_nodc` to be non-NULL/non-blank — a blank NODC (DuckDB's `CONCAT()`
#' treats NULL as `''`) used to mint `YYYY-MM-` silently (WS-B / the July 2019
#' Bold Horizon cruise, `cruise_key = "2019-07-"`); those rows now stay
#' unresolved (`cruise_key` NULL, method NULL) rather than shipping a malformed
#' key.
#'
#' @param con DBI connection to DuckDB holding `table_name`, `cruise_tbl` (with
#'   `date_min`/`date_max` from [add_cruise_date_span()]) and `ship_tbl`.
#' @param table_name Event table to annotate.
#' @param datetime_col Timestamp/date column on the event table.
#' @param ship_key_col Column holding the matched `ship_key` (default
#'   `"ship_key"`); rows with NULL get no key.
#' @param cruise_ym_col Optional column carrying the source's cruise designation
#'   as YYYYMM (e.g. the bottle database's `Cruise`). Values not matching
#'   `^\\d{4}(0[1-9]|1[0-2])$` are ignored for that row.
#' @param cruise_tbl,ship_tbl Reference table names.
#' @param tolerance_days Days added to each end of a cruise's observed span
#'   before testing containment (default 3 — a hydrocast can precede the first
#'   plankton tow by a day or two).
#' @param require_in_cruise If TRUE, keys from steps 2–3 that do not exist in
#'   `cruise_tbl` are left NULL (use for datasets that only join to known
#'   cruises); step-1 keys always exist by construction.
#' @param method_col Name of the column recording which step resolved each row
#'   (`"span"`, `"source"`, `"month"`, or NULL). Set to `NULL` to not record it.
#' @return A tibble with one row per method: `method`, `n`, `n_in_cruise`.
#' @export
#' @concept ship
#' @importFrom DBI dbExecute dbGetQuery dbListFields dbListTables dbQuoteIdentifier
#' @importFrom glue glue
resolve_cruise_key <- function(con,
                               table_name,
                               datetime_col,
                               ship_key_col      = "ship_key",
                               cruise_ym_col     = NULL,
                               cruise_tbl        = "cruise",
                               ship_tbl          = "ship",
                               tolerance_days    = 3L,
                               require_in_cruise = FALSE,
                               method_col        = "cruise_key_method") {
  tbls <- DBI::dbListTables(con)
  stopifnot(
    "target table required" = table_name %in% tbls,
    "cruise table required" = cruise_tbl %in% tbls,
    "ship table required"   = ship_tbl   %in% tbls)
  flds <- DBI::dbListFields(con, table_name)
  stopifnot(
    "target table needs the datetime_col" = datetime_col %in% flds,
    "target table needs the ship_key_col" = ship_key_col %in% flds,
    "target table needs the cruise_ym_col" =
      is.null(cruise_ym_col) || cruise_ym_col %in% flds)
  cf <- DBI::dbListFields(con, cruise_tbl)
  if (!all(c("date_min", "date_max") %in% cf))
    stop("`", cruise_tbl, "` has no date_min/date_max columns, so events cannot ",
         "be matched to cruises by date span. Run add_cruise_date_span() in the ",
         "ingest that builds the cruise reference (ingest_swfsc_ichthyo.qmd) and ",
         "re-run it so the shard downstream ingests load carries the span.",
         call. = FALSE)
  stopifnot("ship table needs ship_key + ship_nodc" =
              all(c("ship_key", "ship_nodc") %in% DBI::dbListFields(con, ship_tbl)))

  tbl <- DBI::dbQuoteIdentifier(con, table_name)
  ct  <- DBI::dbQuoteIdentifier(con, cruise_tbl)
  st  <- DBI::dbQuoteIdentifier(con, ship_tbl)
  dt  <- DBI::dbQuoteIdentifier(con, datetime_col)
  sk  <- DBI::dbQuoteIdentifier(con, ship_key_col)
  tol <- as.integer(tolerance_days)
  mc  <- if (is.null(method_col)) NULL else DBI::dbQuoteIdentifier(con, method_col)

  DBI::dbExecute(con, glue::glue(
    "ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS cruise_key TEXT"))
  DBI::dbExecute(con, glue::glue("UPDATE {tbl} SET cruise_key = NULL"))
  if (!is.null(mc)) {
    DBI::dbExecute(con, glue::glue(
      "ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {mc} TEXT"))
    DBI::dbExecute(con, glue::glue("UPDATE {tbl} SET {mc} = NULL"))
  }
  stamp <- function(method) {
    if (is.null(mc)) return(invisible())
    DBI::dbExecute(con, glue::glue(
      "UPDATE {tbl} SET {mc} = '{method}' WHERE cruise_key IS NOT NULL AND {mc} IS NULL"))
  }
  in_cruise <- if (isTRUE(require_in_cruise))
    glue::glue(" AND cruise_key IN (SELECT cruise_key FROM {ct})") else ""

  # step 1: span containment ----
  DBI::dbExecute(con, glue::glue("
    UPDATE {tbl} SET cruise_key = (
      SELECT cr.cruise_key FROM {ct} cr
      WHERE cr.ship_key = {tbl}.{sk}
        AND cr.date_min IS NOT NULL AND cr.date_max IS NOT NULL
        AND CAST({tbl}.{dt} AS DATE)
            BETWEEN cr.date_min - to_days({tol}) AND cr.date_max + to_days({tol})
      ORDER BY GREATEST(DATE_DIFF('day', CAST({tbl}.{dt} AS DATE), cr.date_min),
                        DATE_DIFF('day', cr.date_max, CAST({tbl}.{dt} AS DATE)), 0),
               cr.cruise_key
      LIMIT 1)
    WHERE {sk} IS NOT NULL AND {dt} IS NOT NULL"))
  stamp("span")

  # step 2: the source's own designation ----
  if (!is.null(cruise_ym_col)) {
    ym <- DBI::dbQuoteIdentifier(con, cruise_ym_col)
    # the designation may arrive as VARCHAR, INTEGER or — when a CSV reader typed
    # an all-digit column as numeric — DOUBLE, whose VARCHAR form is '195508.0';
    # normalise before matching so a type choice upstream cannot silently turn
    # every row into a month-rule fallback
    ym_norm <- glue::glue(
      "regexp_replace(trim(CAST(t.{ym} AS VARCHAR)), '\\.0+$', '')")
    DBI::dbExecute(con, glue::glue("
      UPDATE {tbl} SET cruise_key = k.key
      FROM (
        SELECT t.rowid AS rid,
               CONCAT(SUBSTR({ym_norm}, 1, 4), '-', SUBSTR({ym_norm}, 5, 2), '-',
                      s.ship_nodc) AS key
        FROM {tbl} t JOIN {st} s ON s.ship_key = t.{sk}
        WHERE t.cruise_key IS NULL
          AND s.ship_nodc IS NOT NULL AND s.ship_nodc <> ''
          AND regexp_matches({ym_norm}, '^\\d{{4}}(0[1-9]|1[0-2])$')) k
      WHERE {tbl}.rowid = k.rid AND {tbl}.cruise_key IS NULL
        {sub('cruise_key IN', 'k.key IN', in_cruise)}"))
    stamp("source")
  }

  # step 3: the event's own month (legacy rule) ----
  DBI::dbExecute(con, glue::glue("
    UPDATE {tbl} SET cruise_key = k.key
    FROM (
      SELECT t.rowid AS rid,
             CONCAT(strftime(CAST(t.{dt} AS DATE), '%Y-%m'), '-', s.ship_nodc) AS key
      FROM {tbl} t JOIN {st} s ON s.ship_key = t.{sk}
      WHERE t.cruise_key IS NULL AND t.{dt} IS NOT NULL
        AND s.ship_nodc IS NOT NULL AND s.ship_nodc <> '') k
    WHERE {tbl}.rowid = k.rid AND {tbl}.cruise_key IS NULL
      {sub('cruise_key IN', 'k.key IN', in_cruise)}"))
  stamp("month")

  # stats ----
  method_expr <- if (is.null(mc))
    "CASE WHEN cruise_key IS NULL THEN 'none' ELSE 'resolved' END" else
    glue::glue("COALESCE({mc}, 'none')")
  out <- DBI::dbGetQuery(con, glue::glue("
    SELECT {method_expr} AS method, COUNT(*) AS n,
           COUNT(*) FILTER (WHERE cruise_key IN (SELECT cruise_key FROM {ct})) AS n_in_cruise
    FROM {tbl} GROUP BY 1
    ORDER BY CASE method WHEN 'span' THEN 1 WHEN 'source' THEN 2
                         WHEN 'month' THEN 3 ELSE 4 END"))
  message(glue::glue("cruise_key on {table_name}: ",
    paste(glue::glue("{out$method} {out$n}"), collapse = ", ")))
  tibble::as_tibble(out)
}
