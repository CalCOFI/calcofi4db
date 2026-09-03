# cruise_key integrity + provider UUIDs (WS-B, Ed Weber's ask) -----------------
#
# The `cruise` reference is the SWFSC ichthyo export's station-occupation
# cruises (691 at v2026.08.25), and it is not everything the OTHER 15 datasets
# key events to: bottle/CTD/METS/picoplankton keep sampling on cruises the
# export has no site rows for (pre-1951 and post-export years). Nothing
# enforced that `sample.cruise_key` / `obs.cruise_key` named a real `cruise`
# row, so 152 cruise_keys (153,306 sample rows, 3.8M obs rows) referenced
# nothing and no FK ever failed — plus one malformed key ("2019-07-", the July
# 2019 Bold Horizon cruise, minted from a blank ship_nodc before a correction
# ran). See the WS-B design memo (`.claude/plans_todo/2026-09-03 WS-B …md`,
# "Decided" section) for the full numbers and reasoning; this file is D3-D8's
# implementation.
#
# Three functions, meant to run in this order at release time:
#   1. complete_cruise_reference() — adds a `cruise` row (cruise_key_method =
#      'derived') for every cruise_key a dataset designates that the SWFSC
#      export itself has no row for, so every FK holds by construction.
#   2. check_cruise_key_integrity() — the hard gate: format, FK, NODC,
#      date_ym, cruise_uuid hygiene, date-span containment, plus three
#      ratchets (derived-row count, span overlaps, NULL cruise_key backlog).
#   3. match_station_occupation() — stamps sample.station_uuid, the SWFSC
#      station occupation (ichthyo `site`) an event belongs to, on every
#      sample row (not just cruise-level — the finer, per-event join Ed's ask
#      was really about).

# complete_cruise_reference -----------------------------------------------------

#' Complete the `cruise` reference with cruises no SWFSC site row names
#'
#' The SWFSC ichthyo export's `cruise` table is a station-occupation cruise
#' list, not a designation registry: 152 `cruise_key`s that `sample` carries
#' (bottle, CTD, METS, picoplankton — 1949-1950 and post-export years mostly)
#' name no row in it. This adds one `cruise` row per such key so
#' [check_cruise_key_integrity()]'s FK check (and every ordinary FK join) holds
#' by construction, stamping `cruise_key_method` (`"swfsc"` for the pre-existing
#' rows, `"derived"` for the added ones) and `cruise_key_datasets` (a sorted
#' comma list of every dataset that keys events to that cruise) on ALL rows —
#' not just the ones it adds — so a consumer can always tell which kind of row
#' it is looking at.
#'
#' A derived row's `ship_key` is resolved from the key's own NODC segment
#' (`split_part(cruise_key, '-', 3)`) against `ship_tbl`; a key naming an NODC
#' `ship_tbl` does not know is an error (a derivation the release cannot stand
#' behind), collected across every offending key into one message rather than
#' failing on the first. `date_ym` is the key's own `YYYY-MM`; `date_min` /
#' `date_max` are the min/max event date of the sample rows carrying that key
#' (a derived row's span is therefore always contained in itself — see
#' [check_cruise_key_integrity()]'s check 6). `cruise_uuid` is left `NULL`: no
#' derived cruise has one, by definition.
#'
#' `cruise_tbl` may arrive as a VIEW over static parquet (as it does in
#' `release_database.qmd`, before the per-cruise enrichment step rebuilds it as
#' a TABLE) — this materializes it into a TABLE either way, because it must add
#' both columns and rows.
#'
#' @param con DBI connection holding `sample_tbl`, `cruise_tbl`, `ship_tbl`.
#' @param sample_tbl,cruise_tbl,ship_tbl Table names.
#' @return Invisibly, a tibble of the rows ADDED (empty if none were needed) —
#'   `cruise_key`, `ship_key`, `date_ym`, `date_min`, `date_max`,
#'   `cruise_key_datasets`.
#' @export
#' @concept keys
#' @importFrom DBI dbExecute dbGetQuery dbListFields dbListTables dbQuoteIdentifier
#' @importFrom glue glue
complete_cruise_reference <- function(con, sample_tbl = "sample",
                                      cruise_tbl = "cruise", ship_tbl = "ship") {
  tbls <- DBI::dbListTables(con)
  stopifnot(
    "sample table required" = sample_tbl %in% tbls,
    "cruise table required" = cruise_tbl %in% tbls,
    "ship table required"   = ship_tbl   %in% tbls)
  cur_cols <- DBI::dbListFields(con, cruise_tbl)
  stopifnot(
    "cruise table needs cruise_key" = "cruise_key" %in% cur_cols,
    "cruise table needs ship_key"   = "ship_key"   %in% cur_cols,
    "cruise table needs date_ym"    = "date_ym"    %in% cur_cols,
    "cruise table needs date_min/date_max (add_cruise_date_span())" =
      all(c("date_min", "date_max") %in% cur_cols),
    "ship table needs ship_key + ship_nodc" =
      all(c("ship_key", "ship_nodc") %in% DBI::dbListFields(con, ship_tbl)))

  # every distinct cruise_key sample carries, with its dataset list and
  # observed span — covers BOTH the existing (swfsc) rows and the candidates
  # to add, in one pass
  DBI::dbExecute(con, glue::glue("
    CREATE OR REPLACE TEMP TABLE _ckr AS
    SELECT cruise_key,
           string_agg(DISTINCT dataset_key, ',' ORDER BY dataset_key) AS cruise_key_datasets,
           MIN(CAST(datetime AS DATE)) AS ev_date_min,
           MAX(CAST(datetime AS DATE)) AS ev_date_max
    FROM {sample_tbl}
    WHERE cruise_key IS NOT NULL
    GROUP BY 1"))

  missing <- DBI::dbGetQuery(con, glue::glue(
    "SELECT cruise_key FROM _ckr
      WHERE cruise_key NOT IN (SELECT cruise_key FROM {cruise_tbl})
      ORDER BY cruise_key"))$cruise_key

  # resolve every missing key's ship BEFORE materializing anything, and fail
  # loudly (all offenders at once) rather than derive a row with no ship
  ship_by_nodc <- DBI::dbGetQuery(con, glue::glue(
    "SELECT ship_key,
            ROW_NUMBER() OVER (PARTITION BY ship_nodc ORDER BY ship_key) AS rn,
            ship_nodc
     FROM {ship_tbl}
     WHERE ship_nodc IS NOT NULL AND ship_nodc <> ''"))
  ship_by_nodc <- ship_by_nodc[ship_by_nodc$rn == 1, c("ship_key", "ship_nodc")]
  if (length(missing)) {
    nodc <- toupper(vapply(strsplit(missing, "-"), function(p) p[[3]], ""))
    m <- match(nodc, ship_by_nodc$ship_nodc)
    unresolved <- missing[is.na(m)]
    if (length(unresolved))
      stop(glue::glue(
        "complete_cruise_reference(): {length(unresolved)} cruise_key(s) name a ",
        "ship NODC not in `{ship_tbl}`, so no derived row can be built: ",
        "{paste(unresolved, collapse = ', ')}."), call. = FALSE)
  }

  # materialize cruise_tbl as a real TABLE (it may be a VIEW over parquet) and
  # add the two tracking columns — idempotent: re-running this leaves
  # already-stamped rows untouched
  kind <- DBI::dbGetQuery(con, glue::glue(
    "SELECT table_type FROM information_schema.tables WHERE table_name = '{cruise_tbl}'"))
  if (nrow(kind) && grepl("VIEW", kind$table_type[1], ignore.case = TRUE)) {
    DBI::dbExecute(con, glue::glue(
      "CREATE TABLE _cruise_mat AS SELECT * FROM {cruise_tbl}"))
    DBI::dbExecute(con, glue::glue("DROP VIEW {cruise_tbl}"))
    DBI::dbExecute(con, glue::glue("ALTER TABLE _cruise_mat RENAME TO {cruise_tbl}"))
  }
  DBI::dbExecute(con, glue::glue(
    "ALTER TABLE {cruise_tbl} ADD COLUMN IF NOT EXISTS cruise_key_method VARCHAR"))
  DBI::dbExecute(con, glue::glue(
    "ALTER TABLE {cruise_tbl} ADD COLUMN IF NOT EXISTS cruise_key_datasets VARCHAR"))

  DBI::dbExecute(con, glue::glue(
    "UPDATE {cruise_tbl} SET cruise_key_method = 'swfsc' WHERE cruise_key_method IS NULL"))
  DBI::dbExecute(con, glue::glue(
    "UPDATE {cruise_tbl} c SET cruise_key_datasets = k.cruise_key_datasets
       FROM _ckr k WHERE c.cruise_key = k.cruise_key AND c.cruise_key_datasets IS NULL"))

  added <- tibble::tibble(
    cruise_key = character(), ship_key = character(),
    date_ym = as.Date(character()), date_min = as.Date(character()),
    date_max = as.Date(character()), cruise_key_datasets = character())
  if (length(missing)) {
    DBI::dbWriteTable(con, "_ship_by_nodc", ship_by_nodc, overwrite = TRUE)
    has_uuid <- "cruise_uuid" %in% cur_cols
    ins_cols <- c("cruise_key", "ship_key", "date_ym", "date_min", "date_max",
                  if (has_uuid) "cruise_uuid",
                  "cruise_key_method", "cruise_key_datasets")
    uuid_sel <- if (has_uuid) ", NULL::UUID" else ""
    DBI::dbExecute(con, glue::glue("
      INSERT INTO {cruise_tbl} ({paste(ins_cols, collapse = ', ')})
      SELECT k.cruise_key, sn.ship_key,
             CAST(split_part(k.cruise_key, '-', 1) || '-' ||
                  split_part(k.cruise_key, '-', 2) || '-01' AS DATE) AS date_ym,
             k.ev_date_min, k.ev_date_max{uuid_sel},
             'derived', k.cruise_key_datasets
      FROM _ckr k
      JOIN _ship_by_nodc sn ON sn.ship_nodc = upper(split_part(k.cruise_key, '-', 3))
      WHERE k.cruise_key IN ({paste(DBI::dbQuoteString(con, missing), collapse = ', ')})"))
    DBI::dbExecute(con, "DROP TABLE _ship_by_nodc")
    added <- tibble::as_tibble(DBI::dbGetQuery(con, glue::glue(
      "SELECT cruise_key, ship_key, date_ym, date_min, date_max, cruise_key_datasets
       FROM {cruise_tbl}
       WHERE cruise_key IN ({paste(DBI::dbQuoteString(con, missing), collapse = ', ')})
       ORDER BY cruise_key")))
  }
  DBI::dbExecute(con, "DROP TABLE _ckr")

  n_swfsc <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {cruise_tbl} WHERE cruise_key_method = 'swfsc'"))$n
  message(glue::glue(
    "cruise reference: {n_swfsc} swfsc + {nrow(added)} derived = ",
    "{n_swfsc + nrow(added)} rows"))
  invisible(added)
}

# check_cruise_key_integrity -----------------------------------------------------

#' Fail (or ratchet) the release on a `cruise_key` that does not hold up
#'
#' Ten checks over `cruise_key` as it is actually used, not as it is assumed to
#' behave — see the WS-B design memo for the incidents each one guards against.
#' Checks 1-5 and 7 are hard failures from the first run (`n` must be exactly
#' 0); check 6 is hard with `known_outside_span` as named exceptions (an
#' UNLISTED violator still fails); checks 8-10 are ratchets — the current count
#' must not exceed the allowance, which may only ever be lowered.
#'
#' \enumerate{
#'   \item `cruise_key` matches `^\\d{4}-(0[1-9]|1[0-2])-[A-Za-z0-9]{4}$` on
#'     `cruise_tbl`, `sample_tbl`, `obs_tbl`.
#'   \item `cruise.date_ym`'s `YYYY-MM` equals the key's own.
#'   \item the key's NODC segment equals `ship.ship_nodc` of `cruise.ship_key`.
#'   \item every non-NULL `sample.cruise_key` / `obs.cruise_key` names a real
#'     `cruise` row (run [complete_cruise_reference()] first, or this is the
#'     153,306-row finding it exists to close).
#'   \item `cruise_key_method = 'swfsc'` rows have a unique non-NULL
#'     `cruise_uuid`; `'derived'` rows have none.
#'   \item every event's date falls within `[date_min - tolerance_days,
#'     date_max + tolerance_days]` of its cruise — for `'swfsc'` rows only (a
#'     `'derived'` row's span is its own events' min/max by construction, so it
#'     cannot be violated); `known_outside_span` names exempt `sample_key`s.
#'   \item the ichthyo notebook's own `cruise_uuid` vs `cruise_key` check
#'     (`manifest_ichthyo`, its `mismatches$cruise_uuid` count) is 0, and every
#'     ichthyo `site`/`tow`/`net` row has a non-NULL `source_uuid`.
#'   \item (ratchet `span_overlaps_max`) event spans of two cruises of one ship
#'     overlap by more than 3 days.
#'   \item (ratchet `derived_max`) `cruise_key_method = 'derived'` row count.
#'   \item (ratchet `key_null_max`, per `dataset_key`) root samples with a
#'     `NULL` `cruise_key`.
#' }
#'
#' @param con DBI connection holding `sample_tbl`, `obs_tbl`, `cruise_tbl`,
#'   `ship_tbl`.
#' @param tolerance_days Days a `'swfsc'` cruise's span may be exceeded by one
#'   of its own events before check 6 flags it (default 31 — 99.97% of the
#'   21,987 outside-span events measured at v2026.08.25 are within this).
#' @param known_outside_span `sample_key`s exempt from check 6 (named or not;
#'   only the values are used) — an UNLISTED violator still fails.
#' @param manifest_ichthyo A single integer: the ichthyo ingest's own
#'   `mismatches$cruise_uuid` `n_mismatch` count (from `manifest.json`), or
#'   `NULL` if not supplied (check 7 then fails, naming the gap).
#' @param ratchets A list with `span_overlaps_max`, `derived_max` (both single
#'   integers) and `key_null_max` (a named integer vector by `dataset_key`;
#'   an un-named dataset's allowance is 0, so a first NULL there fails).
#' @param halt `stop()` on any failing hard check or exceeded ratchet
#'   (default `TRUE`).
#' @param sample_tbl,obs_tbl,cruise_tbl,ship_tbl Table names.
#' @return A tibble: `check`, `dataset_key` (or a table/scope label where the
#'   check is not per-dataset), `n`, `mode` (`"fail"` | `"ratchet"`),
#'   `finding`.
#' @export
#' @concept keys
#' @importFrom DBI dbExecute dbGetQuery dbListFields dbListTables dbQuoteString
#' @importFrom glue glue
check_cruise_key_integrity <- function(
    con, tolerance_days = 31L, known_outside_span = character(),
    manifest_ichthyo = NULL,
    ratchets = list(span_overlaps_max = 2L, derived_max = 152L,
                    key_null_max = c(calcofi_dic = 3255L,
                                      "sio_pic-zooplankton" = 5087L,
                                      "cdfw_dungeness-crab" = 1639L,
                                      calcofi_bottle = 49L)),
    halt = TRUE,
    sample_tbl = "sample", obs_tbl = "obs",
    cruise_tbl = "cruise", ship_tbl = "ship") {
  tbls <- DBI::dbListTables(con)
  stopifnot(
    "sample table required" = sample_tbl %in% tbls,
    "cruise table required" = cruise_tbl %in% tbls,
    "ship table required"   = ship_tbl   %in% tbls)
  has_obs <- obs_tbl %in% tbls
  has_method <- "cruise_key_method" %in% DBI::dbListFields(con, cruise_tbl)
  # bare (unaliased `cruise_key_method`, checks 5/9 against {cruise_tbl} alone)
  # and `c.`-aliased (check 6, joined FROM sample s ... JOIN cruise c) forms —
  # kept as two literals rather than string surgery on one, which silently
  # replaces only the FIRST "cruise_key_method" occurrence with sub()
  method_swfsc    <- if (has_method) "(cruise_key_method = 'swfsc' OR cruise_key_method IS NULL)" else "TRUE"
  method_derived  <- if (has_method) "cruise_key_method = 'derived'" else "FALSE"
  method_swfsc_c  <- if (has_method) "(c.cruise_key_method = 'swfsc' OR c.cruise_key_method IS NULL)" else "TRUE"

  FMT_RE <- "'^\\d{4}-(0[1-9]|1[0-2])-[A-Za-z0-9]{4}$'"
  rows <- list()
  add <- function(check, dataset_key, n, mode, finding)
    rows[[length(rows) + 1]] <<- data.frame(
      check = check, dataset_key = as.character(dataset_key), n = as.integer(n),
      mode = mode, finding = finding, stringsAsFactors = FALSE)

  # 1. cruise_key format, on every table that carries one
  for (tb in intersect(c(cruise_tbl, sample_tbl, obs_tbl), tbls)) {
    n <- DBI::dbGetQuery(con, glue::glue(
      "SELECT COUNT(*) AS n FROM {tb}
        WHERE cruise_key IS NOT NULL AND NOT regexp_matches(cruise_key, {FMT_RE})"))$n
    add("cruise_key_format", tb, n, "fail",
        glue::glue("{n} cruise_key value(s) on `{tb}` fail YYYY-MM-NODC"))
  }

  # 2. cruise.date_ym's own YYYY-MM equals the key's
  n <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {cruise_tbl}
      WHERE cruise_key IS NOT NULL AND date_ym IS NOT NULL
        AND strftime(date_ym, '%Y-%m') <>
            (split_part(cruise_key, '-', 1) || '-' || split_part(cruise_key, '-', 2))"))$n
  add("date_ym_mismatch", cruise_tbl, n, "fail",
      glue::glue("{n} `{cruise_tbl}` row(s) where date_ym's month disagrees with cruise_key"))

  # 3. the key's NODC segment == ship.ship_nodc of cruise.ship_key — joined
  # through ship_tbl rather than a cruise.ship_nodc column, so this does not
  # depend on release_database.qmd's later per-cruise enrichment having run
  d3 <- DBI::dbGetQuery(con, glue::glue(
    "SELECT s.dataset_key, COUNT(*) AS n
     FROM {sample_tbl} s
     JOIN {cruise_tbl} c ON c.cruise_key = s.cruise_key
     JOIN {ship_tbl} sh ON sh.ship_key = c.ship_key
     WHERE s.cruise_key IS NOT NULL
       AND split_part(s.cruise_key, '-', 3) <> sh.ship_nodc
     GROUP BY 1 ORDER BY 1"))
  if (nrow(d3) == 0) d3 <- data.frame(dataset_key = "(none)", n = 0L)
  for (i in seq_len(nrow(d3)))
    add("nodc_mismatch", d3$dataset_key[i], d3$n[i], "fail",
        glue::glue("{d3$n[i]} `{sample_tbl}` row(s) of {d3$dataset_key[i]} whose ",
                   "cruise_key NODC disagrees with its cruise's ship"))

  # 4. every non-NULL cruise_key names a real cruise row (sample AND obs)
  for (tb in intersect(c(sample_tbl, obs_tbl), tbls)) {
    d4 <- DBI::dbGetQuery(con, glue::glue(
      "SELECT t.dataset_key, COUNT(*) AS n
       FROM {tb} t LEFT JOIN {cruise_tbl} c ON c.cruise_key = t.cruise_key
       WHERE t.cruise_key IS NOT NULL AND c.cruise_key IS NULL
       GROUP BY 1 ORDER BY 1"))
    if (nrow(d4) == 0) d4 <- data.frame(dataset_key = "(none)", n = 0L)
    for (i in seq_len(nrow(d4)))
      add(glue::glue("fk_orphan_{tb}"), d4$dataset_key[i], d4$n[i], "fail",
          glue::glue("{d4$n[i]} `{tb}` row(s) of {d4$dataset_key[i]} carry a cruise_key ",
                     "naming no `{cruise_tbl}` row — run complete_cruise_reference() first"))
  }

  # 5. cruise_uuid hygiene: swfsc rows non-NULL + unique, derived rows NULL
  d5 <- DBI::dbGetQuery(con, glue::glue(
    "SELECT
       COUNT(*) FILTER (WHERE {method_swfsc} AND cruise_uuid IS NULL) AS n_swfsc_null,
       COUNT(*) FILTER (WHERE {method_derived} AND cruise_uuid IS NOT NULL) AS n_derived_nonnull
     FROM {cruise_tbl}"))
  dup_uuid <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM (
       SELECT cruise_uuid FROM {cruise_tbl}
       WHERE cruise_uuid IS NOT NULL GROUP BY 1 HAVING COUNT(*) > 1) x"))$n
  n5 <- d5$n_swfsc_null + d5$n_derived_nonnull + dup_uuid
  add("cruise_uuid_hygiene", cruise_tbl, n5, "fail", glue::glue(
    "{d5$n_swfsc_null} swfsc row(s) with no cruise_uuid, ",
    "{d5$n_derived_nonnull} derived row(s) WITH one, {dup_uuid} duplicated"))

  # 6. event date within [date_min - tol, date_max + tol] of its (swfsc) cruise
  d6 <- DBI::dbGetQuery(con, glue::glue(
    "SELECT s.dataset_key, s.sample_key
     FROM {sample_tbl} s JOIN {cruise_tbl} c ON c.cruise_key = s.cruise_key
     WHERE {method_swfsc_c}
       AND s.cruise_key IS NOT NULL AND s.datetime IS NOT NULL
       AND c.date_min IS NOT NULL AND c.date_max IS NOT NULL
       AND (CAST(s.datetime AS DATE) < c.date_min - to_days({as.integer(tolerance_days)})
         OR CAST(s.datetime AS DATE) > c.date_max + to_days({as.integer(tolerance_days)}))"))
  unlisted <- d6[!d6$sample_key %in% known_outside_span, , drop = FALSE]
  d6_by_ds <- if (nrow(unlisted))
    stats::aggregate(sample_key ~ dataset_key, unlisted, length) else
    data.frame(dataset_key = "(none)", sample_key = 0L)
  names(d6_by_ds)[2] <- "n"
  for (i in seq_len(nrow(d6_by_ds)))
    add("event_outside_span", d6_by_ds$dataset_key[i], d6_by_ds$n[i], "fail",
        glue::glue("{d6_by_ds$n[i]} `{sample_tbl}` row(s) of {d6_by_ds$dataset_key[i]} more ",
                   "than {tolerance_days}d outside their cruise's span, unlisted in ",
                   "known_outside_span ({nrow(d6) - nrow(unlisted)} listed and excluded)"))

  # 7. ichthyo's own cruise_uuid/cruise_key check (from its manifest) + every
  #    site/tow/net row has source_uuid
  mi <- if (is.null(manifest_ichthyo)) NA_integer_ else as.integer(manifest_ichthyo)
  d7uuid <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) FILTER (WHERE dataset_key = 'swfsc_ichthyo'
              AND sample_type IN ('site','tow','net') AND source_uuid IS NULL) AS n
     FROM {sample_tbl}"))$n
  n7 <- (if (is.na(mi)) 1L else mi) + d7uuid
  add("ichthyo_uuid_check", "swfsc_ichthyo", n7, "fail", if (is.na(mi))
    glue::glue("manifest_ichthyo not supplied (re-render the ichthyo notebook first); ",
              "{d7uuid} site/tow/net row(s) with no source_uuid") else
    glue::glue("ichthyo manifest cruise_uuid mismatch = {mi}; ",
              "{d7uuid} site/tow/net row(s) with no source_uuid"))

  # 8. (ratchet) event spans of two cruises of the same ship overlap > 3 days
  n8 <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM (
       SELECT a.cruise_key FROM {cruise_tbl} a JOIN {cruise_tbl} b
         ON a.ship_key = b.ship_key AND a.cruise_key < b.cruise_key
       WHERE a.date_min IS NOT NULL AND b.date_min IS NOT NULL
         AND a.date_max >= b.date_min + 3 AND b.date_max >= a.date_min + 3) x"))$n
  add("span_overlap", cruise_tbl, n8, "ratchet",
      glue::glue("{n8} pair(s) of one ship's cruises overlap by > 3 days"))

  # 9. (ratchet) derived-row count
  n9 <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {cruise_tbl} WHERE {method_derived}"))$n
  add("derived_rows", cruise_tbl, n9, "ratchet",
      glue::glue("{n9} `{cruise_tbl}` row(s) with cruise_key_method = 'derived'"))

  # 10. (ratchet, per dataset_key) root samples with NULL cruise_key
  d10 <- DBI::dbGetQuery(con, glue::glue(
    "SELECT dataset_key, COUNT(*) AS n FROM {sample_tbl}
      WHERE sample_key = root_sample_key AND cruise_key IS NULL
      GROUP BY 1 ORDER BY 1"))
  for (i in seq_len(nrow(d10)))
    add("null_cruise_key", d10$dataset_key[i], d10$n[i], "ratchet",
        glue::glue("{d10$n[i]} root `{sample_tbl}` row(s) of {d10$dataset_key[i]} ",
                   "with NULL cruise_key"))

  out <- tibble::as_tibble(do.call(rbind, rows))

  key_null_allow <- ratchets$key_null_max
  allow <- function(check, dataset_key) {
    if (check == "span_overlap") return(ratchets$span_overlaps_max %||% 0L)
    if (check == "derived_rows") return(ratchets$derived_max %||% 0L)
    if (check == "null_cruise_key")
      return(if (dataset_key %in% names(key_null_allow)) key_null_allow[[dataset_key]] else 0L)
    0L
  }
  out$allowance <- ifelse(out$mode == "ratchet",
                          mapply(allow, out$check, out$dataset_key), 0L)
  over <- out[(out$mode == "fail" & out$n > 0) |
              (out$mode == "ratchet" & out$n > out$allowance), , drop = FALSE]

  if (nrow(over) > 0) {
    detail <- paste(sprintf("  [%s] %s (n=%d%s): %s", over$mode, over$check, over$n,
                            ifelse(over$mode == "ratchet",
                                   paste0(", allowance=", over$allowance), ""),
                            over$finding), collapse = "\n")
    msg <- paste0("check_cruise_key_integrity(): cruise_key integrity violated:\n", detail)
    if (halt) stop(msg, call. = FALSE) else warning(msg, call. = FALSE)
  } else {
    message("check_cruise_key_integrity(): all hard checks pass; ratchets within allowance")
  }
  out
}

# match_station_occupation --------------------------------------------------

#' Stamp `sample.station_uuid`: the SWFSC station occupation an event belongs to
#'
#' Every `sample` root (an event with no parent within its own dataset — a
#' bottle cast, a CTD cast, an ichthyo site, ...) is matched to the ichthyo
#' `site` row (its `source_uuid`) representing the same SWFSC station
#' occupation, in priority order:
#' \enumerate{
#'   \item **self** — the root itself IS an ichthyo site (`dataset_key =
#'     'swfsc_ichthyo'`): its own `source_uuid`.
#'   \item **order_occ** — exactly one ichthyo site shares `(cruise_key,
#'     site_key, order_occ)`.
#'   \item **datetime** — exactly one ichthyo site at `(cruise_key, site_key)`
#'     has a `datetime` within `tolerance_hours` of the root's (whether it is
#'     the only candidate at all, or the only one inside the window).
#'   \item otherwise `NULL` (`station_uuid_method` `NULL` too).
#' }
#' The match is computed once per ROOT and copied to every row sharing its
#' `root_sample_key`, which is what makes the crab's examined subsamples
#' (parented DIRECTLY to an ichthyo site via `parent_sample_key` /
#' `root_sample_key` — they never enter the match SQL, whose `roots` CTE only
#' sees rows where `sample_key = root_sample_key`) inherit that site's
#' `station_uuid` for free, with no separate matching logic needed. Their
#' `station_uuid_method` is relabeled **`"parent"`** rather than `"self"` in
#' that copy step, purely so a consumer can tell "this row IS the SWFSC
#' station occupation" (`"self"`, ichthyo's own site/tow/net) apart from "this
#' row is a foreign dataset's row directly under one" (`"parent"`).
#'
#' Rebuilds `sample_tbl` (DuckDB cannot `UPDATE` a table with a CRS-tagged
#' `geom` column), and asserts afterward that its row count and `sample_key`
#' uniqueness are unchanged — the v2026.08.25 lesson (a join that fans out is a
#' bug in the match, never data to accept).
#'
#' @param con DBI connection holding `sample_tbl` (with `source_uuid`,
#'   `root_sample_key`, `cruise_key`, `site_key`, `order_occ`, `datetime`).
#' @param sample_tbl Table name (default `"sample"`).
#' @param tolerance_hours Hours a candidate's `datetime` may differ from the
#'   root's before it stops counting (default 24).
#' @return Invisibly, a tibble: `dataset_key`, `method` (`"self"` |
#'   `"order_occ"` | `"datetime"` | `"none"`), `n` — over ROOT samples only
#'   (`"parent"` never appears here: by construction it only ever labels a
#'   NON-root row).
#' @export
#' @concept keys
#' @importFrom DBI dbExecute dbGetQuery dbListFields dbListTables dbQuoteIdentifier
#' @importFrom glue glue
match_station_occupation <- function(con, sample_tbl = "sample", tolerance_hours = 24) {
  .load_spatial(con)
  flds0 <- DBI::dbListFields(con, sample_tbl)
  stopifnot(
    "sample table required" = sample_tbl %in% DBI::dbListTables(con),
    "sample table needs source_uuid (append_sample() >= 3.32.0)" =
      "source_uuid" %in% flds0,
    "sample table needs root_sample_key, cruise_key, site_key, order_occ, datetime" =
      all(c("root_sample_key", "cruise_key", "site_key", "order_occ", "datetime") %in% flds0))

  tol_sec <- as.numeric(tolerance_hours) * 3600
  DBI::dbExecute(con, glue::glue("
    CREATE OR REPLACE TEMP TABLE _station_match AS
    WITH roots AS (
      SELECT sample_key AS root_sample_key, dataset_key, cruise_key, site_key,
             order_occ, datetime
      FROM {sample_tbl} WHERE sample_key = root_sample_key),
    ich AS (
      SELECT sample_key AS ich_key, source_uuid, cruise_key, site_key, order_occ, datetime
      FROM {sample_tbl} WHERE dataset_key = 'swfsc_ichthyo' AND sample_type = 'site'),
    self_m AS (
      SELECT r.root_sample_key, i.source_uuid AS station_uuid, 'self' AS station_uuid_method
      FROM roots r JOIN ich i ON i.ich_key = r.root_sample_key
      WHERE r.dataset_key = 'swfsc_ichthyo'),
    occ_cand AS (
      SELECT r.root_sample_key, i.source_uuid
      FROM roots r
      JOIN ich i ON i.cruise_key = r.cruise_key AND i.site_key = r.site_key
                AND i.order_occ = r.order_occ
      WHERE r.order_occ IS NOT NULL AND r.dataset_key <> 'swfsc_ichthyo'),
    occ_m AS (
      SELECT root_sample_key, MIN(source_uuid) AS station_uuid, 'order_occ' AS station_uuid_method
      FROM occ_cand GROUP BY 1 HAVING COUNT(*) = 1),
    dt_cand AS (
      SELECT r.root_sample_key, i.source_uuid
      FROM roots r
      JOIN ich i ON i.cruise_key = r.cruise_key AND i.site_key = r.site_key
      WHERE r.dataset_key <> 'swfsc_ichthyo'
        AND r.root_sample_key NOT IN (SELECT root_sample_key FROM occ_m)
        AND r.datetime IS NOT NULL AND i.datetime IS NOT NULL
        AND ABS(EXTRACT(EPOCH FROM (i.datetime - r.datetime))) <= {tol_sec}),
    dt_m AS (
      SELECT root_sample_key, MIN(source_uuid) AS station_uuid, 'datetime' AS station_uuid_method
      FROM dt_cand GROUP BY 1 HAVING COUNT(*) = 1)
    SELECT * FROM self_m
    UNION ALL SELECT * FROM occ_m
    UNION ALL SELECT * FROM dt_m"))

  dup <- DBI::dbGetQuery(con,
    "SELECT root_sample_key FROM _station_match GROUP BY 1 HAVING COUNT(*) > 1")
  if (nrow(dup) > 0)
    stop("match_station_occupation(): _station_match is not unique on ",
         "root_sample_key (", nrow(dup), " duplicated) — a bug in the match ",
         "SQL, not the data.", call. = FALSE)

  n_before <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {sample_tbl}"))$n

  flds <- setdiff(flds0, c("station_uuid", "station_uuid_method"))
  cols <- paste(DBI::dbQuoteIdentifier(con, flds), collapse = ", ")
  DBI::dbExecute(con, glue::glue("
    CREATE OR REPLACE TABLE _sample_stationed AS
    SELECT s.{gsub(', ', ', s.', cols)}, m.station_uuid,
           -- 'self' from _station_match means \"the ROOT donating this value is
           -- an ichthyo site\" — true both for ichthyo's own site/tow/net (this
           -- row IS or descends from that site) and for a foreign row parented
           -- DIRECTLY to that site (the crab's examined subsamples,
           -- parent_sample_key/root_sample_key = the ichthyo site's sample_key,
           -- never entering the match SQL at all). Split those apart by the
           -- row's OWN dataset_key so 'self' means \"this is ichthyo\" and
           -- 'parent' means \"this is someone else's row directly under an
           -- ichthyo site\" — the vocabulary field_dictionary.csv documents.
           CASE WHEN m.station_uuid_method = 'self' AND s.dataset_key <> 'swfsc_ichthyo'
                THEN 'parent' ELSE m.station_uuid_method END AS station_uuid_method
    FROM {sample_tbl} s LEFT JOIN _station_match m USING (root_sample_key)"))
  DBI::dbExecute(con, glue::glue("DROP TABLE {sample_tbl}"))
  DBI::dbExecute(con, glue::glue("ALTER TABLE _sample_stationed RENAME TO {sample_tbl}"))
  DBI::dbExecute(con, "DROP TABLE _station_match")

  n_after <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {sample_tbl}"))$n
  if (n_after != n_before)
    stop(glue::glue(
      "match_station_occupation(): row count changed on {sample_tbl} ",
      "({n_before} -> {n_after}) — the join fanned out; fix the match SQL, ",
      "not the data."), call. = FALSE)
  dup2 <- DBI::dbGetQuery(con, glue::glue(
    "SELECT sample_key FROM {sample_tbl} GROUP BY 1 HAVING COUNT(*) > 1"))
  if (nrow(dup2) > 0)
    stop(glue::glue(
      "match_station_occupation(): {nrow(dup2)} sample_key(s) duplicated ",
      "after the rebuild."), call. = FALSE)

  rpt <- DBI::dbGetQuery(con, glue::glue("
    SELECT dataset_key, COALESCE(station_uuid_method, 'none') AS method, COUNT(*) AS n
    FROM {sample_tbl} WHERE sample_key = root_sample_key
    GROUP BY 1, 2 ORDER BY 1, 2"))
  message(glue::glue(
    "station_uuid on {sample_tbl}: {sum(rpt$n[rpt$method != 'none'])} of ",
    "{sum(rpt$n)} root sample(s) matched"))
  invisible(tibble::as_tibble(rpt))
}
