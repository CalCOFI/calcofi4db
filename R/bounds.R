# declared measurement bounds: check, report, enforce ---------------------------
#
# `metadata/measurement_type.csv` has carried `valid_min` / `valid_max` since the
# CTD registry was built, and for most of that time NOTHING applied them. The
# columns were emitted as netCDF variable attributes and shown on the schema site,
# which made them look enforced; they were documentation.
#
# v2026.08.07 is what that costs. It shipped ~31k impossible CTD values (pH down
# to -10, oxygen to -79.5 ml/l, temperature_ave to -47.6 C), and the fix landed as
# an inline DELETE in `ingest_calcofi_ctd-cast.qmd` — one notebook, one dataset.
#
# The wider problem is not that 15 datasets declare bounds nobody checks. It is
# that they declare NOTHING: at v2026.08.07, 73 of the 98 (dataset, type) pairs in
# `obs` — 17.6M of 26.3M rows, 67% — had neither a `valid_min` nor a `valid_max`,
# and only `calcofi_ctd-cast` and one `calcofi_mets` type had any. So a
# release-time guard alone would have protected a third of the release and gone
# quiet on the rest, including a `-99` sentinel sitting in `calcofi_mets.uws_flow`
# and a `spar` spanning -3.07e17 to 2.01e16.
#
# Hence [check_measurement_bounds()] reports BOTH failure modes in one tally —
# `out_of_range` (declared and violated) and `undeclared` (nothing to violate) —
# because at this coverage the second is the bigger finding, and an ingest that
# cannot answer "what range is plausible?" has found a question for its provider.
# Enforcement is deliberately a separate call: a bound must be agreed before it is
# allowed to delete data.

.bounds_registry <- function(con, mt) {
  if (is.null(mt)) {
    tbls <- DBI::dbListTables(con)
    if (!"measurement_type" %in% tbls)
      stop("no `mt` supplied and no `measurement_type` table in the connection.\n",
           "  Pass the registry: mt = read_measurement_type(",
           "here::here('metadata/measurement_type.csv'))", call. = FALSE)
    mt <- DBI::dbGetQuery(con, "SELECT * FROM measurement_type")
  } else if (is.character(mt) && length(mt) == 1) {
    mt <- read_measurement_type(mt)
  }
  need <- c("measurement_type", "valid_min", "valid_max")
  miss <- setdiff(need, names(mt))
  if (length(miss))
    stop("measurement registry is missing column(s): ",
         paste(miss, collapse = ", "), call. = FALSE)
  # A registry that has been through write_csv(na = "NA") carries the literal
  # string, which as.numeric() turns into NA *with a warning* rather than an
  # error — i.e. a corrupted bound silently becomes "undeclared" and this check
  # reports coverage it does not have. read_measurement_type() is the guard; this
  # is the belt for a caller who passed a hand-read data.frame.
  check_registry_na_strings(mt)
  for (cl in c("valid_min", "valid_max",
               "valid_depth_min_m", "valid_depth_max_m"))
    if (cl %in% names(mt)) mt[[cl]] <- suppressWarnings(as.numeric(mt[[cl]]))
  mt
}

#' Check measured values against the registry's declared bounds
#'
#' The standard bounds check for an ingest notebook and for the release. Compares
#' every value in a long-format measurement table against `valid_min` /
#' `valid_max` from `metadata/measurement_type.csv`, and reports the types that
#' violate a bound **alongside the types that declare none** — see the note in
#' `R/bounds.R` for why the second matters at least as much as the first.
#'
#' Read-only: it deletes and rewrites nothing. Enforce with
#' [drop_out_of_bounds()], and only once the bound is agreed.
#'
#' Bounds may be one-sided. `valid_min = 0` with no `valid_max` is the useful case
#' for abundances and counts — "never negative" is agreeable without knowing the
#' ceiling — and a type is `undeclared` only when *both* are missing.
#'
#' @param con DuckDB connection
#' @param tbl table or view to check (default `"obs"`). Works on any long-format
#'   table: the per-dataset `{dataset}_measurement` during wrangling, the emitted
#'   `obs`, or `sample_measurement`.
#' @param mt the measurement registry: a data.frame, a path to
#'   `measurement_type.csv`, or `NULL` (default) to read a `measurement_type`
#'   table from `con`.
#' @param dataset_key optional `dataset_key` to filter to, when `tbl` holds more
#'   than one dataset. Ignored if `tbl` has no `dataset_key` column.
#' @param type_col,value_col column names (default `measurement_type` /
#'   `measurement_value`)
#' @param depth_col optional depth column enabling the depth-window check against
#'   `valid_depth_min_m` / `valid_depth_max_m` — the depth over which a type is
#'   *defined*. A non-null value outside that window is a finding: it means the
#'   type was emitted where the registry says it does not exist.
#' @param include_undeclared report types with no declared bound (default TRUE).
#'   Set FALSE for a violations-only view.
#'
#' @return A [tibble][tibble::tibble], one row per measurement type present,
#'   ordered worst-first (violations by count, then undeclared by count):
#'   \describe{
#'     \item{`status`}{`out_of_range` (declared and violated), `undeclared`
#'       (nothing declared), `ok` (declared and respected)}
#'     \item{`n_total`, `n_bad`, `pct_bad`}{rows checked, rows outside, percent}
#'     \item{`n_low`, `n_high`}{split by which bound was broken}
#'     \item{`v_min`, `v_max`}{observed range, for proposing a bound}
#'     \item{`valid_min`, `valid_max`}{what the registry declares}
#'     \item{`n_outside_depth`}{present only when `depth_col` is given}
#'     \item{`finding`}{a one-line summary, ready to paste into the `context`
#'       column of a `questions.csv` row}
#'   }
#' @export
#' @concept validate
#' @seealso [drop_out_of_bounds()] to enforce, [bounds_datatable()] to render,
#'   [read_measurement_type()] for the registry, [register_measurement_types()]
#'   to declare a new bound.
#' @importFrom DBI dbGetQuery dbListTables dbListFields
#' @importFrom glue glue
#' @examples
#' \dontrun{
#' b <- check_measurement_bounds(
#'   con, "ctd_measurement",
#'   mt = here::here("metadata/measurement_type.csv"))
#' bounds_datatable(b)
#' }
check_measurement_bounds <- function(con,
                                     tbl                = "obs",
                                     mt                 = NULL,
                                     dataset_key        = NULL,
                                     type_col           = "measurement_type",
                                     value_col          = "measurement_value",
                                     depth_col          = NULL,
                                     include_undeclared = TRUE) {

  mt <- .bounds_registry(con, mt)

  flds <- DBI::dbListFields(con, tbl)
  for (cl in c(type_col, value_col))
    if (!cl %in% flds)
      stop("`", tbl, "` has no column `", cl, "`", call. = FALSE)

  where <- ""
  if (!is.null(dataset_key) && "dataset_key" %in% flds)
    where <- glue::glue("WHERE dataset_key = '{.sql_esc(dataset_key)}'")

  obs <- DBI::dbGetQuery(con, glue::glue("
    SELECT {type_col} AS measurement_type,
           COUNT(*)                                    AS n_rows,
           COUNT({value_col})                          AS n_total,
           MIN({value_col})                            AS v_min,
           MAX({value_col})                            AS v_max
    FROM {tbl} {where}
    GROUP BY 1"))
  if (!nrow(obs)) return(.bounds_empty(depth_col))

  d <- merge(obs, mt[, intersect(
    c("measurement_type", "valid_min", "valid_max",
      "valid_depth_min_m", "valid_depth_max_m", "units"), names(mt))],
    by = "measurement_type", all.x = TRUE)

  has_lo <- !is.na(d$valid_min)
  has_hi <- !is.na(d$valid_max)
  d$declared <- has_lo | has_hi

  # Counts stay DOUBLE, as DuckDB returns them, rather than being coerced to
  # integer for tidiness: `obs_ctd_full` is ~216M rows today and as.integer()
  # goes NA-with-a-warning past 2^31, which would turn a real violation count
  # into a missing one on exactly the biggest table. A double is exact to 2^53.
  d$n_low <- d$n_high <- 0
  if (any(d$declared)) {
    cases <- vapply(which(d$declared), function(i) {
      t_esc <- .sql_esc(d$measurement_type[i])
      lo <- if (has_lo[i]) glue::glue("{value_col} < {d$valid_min[i]}") else NULL
      hi <- if (has_hi[i]) glue::glue("{value_col} > {d$valid_max[i]}") else NULL
      glue::glue(
        "SELECT '{t_esc}' AS measurement_type,",
        " COUNT(*) FILTER (WHERE {if (is.null(lo)) 'FALSE' else lo}) AS n_low,",
        " COUNT(*) FILTER (WHERE {if (is.null(hi)) 'FALSE' else hi}) AS n_high",
        " FROM {tbl} WHERE {type_col} = '{t_esc}'",
        if (nzchar(where)) glue::glue(" AND {sub('^WHERE ', '', where)}") else "")
    }, character(1))
    v <- DBI::dbGetQuery(con, paste(cases, collapse = "\nUNION ALL\n"))
    i <- match(v$measurement_type, d$measurement_type)
    d$n_low[i]  <- as.numeric(v$n_low)
    d$n_high[i] <- as.numeric(v$n_high)
  }

  d$n_bad   <- d$n_low + d$n_high
  d$pct_bad <- ifelse(d$n_total > 0, round(100 * d$n_bad / d$n_total, 4), NA_real_)
  d$status  <- ifelse(!d$declared, "undeclared",
                      ifelse(d$n_bad > 0, "out_of_range", "ok"))

  if (!is.null(depth_col)) d <- .bounds_depth(con, d, tbl, type_col, value_col,
                                              depth_col, where, flds)

  d$finding <- .bounds_finding(d)

  keep <- c("measurement_type", "status", "n_total", "n_bad", "pct_bad",
            "n_low", "n_high", "v_min", "v_max", "valid_min", "valid_max",
            if (!is.null(depth_col)) "n_outside_depth", "units", "finding")
  d <- d[, intersect(keep, names(d)), drop = FALSE]

  if (!isTRUE(include_undeclared)) d <- d[d$status != "undeclared", , drop = FALSE]

  # worst first: violations by count, then undeclared by count, then the rest
  d <- d[order(match(d$status, c("out_of_range", "undeclared", "ok")),
               -d$n_bad, -d$n_total), , drop = FALSE]
  tibble::as_tibble(d)
}

.bounds_empty <- function(depth_col) {
  d <- tibble::tibble(
    measurement_type = character(), status = character(),
    n_total = numeric(), n_bad = numeric(), pct_bad = numeric(),
    n_low = numeric(), n_high = numeric(),
    v_min = numeric(), v_max = numeric(),
    valid_min = numeric(), valid_max = numeric(),
    units = character(), finding = character())
  if (!is.null(depth_col)) d$n_outside_depth <- numeric()
  d
}

.bounds_depth <- function(con, d, tbl, type_col, value_col, depth_col, where, flds) {
  d$n_outside_depth <- NA_integer_
  if (!depth_col %in% flds) {
    warning("`", tbl, "` has no column `", depth_col,
            "`; skipping the depth-window check", call. = FALSE)
    return(d)
  }
  has_dep <- !is.na(d$valid_depth_min_m) | !is.na(d$valid_depth_max_m)
  if (!any(has_dep)) return(d)
  cases <- vapply(which(has_dep), function(i) {
    t_esc <- .sql_esc(d$measurement_type[i])
    cnd <- c(
      if (!is.na(d$valid_depth_min_m[i]))
        glue::glue("{depth_col} < {d$valid_depth_min_m[i]}"),
      if (!is.na(d$valid_depth_max_m[i]))
        glue::glue("{depth_col} > {d$valid_depth_max_m[i]}"))
    glue::glue(
      "SELECT '{t_esc}' AS measurement_type, COUNT(*) AS n_outside_depth",
      " FROM {tbl} WHERE {type_col} = '{t_esc}'",
      " AND {value_col} IS NOT NULL AND ({paste(cnd, collapse = ' OR ')})",
      if (nzchar(where)) glue::glue(" AND {sub('^WHERE ', '', where)}") else "")
  }, character(1))
  v <- DBI::dbGetQuery(con, paste(cases, collapse = "\nUNION ALL\n"))
  d$n_outside_depth[match(v$measurement_type, d$measurement_type)] <-
    as.numeric(v$n_outside_depth)
  d
}

.bounds_finding <- function(d) {
  fmt <- function(x) ifelse(is.na(x), "—", format(signif(x, 4), trim = TRUE))
  rng <- glue::glue("observed {fmt(d$v_min)}..{fmt(d$v_max)}")
  ifelse(
    d$status == "undeclared",
    glue::glue("No valid_min/valid_max declared; {rng} over ",
               "{format(d$n_total, big.mark = ',', trim = TRUE)} values. ",
               "What range is physically possible?"),
    ifelse(
      d$status == "out_of_range",
      glue::glue("{format(d$n_bad, big.mark = ',', trim = TRUE)} of ",
                 "{format(d$n_total, big.mark = ',', trim = TRUE)} values ",
                 "({d$pct_bad}%) outside the declared ",
                 "{fmt(d$valid_min)}..{fmt(d$valid_max)}; {rng}."),
      glue::glue("Within the declared {fmt(d$valid_min)}..{fmt(d$valid_max)}; {rng}.")))
}

#' Render a bounds check as the standard notebook table
#'
#' The `#### Values outside their declared range` section an ingest notebook
#' shows. Colours `status` so `out_of_range` and `undeclared` are legible at a
#' glance, and drops the `finding` column — it is long prose meant for a
#' `questions.csv` `context` cell, not for a table.
#'
#' @param x a tibble from [check_measurement_bounds()]
#' @param caption table caption
#' @param page_length rows per page
#'
#' @return A [DT::datatable()] htmlwidget.
#' @export
#' @concept validate
#' @importFrom DT datatable formatStyle styleEqual
#' @examples
#' \dontrun{
#' bounds_datatable(check_measurement_bounds(con, "obs"))
#' }
bounds_datatable <- function(x,
                             caption = paste(
                               "Measured values against the registry's declared bounds.",
                               "`undeclared` is a finding too: nothing was checked."),
                             page_length = 25) {
  d <- x[, setdiff(names(x), "finding"), drop = FALSE]
  DT::datatable(
    d, caption = caption, rownames = FALSE,
    options = list(pageLength = page_length, scrollX = TRUE, dom = "tip")) |>
    DT::formatStyle(
      "status",
      color = DT::styleEqual(
        c("ok", "undeclared", "out_of_range"),
        c("#1a7f37", "#b06000", "#d03b3b")),
      fontWeight = DT::styleEqual("out_of_range", "bold", default = "normal"))
}

#' Delete values outside their declared bounds
#'
#' Enforcement, kept separate from [check_measurement_bounds()] so that a bound
#' must be *agreed* before it is allowed to delete data. Run the check first, put
#' anything surprising to the provider as a question, and call this only for
#' bounds you are confident describe the impossible.
#'
#' DELETE rather than flag, for the same reason the `-99` sentinel is deleted: in
#' a long-format table a row IS an assertion that a value was measured. A pH of
#' -10 left in place silently corrupts every mean, minimum and anomaly a consumer
#' computes downstream, and there is no in-band way to mark it as not-a-value.
#'
#' Bounds are meant to be **generous** — impossible, not merely unusual — so this
#' drops nothing an oceanographer would want to see. If it removes something
#' interesting, the bound is wrong, not the reading.
#'
#' @inheritParams check_measurement_bounds
#' @param quiet suppress the summary message
#'
#' @return The pre-delete tally from [check_measurement_bounds()], invisibly,
#'   restricted to the `out_of_range` rows that were acted on. `n_bad` is what was
#'   deleted per type.
#' @export
#' @concept validate
#' @importFrom DBI dbExecute
#' @importFrom glue glue
#' @examples
#' \dontrun{
#' oob <- drop_out_of_bounds(con, "ctd_measurement",
#'                           mt = here::here("metadata/measurement_type.csv"))
#' }
drop_out_of_bounds <- function(con,
                               tbl         = "obs",
                               mt          = NULL,
                               dataset_key = NULL,
                               type_col    = "measurement_type",
                               value_col   = "measurement_value",
                               quiet       = FALSE) {

  tally <- check_measurement_bounds(
    con, tbl = tbl, mt = mt, dataset_key = dataset_key,
    type_col = type_col, value_col = value_col, include_undeclared = FALSE)
  bad <- tally[tally$status == "out_of_range", , drop = FALSE]
  if (!nrow(bad)) {
    if (!quiet) message(glue::glue("{tbl}: no values outside declared bounds"))
    return(invisible(bad))
  }

  n_del <- 0L
  for (i in seq_len(nrow(bad))) {
    t_esc <- .sql_esc(bad$measurement_type[i])
    cnd <- c(
      if (!is.na(bad$valid_min[i])) glue::glue("{value_col} < {bad$valid_min[i]}"),
      if (!is.na(bad$valid_max[i])) glue::glue("{value_col} > {bad$valid_max[i]}"))
    sql <- glue::glue(
      "DELETE FROM {tbl} WHERE {type_col} = '{t_esc}'",
      " AND ({paste(cnd, collapse = ' OR ')})")
    if (!is.null(dataset_key) &&
        "dataset_key" %in% DBI::dbListFields(con, tbl))
      sql <- glue::glue("{sql} AND dataset_key = '{.sql_esc(dataset_key)}'")
    n_del <- n_del + DBI::dbExecute(con, sql)
  }

  if (!quiet)
    message(glue::glue(
      "{tbl}: deleted {format(n_del, big.mark = ',')} value(s) outside declared ",
      "bounds across {nrow(bad)} type(s) — ",
      "{paste(bad$measurement_type, collapse = ', ')}"))
  stopifnot("bounds delete accounting" = n_del == sum(bad$n_bad))
  invisible(bad)
}
