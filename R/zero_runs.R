# a run of exact zeros inside a cast ------------------------------------------------
#
# A whole-cast test (check_depth_constant_series()) cannot see a series that varies and
# then holds 0 for hundreds of metres: the CTD team's station-corrected oxygen ramps to 0
# and stays there when the per-cast regression fails (2408SR cast 033: sensor 26 umol/kg
# at 322 m, Ox1uM_StaCorr 0.0 from 322 to 517 m), and an exact 0 that enters a
# sensor-pair average halves it.

#' Runs of exact zeros within a cast
#'
#' Finds each run of **exact zeros** in a series within one cast: consecutive values,
#' in depth order, that are all `0`, hold at least `min_n` finite values and span at
#' least `min_span_m` metres. A non-zero value ends a run; so does a step in depth
#' larger than `max_gap_m` between two zeros (a missing bin or two does not). It tests
#' a different thing from [check_depth_constant_series()], which asks whether the
#' **whole** cast holds one value: a cast of zeros only is found by both, so a caller
#' that withholds both applies that one first and this one to what remains. Only an
#' exact `0` counts: a series that approaches zero (0.003, 0.001) is a measurement.
#'
#' Which series to test is the caller's decision, and it matters. An exact 0 is a fill
#' or a failed regression where the quantity cannot be zero (oxygen at 500 m off
#' California beside a sensor reading 15-30 umol/kg), but it can be a real estimate
#' clipped at zero where the quantity truly vanishes (nitrate in a depleted surface
#' layer, chlorophyll below the chlorophyll layer); pass `types` accordingly.
#'
#' @inheritParams check_depth_constant_series
#' @param min_n minimum values in a run (default 6).
#' @param min_span_m minimum depth span of a run, metres (default 50).
#' @param max_gap_m largest depth step, metres, between two zeros of one run
#'   (default 5: CTD bins are 1 m, and a step of 2-5 m is a scan or two removed).
#'
#' @return A [tibble][tibble::tibble], one row per run, ordered by type, cast and
#'   depth: the `cast_col` column(s), `measurement_type`, `run` (1, 2, ... within the
#'   cast and type), `n`, `depth_min_m`, `depth_max_m`, `span_m`. Zero rows when none.
#'   Every value of a cast and type at a depth in `[depth_min_m, depth_max_m]` that
#'   is exactly 0 belongs to the run (a non-zero value there would have ended it).
#' @export
#' @concept check
#' @seealso [check_depth_constant_series()] for a series constant over the whole cast.
#' @importFrom DBI dbGetQuery dbListFields dbWriteTable dbDisconnect
#' @importFrom glue glue
#' @importFrom tibble as_tibble
#' @importFrom stats ave
#' @examples
#' d <- data.frame(cast = "a", measurement_type = "oxygen_sta_corr",
#'   depth_m = 0:120, measurement_value = c(seq(250, 30, length.out = 60), rep(0, 61)))
#' check_zero_runs(d, cast_col = "cast")
check_zero_runs <- function(x,
                            tbl         = "ctd_measurement",
                            cast_col    = "ctd_cast_uuid",
                            type_col    = "measurement_type",
                            value_col   = "measurement_value",
                            depth_col   = "depth_m",
                            types       = NULL,
                            min_n       = 6,
                            min_span_m  = 50,
                            max_gap_m   = 5) {
  stopifnot(
    "`min_n` must be one number >= 2" = is.numeric(min_n) && length(min_n) == 1 && min_n >= 2,
    "`min_span_m` must be one number >= 0" =
      is.numeric(min_span_m) && length(min_span_m) == 1 && min_span_m >= 0,
    "`max_gap_m` must be one number > 0" =
      is.numeric(max_gap_m) && length(max_gap_m) == 1 && max_gap_m > 0,
    "`cast_col` must name one or more columns" =
      is.character(cast_col) && length(cast_col) >= 1 && !anyNA(cast_col),
    "`cast_col` may not repeat `type_col`, `value_col` or `depth_col`" =
      !any(cast_col %in% c(type_col, value_col, depth_col)))

  con <- x
  if (is.data.frame(x)) {
    miss <- setdiff(c(cast_col, type_col, value_col, depth_col), names(x))
    if (length(miss))
      stop("data frame has no column(s): ", paste(miss, collapse = ", "), call. = FALSE)
    con <- get_duckdb_con(":memory:")
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    DBI::dbWriteTable(con, "x", as.data.frame(x))
    tbl <- "x"
  } else {
    stopifnot("`x` must be a DBI connection or a data frame" = inherits(x, "DBIConnection"))
    flds <- DBI::dbListFields(con, tbl)
    for (cl in c(cast_col, type_col, value_col, depth_col))
      if (!cl %in% flds)
        stop("`", tbl, "` has no column `", cl, "`", call. = FALSE)
  }

  qi <- function(v) paste0('"', gsub('"', '""', v, fixed = TRUE), '"')
  tc <- qi(type_col); vc <- qi(value_col); dc <- qi(depth_col)
  k_as  <- paste0(qi(cast_col), " AS k", seq_along(cast_col), collapse = ", ")
  k_grp <- paste0("k", seq_along(cast_col), collapse = ", ")
  where_t <- ""
  if (length(types)) {
    in_list <- paste0("'", .sql_esc(types), "'", collapse = ", ")
    where_t <- glue::glue("AND {tc} IN ({in_list})")
  }

  # gaps and islands over the depth-ordered finite values of each (cast, type): a run
  # starts at a zero whose predecessor is not a zero, or lies more than max_gap_m above
  r <- DBI::dbGetQuery(con, glue::glue("
    WITH v AS (
      SELECT {k_as}, {tc} AS measurement_type,
             CAST({dc} AS DOUBLE) AS depth_m, CAST({vc} AS DOUBLE) AS value
      FROM {tbl}
      WHERE {vc} IS NOT NULL AND {dc} IS NOT NULL
        AND isfinite(CAST({vc} AS DOUBLE)) AND isfinite(CAST({dc} AS DOUBLE))
        {where_t}),
    o AS (
      SELECT *, lag(value)   OVER w AS v_prev,
                lag(depth_m) OVER w AS d_prev
      FROM v WINDOW w AS (PARTITION BY {k_grp}, measurement_type ORDER BY depth_m, value)),
    f AS (
      SELECT *, CAST(value = 0 AND (v_prev IS NULL OR v_prev <> 0 OR
                                    depth_m - d_prev > {max_gap_m}) AS INTEGER) AS new_run
      FROM o),
    g AS (
      SELECT *, SUM(new_run) OVER (PARTITION BY {k_grp}, measurement_type
                                   ORDER BY depth_m, value ROWS UNBOUNDED PRECEDING) AS run_id
      FROM f)
    SELECT {k_grp}, measurement_type, run_id,
           COUNT(*) AS n, MIN(depth_m) AS depth_min_m, MAX(depth_m) AS depth_max_m,
           MAX(depth_m) - MIN(depth_m) AS span_m
    FROM g WHERE value = 0
    GROUP BY ALL
    HAVING COUNT(*) >= {min_n} AND MAX(depth_m) - MIN(depth_m) >= {min_span_m}"))

  keys <- r[paste0("k", seq_along(cast_col))]
  names(keys) <- cast_col
  out <- data.frame(keys, measurement_type = r$measurement_type,
                    n = as.numeric(r$n), depth_min_m = r$depth_min_m,
                    depth_max_m = r$depth_max_m, span_m = r$span_m,
                    stringsAsFactors = FALSE, check.names = FALSE)
  out <- out[do.call(order, c(list(out$measurement_type), unname(as.list(out[cast_col])),
                              list(out$depth_min_m))), , drop = FALSE]
  # number the reported runs 1, 2, ... within each cast and type
  grp <- do.call(paste, c(unname(as.list(out[cast_col])), list(out$measurement_type), sep = "\r"))
  run <- if (nrow(out)) as.integer(stats::ave(seq_along(grp), grp, FUN = seq_along)) else integer()
  out <- data.frame(out[c(cast_col, "measurement_type")], run = run,
                    out[c("n", "depth_min_m", "depth_max_m", "span_m")],
                    check.names = FALSE)
  rownames(out) <- NULL
  tibble::as_tibble(out)
}
