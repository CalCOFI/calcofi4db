# a derived series that does not vary with depth ------------------------------------
#
# A value that is physically possible and carries no flag can still not be a profile.
# The CTD provider column `EstNO3_CruiseCorr` holds ONE value over the whole cast on 441
# of 5,066 judged casts (every judged cast of 2504SH, 2304SH, 2307SR and four more cruises;
# census 2026-10-01), mostly exact 0, while `EstNO3_StaCorr` on the same casts rises
# ~1 -> 40 uM as nitrate should: a fill or an offset, not a profile. A bound cannot see it (the value is
# in range) and neither can a flag (the provider set none), so the test is on the
# SHAPE of the series: many values over a real depth span that do not move.

#' Casts whose series is constant over depth
#'
#' Finds each (cast, measurement type) whose values are **identical at every depth**
#' although the series was measured at many depths over a real span. A measured or
#' modelled profile of temperature, nitrate, oxygen or chlorophyll does not do that;
#' a per-cast offset or intercept written where a profile belongs does. Neither a
#' declared bound ([check_measurement_bounds()]) nor a provider flag can catch it,
#' because the value is physically possible and unflagged.
#'
#' A cast is **judged** only when it has at least `min_n` finite values spanning at
#' least `min_span_m` metres; a short cast (a bottle cast with three depths, a
#' surface-only series) is never flagged, because a constant over two bottles proves
#' nothing. A judged cast is **constant** when its values' range is below `tol`.
#'
#' @param x a `DBIConnection` (then `tbl` names the long table) or a data frame with
#'   the long-format columns below.
#' @param tbl table name on `con`; ignored when `x` is a data frame.
#' @param cast_col,type_col,value_col,depth_col column names: the cast key, the
#'   measurement type, the value and the depth in metres. The defaults are the
#'   CTD ingest's `ctd_measurement`. `cast_col` may name **several** columns, which
#'   together identify a cast (e.g. `c("cruise_key", "cast_key", "cast_dir")`). Group
#'   by the real cast: a key that is unique per depth scan (the CTD ingest's
#'   `ctd_cast_uuid` hashes the scan's time) gives one value per group, so nothing is
#'   ever judged.
#' @param types optional character vector of `measurement_type`s to judge; the
#'   default `NULL` judges every type present.
#' @param min_n minimum finite values on a cast for it to be judged (default 6).
#' @param min_span_m minimum depth span, metres, for it to be judged (default 50).
#' @param tol the series is constant when `max - min` is below this (default 1e-9).
#'
#' @return A [tibble][tibble::tibble], one row per constant (cast, type), ordered by
#'   type then cast: the `cast_col` column(s), `measurement_type`, `n` (finite values),
#'   `depth_min_m`, `depth_max_m`, `span_m`, `value` (the one value it holds). Zero
#'   rows when nothing is constant. The `judged` attribute is a named integer vector: per
#'   type, how many (cast, type) pairs met `min_n` and `min_span_m`, so a caller can
#'   report "constant on 14 of 14".
#' @export
#' @concept check
#' @seealso [check_measurement_bounds()] for the value's physical range.
#' @importFrom DBI dbGetQuery dbListFields dbWriteTable dbDisconnect
#' @importFrom glue glue
#' @importFrom tibble as_tibble
#' @examples
#' d <- data.frame(
#'   cast = rep(c("a", "b"), each = 8), measurement_type = "est_nitrate",
#'   depth_m = rep(seq(0, 350, by = 50), 2),
#'   measurement_value = c(rep(12.5, 8), seq(1, 40, length.out = 8)))
#' check_depth_constant_series(d, cast_col = "cast")
check_depth_constant_series <- function(x,
                                        tbl         = "ctd_measurement",
                                        cast_col    = "ctd_cast_uuid",
                                        type_col    = "measurement_type",
                                        value_col   = "measurement_value",
                                        depth_col   = "depth_m",
                                        types       = NULL,
                                        min_n       = 6,
                                        min_span_m  = 50,
                                        tol         = 1e-9) {
  stopifnot(
    "`min_n` must be one number >= 2" = is.numeric(min_n) && length(min_n) == 1 && min_n >= 2,
    "`min_span_m` must be one number >= 0" =
      is.numeric(min_span_m) && length(min_span_m) == 1 && min_span_m >= 0,
    "`tol` must be one number >= 0" = is.numeric(tol) && length(tol) == 1 && tol >= 0,
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

  # quote the column names: `cast` is a reserved word, and a caller's key may be anything
  qi <- function(v) paste0('"', gsub('"', '""', v, fixed = TRUE), '"')
  tc <- qi(type_col); vc <- qi(value_col); dc <- qi(depth_col)
  # the cast key, one or several columns, aliased k1..kn so no name can collide
  k_as  <- paste0(qi(cast_col), " AS k", seq_along(cast_col), collapse = ", ")
  k_grp <- paste0("k", seq_along(cast_col), collapse = ", ")

  where_t <- ""
  if (length(types)) {
    in_list <- paste0("'", .sql_esc(types), "'", collapse = ", ")
    where_t <- glue::glue("AND {tc} IN ({in_list})")
  }

  # one pass: per (cast, type) the finite-value count, depth span and value range;
  # NaN / +-Inf are not values (isnan() survives IS NOT NULL, see the release rules)
  g <- DBI::dbGetQuery(con, glue::glue("
    WITH v AS (
      SELECT {k_as}, {tc} AS measurement_type,
             CAST({dc} AS DOUBLE) AS depth_m, CAST({vc} AS DOUBLE) AS value
      FROM {tbl}
      WHERE {vc} IS NOT NULL AND {dc} IS NOT NULL
        AND isfinite(CAST({vc} AS DOUBLE)) AND isfinite(CAST({dc} AS DOUBLE))
        {where_t})
    SELECT {k_grp}, measurement_type,
           COUNT(*)            AS n,
           MIN(depth_m)        AS depth_min_m,
           MAX(depth_m)        AS depth_max_m,
           MAX(depth_m) - MIN(depth_m) AS span_m,
           MIN(value)          AS v_min,
           MAX(value)          AS v_max
    FROM v GROUP BY ALL"))

  judged <- g[g$n >= min_n & g$span_m >= min_span_m, , drop = FALSE]
  const  <- judged[(judged$v_max - judged$v_min) < tol, , drop = FALSE]

  keys <- const[paste0("k", seq_along(cast_col))]
  names(keys) <- cast_col
  out <- data.frame(keys, measurement_type = const$measurement_type, n = as.numeric(const$n),
                    depth_min_m = const$depth_min_m, depth_max_m = const$depth_max_m,
                    span_m = const$span_m, value = const$v_min,
                    stringsAsFactors = FALSE, check.names = FALSE)
  out <- out[do.call(order, c(list(out$measurement_type), unname(as.list(out[cast_col])))), ,
             drop = FALSE]
  rownames(out) <- NULL

  out <- tibble::as_tibble(out)
  attr(out, "judged") <- vapply(split(judged$n, judged$measurement_type), length, integer(1))
  out
}
