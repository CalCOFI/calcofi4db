# The seasonal climatology every anomaly is a departure from (2026-08-31).
#
# Three consumers each kept their own baseline and had drifted apart without anyone noticing:
# ctd-transects pooled one arbitrary cast per grid cell into 5 m bins over 1993-2013 by calendar
# month; the explorer pooled every observation of every month of whatever year range the slider
# held; calcofi4r::cc_climatology() was a third implementation the first two claimed to mirror.
# Line 90 in July 2026 came out +1.4 degC in one product and looked like nothing in the other, for
# reasons that had little to do with the data. The release now ships ONE `climatology` table and
# every product subtracts it.

#' Build the release's `climatology` table
#'
#' A plain mean per **dataset, station, calendar month, 10 m depth bin and measurement type** over
#' the env realm of `obs` across a fixed window of years — the baseline every CalCOFI anomaly
#' (ctd-transects, the CalCOFI Explorer's Sections lens, [calcofi4r::cc_climatology()]) is a
#' departure from. Written once at release time so the products cannot disagree.
#'
#' Why each part of the grain:
#' - **Calendar month.** Quarterly-ish cruises over decades give many *years* per calendar month at
#'   a station but only a handful of days, so month is the finest season CalCOFI supports — and the
#'   coarsest that works: a baseline pooled over all months is a map of the seasonal cycle, not an
#'   anomaly (line 90 surface: January 15.2, July 18.3, annual mean 16.8 degC; at 50–100 m the
#'   sign flips). A plain mean rather than harmonics: Rudnick et al. (2017) fit annual and
#'   semiannual harmonics for the CUGN glider climatology, which suits continuous glider
#'   sampling; CalCOFI's is episodic, and a monthly mean is something a reader can state exactly.
#' - **10 m floor bins** — `floor(depth_min_m / 10) * 10`, the `obs_env.depth_bin` convention,
#'   labelled by the shallow edge. `obs` carries the *thinned* CTD series (a 10 m grid plus RDP
#'   inflection points plus bottle depths), so at 5 m the off-grid bins hold about a third of the
#'   casts, sampled exactly where the profile bends, and their means sit visibly off their
#'   neighbours'. Every 10 m bin holds every cast.
#' - **The window** (`yr_min`–`yr_max`, default 1993–2013: Rasmus Swalethorp's CCIEA request; the
#'   Wilkinson archive fills 1993–2002 so it does not quietly mean "1998 plus 2003–2013"). 21 years
#'   with both phases of the 1997–99 ENSO inside it, ending before the 2014–16 marine heatwave so
#'   the heatwave and everything after read as departures. Not a WMO normal. The bounds are stamped
#'   on every row (`clim_yr_min`, `clim_yr_max`), so a consumer reading the parquet alone knows what
#'   the mean is a mean of.
#' - **A floor in cruises, not observations** (`min_cruises`, default 3). A grid cell can hold
#'   several stations' casts from one cruise (`st30-ln90` holds 90.30, 90.28, 90.27.7 and
#'   88.5/30.1), so an observation floor is met by one lucky cruise. `clim_n` and `n_cruises` both
#'   ship, so a thin cell is visible rather than silently trusted.
#' - **One row per dataset.** A CTD-only consumer filters `dataset_key`; a cross-dataset one (the
#'   explorer's "temperature" = bottle `temperature` + CTD `temperature_ave`) pools rows weighted by
#'   `clim_n` — `sum(clim_mean * clim_n) / sum(clim_n)` is exactly the mean over the pooled
#'   observations, so the two ways of reading the table cannot disagree either.
#' - **`clim_mean`/`clim_sd` are rounded to `round_digits` (default 6 decimal places).** DuckDB's
#'   `avg()`/`stddev_samp()` are computed by combining per-thread partial sums in parallel, and
#'   floating-point addition is not associative — the combine order (and so the last 1-2 bits of the
#'   double) varies run to run with no change to the input rows. Measured on a 200-cell/1.6M-row
#'   synthetic fixture: `clim_n` and `n_cruises` (integer) were identical across repeated builds, but
#'   `clim_mean`/`clim_sd` differed on distinct runs with |relative diff| up to ~1.8e-16 (machine
#'   epsilon) — this is what turned 60 of 71 real `climatology` partitions non-reproducible. Six
#'   decimal places is a fixed point ~1e9 times coarser than that noise floor (safe up to values on
#'   the order of 1e9, far beyond any CalCOFI variable's range) while being far finer than any
#'   instrument's resolution (CTD temperature/salinity ship to 3-4 decimal places, bottle nutrients
#'   to 2-3, oxygen to 2) — so rounding this way discards only reproducibility-breaking noise, never
#'   scientifically meaningful precision. Applied to the finished aggregate, so it is a pure function
#'   of the (order-independent) value, not of how it was summed.
#'
#' Rows without a station (`grid_key`), a time or a depth, non-finite values, and values the quality
#' predicate rejects are left out; nothing is interpolated. A cell that is absent has no baseline —
#' a consumer must leave its anomaly blank, never 0.
#'
#' @param con DuckDB connection holding `obs` (with `realm`, `grid_key`, `datetime`, `depth_min_m`,
#'   `measurement_value`, `measurement_qual`, `cruise_key`, `dataset_key`, `measurement_type`).
#' @param qual_ok_sql the quality predicate over alias `o` — `calcofi4r::cc_qual_ok_sql("o")`,
#'   passed in so this package never carries a second copy of it.
#' @param yr_min,yr_max the baseline window, inclusive years.
#' @param min_cruises a cell needs this many distinct cruises to be a baseline at all.
#' @param depth_bin_m bin width in metres (floor bins, labelled by the shallow edge).
#' @param depth_max_m deepest bin kept (its shallow edge); the release's sections stop at 500 m.
#' @param round_digits decimal places `clim_mean`/`clim_sd` are rounded to — chosen to be far below
#'   sensor resolution but well above the parallel-aggregation floating-point noise floor (see
#'   above), so the table is byte-identical across re-exports of unchanged data.
#' @param tbl output table (default `climatology`).
#' @return Invisibly, the row count.
#' @examples
#' \dontrun{
#' n <- build_climatology(con, qual_ok_sql = calcofi4r::cc_qual_ok_sql("o"))
#' DBI::dbGetQuery(con, "SELECT * FROM climatology WHERE grid_key = 'st60-ln90' AND month = 7")
#' }
#' @export
#' @concept release
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
build_climatology <- function(con, qual_ok_sql, yr_min = 1993L, yr_max = 2013L, min_cruises = 3L,
                              depth_bin_m = 10L, depth_max_m = 500L, round_digits = 6L,
                              tbl = "climatology") {
  stopifnot(is.character(qual_ok_sql), length(qual_ok_sql) == 1, nzchar(qual_ok_sql),
            is.numeric(yr_min), is.numeric(yr_max), length(yr_min) == 1, length(yr_max) == 1,
            yr_min <= yr_max, is.numeric(min_cruises), min_cruises >= 1,
            is.numeric(depth_bin_m), depth_bin_m > 0, is.numeric(depth_max_m), depth_max_m >= 0,
            is.numeric(round_digits), length(round_digits) == 1, round_digits >= 0)
  yr_min <- as.integer(yr_min); yr_max <- as.integer(yr_max)
  min_cruises  <- as.integer(min_cruises); depth_bin_m <- as.integer(depth_bin_m)
  round_digits <- as.integer(round_digits)
  dbExecute(con, glue("
    CREATE OR REPLACE TABLE {tbl} AS
    SELECT o.dataset_key, o.grid_key,
           month(o.datetime)::TINYINT                                  AS month,
           (floor(o.depth_min_m / {depth_bin_m}) * {depth_bin_m})::INTEGER AS depth_bin,
           o.measurement_type,
           round(avg(o.measurement_value), {round_digits})             AS clim_mean,
           round(stddev_samp(o.measurement_value), {round_digits})     AS clim_sd,
           count(*)::INTEGER                                           AS clim_n,
           count(DISTINCT o.cruise_key)::INTEGER                       AS n_cruises,
           {yr_min}::SMALLINT                                          AS clim_yr_min,
           {yr_max}::SMALLINT                                          AS clim_yr_max
    FROM obs o
    WHERE o.realm = 'env'
      AND o.grid_key IS NOT NULL AND o.datetime IS NOT NULL
      AND o.depth_min_m IS NOT NULL AND o.depth_min_m >= 0
      AND o.depth_min_m < {depth_max_m} + {depth_bin_m}
      AND o.measurement_value IS NOT NULL AND isfinite(o.measurement_value)
      AND year(o.datetime) BETWEEN {yr_min} AND {yr_max}
      AND ({qual_ok_sql})
    GROUP BY ALL
    HAVING count(DISTINCT o.cruise_key) >= {min_cruises}"))
  n <- dbGetQuery(con, glue("SELECT count(*) AS n FROM {tbl}"))$n
  invisible(n)
}
