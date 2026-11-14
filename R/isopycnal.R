# the released isopycnal table: conditions on sigma-theta surfaces for every CTD down cast, built by
# calling calcofi4r::cc_isopycnal() (the one implementation of the rule) cruise by cruise ----------

#' Build the release's isopycnal table: conditions on density surfaces, every CTD down cast
#'
#' Calls [calcofi4r::cc_isopycnal()] — the one implementation of the isopycnal rule, never a copy of
#' it — over every `calcofi_ctd-cast` cast in the connection, `batch` cruises at a time (the down
#' cast of an occupation is chosen within its cruise, so batching changes nothing), and writes the
#' result as one table in the release's conventions: one row per cast x surface x type, the type
#' named by [calcofi4r::cc_isopycnal_type()] (`iso_` + `cc_isopycnal()`'s variable: `iso_depth_m`,
#' `iso_temperature_ave`, `iso_salinity_ave_corr`, `iso_oxygen_ml_l_ave_sta_corr`,
#' `iso_est_nitrate_sta_corr`, `iso_spiciness0`), so its rows can never be mistaken for an `obs`
#' series of the same quantity at a depth.
#'
#' Asked for by Rasmus Swalethorp (2026-11-12/13): sigma-theta 24.0 to 27.0 by 0.2 plus 25.5 and
#' 26.5, the shallowest crossing, depth / temperature / salinity / oxygen / estimated nitrate / spice
#' on each surface, down casts, from the 1 m bins. Those are `cc_isopycnal()`'s defaults, which this
#' function passes through unchanged unless told otherwise.
#'
#' Every cast x surface keeps its `iso_depth_m` row whatever its `status` (`"ok"`, `"at_surface"`,
#' `"no_bracket"`, `"not_reached"`, `"no_data"`; `measurement_value` NULL unless `"ok"`), so a
#' surface that outcrops or is never reached stays distinguishable from a missing cast; a
#' property's row is present only where the surface is `"ok"` and the property has a value there.
#' The table carries no `measurement_qual`: every value is interpolated from bins the provider does
#' not flag 8/9, after the sensor-pair rule.
#'
#' [build_climatology()]`(isopycnal_tbl = )` pools it into the surface baseline.
#'
#' @param con DuckDB connection holding `obs_ctd_full`, `sample` and `cruise` (with
#'   `month_median` / `date_median`, [add_cruise_median_date()]).
#' @param tbl output table (default `"isopycnal"`).
#' @param sigma,variables,direction,surface_max_m,min_n passed to [calcofi4r::cc_isopycnal()];
#'   `NULL` keeps its default (the provider's list).
#' @param batch cruises per call.
#' @param dataset_key the CTD dataset whose casts are read.
#' @return Invisibly, the row count of `tbl`. Columns: `dataset_key`, `sample_key`, `cruise_key`,
#'   `site_key`, `grid_key`, `longitude`, `latitude`, `datetime`, `data_stage`, `year`, `month`
#'   (the cruise's), `sigma_theta`, `status`, `measurement_type`, `measurement_value`.
#' @export
#' @concept release
#' @importFrom DBI dbExecute dbGetQuery dbWriteTable dbListTables
#' @importFrom glue glue
build_isopycnal <- function(con, tbl = "isopycnal", sigma = NULL, variables = NULL,
                            direction = "down", surface_max_m = NULL, min_n = NULL,
                            batch = 20L, dataset_key = "calcofi_ctd-cast") {
  if (!requireNamespace("calcofi4r", quietly = TRUE) ||
      utils::packageVersion("calcofi4r") < "1.34.0")
    stop("build_isopycnal() calls calcofi4r::cc_isopycnal() and cc_isopycnal_type(): install ",
         "calcofi4r >= 1.34.0", call. = FALSE)
  stopifnot(is.character(tbl), length(tbl) == 1, nzchar(tbl),
            is.numeric(batch), length(batch) == 1, batch >= 1,
            all(c("obs_ctd_full", "sample") %in% DBI::dbListTables(con)))
  cruises <- DBI::dbGetQuery(con, glue(
    "SELECT DISTINCT cruise_key FROM sample
     WHERE dataset_key = '{dataset_key}' AND sample_type = 'cast' AND cruise_key IS NOT NULL
     ORDER BY 1"))$cruise_key
  args <- Filter(Negate(is.null), list(
    sigma = sigma, variables = variables, direction = direction,
    surface_max_m = surface_max_m, min_n = min_n))
  groups <- split(cruises, ceiling(seq_along(cruises) / batch))
  out <- lapply(groups, function(cr)
    do.call(calcofi4r::cc_isopycnal, c(list(con = con, cruise_key = cr), args)))
  iso <- do.call(rbind, out)
  if (is.null(iso)) iso <- calcofi4r::cc_isopycnal(con, cruise_key = character(0))

  iso <- data.frame(
    dataset_key       = rep(dataset_key, nrow(iso)),
    sample_key        = iso$sample_key,
    cruise_key        = iso$cruise_key,
    site_key          = iso$site_key,
    grid_key          = iso$grid_key,
    longitude         = iso$longitude,
    latitude          = iso$latitude,
    datetime          = iso$datetime,
    data_stage        = iso$data_stage,
    year              = as.integer(iso$year),
    month             = as.integer(iso$month),
    sigma_theta       = as.numeric(iso$sigma_theta),
    status            = iso$status,
    measurement_type  = calcofi4r::cc_isopycnal_type(iso$variable),
    measurement_value = as.numeric(iso$value),
    stringsAsFactors  = FALSE)
  stopifnot(
    "build_isopycnal(): one row per (sample_key, sigma_theta, measurement_type)" =
      !anyDuplicated(iso[c("sample_key", "sigma_theta", "measurement_type")]),
    "build_isopycnal(): every cast x surface keeps its iso_depth_m row" =
      nrow(unique(iso[c("sample_key", "sigma_theta")])) ==
        sum(iso$measurement_type == calcofi4r::cc_isopycnal_type("depth_m")),
    "build_isopycnal(): only an \"ok\" surface carries a value" =
      all(is.na(iso$measurement_value[iso$status != "ok"])))
  DBI::dbWriteTable(con, tbl, iso, overwrite = TRUE)
  invisible(nrow(iso))
}
