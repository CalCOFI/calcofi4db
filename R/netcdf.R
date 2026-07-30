# netcdf.R — dataset-agnostic planning for CF / netCDF-4 publication.
#
# WHY THIS EXISTS
#
# Publishing a dataset as netCDF needs two things the normalized core does not
# state directly: the SHAPE of the file (a flat CF profile vs a nested netCDF-4
# group hierarchy) and, if nested, the ORDER of the levels. Historically each
# dataset answered both by hand, in its own `publish_{dataset}_to-netcdf.qmd`:
# ichthyo hardcoded site -> tow -> net -> occurrence -> bin, ctd-cast hardcoded a
# single cast level. `libs/publish_netcdf.R` justified that with "the nesting
# differs per dataset, which is why these are notebooks rather than one generic
# script."
#
# That justification predates the consolidated core. Since every ingest projects
# into `sample` with `sample_type` + `parent_sample_key`, **the nesting is now
# data, not code** — an adjacency list that can be walked. These functions walk
# it, so one publish step can serve every dataset.
#
# Per-dataset hardcoding is not merely repetitive, it is a bug surface: the
# ctd-cast full-resolution file shipped with 32 of 54 measurement types because
# that notebook inferred its variable list from a single cruise partition. A
# shared planner that reports what it found makes that class of silent truncation
# visible.

#' Discover a dataset's sampling hierarchy from the core `sample` table
#'
#' Walks the `parent_sample_key` adjacency list to recover the sampling levels
#' and their nesting order, without any per-dataset configuration. This is the
#' generic replacement for the hand-written level lists in the per-dataset
#' `publish_*_to-netcdf.qmd` notebooks.
#'
#' Levels are returned in **topological order** (roots first), which is the order
#' a netCDF-4 file must define them in: a child's `parent_index` points into its
#' parent's dimension, so the parent must already exist.
#'
#' @param con DuckDB connection carrying a core `sample` table
#' @param dataset_key Dataset provenance stamp, e.g. `"swfsc_ichthyo"`
#'
#' @return A tibble, one row per `sample_type`, ordered root-first:
#'   \describe{
#'     \item{sample_type}{the level name}
#'     \item{n}{rows at this level}
#'     \item{parent_sample_type}{the parent level, or `NA` for a root}
#'     \item{depth}{0 for a root, 1 for its children, …}
#'     \item{n_orphan}{rows whose `parent_sample_key` does not resolve}
#'   }
#'   Returns a zero-row tibble when the dataset has no `sample` rows.
#'
#' @details
#' A level's parent is determined by majority vote over the resolved parents of
#' its rows: `sample_type` is a categorical label, and a single mislabelled row
#' should not invent a whole extra level. Rows whose parent does not resolve are
#' counted in `n_orphan` rather than dropped, because an orphan is a data problem
#' the caller must see — silently discarding it is how a level's row count stops
#' matching the table it came from.
#'
#' @examples
#' \dontrun{
#' con <- cc_get_db()
#' discover_sample_levels(con, "swfsc_ichthyo")
#' #> sample_type n      parent_sample_type depth n_orphan
#' #> site        13108  NA                     0        0
#' #> tow         26216  site                   1        0
#' #> net         52432  tow                    2        0
#' }
#' @export
#' @concept publish
#' @importFrom glue glue
discover_sample_levels <- function(con, dataset_key) {
  stopifnot(is.character(dataset_key), length(dataset_key) == 1)

  lv <- DBI::dbGetQuery(con, glue::glue("
    SELECT sample_type, COUNT(*) AS n
    FROM sample WHERE dataset_key = '{dataset_key}'
    GROUP BY 1 ORDER BY 1"))
  if (!nrow(lv)) {
    return(tibble::tibble(
      sample_type = character(), n = integer(),
      parent_sample_type = character(), depth = integer(), n_orphan = integer()))
  }

  # majority-vote parent per level + unresolved-parent count
  edges <- DBI::dbGetQuery(con, glue::glue("
    WITH s AS (SELECT * FROM sample WHERE dataset_key = '{dataset_key}')
    SELECT c.sample_type,
           p.sample_type                                   AS parent_sample_type,
           COUNT(*)                                        AS n
    FROM s c LEFT JOIN sample p ON c.parent_sample_key = p.sample_key
    WHERE c.parent_sample_key IS NOT NULL
    GROUP BY 1, 2"))

  orphan <- edges[is.na(edges$parent_sample_type), c("sample_type", "n")]
  named  <- edges[!is.na(edges$parent_sample_type), ]

  parent_of <- stats::setNames(rep(NA_character_, nrow(lv)), lv$sample_type)
  for (st in unique(named$sample_type)) {
    cand <- named[named$sample_type == st, ]
    parent_of[[st]] <- cand$parent_sample_type[which.max(cand$n)]
  }

  # a self-referential level (parent == itself) is not a nesting level: it is a
  # within-level chain, and treating it as its own parent would loop forever
  self <- !is.na(parent_of) & parent_of == names(parent_of)
  parent_of[self] <- NA_character_

  # topological depth by walking up; a cycle would hang, so bound the walk
  depth_of <- vapply(names(parent_of), function(st) {
    d <- 0L; cur <- st
    while (!is.na(parent_of[[cur]]) && d <= length(parent_of)) {
      cur <- parent_of[[cur]]; d <- d + 1L
    }
    if (d > length(parent_of)) NA_integer_ else d
  }, integer(1))
  if (anyNA(depth_of)) {
    stop(glue::glue(
      "cycle in sample_type hierarchy for {dataset_key}: ",
      "{paste(names(depth_of)[is.na(depth_of)], collapse = ', ')}"))
  }

  out <- tibble::tibble(
    sample_type        = lv$sample_type,
    n                  = as.integer(lv$n),
    parent_sample_type = unname(parent_of[lv$sample_type]),
    depth              = unname(depth_of[lv$sample_type]),
    n_orphan           = as.integer(
      orphan$n[match(lv$sample_type, orphan$sample_type)]))
  out$n_orphan[is.na(out$n_orphan)] <- 0L
  out[order(out$depth, out$sample_type), ]
}

#' Plan the netCDF shape for a dataset
#'
#' Decides whether a dataset publishes as a **flat CF Discrete Sampling Geometry
#' profile** or as a **nested netCDF-4 group hierarchy**, and enumerates the
#' variable groups either way. Replaces the per-dataset judgement previously
#' baked into each `publish_*_to-netcdf.qmd`.
#'
#' @param con DuckDB connection carrying the core tables
#' @param dataset_key Dataset provenance stamp
#' @param obs_tbl Observation table to plan from (default `"obs"`); pass
#'   `"obs_ctd_full"` to plan the supplemental full-resolution CTD scans.
#'
#' @return A list with:
#'   \describe{
#'     \item{dataset_key, obs_tbl}{echoed inputs}
#'     \item{levels}{the [discover_sample_levels()] tibble}
#'     \item{shape}{`"profile"` or `"groups"`}
#'     \item{feature_type}{`"profile"` for CF DSG, else `NA`}
#'     \item{has_depth_axis}{whether `obs` carries a usable depth axis}
#'     \item{measurement_types}{every `measurement_type` in `obs_tbl`, the union
#'       across ALL partitions}
#'     \item{attribute_types}{`obs_attribute` measurement types, each of which
#'       becomes its own group (they carry different units)}
#'     \item{effort_types}{`sample_measurement` types, widened onto their level}
#'   }
#'
#' @details
#' **Shape rule.** One sampling level plus a depth axis is exactly a CF profile,
#' so it is emitted as one (`featureType=profile`, contiguous ragged array) and
#' needs no extension to the standard. More than one level has no CF feature
#' type, so it becomes netCDF-4 groups with explicit `parent_index` links. Being
#' explicit about which half of that split a file falls in is what lets the file
#' claim CF compliance honestly rather than approximately.
#'
#' **`measurement_types` is a union, deliberately.** Sampling one partition is
#' how `ctd-cast_full.nc` came to declare 32 of 54 variables: bottle nutrients
#' were not folded into the CTD files until 2008, so the alphabetically-first
#' cruise (1998) simply had no column for them, and every later-introduced type
#' was silently dropped from a file advertised as full resolution.
#'
#' @examples
#' \dontrun{
#' con <- cc_get_db()
#' plan_dataset_netcdf(con, "calcofi_ctd-cast")$shape   # "profile"
#' plan_dataset_netcdf(con, "swfsc_ichthyo")$shape      # "groups"
#' }
#' @export
#' @concept publish
#' @importFrom glue glue
plan_dataset_netcdf <- function(con, dataset_key, obs_tbl = "obs") {
  stopifnot(is.character(obs_tbl), length(obs_tbl) == 1)
  tbls <- DBI::dbListTables(con)
  if (!obs_tbl %in% tbls) stop(glue::glue("table '{obs_tbl}' not found"))

  levels <- discover_sample_levels(con, dataset_key)

  distinct_of <- function(tbl, col, where = "") {
    if (!tbl %in% tbls) return(character())
    DBI::dbGetQuery(con, glue::glue(
      "SELECT DISTINCT {col} AS v FROM {tbl}
        WHERE dataset_key = '{dataset_key}' {where} ORDER BY 1"))$v
  }

  meas <- distinct_of(obs_tbl, "measurement_type")
  attr_types <- distinct_of("obs_attribute", "measurement_type")
  eff_types  <- distinct_of("sample_measurement", "measurement_type")

  depth_probe <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(depth_min_m) AS n_depth FROM {obs_tbl}
      WHERE dataset_key = '{dataset_key}'"))$n_depth
  has_depth <- isTRUE(depth_probe > 0)

  shape <- if (nrow(levels) <= 1 && has_depth) "profile" else "groups"

  list(
    dataset_key       = dataset_key,
    obs_tbl           = obs_tbl,
    levels            = levels,
    shape             = shape,
    feature_type      = if (shape == "profile") "profile" else NA_character_,
    has_depth_axis    = has_depth,
    measurement_types = meas,
    attribute_types   = attr_types,
    effort_types      = eff_types)
}

#' Summarise a netCDF plan as one row
#'
#' Convenience for the generic publish notebook, which reports a table of every
#' dataset it is about to write.
#'
#' @param plan A [plan_dataset_netcdf()] result
#' @return A one-row tibble
#' @export
#' @concept publish
summarise_netcdf_plan <- function(plan) {
  tibble::tibble(
    dataset_key   = plan$dataset_key,
    obs_tbl       = plan$obs_tbl,
    shape         = plan$shape,
    levels        = paste(plan$levels$sample_type, collapse = " -> "),
    n_levels      = nrow(plan$levels),
    n_meas_types  = length(plan$measurement_types),
    n_attr_groups = length(plan$attribute_types),
    n_effort      = length(plan$effort_types),
    orphans       = sum(plan$levels$n_orphan))
}
