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

# ---- WRITING -----------------------------------------------------------------
#
# The planner above answers "what shape is this dataset?". Everything below
# WRITES that shape. Both halves used to live in the workflows repo — the planner
# nowhere (each notebook decided by hand) and the writers in
# `libs/publish_netcdf.R`, sourced by notebooks. They are here because they are
# the scientific content of the product, not deploy plumbing: what units a
# variable claims, whether a child level double-counts its parent's effort, and
# whether a profile's ragged-array index is consistent are all assertable rules.
# The upload/index machinery (`cc_netcdf_publish()` and friends) stays in the
# workflows repo, where the GCS buckets and page skin live.

# ncdf4 needs the system netCDF library, so it is a Suggests: a wrangling-only
# install must not fail on it. Only the writers below require it.
.need_ncdf4 <- function() {
  if (!requireNamespace("ncdf4", quietly = TRUE))
    stop("the netCDF writers need the 'ncdf4' package (install.packages('ncdf4'))")
  invisible(TRUE)
}

# Blank-coalescing: treats NULL / length-0 / NA / "" as absent. The NA and ""
# tests apply ONLY to a length-1 atomic — calling is.na() on a var_meta list
# returns a vector, and `||` rejects that in R >= 4.3 with
# "'length = 4' in coercion to 'logical(1)'". Deliberately NOT the package's
# `%||%`, which coalesces on NULL alone; here an empty CSV cell must fall back
# too, and widening the shared operator would change unrelated call sites.
.nz <- function(a, b) {
  if (is.null(a) || length(a) == 0) return(b)
  if (length(a) == 1 && is.atomic(a) &&
      (is.na(a) || !nzchar(as.character(a)))) return(b)
  a
}

# netCDF default fill values. A double variable written with missval = this is
# what CF readers interpret as missing; the integer one is netCDF's own default.
NC_FILL_DOUBLE <- 9.969209968386869e36
NC_FILL_INT    <- -2147483647L

# Coordinate variables carry fixed CF units regardless of the registry: `time` is
# epoch seconds because that is what a numeric CF time axis means here, and
# lat/lon/depth units are not measurement-registry entries.
.NC_COORD_UNITS <- c(
  time      = "seconds since 1970-01-01T00:00:00Z",
  latitude  = "degrees_north",
  longitude = "degrees_east",
  depth     = "m")

#' CF variable metadata from the `measurement_type` registry
#'
#' Turns the canonical measurement registry into the per-variable lookup the
#' netCDF writers use: a variable needs one `units`, one `long_name` and
#' optionally one `standard_name`. This is the widening half of publishing —
#' the database stores every quantity in a single `measurement_value` column, so
#' the units live in the registry rather than on the value.
#'
#' @param mt data.frame from [read_measurement_type()]. Only
#'   `measurement_type` is required; `units`, `description`, `standard_name`,
#'   `is_canonical`, `valid_min` and `valid_max` are used when present.
#'
#' @return Named list keyed by `measurement_type`, each element
#'   `list(units, long_name, standard_name, canonical, valid_min, valid_max)`.
#'   An empty registry cell becomes `""` for `units` (never the string `"NA"`)
#'   and falls back to the type name for `long_name`, because a CF variable with
#'   `long_name = "NA"` is worse than one with no `long_name`.
#'
#' @export
#' @concept publish
#' @examples
#' mt <- data.frame(measurement_type = "temperature_ave", units = "degree_C",
#'                  description = "average temperature", stringsAsFactors = FALSE)
#' measurement_var_meta(mt)$temperature_ave$units
measurement_var_meta <- function(mt) {
  stopifnot(is.data.frame(mt), "measurement_type" %in% names(mt))
  col <- function(nm, i) if (nm %in% names(mt)) mt[[nm]][i] else NULL
  stats::setNames(lapply(seq_len(nrow(mt)), function(i) list(
    units         = as.character(.nz(col("units", i), "")),
    long_name     = as.character(.nz(col("description", i), mt$measurement_type[i])),
    standard_name = .nz(col("standard_name", i), NA_character_),
    canonical     = isTRUE(col("is_canonical", i)) ||
                    identical(as.character(col("is_canonical", i)), "TRUE"),
    valid_min     = suppressWarnings(as.numeric(.nz(col("valid_min", i), NA))),
    valid_max     = suppressWarnings(as.numeric(.nz(col("valid_max", i), NA)))
  )), mt$measurement_type)
}

# units for one variable: coordinate names win, then the registry, then blank
.nc_units <- function(nm, var_meta) {
  if (nm %in% names(.NC_COORD_UNITS)) return(unname(.NC_COORD_UNITS[[nm]]))
  as.character(.nz(var_meta[[nm]]$units, ""))
}

# ---- netCDF-4 groups: the nested (non-CF) shape -------------------------------

# GROUPS IN ncdf4 — verified 2026-07-28, ncdf4 1.24.
# ncdf4 exposes NO group API (there is no ncgrp_def, no group argument), which
# reads as "R cannot write netCDF-4 groups". It can: a SLASH-SEPARATED variable
# name creates a real group. Confirmed independently, not just by round-tripping
# through ncdf4 itself —
#   ncvar_def("tow/volume_sampled", ...)
#   ncdump -h ->  group: tow { double volume_sampled(tow_n) ; }
#   h5dump -n ->  group /tow ; dataset /tow/volume_sampled
# so it is a true HDF5/netCDF-4 group, not a variable with a slash in its name.
# Dimensions are defined at the root and referenced from any group, which is what
# lets a child level index into its parent.

#' Build the variable definitions for one level of a nested dataset
#'
#' Each sampling level becomes a netCDF-4 group. The link to the parent is an
#' explicit index variable, **not** repetition of the parent's columns — that is
#' the whole reason for netCDF-4 here rather than a flat table: tow effort is
#' stored once, and a length-frequency bin points at the tow it came from.
#' Flattening instead repeats each net's `volume_sampled` onto every one of its
#' size bins, which turned 76,512 real ichthyo values into 369,978 repeated ones
#' and inflated any naive `SUM()` of effort by ~5x.
#'
#' @param group Group name, e.g. `"tow"`, `"occurrence"`, `"length_bin"`.
#' @param df data.frame for this level, ordered so children are contiguous.
#' @param dim The `ncdim4` for this level.
#' @param parent_dim Parent level's `ncdim4`, or `NULL` at the root.
#' @param parent_index 1-based index into the parent level, one per row of `df`.
#' @param var_meta Named list from [measurement_var_meta()].
#' @param strlen Fixed character length for string variables.
#'
#' @return Named list of `ncvar4` objects to pass to `ncdf4::nc_create()`. The
#'   parent link, when present, is the element named `__parent_index`.
#' @export
#' @concept publish
nc_level_vars <- function(group, df, dim, parent_dim = NULL, parent_index = NULL,
                          var_meta = list(), strlen = 64L) {
  .need_ncdf4()
  stopifnot(is.data.frame(df))
  d_str <- ncdf4::ncdim_def(glue::glue("{group}_strlen"), "", seq_len(strlen),
                            create_dimvar = FALSE)
  vars <- list()
  for (nm in names(df)) {
    x  <- df[[nm]]
    nc_nm <- glue::glue("{group}/{nm}")
    vars[[nm]] <- if (is.character(x) || is.factor(x)) {
      ncdf4::ncvar_def(nc_nm, "", list(d_str, dim), prec = "char")
    } else if (is.integer(x)) {
      ncdf4::ncvar_def(nc_nm, .nc_units(nm, var_meta), dim,
                       prec = "integer", missval = NC_FILL_INT)
    } else {
      ncdf4::ncvar_def(nc_nm, .nc_units(nm, var_meta), dim,
                       prec = "double", missval = NC_FILL_DOUBLE)
    }
  }
  if (!is.null(parent_index) && !is.null(parent_dim))
    vars[["__parent_index"]] <- ncdf4::ncvar_def(
      glue::glue("{group}/parent_index"), "", dim, prec = "integer")
  vars
}

#' Write the data and attributes for one level defined by [nc_level_vars()]
#'
#' @param nc Open `ncdf4` handle.
#' @param group Group name, matching the [nc_level_vars()] call.
#' @param df The same data.frame passed to [nc_level_vars()].
#' @param vars The [nc_level_vars()] result.
#' @param parent_index 1-based parent index, or `NULL` at the root.
#' @param var_meta Named list from [measurement_var_meta()].
#' @param parent_group Name of the parent group, used in the `parent_index`
#'   documentation attributes.
#'
#' @return `TRUE`, invisibly.
#' @export
#' @concept publish
nc_level_put <- function(nc, group, df, vars, parent_index = NULL,
                         var_meta = list(), parent_group = NA_character_) {
  .need_ncdf4()
  for (nm in names(df)) {
    x <- df[[nm]]
    if (is.factor(x)) x <- as.character(x)
    # NA in a character column has no netCDF representation: writing it yields
    # the literal string "NA" inside the file, indistinguishable from real data.
    if (is.character(x)) x[is.na(x)] <- ""
    ncdf4::ncvar_put(nc, vars[[nm]], x)
    md <- var_meta[[nm]] %||% list()
    vn <- glue::glue("{group}/{nm}")
    ncdf4::ncatt_put(nc, vn, "long_name",
                     as.character(.nz(md$long_name, gsub("_", " ", nm))))
    sn <- .nz(md$standard_name, NA_character_)
    if (!is.na(sn)) ncdf4::ncatt_put(nc, vn, "standard_name", as.character(sn))
  }
  if (!is.null(parent_index) && !is.null(vars[["__parent_index"]])) {
    ncdf4::ncvar_put(nc, vars[["__parent_index"]], as.integer(parent_index))
    vn <- glue::glue("{group}/parent_index")
    ncdf4::ncatt_put(nc, vn, "long_name",
                     glue::glue("1-based index into the {parent_group} group"))
    ncdf4::ncatt_put(nc, vn, "comment", paste(
      "Explicit parent link. Each row belongs to the", parent_group,
      "record at this index; parent values are stored ONCE there rather than",
      "repeated here, so summing a parent-level quantity over this group would",
      "double-count."))
    ncdf4::ncatt_put(nc, vn, "instance_dimension", parent_group)
  }
  invisible(TRUE)
}

# ---- CF profile: the flat, fully-CF shape -------------------------------------

#' Define a CF Discrete-Sampling-Geometry profile file
#'
#' A single sampling level with a depth axis *is* a CF profile, so it is written
#' as one — `featureType=profile` with a contiguous ragged array — and needs no
#' extension to the standard. This defines the dimensions and variables; feed the
#' `vars` to `ncdf4::nc_create()` and then call [nc_profile_write()] one or more
#' times.
#'
#' Dimensions must be sized at creation time, which is why `n_profile`/`n_obs`
#' are arguments rather than being inferred from the data: a multi-hundred-million
#' row table is written in chunks, and a wrong guess means rewriting a multi-GB
#' file. Size them with a cheap counting pass first.
#'
#' @param n_profile Number of profiles (the instance dimension).
#' @param n_obs Total number of depth levels across all profiles.
#' @param profile_proto data.frame whose **columns and types** define the
#'   profile-level variables (the rows are ignored, so a zero-row frame or the
#'   full frame both work). Typically `profile_id`, `time`, `latitude`,
#'   `longitude` plus keys such as `cruise_key`.
#' @param obs_types Character vector of obs-level measurement variable names —
#'   each becomes its own double variable, which is the point of the widening.
#' @param var_meta Named list from [measurement_var_meta()].
#' @param strlen Fixed character length for string variables.
#'
#' @return `list(dims = list(profile, obs, strlen), vars = <named list>)`.
#'   `vars` always includes `rowSize` (the ragged-array index) and `depth`.
#' @export
#' @concept publish
nc_profile_def <- function(n_profile, n_obs, profile_proto, obs_types,
                           var_meta = list(), strlen = 64L) {
  .need_ncdf4()
  stopifnot(is.data.frame(profile_proto), n_profile >= 1, n_obs >= 1,
            !"depth" %in% names(profile_proto))
  d_prof <- ncdf4::ncdim_def("profile", "", seq_len(n_profile), create_dimvar = FALSE)
  d_obs  <- ncdf4::ncdim_def("obs",     "", seq_len(n_obs),     create_dimvar = FALSE)
  d_str  <- ncdf4::ncdim_def("name_strlen", "", seq_len(strlen), create_dimvar = FALSE)

  vars <- list()
  for (nm in names(profile_proto)) {
    x <- profile_proto[[nm]]
    vars[[nm]] <- if (is.character(x) || is.factor(x)) {
      ncdf4::ncvar_def(nm, "", list(d_str, d_prof), prec = "char")
    } else if (is.integer(x)) {
      ncdf4::ncvar_def(nm, .nc_units(nm, var_meta), d_prof,
                       prec = "integer", missval = NC_FILL_INT)
    } else {
      ncdf4::ncvar_def(nm, .nc_units(nm, var_meta), d_prof,
                       prec = "double", missval = NC_FILL_DOUBLE)
    }
  }
  vars[["rowSize"]] <- ncdf4::ncvar_def("rowSize", "", d_prof, prec = "integer")
  vars[["depth"]]   <- ncdf4::ncvar_def("depth", "m", d_obs, prec = "double",
                                        missval = NC_FILL_DOUBLE)
  for (nm in obs_types)
    vars[[nm]] <- ncdf4::ncvar_def(nm, .nc_units(nm, var_meta), d_obs,
                                   prec = "double", missval = NC_FILL_DOUBLE)
  list(dims = list(profile = d_prof, obs = d_obs, strlen = d_str), vars = vars)
}

#' Write one chunk of a CF profile file
#'
#' Writes profile-level variables (one value per profile, taken from each
#' profile's first row), the `rowSize` ragged-array index, and the obs-level
#' `depth` + measurement variables. Call once for a whole dataset, or repeatedly
#' with advancing offsets to stream a table too large to materialize — the
#' 216M-row `obs_ctd_full` is written one cruise partition at a time, holding
#' ~15 MB rather than the whole table.
#'
#' @param nc Open `ncdf4` handle created from [nc_profile_def()]'s `vars`.
#' @param vars The `vars` element of [nc_profile_def()].
#' @param wide Wide data.frame, one row per (profile, depth), **ordered by
#'   profile then depth** so each profile's rows are contiguous. Must contain
#'   `profile_id_col`, `depth`, and the profile-level and obs-level columns.
#' @param profile_cols Character vector of profile-level column names.
#' @param obs_types Character vector of obs-level measurement column names.
#' @param profile_id_col Column identifying the profile.
#' @param start_profile,start_obs 1-based write offsets into the profile and obs
#'   dimensions.
#' @param strlen Fixed character length, matching [nc_profile_def()].
#'
#' @return `list(n_profile, n_obs)` — the counts written, for advancing the
#'   offsets on the next chunk.
#'
#' @details
#' Non-contiguous profile rows are a **hard stop**, not a warning. A contiguous
#' ragged array encodes each profile as a run of `rowSize` consecutive rows, so
#' rows interleaved between profiles produce a file that reads cleanly and
#' assigns depths to the wrong casts. Ordering is the caller's job (`ORDER BY
#' profile, depth`); verifying it is this function's.
#'
#' @export
#' @concept publish
nc_profile_write <- function(nc, vars, wide, profile_cols, obs_types,
                             profile_id_col = "profile_id",
                             start_profile = 1L, start_obs = 1L, strlen = 64L) {
  .need_ncdf4()
  stopifnot(is.data.frame(wide), profile_id_col %in% names(wide),
            "depth" %in% names(wide))
  miss <- setdiff(c(profile_cols, obs_types), names(wide))
  if (length(miss))
    stop(glue::glue("wide is missing column(s): {paste(miss, collapse = ', ')}"))
  if (!nrow(wide)) return(list(n_profile = 0L, n_obs = 0L))

  ids <- wide[[profile_id_col]]
  r   <- rle(as.character(ids))
  if (length(r$values) != length(unique(ids)))
    stop(glue::glue(
      "profile rows are not contiguous ({length(r$values)} runs for ",
      "{length(unique(ids))} profiles) — ORDER BY {profile_id_col}, depth ",
      "before writing, or the ragged array assigns depths to the wrong profile"))
  first_ix <- match(r$values, as.character(ids))
  n_p <- length(r$values)

  put_char <- function(v, x, off, n) {
    x <- as.character(x); x[is.na(x)] <- ""
    long <- nchar(x) > strlen
    if (any(long))
      stop(glue::glue(
        "{v$name}: {sum(long)} value(s) exceed strlen={strlen} (longest ",
        "{max(nchar(x))}) — silently truncating an identifier would corrupt joins"))
    ncdf4::ncvar_put(nc, v, x, start = c(1, off), count = c(strlen, n))
  }
  for (nm in profile_cols) {
    x <- wide[[nm]][first_ix]
    v <- vars[[nm]]
    if (is.null(v)) stop(glue::glue("no variable defined for profile column '{nm}'"))
    if (is.character(x) || is.factor(x)) put_char(v, x, start_profile, n_p)
    else ncdf4::ncvar_put(nc, v, x, start = start_profile, count = n_p)
  }
  ncdf4::ncvar_put(nc, vars[["rowSize"]], as.integer(r$lengths),
                   start = start_profile, count = n_p)
  ncdf4::ncvar_put(nc, vars[["depth"]], as.numeric(wide$depth),
                   start = start_obs, count = nrow(wide))
  for (nm in obs_types)
    ncdf4::ncvar_put(nc, vars[[nm]], as.numeric(wide[[nm]]),
                     start = start_obs, count = nrow(wide))

  list(n_profile = n_p, n_obs = nrow(wide))
}

#' Write the CF Discrete-Sampling-Geometry attributes of a profile file
#'
#' These attributes are what make the file a CF *profile dataset* rather than a
#' table that happens to be stored in netCDF: `cf_role` marks the instance
#' identifier and `sample_dimension` on `rowSize` declares the contiguous ragged
#' array. Without them a CF-aware reader sees two unrelated dimensions.
#'
#' @param nc Open `ncdf4` handle.
#' @param obs_types Obs-level measurement variable names.
#' @param var_meta Named list from [measurement_var_meta()].
#' @param profile_vars Profile-level variable names present in the file; the
#'   coordinate ones among them get their `standard_name`/`axis`.
#' @param profile_id_var The instance-identifier variable.
#'
#' @return `TRUE`, invisibly.
#' @export
#' @concept publish
nc_profile_atts <- function(nc, obs_types, var_meta = list(),
                            profile_vars = character(),
                            profile_id_var = "profile_id") {
  .need_ncdf4()
  if (profile_id_var %in% c(profile_vars, names(nc$var))) {
    ncdf4::ncatt_put(nc, profile_id_var, "cf_role", "profile_id")
    ncdf4::ncatt_put(nc, profile_id_var, "long_name", "Profile identifier")
  }
  axis_of <- c(time = "T", latitude = "Y", longitude = "X", depth = "Z")
  for (nm in intersect(names(axis_of), c(profile_vars, "depth"))) {
    ncdf4::ncatt_put(nc, nm, "standard_name", nm)
    ncdf4::ncatt_put(nc, nm, "axis", unname(axis_of[[nm]]))
  }
  ncdf4::ncatt_put(nc, "depth", "standard_name", "depth")
  ncdf4::ncatt_put(nc, "depth", "axis", "Z")
  ncdf4::ncatt_put(nc, "depth", "positive", "down")
  ncdf4::ncatt_put(nc, "rowSize", "long_name",
                   "Number of observations for this profile")
  ncdf4::ncatt_put(nc, "rowSize", "sample_dimension", "obs")

  coords <- paste(intersect(c("time", "latitude", "longitude"), profile_vars),
                  collapse = " ")
  coords <- trimws(paste(coords, "depth"))
  for (nm in obs_types) {
    md <- var_meta[[nm]] %||% list()
    ncdf4::ncatt_put(nc, nm, "long_name",
                     as.character(.nz(md$long_name, gsub("_", " ", nm))))
    ncdf4::ncatt_put(nc, nm, "coordinates", coords)
    sn <- .nz(md$standard_name, NA_character_)
    if (!is.na(sn)) ncdf4::ncatt_put(nc, nm, "standard_name", as.character(sn))
    # emitted only when the registry actually carries a range — an invented
    # valid_min/valid_max would license a reader to discard real data
    if (!is.null(md$valid_min) && !is.na(md$valid_min))
      ncdf4::ncatt_put(nc, nm, "valid_min", as.numeric(md$valid_min))
    if (!is.null(md$valid_max) && !is.na(md$valid_max))
      ncdf4::ncatt_put(nc, nm, "valid_max", as.numeric(md$valid_max))
  }
  invisible(TRUE)
}

# ---- global attributes -------------------------------------------------------

#' Build the CF/ACDD global attributes for a published dataset
#'
#' Derives the file's self-description from the ingest notebook's
#' `calcofi.dataset_meta` YAML — the same block that feeds the release `dataset`
#' table — so the netCDF, the database and the schema site cannot describe the
#' same dataset differently. Per-file overrides go in `calcofi.netcdf`.
#'
#' @param dataset_key Provenance stamp, e.g. `"calcofi_ctd-cast"`.
#' @param dataset_meta The ingest's `dataset_meta` list (`dataset_name`,
#'   `description`, `citation_main`, `coverage_temporal`, `coverage_spatial`,
#'   `license`, …). Missing keys simply omit their attribute. A `title` or
#'   `description` here wins, so a caller overrides the derived text by merging
#'   its `calcofi.netcdf` block over `dataset_meta` before passing it.
#' @param release Database release the file was built from, e.g. `"v2026.07.30"`.
#' @param shape `"profile"` or `"groups"`, from [plan_dataset_netcdf()].
#' @param cf_scope Override for the honesty statement about CF coverage;
#'   defaults to text appropriate to `shape`.
#' @param workflow_url Rendered notebook URL, written as `references`.
#' @param date_created ACDD creation date. Defaults to the **release** date
#'   parsed from `release`, deliberately: see Details.
#' @param extra Named list of additional or overriding global attributes.
#'
#' @return Named list of global attributes, ready for `ncdf4::ncatt_put(nc, 0, …)`.
#'
#' @details
#' **Why `date_created` is the release date, not `Sys.time()`.** The publisher
#' skips re-uploading a file whose sha256 matches an earlier release
#' (release-named paths, bytes written once). A wall-clock `date_created` puts a
#' fresh timestamp inside every build, so no rebuild is ever byte-identical and
#' that check can never fire — it silently degrades to "always re-upload". Tying
#' the attribute to the release makes a rebuild of the same release reproducible,
#' which is also the more useful claim: it dates the data product, not the run.
#'
#' @export
#' @concept publish
nc_global_atts <- function(dataset_key, dataset_meta = list(), release,
                           shape = c("profile", "groups"), cf_scope = NULL,
                           workflow_url = NULL, date_created = NULL,
                           extra = list()) {
  shape <- match.arg(shape)
  dm <- dataset_meta %||% list()
  name <- as.character(.nz(dm$dataset_name, dataset_key))

  if (is.null(date_created)) {
    d <- sub("^v", "", as.character(release))
    date_created <- if (grepl("^\\d{4}[.-]\\d{2}[.-]\\d{2}$", d))
      paste0(gsub("\\.", "-", d), "T00:00:00Z") else NA_character_
  }
  if (is.null(cf_scope)) cf_scope <- if (shape == "profile") paste(
    "Fully CF: a single sampling level with a depth axis is a CF profile, so",
    "this dataset needs no extension beyond the standard.") else paste(
    "CF-1.10 where CF applies (coordinates, units, standard names, time).",
    "The sampling hierarchy uses netCDF-4 groups with explicit parent_index",
    "links; CF defines no feature type for this nesting.")

  atts <- list(
    title       = as.character(.nz(dm$title, glue::glue("{name} — CF NetCDF"))),
    summary     = as.character(.nz(dm$description,
                    glue::glue("{name}, from the CalCOFI integrated database."))),
    Conventions = "CF-1.10, ACDD-1.3",
    cf_scope    = cf_scope,
    institution = "CalCOFI",
    source      = glue::glue("CalCOFI integrated database release {release}"),
    db_release  = as.character(release),
    dataset_key = dataset_key,
    license      = as.character(.nz(dm$license, "CC-BY 4.0")),
    creator_name = "CalCOFI",
    creator_url  = "https://calcofi.io")
  if (shape == "profile") {
    atts$featureType   <- "profile"
    atts$cdm_data_type <- "Profile"
  }
  # summary is squashed to one line: YAML folded scalars keep their newlines, and
  # a multi-line netCDF attribute renders as a broken blob in ncdump/THREDDS
  atts$summary <- gsub("[[:space:]]+", " ", trimws(atts$summary))

  # `time_coverage` / `geospatial_coverage` rather than ACDD's
  # time_coverage_start / geospatial_bounds: the YAML values are free text
  # ("2004-01 to 2022-11", "28-37°N, -125 to -117°W"), and ACDD's typed
  # attributes want an ISO instant and a WKT geometry. Putting prose in them
  # would make the file claim a standard it does not satisfy.
  opt <- list(
    references          = workflow_url,
    citation            = dm$citation_main,
    time_coverage       = dm$coverage_temporal,
    geospatial_coverage = dm$coverage_spatial,
    date_created        = date_created)
  for (nm in names(opt)) {
    v <- .nz(opt[[nm]], NA_character_)
    if (!identical(as.character(v), NA_character_) && !is.na(v))
      atts[[nm]] <- as.character(v)
  }
  for (nm in names(extra)) atts[[nm]] <- extra[[nm]]
  atts
}
