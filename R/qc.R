# QC rule engine ---------------------------------------------------------------
#
# Rules are DATA, not code: they live in the workflows repo at
# `metadata/qc_rules/` (`rules.csv` + `sql/*.sql`) so they version with the
# pipeline that produces the data they check, and so a data manager can review one
# in a diff without opening an app.
#
# SQL lives in its own file per rule rather than in a `sql` column of the CSV. A
# multi-line query wedged into a CSV cell is unreviewable and un-diffable, which
# defeats the point of making the rules data in the first place.
#
# A rule's SQL MUST return at least:
#   subject_key  what is being flagged (a sample_key) — the unit of review
#   detail       one human-readable sentence naming the problem
#
# and, WHEN THE FINDING IS ABOUT A PARTICULAR SCAN rather than a whole cast:
#   depth_min_m       the depth it concerns
#   measurement_type  the variable it concerns
#
# Those two are a contract, not a convention. They are what lets a reviewer click
# a finding and land on the right profile at the right depth without the app
# knowing anything about the rule that produced it. A rule that omits them can
# still run, but its findings are un-plottable — which is how the spike and
# up/down rules behaved until they were made to declare them.
#
# Any further columns are passed through to the caller as-is.
#
# THIS ENGINE LIVES IN THE PACKAGE, not in the app that first used it. It ran as a
# private copy inside `apps/ctd-qaqc/R/rules.R` while it had one caller; it now has
# two (the app and `ingest_calcofi_ctd-cast.qmd`, which reports the condition of
# the data it just published). Two copies of a scientific rule is exactly the drift
# CLAUDE.md forbids, and it is the same failure the core-projection `switch()` arms
# had. One copy, with tests.

#' Parse a rule's `params` cell into a named list
#'
#' Format is `k=v;k=v` — deliberately flat. Anything needing more structure than
#' that is a sign the logic belongs in the rule's SQL file, not in the index.
#'
#' @param x a single `params` cell (character; `NA` or empty gives an empty list)
#'
#' @return a named list of character values
#' @export
#' @concept qc
#' @examples
#' qc_parse_params("threshold=0.5;units=degC")
qc_parse_params <- function(x) {
  if (is.na(x) || !nzchar(trimws(x))) return(list())
  kv <- strsplit(trimws(x), ";")[[1]]
  kv <- kv[nzchar(trimws(kv))]
  out <- lapply(kv, function(p) {
    parts <- strsplit(p, "=", fixed = TRUE)[[1]]
    if (length(parts) < 2) stop("malformed param: '", p, "'", call. = FALSE)
    trimws(paste(parts[-1], collapse = "="))
  })
  names(out) <- vapply(kv, function(p)
    trimws(strsplit(p, "=", fixed = TRUE)[[1]][1]), character(1))
  out
}

#' Substitute `{{param}}` placeholders into a rule's SQL
#'
#' Errors on a placeholder with no matching param. Silently leaving `{{threshold}}`
#' in the query would produce a DuckDB parse error far from its cause, and worse, a
#' *missing* threshold could otherwise render as an empty string and quietly change
#' the rule's meaning rather than failing.
#'
#' @param sql rule SQL text
#' @param params named list, e.g. from [qc_parse_params()]
#'
#' @return `sql` with every placeholder substituted
#' @export
#' @concept qc
#' @examples
#' qc_render_sql("SELECT * FROM obs WHERE v > {{threshold}}", list(threshold = "3"))
qc_render_sql <- function(sql, params) {
  m <- regmatches(sql, gregexpr("\\{\\{[a-z_]+\\}\\}", sql))[[1]]
  needed <- unique(gsub("^\\{\\{|\\}\\}$", "", m))
  missing <- setdiff(needed, names(params))
  if (length(missing)) {
    stop("rule SQL needs param(s) not supplied: ", paste(missing, collapse = ", "),
         call. = FALSE)
  }
  for (nm in needed) {
    sql <- gsub(paste0("{{", nm, "}}"), params[[nm]], sql, fixed = TRUE)
  }
  sql
}

#' Read the rule registry, attaching SQL text and parsed params
#'
#' @param dir directory holding `rules.csv` and `sql/`
#' @param active_only drop rules parked for a later phase (`active = FALSE`)
#'
#' @return a tibble, one row per rule, with `sql` (character) and `params`
#'   (list-column) added
#' @export
#' @concept qc
#' @examples
#' \dontrun{
#' qc_read_rules(here::here("metadata/qc_rules"))
#' }
qc_read_rules <- function(dir, active_only = TRUE) {
  path <- file.path(dir, "rules.csv")
  stopifnot("rules.csv not found" = file.exists(path))
  d <- readr::read_csv(path, show_col_types = FALSE,
                       col_types = readr::cols(.default = "c"))
  d$active <- tolower(d$active) %in% c("true", "t", "yes", "1")

  if (active_only) d <- d[d$active, , drop = FALSE]

  d$sql <- vapply(seq_len(nrow(d)), function(i) {
    f <- d$sql_file[i]
    if (is.na(f) || !nzchar(f)) return(NA_character_)
    p <- file.path(dir, "sql", f)
    if (!file.exists(p))
      stop("rule '", d$rule_key[i], "' references missing SQL: ", f, call. = FALSE)
    paste(readLines(p, warn = FALSE), collapse = "\n")
  }, character(1))

  # an active rule with no SQL is a registry error, not an empty result set
  bad <- d$rule_key[d$active & is.na(d$sql)]
  if (length(bad)) {
    stop("active rule(s) with no sql_file: ", paste(bad, collapse = ", "),
         "\n  park them with active=FALSE until their SQL exists", call. = FALSE)
  }

  d$params <- lapply(d$params, qc_parse_params)
  d
}

#' Which measurement types actually exist for a dataset
#'
#' Computed once and passed to every rule: one `DISTINCT` scan instead of one
#' presence query per rule.
#'
#' @param con a DBI connection carrying an `obs` table or view
#' @param dataset_key dataset to restrict to
#'
#' @return character vector of `measurement_type` values present
#' @export
#' @concept qc
qc_present_types <- function(con, dataset_key = "calcofi_ctd-cast") {
  DBI::dbGetQuery(con, glue::glue(
    "SELECT DISTINCT measurement_type FROM obs
     WHERE dataset_key = '{dataset_key}'"))$measurement_type
}

#' Execute one rule, returning its findings
#'
#' PRECONDITIONS ARE CHECKED FIRST, and this is not a nicety. A rule whose input
#' measurement type is absent returns zero rows, which is indistinguishable from
#' "the data is clean" — a false pass. The three bottle-vs-sensor calibration rules
#' did exactly that against release `v2026.07.30`, which carries only `btl_ammonium`
#' because it predates the change making the other bottle-reference types canonical.
#' A QA/QC tool that reports green without having checked anything is worse than no
#' tool, so an unmet precondition is `skip`, never `pass`.
#'
#' @param con a DBI connection carrying the tables the rule targets
#' @param rule one row of [qc_read_rules()]
#' @param limit cap on rows returned. The `COUNT` is always computed over the full
#'   result, so a truncated display never understates the problem — a rule that
#'   silently showed 500 of 40,000 hits would read as "minor".
#' @param present_types output of [qc_present_types()]; `NULL` disables the check
#' @param scope_values named list supplying scope parameters, e.g.
#'   `list(cruise_key = "2023-11-33P4")`. A rule with `scope = "cruise"` reads the
#'   full-resolution `obs_ctd_full` and is meaningless unscoped, so it SKIPS rather
#'   than silently scanning everything.
#'
#' @return list with `rule_key`, `n`, `findings`, `elapsed_s`, `error`, `skipped`,
#'   `skip_reason`
#' @export
#' @concept qc
qc_run_rule <- function(con, rule, limit = 500L, present_types = NULL,
                        scope_values = list()) {
  t0 <- Sys.time()
  out <- list(rule_key = rule$rule_key, n = NA_integer_, findings = NULL,
              elapsed_s = NA_real_, error = NA_character_,
              skipped = FALSE, skip_reason = NA_character_)

  req <- rule$requires_types
  if (!is.null(present_types) && !is.na(req) && nzchar(trimws(req))) {
    need    <- trimws(strsplit(req, ",")[[1]])
    need    <- need[nzchar(need)]
    missing <- setdiff(need, present_types)
    if (length(missing)) {
      out$skipped     <- TRUE
      out$skip_reason <- paste0("input absent from obs: ",
                                paste(missing, collapse = ", "))
      out$elapsed_s   <- 0
      return(out)
    }
  }

  scope <- rule$scope %||% NA_character_
  if (!is.na(scope) && scope == "cruise" &&
      !nzchar(scope_values$cruise_key %||% "")) {
    out$skipped     <- TRUE
    out$skip_reason <- paste(
      "needs a cruise — this rule reads the full-resolution obs_ctd_full",
      "and is only run one cruise at a time")
    out$elapsed_s   <- 0
    return(out)
  }

  res <- try({
    # `rule` is a one-row tibble, so rule$params is a LIST COLUMN — a list of one
    # containing the params. Unwrap it, or every {{placeholder}} silently fails to
    # resolve and the rule errors far from its cause.
    prm <- rule$params
    if (is.list(prm) && length(prm) == 1 && is.list(prm[[1]])) prm <- prm[[1]]
    # scope values (e.g. cruise_key) are supplied at run time, not in the registry
    prm <- utils::modifyList(prm, scope_values)
    sql <- qc_render_sql(rule$sql, prm)
    n <- DBI::dbGetQuery(con, glue::glue("SELECT COUNT(*) AS n FROM ({sql})"))$n
    f <- if (n > 0)
      DBI::dbGetQuery(con, glue::glue("SELECT * FROM ({sql}) LIMIT {limit}")) else
        data.frame()
    list(n = as.integer(n), findings = f)
  }, silent = TRUE)

  out$elapsed_s <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 2)
  if (inherits(res, "try-error")) {
    out$error <- trimws(as.character(res))
  } else {
    out$n <- res$n; out$findings <- res$findings
  }
  out
}

#' Run every rule in a registry, one at a time
#'
#' Sequential on purpose: these are multi-GB scans and running them concurrently
#' against one DuckDB just contends for the same buffer pool.
#'
#' @inheritParams qc_run_rule
#' @param rules a rule registry from [qc_read_rules()]
#' @param on_progress optional `function(i, n, rule_key)` callback
#'
#' @return list of [qc_run_rule()] results, in registry order
#' @export
#' @concept qc
qc_run_all <- function(con, rules, limit = 500L, on_progress = NULL,
                       present_types = qc_present_types(con),
                       scope_values = list()) {
  lapply(seq_len(nrow(rules)), function(i) {
    if (!is.null(on_progress)) on_progress(i, nrow(rules), rules$rule_key[i])
    qc_run_rule(con, rules[i, ], limit = limit, present_types = present_types,
                scope_values = scope_values)
  })
}

#' Collapse rule results into one row per rule
#'
#' `skip` is deliberately its own status rather than folded into `pass`: they mean
#' opposite things about how much you should trust the run.
#'
#' @param results list from [qc_run_all()]
#' @param rules the registry those results came from
#'
#' @return a tibble, one row per rule, with a `status` of `pass` / `flag` / `FAIL`
#'   / `ERROR` / `skip`
#' @export
#' @concept qc
qc_summarize <- function(results, rules) {
  d <- tibble::tibble(
    rule_key    = vapply(results, function(r) r$rule_key, character(1)),
    n           = vapply(results, function(r)
      as.integer(r$n %||% NA_integer_), integer(1)),
    elapsed_s   = vapply(results, function(r)
      as.numeric(r$elapsed_s %||% NA_real_), numeric(1)),
    error       = vapply(results, function(r) r$error %||% NA_character_, character(1)),
    skipped     = vapply(results, function(r) isTRUE(r$skipped), logical(1)),
    skip_reason = vapply(results, function(r)
      r$skip_reason %||% NA_character_, character(1)))

  d <- dplyr::left_join(
    d,
    dplyr::select(rules, "rule_key", "rule_type", "severity", "target",
                  "description", dplyr::any_of("scope")),
    by = "rule_key")

  d$status <- dplyr::case_when(
    d$skipped            ~ "skip",
    !is.na(d$error)      ~ "ERROR",
    d$n == 0             ~ "pass",
    d$severity == "error" ~ "FAIL",
    TRUE                 ~ "flag")
  d$note <- dplyr::coalesce(d$error, d$skip_reason)

  dplyr::select(d, "rule_key", "status", "n", "severity", "rule_type", "target",
                "description", dplyr::any_of("scope"), "elapsed_s", "note")
}

# reference staging ------------------------------------------------------------

#' Stage the QC reference tables a rule registry expects
#'
#' The rules join against reference data that is deliberately NOT part of the
#' release: the quality-code vocabulary, the harmonic climatology and station
#' bottom depths mined from the CalCOFI hydrographic Access master, and a seafloor
#' depth per cast derived from bathymetry. This puts all of them on one connection
#' so both the app and the ingest notebook get the same reference inputs.
#'
#' `measurement_type` comes from the WORKFLOWS REGISTRY, not from a release: the
#' registry is the source of truth and moves ahead of the release (`valid_min` /
#' `valid_max` existed there before any release carried them), so sourcing it from
#' a release would silently disable every range rule.
#'
#' A missing input is left as a MISSING TABLE rather than an empty one. An empty
#' reference table makes its rules return zero rows, which reads as "clean"; a
#' missing table makes them error, which reads as "not checked". The second is the
#' truth.
#'
#' @param con a DBI connection to stage into
#' @param dir_workflows root of the `CalCOFI/workflows` checkout
#' @param gebco_tif optional path to a positive-down bathymetry GeoTIFF (the one
#'   `apps/ctd-viz` crops from GEBCO 2025). When supplied — and when `terra` is
#'   installed — `sample_seafloor` is built by extracting it at each cast position.
#' @param sample_tbl table or view holding `sample_key` / `longitude` / `latitude`
#' @param quiet suppress the per-table progress lines
#'
#' @return character vector of the tables actually staged, invisibly
#' @export
#' @concept qc
qc_stage_reference <- function(con, dir_workflows, gebco_tif = NULL,
                               sample_tbl = "sample", quiet = FALSE) {
  say <- function(...) if (!quiet) cat(..., "\n", sep = "")
  staged <- character(0)

  # measurement_type — strict read, so a corrupted registry fails here rather than
  # reaching a range rule as the literal string "NA" (see R/registry.R)
  p_mt <- file.path(dir_workflows, "metadata/measurement_type.csv")
  if (file.exists(p_mt)) {
    d_mt <- read_measurement_type(p_mt)
    DBI::dbWriteTable(con, "measurement_type", as.data.frame(d_mt), overwrite = TRUE)
    staged <- c(staged, "measurement_type")
    n_rng <- sum(!is.na(d_mt$valid_min))
    say(sprintf("  %-22s %s rows (%d with a declared range)",
                "measurement_type", format(nrow(d_mt), big.mark = ","), n_rng))
  } else {
    say("  measurement_type       MISSING — range rules will error, not pass")
  }

  # measurement_qual — qual_code forced to character: every code is numeric, so a
  # type-guessing read makes it a double and joining that to a VARCHAR column is a
  # hard error rather than a silent miss
  p_mq <- file.path(dir_workflows, "metadata/measurement_qual.csv")
  if (file.exists(p_mq)) {
    d_mq <- readr::read_csv(p_mq, col_types = readr::cols(
      qual_code = readr::col_character(), .default = readr::col_guess()))
    DBI::dbWriteTable(con, "measurement_qual", as.data.frame(d_mq), overwrite = TRUE)
    staged <- c(staged, "measurement_qual")
    say(sprintf("  %-22s %s codes", "measurement_qual", nrow(d_mq)))
  } else {
    say("  measurement_qual       MISSING — the vocabulary rule will error")
  }

  # QC reference data mined from the Access master (small, committed CSVs)
  dir_ref <- file.path(dir_workflows, "metadata/calcofi/hydro-master/reference")
  for (t in c("climatology_harmonic", "station", "standard_depth", "station_class",
              "mld_sigma", "nutclinedepth")) {
    p <- file.path(dir_ref, paste0(t, ".csv"))
    if (!file.exists(p)) {
      say(sprintf("  %-22s MISSING", t))
      next
    }
    d <- readr::read_csv(p, show_col_types = FALSE)
    DBI::dbWriteTable(con, t, as.data.frame(d), overwrite = TRUE)
    staged <- c(staged, t)
    say(sprintf("  %-22s %s rows", t, format(nrow(d), big.mark = ",")))
  }

  # seafloor depth at each cast position. CTD casts carry NO reported bottom
  # depth — `bottom_depth` exists in sample_measurement for 33,363 BOTTLE casts
  # and for 0 of 14,336 CTD casts — so a bathymetry model is the only available
  # reference for "is this cast in water this deep".
  if (!is.null(gebco_tif) && file.exists(gebco_tif) &&
      requireNamespace("terra", quietly = TRUE)) {
    pos <- DBI::dbGetQuery(con, glue::glue(
      "SELECT sample_key, longitude, latitude FROM {sample_tbl}
       WHERE longitude IS NOT NULL AND latitude IS NOT NULL"))
    pos$seafloor_depth_m <- terra::extract(
      terra::rast(gebco_tif), as.matrix(pos[, c("longitude", "latitude")]))[, 1]
    DBI::dbWriteTable(con, "sample_seafloor",
                      pos[, c("sample_key", "seafloor_depth_m")], overwrite = TRUE)
    staged <- c(staged, "sample_seafloor")
    say(sprintf("  %-22s %s casts (%s with a depth)", "sample_seafloor",
                format(nrow(pos), big.mark = ","),
                format(sum(!is.na(pos$seafloor_depth_m)), big.mark = ",")))
  } else {
    say(paste0("  sample_seafloor        MISSING — the two bathymetry rules will ",
               "error rather than silently pass"))
  }

  invisible(staged)
}

# cast profiles ----------------------------------------------------------------
#
# A finding is a key and a number. A reviewer cannot judge one without seeing the
# value in the profile it came from — which means fetching the cast's scans, both
# directions, at full resolution. That fetch has two traps in it, which is why it
# is here with tests rather than inline in an app callback.

#' Strip the direction suffix from a CTD cast `sample_key`
#'
#' CTD `sample_key`s end in the cast direction — `…:cast:9802_008d` /
#' `…:cast:9802_008u` — so a station occupation that logged both directions is two
#' `sample` rows sharing a base.
#'
#' THE OBVIOUS IMPLEMENTATION IS WRONG. `sub("d$", "", x)` is fine, but
#' `gsub("d", "", x)` or `replace(x, 'd', '')` also eats the `d` in the
#' `calcofi_ctd-cast` prefix and silently returns a key that matches nothing. The
#' same trap is called out in `ctd_updown_disagreement.sql`, which is where it was
#' first hit.
#'
#' @param sample_key character vector of cast keys
#'
#' @return the keys with a single trailing `d`/`u` removed; keys without one are
#'   returned unchanged
#' @export
#' @concept qc
#' @examples
#' qc_cast_base("calcofi_ctd-cast:cast:9802_008d")
qc_cast_base <- function(sample_key) {
  sub("[du]$", "", sample_key)
}

#' Direction of a CTD cast `sample_key`
#'
#' @param sample_key character vector of cast keys
#'
#' @return `"down"`, `"up"`, or `NA` for a key with no direction suffix
#' @export
#' @concept qc
qc_cast_direction <- function(sample_key) {
  s <- substr(sample_key, nchar(sample_key), nchar(sample_key))
  out <- rep(NA_character_, length(sample_key))
  out[s == "d"] <- "down"
  out[s == "u"] <- "up"
  out[is.na(sample_key)] <- NA_character_
  out
}

#' Fetch one physical cast's profile, both directions
#'
#' Returns the full-resolution scans for the cast a `sample_key` belongs to —
#' **both** the down- and upcast, since the point of plotting a profile during
#' review is to see them overlaid. `obs` carries only one direction per physical
#' cast (that is what thinning does), so the default source is the supplemental
#' `obs_ctd_full`.
#'
#' `cruise_key` is not a filter for the caller's convenience, it is a performance
#' precondition: `obs_ctd_full` is hive-partitioned by `cruise_key`, so supplying
#' it prunes ~212M rows to one cruise. When it is not supplied this looks it up
#' from `sample` — one cheap query, rather than letting a profile fetch scan the
#' whole archive.
#'
#' @param con a DBI connection carrying `obs_ctd_full` (or `obs_tbl`) and `sample`
#' @param sample_key any one direction's key; both are returned
#' @param measurement_types restrict to these types; `NULL` for all
#' @param obs_tbl source table (`"obs_ctd_full"`, or `"obs"` for the thinned set)
#' @param cruise_key partition to prune to; `NULL` looks it up from `sample`
#'
#' @return a data frame of `sample_key`, `cast_dir` (`down`/`up`), `depth_m`,
#'   `measurement_type`, `measurement_value`, `measurement_qual`, `datetime`,
#'   ordered by type, direction and depth
#' @export
#' @concept qc
qc_cast_profile <- function(con, sample_key, measurement_types = NULL,
                            obs_tbl = "obs_ctd_full", cruise_key = NULL) {
  stopifnot("sample_key must be a single key" = length(sample_key) == 1)
  base <- qc_cast_base(sample_key)

  if (is.null(cruise_key) || is.na(cruise_key) || !nzchar(cruise_key)) {
    ck <- try(DBI::dbGetQuery(con, glue::glue(
      "SELECT DISTINCT cruise_key FROM sample
       WHERE sample_key LIKE '{.sql_esc(base)}%'")), silent = TRUE)
    cruise_key <- if (!inherits(ck, "try-error") && nrow(ck) == 1)
      ck$cruise_key else NULL
  }

  where <- c(glue::glue("sample_key LIKE '{.sql_esc(base)}%'"))
  if (!is.null(cruise_key) && !is.na(cruise_key) && nzchar(cruise_key))
    where <- c(glue::glue("cruise_key = '{.sql_esc(cruise_key)}'"), where)
  if (!is.null(measurement_types) && length(measurement_types)) {
    lst <- paste0("'", .sql_esc(measurement_types), "'", collapse = ", ")
    where <- c(where, glue::glue("measurement_type IN ({lst})"))
  }

  d <- DBI::dbGetQuery(con, glue::glue("
    SELECT sample_key,
           CASE right(sample_key, 1)
             WHEN 'd' THEN 'down' WHEN 'u' THEN 'up' ELSE NULL END AS cast_dir,
           depth_min_m AS depth_m,
           measurement_type, measurement_value, measurement_qual, datetime
    FROM {obs_tbl}
    WHERE {paste(where, collapse = ' AND ')}
    ORDER BY measurement_type, cast_dir, depth_m"))
  d
}

# single-quote escaping for values interpolated into SQL. The keys here are
# internal and well-formed, but a helper that builds SQL by interpolation should
# not be the one place that assumes so.
.sql_esc <- function(x) gsub("'", "''", as.character(x), fixed = TRUE)
