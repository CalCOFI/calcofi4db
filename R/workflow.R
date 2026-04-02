# workflow output functions for calcofi data ingestion
# writes parquet files + json manifests to GCS

#' Write Ingest Workflow Outputs to GCS
#'
#' Transforms data and writes parquet files + manifest to GCS ingest folder.
#' Each ingest workflow produces parquet files for its tables and a manifest
#' tracking provenance back to the source archive.
#'
#' @param data_info Output from `read_csv_files()`
#' @param provider Data provider (e.g., "swfsc")
#' @param dataset Dataset name (e.g., "ichthyo")
#' @param gcs_bucket GCS bucket for ingest outputs (default: "calcofi-db")
#' @param compression Parquet compression method (default: "snappy")
#'
#' @return List with:
#'   \itemize{
#'     \item \code{gcs_base}: Base GCS path for this ingest
#'     \item \code{parquet_paths}: Named list of GCS paths to parquet files
#'     \item \code{manifest_path}: GCS path to manifest.json
#'     \item \code{manifest}: The manifest data as a list
#'   }
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' d <- read_csv_files(
#'   provider     = "swfsc",
#'   dataset      = "ichthyo",
#'   metadata_dir = "metadata")
#'
#' result <- write_ingest_outputs(
#'   data_info  = d,
#'   provider   = "swfsc",
#'   dataset    = "ichthyo")
#'
#' # check manifest
#' result$manifest$tables
#' }
#' @importFrom glue glue
#' @importFrom arrow write_parquet
#' @importFrom jsonlite write_json
write_ingest_outputs <- function(
    data_info,
    provider,
    dataset,
    gcs_bucket  = "calcofi-db",
    compression = "snappy") {

  # transform data
  transformed <- transform_data(data_info)

  # base GCS path for this ingest
  gcs_base <- glue::glue("gs://{gcs_bucket}/ingest/{provider}/{dataset}")

  parquet_paths <- list()
  table_stats   <- list()

  message(glue::glue("Writing ingest outputs to {gcs_base}"))

  # write each table to GCS as parquet
  for (i in seq_len(nrow(transformed))) {
    tbl_name <- transformed$tbl_new[i]
    data     <- transformed$data_new[[i]]

    if (is.null(data) || nrow(data) == 0) {
      message(glue::glue("  Skipping empty table: {tbl_name}"))
      next
    }

    # write to temp file, then upload to GCS
    temp_path <- tempfile(fileext = ".parquet")
    arrow::write_parquet(data, temp_path, compression = compression)

    gcs_path <- glue::glue("{gcs_base}/parquet/{tbl_name}.parquet")
    put_gcs_file(temp_path, gcs_path)
    unlink(temp_path)

    parquet_paths[[tbl_name]] <- gcs_path
    table_stats[[tbl_name]]   <- list(
      rows = nrow(data),
      cols = ncol(data))

    message(glue::glue("  Uploaded: {tbl_name}.parquet ({nrow(data)} rows)"))
  }

  # create manifest with source file provenance
  manifest <- list(
    created_at    = format(Sys.time(), "%Y-%m-%dT%H:%M:%S"),
    provider      = provider,
    dataset       = dataset,
    source_files  = data_info$source_files,
    tables        = table_stats,
    parquet_files = parquet_paths)

  # write manifest to GCS
  manifest_path <- glue::glue("{gcs_base}/manifest.json")
  temp_manifest <- tempfile(fileext = ".json")
  jsonlite::write_json(manifest, temp_manifest, pretty = TRUE, auto_unbox = TRUE)
  put_gcs_file(temp_manifest, manifest_path)
  unlink(temp_manifest)

  message(glue::glue("  Uploaded: manifest.json"))

  list(
    gcs_base      = gcs_base,
    parquet_paths = parquet_paths,
    manifest_path = manifest_path,
    manifest      = manifest)
}

#' Read Ingest Manifest from GCS
#'
#' Retrieves the manifest.json for a specific ingest workflow output.
#'
#' @param provider Data provider (e.g., "swfsc")
#' @param dataset Dataset name (e.g., "ichthyo")
#' @param gcs_bucket GCS bucket (default: "calcofi-db")
#'
#' @return List containing manifest data
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' manifest <- read_ingest_manifest(
#'   provider = "swfsc",
#'   dataset  = "ichthyo")
#'
#' manifest$source_files
#' manifest$tables
#' }
#' @importFrom glue glue
#' @importFrom jsonlite read_json
read_ingest_manifest <- function(
    provider,
    dataset,
    gcs_bucket = "calcofi-db") {

  gcs_path   <- glue::glue("gs://{gcs_bucket}/ingest/{provider}/{dataset}/manifest.json")
  local_path <- get_gcs_file(gcs_path)
  jsonlite::read_json(local_path)
}

#' Read Ingest Parquet Table from GCS
#'
#' Downloads and reads a parquet table from an ingest output.
#'
#' @param provider Data provider (e.g., "swfsc")
#' @param dataset Dataset name (e.g., "ichthyo")
#' @param table Table name (e.g., "cruise")
#' @param gcs_bucket GCS bucket (default: "calcofi-db")
#'
#' @return Data frame (tibble) of the table
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' cruise <- read_ingest_parquet(
#'   provider = "swfsc",
#'   dataset  = "ichthyo",
#'   table    = "cruise")
#' }
#' @importFrom glue glue
#' @importFrom arrow read_parquet
read_ingest_parquet <- function(
    provider,
    dataset,
    table,
    gcs_bucket = "calcofi-db") {

  gcs_path   <- glue::glue("gs://{gcs_bucket}/ingest/{provider}/{dataset}/parquet/{table}.parquet")
  local_path <- get_gcs_file(gcs_path)
  arrow::read_parquet(local_path)
}

#' List Available Ingest Outputs
#'
#' Lists all providers and datasets that have ingest outputs on GCS.
#'
#' @param gcs_bucket GCS bucket (default: "calcofi-db")
#'
#' @return Tibble with provider, dataset, and manifest_path columns
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' ingests <- list_ingest_outputs()
#' }
#' @importFrom tibble tibble
#' @importFrom stringr str_extract
list_ingest_outputs <- function(gcs_bucket = "calcofi-db") {

  files <- list_gcs_files(gcs_bucket, prefix = "ingest/")

  if (nrow(files) == 0) {
    return(tibble::tibble(
      provider      = character(),
      dataset       = character(),
      manifest_path = character()))
  }

  # find manifest.json files
  manifests <- files |>
    dplyr::filter(grepl("manifest\\.json$", name)) |>
    dplyr::mutate(
      provider = stringr::str_extract(name, "ingest/([^/]+)/", group = 1),
      dataset  = stringr::str_extract(name, "ingest/[^/]+/([^/]+)/", group = 1),
      manifest_path = glue::glue("gs://{gcs_bucket}/{name}")) |>
    dplyr::select(provider, dataset, manifest_path)

  manifests
}

#' Finalize Ingest — Push Parquet Tables to Working DuckLake
#'
#' High-level function each ingest notebook calls at the end to push all
#' parquet tables to the Working DuckLake. Downloads (or creates) the Working
#' DuckLake, ingests each parquet file with provenance, saves back to GCS,
#' and returns ingestion statistics.
#'
#' @param parquet_dir Path to directory containing parquet files to ingest
#' @param source_label Label for provenance tracking (default: basename of parquet_dir)
#' @param tables Optional character vector of table names to ingest. If NULL,
#'   all .parquet files in the directory are ingested.
#' @param geom_tables Character vector of table names with WKB geometry columns
#'   (default: c("grid", "site", "segment")). These are skipped during
#'   ingest_to_working() since geometry columns don't survive dbWriteTable().
#'   Instead they are loaded directly from parquet.
#' @param include_supplemental If FALSE (default), tables listed under
#'   `"supplemental"` in manifest.json are excluded. Set to TRUE to push all
#'   tables including supplemental outputs (e.g. wide-format for ERDDAP).
#'
#' @return Tibble with ingestion statistics per table
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' # at the end of ingest_swfsc_ichthyo.qmd
#' finalize_ingest(
#'   parquet_dir  = here("data/parquet/swfsc_ichthyo"),
#'   source_label = "swfsc_ichthyo")
#' }
#' @importFrom glue glue
#' @importFrom purrr map_dfr
#' @importFrom tibble tibble
#' @importFrom arrow read_parquet
finalize_ingest <- function(
    parquet_dir,
    source_label         = basename(parquet_dir),
    tables               = NULL,
    geom_tables          = c("grid", "site", "segment"),
    include_supplemental = FALSE) {

  stopifnot(dir.exists(parquet_dir))

  # discover parquet sources: single .parquet files + hive-partitioned directories
  pqt_files <- list.files(parquet_dir, pattern = "\\.parquet$", full.names = TRUE)
  pqt_dirs  <- list.dirs(parquet_dir, recursive = FALSE, full.names = TRUE)
  pqt_dirs  <- pqt_dirs[
    vapply(pqt_dirs, function(d)
      length(list.files(d, pattern = "\\.parquet$", recursive = TRUE)) > 0,
      logical(1))
  ]

  # build unified list: list of list(name, path, partitioned)
  sources <- c(
    lapply(pqt_files, function(f) list(
      name        = tools::file_path_sans_ext(basename(f)),
      path        = f,
      partitioned = FALSE)),
    lapply(pqt_dirs, function(d) list(
      name        = basename(d),
      path        = d,
      partitioned = TRUE))
  )

  if (!is.null(tables)) {
    sources <- sources[vapply(sources, function(s) s$name %in% tables, logical(1))]
  }

  # exclude supplemental tables unless requested
  if (!include_supplemental && is.null(tables)) {
    manifest_path <- file.path(parquet_dir, "manifest.json")
    if (file.exists(manifest_path)) {
      manifest <- jsonlite::read_json(manifest_path)
      supp <- unlist(manifest$supplemental)
      if (length(supp) > 0) {
        sources <- sources[
          !vapply(sources, function(s) s$name %in% supp, logical(1))]
        message(glue::glue(
          "Excluding {length(supp)} supplemental table(s): ",
          "{paste(supp, collapse = ', ')}"))
      }
    }
  }

  if (length(sources) == 0) {
    message("No parquet files found to ingest")
    return(tibble::tibble())
  }

  # open Working DuckLake
  con_wdl <- get_working_ducklake()
  load_duckdb_extension(con_wdl, "spatial")

  # ingest each parquet source
  stats <- purrr::map_dfr(sources, function(src) {
    tbl_name <- src$name
    prov_label <- glue::glue("parquet/{source_label}/{tbl_name}")

    # build read expression
    if (src$partitioned) {
      read_expr <- glue::glue(
        "read_parquet('{src$path}/**/*.parquet', hive_partitioning = true)")
    } else {
      read_expr <- glue::glue("read_parquet('{src$path}')")
    }

    # spatial tables: load from parquet with geometry conversion
    if (tbl_name %in% geom_tables) {
      tryCatch({
        DBI::dbExecute(con_wdl, glue::glue(
          "CREATE OR REPLACE TABLE {tbl_name} AS
           SELECT * FROM {read_expr}"))

        # convert WKB BLOB -> GEOMETRY
        blob_cols <- DBI::dbGetQuery(con_wdl, glue::glue(
          "SELECT column_name FROM information_schema.columns
           WHERE table_name = '{tbl_name}'
             AND data_type = 'BLOB'
             AND column_name LIKE '%geom%'"))$column_name

        for (gc in blob_cols) {
          tmp_col <- paste0(gc, "_tmp")
          DBI::dbExecute(con_wdl, glue::glue(
            'ALTER TABLE {tbl_name} ADD COLUMN {tmp_col} GEOMETRY'))
          DBI::dbExecute(con_wdl, glue::glue(
            'UPDATE {tbl_name} SET {tmp_col} = ST_GeomFromWKB({gc})'))
          DBI::dbExecute(con_wdl, glue::glue(
            'ALTER TABLE {tbl_name} DROP COLUMN {gc}'))
          DBI::dbExecute(con_wdl, glue::glue(
            'ALTER TABLE {tbl_name} RENAME COLUMN {tmp_col} TO {gc}'))
        }

        n <- DBI::dbGetQuery(con_wdl, glue::glue(
          "SELECT COUNT(*) AS n FROM {tbl_name}"))$n
        message(glue::glue("Loaded {tbl_name} (spatial): {n} rows"))

        tibble::tibble(
          table       = tbl_name,
          mode        = "replace",
          rows_input  = n,
          rows_after  = n,
          ingested_at = Sys.time())
      }, error = function(e) {
        warning(glue::glue("Failed to load spatial table {tbl_name}: {e$message}"))
        tibble::tibble(
          table = tbl_name, mode = "error",
          rows_input = 0L, rows_after = 0L, ingested_at = Sys.time())
      })
    } else if (src$partitioned) {
      # partitioned tables: use DuckDB SQL to read (avoids loading all into R memory)
      tryCatch({
        DBI::dbExecute(con_wdl, glue::glue(
          "CREATE OR REPLACE TABLE {tbl_name} AS
           SELECT * FROM {read_expr}"))

        n <- DBI::dbGetQuery(con_wdl, glue::glue(
          "SELECT COUNT(*) AS n FROM {tbl_name}"))$n
        message(glue::glue("Loaded {tbl_name} (partitioned): {n} rows"))

        tibble::tibble(
          table       = tbl_name,
          mode        = "replace",
          rows_input  = n,
          rows_after  = n,
          ingested_at = Sys.time())
      }, error = function(e) {
        warning(glue::glue("Failed to load partitioned table {tbl_name}: {e$message}"))
        tibble::tibble(
          table = tbl_name, mode = "error",
          rows_input = 0L, rows_after = 0L, ingested_at = Sys.time())
      })
    } else {
      # non-spatial single-file tables: read and ingest with provenance
      data <- arrow::read_parquet(src$path)

      ingest_to_working(
        con         = con_wdl,
        data        = data,
        table       = tbl_name,
        source_file = prov_label,
        mode        = "replace")
    }
  })

  # list final tables
  working_tables <- list_working_tables(con_wdl)
  message(glue::glue(
    "\nWorking DuckLake: {nrow(working_tables)} tables, ",
    "{sum(working_tables$rows)} total rows"))

  # save to GCS
  save_working_ducklake(con_wdl)

  # close
  close_duckdb(con_wdl)

  stats
}

#' Integrate Ingest Outputs into Working DuckLake
#'
#' Reads parquet files from multiple ingest outputs and integrates them into
#' the Working DuckLake. Tables from different ingests with the same name are
#' combined (e.g., cruise tables from different sources). Provenance columns
#' (`_ingest_provider`, `_ingest_dataset`) are added to track origin.
#'
#' @param ingests List of ingest result lists (from `write_ingest_outputs()`)
#'   or a tibble from `list_ingest_outputs()`
#' @param gcs_bucket GCS bucket (default: "calcofi-db")
#' @param ducklake_path Path to Working DuckLake within bucket
#'   (default: "ducklake/working")
#'
#' @return List with DuckLake path and table info
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' # from targets pipeline
#' result <- integrate_to_working_ducklake(
#'   ingests = list(ingest_swfsc, ingest_bottle))
#'
#' # from existing GCS ingests
#' existing <- list_ingest_outputs()
#' result <- integrate_to_working_ducklake(existing)
#' }
#' @importFrom glue glue
#' @importFrom duckdb duckdb dbConnect dbWriteTable dbDisconnect
integrate_to_working_ducklake <- function(
    ingests,
    gcs_bucket    = "calcofi-db",
    ducklake_path = "ducklake/working") {

  # if ingests is a tibble from list_ingest_outputs(), read manifests
  if (is.data.frame(ingests)) {
    manifest_list <- lapply(seq_len(nrow(ingests)), function(i) {
      read_ingest_manifest(
        provider   = ingests$provider[i],
        dataset    = ingests$dataset[i],
        gcs_bucket = gcs_bucket)
    })
    ingests <- lapply(manifest_list, function(m) {
      list(manifest = m)
    })
  }

  # collect all parquet paths by table name
  all_tables <- list()
  for (ingest in ingests) {
    manifest <- ingest$manifest
    for (tbl_name in names(manifest$parquet_files)) {
      if (!tbl_name %in% names(all_tables)) {
        all_tables[[tbl_name]] <- list()
      }
      all_tables[[tbl_name]] <- c(
        all_tables[[tbl_name]],
        list(list(
          path     = manifest$parquet_files[[tbl_name]],
          provider = manifest$provider,
          dataset  = manifest$dataset)))
    }
  }

  # create local duckdb file
  local_db <- tempfile(fileext = ".duckdb")
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = local_db)
  on.exit(duckdb::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  table_info <- list()

  for (tbl_name in names(all_tables)) {
    sources <- all_tables[[tbl_name]]

    # read all sources for this table
    dfs <- lapply(sources, function(src) {
      local_pq <- get_gcs_file(src$path)
      df <- arrow::read_parquet(local_pq)
      # add source tracking
      df$`_ingest_provider` <- src$provider
      df$`_ingest_dataset`  <- src$dataset
      df
    })

    # combine (bind rows)
    combined <- dplyr::bind_rows(dfs)

    # write to duckdb
    duckdb::dbWriteTable(con, tbl_name, combined, overwrite = TRUE)

    table_info[[tbl_name]] <- list(
      rows    = nrow(combined),
      cols    = ncol(combined),
      sources = length(sources))

    message(glue::glue("  Created table: {tbl_name} ({nrow(combined)} rows from {length(sources)} source(s))"))
  }

  # close connection before upload
  duckdb::dbDisconnect(con, shutdown = TRUE)
  on.exit()  # clear the on.exit handler

  # upload duckdb to GCS (to ducklake/working/)
  # note: for full DuckLake integration, use get_working_ducklake() and ingest_to_working()
  gcs_db_path <- glue::glue("gs://{gcs_bucket}/{ducklake_path}/calcofi.duckdb")
  put_gcs_file(local_db, gcs_db_path)
  unlink(local_db)

  message(glue::glue("Uploaded database to {gcs_db_path}"))

  list(
    ducklake_path = glue::glue("gs://{gcs_bucket}/{ducklake_path}"),
    duckdb_path   = gcs_db_path,
    tables        = table_info)
}

#' Parse YAML Frontmatter from Quarto Notebooks
#'
#' Reads each `.qmd` file in a directory and extracts the `calcofi:` block
#' from the YAML frontmatter. Returns a tibble describing each workflow's
#' target name, type, dependencies, and output path.
#'
#' @param workflows_dir Path to the directory containing `.qmd` files
#'   (default: current directory via `here::here()`)
#' @param pattern Glob pattern for `.qmd` files (default: `"*.qmd"`)
#'
#' @return Tibble with columns: `qmd_file`, `target_name`, `workflow_type`,
#'   `dependency` (list column), `output`
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' wf <- parse_qmd_frontmatter("workflows/")
#' wf |> dplyr::filter(workflow_type == "ingest")
#' }
#' @importFrom yaml yaml.load
#' @importFrom tibble tibble
#' @importFrom purrr map_chr map compact
parse_qmd_frontmatter <- function(
    workflows_dir = here::here(),
    pattern       = "*.qmd") {

  qmd_files <- Sys.glob(file.path(workflows_dir, pattern))

  results <- purrr::compact(lapply(qmd_files, function(f) {
    lines <- readLines(f, n = 50, warn = FALSE)

    # find first and second --- delimiters
    delims <- which(trimws(lines) == "---")
    if (length(delims) < 2) return(NULL)

    yaml_text <- paste(lines[(delims[1] + 1):(delims[2] - 1)], collapse = "\n")
    meta <- tryCatch(
      yaml::yaml.load(yaml_text),
      error = function(e) NULL)
    if (is.null(meta) || is.null(meta$calcofi)) return(NULL)

    cc <- meta$calcofi
    tibble::tibble(
      qmd_file      = basename(f),
      target_name   = cc$target_name   %||% NA_character_,
      workflow_type = cc$workflow_type  %||% NA_character_,
      dependency    = list(cc$dependency %||% character(0)),
      output        = cc$output        %||% NA_character_)
  }))

  if (length(results) == 0) {
    return(tibble::tibble(
      qmd_file      = character(),
      target_name   = character(),
      workflow_type = character(),
      dependency    = list(),
      output        = character()))
  }

  dplyr::bind_rows(results)
}

#' Build Targets List from Quarto Frontmatter
#'
#' Reads `calcofi:` YAML frontmatter from all `.qmd` files in the workflows
#' directory and returns a `list()` of `targets::tar_target()` calls suitable
#' for use as the body of `_targets.R`. Includes a `corrections_csv` target
#' that tracks `metadata/ship_renames.csv` and `metadata/measurement_type.csv`,
#' so any edit to those files forces re-run of dependent ingest targets.
#'
#' Workflows with `dependency: [auto]` (typically `release_database`) auto-
#' depend on all targets whose `workflow_type` is `"ingest"` or `"spatial"`.
#'
#' @param workflows_dir Path to workflows directory (default: `here::here()`)
#' @param corrections Character vector of correction file paths relative to
#'   `workflows_dir` (default: `c("metadata/ship_renames.csv",
#'   "metadata/measurement_type.csv")`)
#' @param verbose Print parsed workflow table (default: TRUE)
#'
#' @return A `list()` of `tar_target()` objects ready for `_targets.R`
#' @export
#' @concept workflow
#'
#' @examples
#' \dontrun{
#' # in _targets.R:
#' library(targets)
#' devtools::load_all(here::here("../calcofi4db"))
#' build_targets_list()
#' }
#' @importFrom glue glue
build_targets_list <- function(
    workflows_dir = here::here(),
    corrections   = c("metadata/ship_renames.csv",
                       "metadata/measurement_type.csv"),
    verbose       = TRUE) {

  if (!requireNamespace("targets", quietly = TRUE)) {
    stop("Package 'targets' is required. Install with: install.packages('targets')")
  }

  wf <- parse_qmd_frontmatter(workflows_dir)

  if (nrow(wf) == 0) {
    stop("No .qmd files with calcofi: frontmatter found in ", workflows_dir)
  }

  if (verbose) {
    message("Parsed ", nrow(wf), " pipeline workflows:")
    for (i in seq_len(nrow(wf))) {
      deps <- paste(wf$dependency[[i]], collapse = ", ")
      message(glue::glue(
        "  {wf$target_name[i]} ({wf$workflow_type[i]}) -> {wf$output[i]}",
        "{if (nchar(deps) > 0) paste0(' [deps: ', deps, ']') else ''}"))
    }
  }

  # resolve [auto] dependencies → all ingest + spatial targets
  auto_targets <- wf$target_name[wf$workflow_type %in% c("ingest", "spatial")]

  # build corrections_csv target
  correction_paths <- file.path(workflows_dir, corrections)
  correction_paths <- correction_paths[file.exists(correction_paths)]

  target_list <- list()

  if (length(correction_paths) > 0) {
    # build c("path1", "path2") expression for targets
    paths_expr <- as.call(c(list(as.symbol("c")),
                             as.list(correction_paths)))
    target_list <- c(target_list, list(
      targets::tar_target_raw(
        "corrections_csv",
        paths_expr,
        format = "file")
    ))
  }

  # build a target for each workflow
  for (i in seq_len(nrow(wf))) {
    row <- wf[i, ]
    deps <- row$dependency[[1]]

    # resolve [auto]
    if (length(deps) == 1 && deps == "auto") {
      deps <- auto_targets
    }

    # build the body expression
    body_parts <- list()

    # add dependency symbols (bare references that targets uses for DAG)
    for (dep in deps) {
      body_parts <- c(body_parts, list(as.symbol(dep)))
    }

    # add corrections_csv dependency for ingest/spatial workflows
    if (row$workflow_type %in% c("ingest", "spatial") &&
        length(correction_paths) > 0) {
      body_parts <- c(body_parts, list(as.symbol("corrections_csv")))
    }

    # add quarto render call
    body_parts <- c(body_parts, list(
      bquote(quarto::quarto_render(.(row$qmd_file)))))

    # add output expression
    if (grepl("\\*", row$output)) {
      # glob pattern (e.g., data/darwincore/ichthyo_*.zip)
      body_parts <- c(body_parts, list(
        bquote(Sys.glob(.(row$output)))))
    } else {
      body_parts <- c(body_parts, list(row$output))
    }

    # wrap in braces
    body_expr <- as.call(c(list(as.symbol("{")), body_parts))

    target_list <- c(target_list, list(
      targets::tar_target_raw(
        row$target_name,
        body_expr,
        format = "file")
    ))
  }

  target_list
}
