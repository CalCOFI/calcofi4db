# freeze operations for calcofi data workflow
# Frozen DuckLake release management

#' Get metadata for a frozen release
#'
#' Retrieves detailed metadata for a specific frozen DuckLake release including
#' version info, table schemas, and statistics.
#'
#' @param version Version string (e.g., "v2026.02") or "latest" (default)
#' @param bucket GCS bucket name (default: "calcofi-db")
#'
#' @return List with:
#'   \itemize{
#'     \item \code{version}: Version string
#'     \item \code{release_date}: Release timestamp
#'     \item \code{release_notes}: Release notes (character)
#'     \item \code{tables}: Tibble with table metadata (name, rows, columns, size_bytes)
#'     \item \code{gcs_path}: GCS path to release files
#'   }
#'
#' @export
#' @concept freeze
#'
#' @examples
#' \dontrun{
#' meta <- get_release_metadata()
#' meta <- get_release_metadata("v2026.02")
#' meta$tables
#' }
#' @importFrom jsonlite fromJSON
#' @importFrom glue glue
get_release_metadata <- function(version = "latest", bucket = "calcofi-db") {
  # resolve "latest" to actual version
  if (version == "latest") {
    releases <- list_frozen_releases(bucket)
    if (nrow(releases) == 0) {
      stop("No frozen releases found")
    }
    version <- releases$version[releases$is_latest][1]
    if (is.na(version)) {
      version <- releases$version[1]
    }
  }

  gcs_base <- glue::glue("gs://{bucket}/ducklake/releases/{version}")

  # try to get catalog.json
  catalog <- tryCatch({
    local_file <- get_gcs_file(glue::glue("{gcs_base}/catalog.json"))
    jsonlite::fromJSON(local_file)
  }, error = function(e) {
    list(version = version, tables = list())
  })

  # try to get release notes
  release_notes <- tryCatch({
    local_file <- get_gcs_file(glue::glue("{gcs_base}/RELEASE_NOTES.md"))
    paste(readLines(local_file), collapse = "\n")
  }, error = function(e) {
    NA_character_
  })

  # get parquet file listing for size info
  parquet_files <- tryCatch({
    list_gcs_files(bucket, prefix = glue::glue("ducklake/releases/{version}/parquet/"))
  }, error = function(e) {
    tibble::tibble(name = character(), size = numeric())
  })

  # build table info
  tables <- if (!is.null(catalog$tables) && length(catalog$tables) > 0) {
    tibble::as_tibble(catalog$tables)
  } else if (nrow(parquet_files) > 0) {
    # derive from parquet files
    tibble::tibble(
      name       = gsub("\\.parquet$", "", basename(parquet_files$name)),
      rows       = NA_integer_,
      columns    = NA_integer_,
      size_bytes = parquet_files$size)
  } else {
    tibble::tibble(
      name       = character(),
      rows       = integer(),
      columns    = integer(),
      size_bytes = numeric())
  }

  list(
    version       = version,
    release_date  = catalog$release_date %||% NA_POSIXct_,
    release_notes = release_notes,
    tables        = tables,
    gcs_path      = gcs_base)
}

#' List available frozen releases
#'
#' Lists all available frozen DuckLake releases with metadata.
#'
#' @param bucket GCS bucket name (default: "calcofi-db")
#'
#' @return Tibble with columns:
#'   \itemize{
#'     \item \code{version}: Version string (e.g., "v2026.02")
#'     \item \code{release_date}: When release was created (POSIXct)
#'     \item \code{tables}: Number of tables
#'     \item \code{total_rows}: Total row count across all tables
#'     \item \code{size_mb}: Total size in megabytes
#'     \item \code{is_latest}: TRUE if this is the latest release
#'   }
#'
#' @export
#' @concept freeze
#'
#' @examples
#' \dontrun{
#' releases <- list_frozen_releases()
#' releases
#' }
#' @importFrom tibble tibble
#' @importFrom glue glue
#' @importFrom purrr map_dfr
list_frozen_releases <- function(bucket = "calcofi-db") {
  # list directories under ducklake/releases/
  files <- tryCatch({
    list_gcs_files(bucket, prefix = "ducklake/releases/")
  }, error = function(e) {
    return(tibble::tibble(
      version      = character(),
      release_date = as.POSIXct(character()),
      tables       = integer(),
      total_rows   = integer(),
      size_mb      = numeric(),
      is_latest    = logical()))
  })

  if (nrow(files) == 0) {
    return(tibble::tibble(
      version      = character(),
      release_date = as.POSIXct(character()),
      tables       = integer(),
      total_rows   = integer(),
      size_mb      = numeric(),
      is_latest    = logical()))
  }

  # extract version directories
  versions <- unique(gsub("^ducklake/releases/([^/]+)/.*$", "\\1", files$name))
  versions <- versions[grepl("^v\\d{4}\\.\\d{2}", versions)]

  if (length(versions) == 0) {
    return(tibble::tibble(
      version      = character(),
      release_date = as.POSIXct(character()),
      tables       = integer(),
      total_rows   = integer(),
      size_mb      = numeric(),
      is_latest    = logical()))
  }

  # get metadata for each version
  releases <- purrr::map_dfr(versions, function(v) {
    v_files <- files |>
      dplyr::filter(grepl(glue::glue("^ducklake/releases/{v}/"), name))

    parquet_files <- v_files |>
      dplyr::filter(grepl("\\.parquet$", name))

    # try to get catalog info
    catalog <- tryCatch({
      local_file <- get_gcs_file(glue::glue("gs://{bucket}/ducklake/releases/{v}/catalog.json"))
      jsonlite::fromJSON(local_file)
    }, error = function(e) {
      list()
    })

    tibble::tibble(
      version      = v,
      release_date = catalog$release_date %||% NA_POSIXct_,
      tables       = nrow(parquet_files),
      total_rows   = catalog$total_rows %||% NA_integer_,
      size_mb      = sum(v_files$size, na.rm = TRUE) / 1e6)
  })

  # determine latest
  if (nrow(releases) > 0) {
    releases <- releases |>
      dplyr::arrange(dplyr::desc(version)) |>
      dplyr::mutate(is_latest = dplyr::row_number() == 1)
  }

  releases
}

#' Validate Working DuckLake for release
#'
#' Runs data quality checks on the Working DuckLake before creating a frozen release.
#' Checks include: null required fields, valid value ranges, foreign key integrity,
#' row count expectations, and data completeness.
#'
#' @param con DuckDB connection from \code{get_working_ducklake()}
#' @param checks Character vector of check names to run, or "all" (default).
#'   Available checks: "nulls", "ranges", "foreign_keys", "row_counts", "completeness"
#' @param strict Logical, fail on warnings as well as errors (default: FALSE)
#' @param config Optional list of validation configuration (expected row counts, etc.)
#'
#' @return List with:
#'   \itemize{
#'     \item \code{passed}: Logical, TRUE if all checks passed
#'     \item \code{checks}: Tibble with individual check results (name, status, message)
#'     \item \code{errors}: Character vector of error messages
#'     \item \code{warnings}: Character vector of warning messages
#'   }
#'
#' @export
#' @concept freeze
#'
#' @examples
#' \dontrun{
#' con <- get_working_ducklake()
#' validation <- validate_for_release(con)
#' if (validation$passed) {
#'   freeze_release(con, version = "v2026.02")
#' }
#' }
#' @importFrom DBI dbListTables dbGetQuery
#' @importFrom tibble tibble
#' @importFrom purrr map_dfr
#' @importFrom glue glue
validate_for_release <- function(
    con,
    checks = "all",
    strict = FALSE,
    config = NULL) {

  errors   <- character()
  warnings <- character()
  results  <- tibble::tibble(
    check   = character(),
    table   = character(),
    status  = character(),
    message = character())

  tables <- DBI::dbListTables(con)
  tables <- tables[!grepl("^_", tables)]  # exclude system tables

  if (length(tables) == 0) {
    errors <- c(errors, "No tables found in database")
    return(list(
      passed   = FALSE,
      checks   = results,
      errors   = errors,
      warnings = warnings))
  }

  run_check <- function(check_name) {
    checks == "all" || check_name %in% checks
  }

  # check 1: null required fields (primary keys, foreign keys should not be null)
  if (run_check("nulls")) {
    for (tbl in tables) {
      # get columns ending in _id, _uuid, _key (likely required)
      cols <- DBI::dbGetQuery(
        con,
        glue::glue("SELECT column_name FROM information_schema.columns
                    WHERE table_name = '{tbl}'"))$column_name

      required_cols <- cols[grepl("(_id|_uuid|_key)$", cols)]
      required_cols <- setdiff(required_cols, c("_source_uuid"))  # exclude provenance

      for (col in required_cols) {
        null_count <- DBI::dbGetQuery(
          con,
          glue::glue("SELECT COUNT(*) as n FROM {tbl} WHERE {col} IS NULL"))$n

        if (null_count > 0) {
          msg <- glue::glue("Table '{tbl}' has {null_count} NULL values in required column '{col}'")
          errors <- c(errors, msg)
          results <- dplyr::bind_rows(results, tibble::tibble(
            check = "nulls", table = tbl, status = "error", message = msg))
        }
      }
    }
  }

  # check 2: valid ranges (lat/lon, dates, counts)
  if (run_check("ranges")) {
    range_checks <- list(
      list(col = "latitude",  min = -90,  max = 90),
      list(col = "longitude", min = -180, max = 180),
      list(col = "lat",       min = -90,  max = 90),
      list(col = "lon",       min = -180, max = 180),
      list(col = "tally",     min = 0,    max = NULL),
      list(col = "count",     min = 0,    max = NULL))

    for (tbl in tables) {
      cols <- DBI::dbGetQuery(
        con,
        glue::glue("SELECT column_name FROM information_schema.columns
                    WHERE table_name = '{tbl}'"))$column_name

      for (rc in range_checks) {
        if (rc$col %in% cols) {
          # check min
          if (!is.null(rc$min)) {
            below_min <- DBI::dbGetQuery(
              con,
              glue::glue("SELECT COUNT(*) as n FROM {tbl}
                          WHERE {rc$col} < {rc$min}"))$n
            if (below_min > 0) {
              msg <- glue::glue("Table '{tbl}': {below_min} rows have {rc$col} < {rc$min}")
              warnings <- c(warnings, msg)
              results <- dplyr::bind_rows(results, tibble::tibble(
                check = "ranges", table = tbl, status = "warning", message = msg))
            }
          }
          # check max
          if (!is.null(rc$max)) {
            above_max <- DBI::dbGetQuery(
              con,
              glue::glue("SELECT COUNT(*) as n FROM {tbl}
                          WHERE {rc$col} > {rc$max}"))$n
            if (above_max > 0) {
              msg <- glue::glue("Table '{tbl}': {above_max} rows have {rc$col} > {rc$max}")
              warnings <- c(warnings, msg)
              results <- dplyr::bind_rows(results, tibble::tibble(
                check = "ranges", table = tbl, status = "warning", message = msg))
            }
          }
        }
      }
    }
  }

  # check 3: row counts (ensure tables are not empty)
  if (run_check("row_counts")) {
    for (tbl in tables) {
      row_count <- DBI::dbGetQuery(
        con,
        glue::glue("SELECT COUNT(*) as n FROM {tbl}"))$n

      if (row_count == 0) {
        msg <- glue::glue("Table '{tbl}' is empty")
        warnings <- c(warnings, msg)
        results <- dplyr::bind_rows(results, tibble::tibble(
          check = "row_counts", table = tbl, status = "warning", message = msg))
      } else {
        results <- dplyr::bind_rows(results, tibble::tibble(
          check = "row_counts", table = tbl, status = "pass",
          message = glue::glue("{row_count} rows")))
      }

      # check against expected if provided in config
      if (!is.null(config$expected_rows[[tbl]])) {
        expected <- config$expected_rows[[tbl]]
        if (row_count < expected * 0.9) {
          msg <- glue::glue("Table '{tbl}' has {row_count} rows, expected ~{expected}")
          warnings <- c(warnings, msg)
          results <- dplyr::bind_rows(results, tibble::tibble(
            check = "row_counts", table = tbl, status = "warning", message = msg))
        }
      }
    }
  }

  # check 4: completeness (expected tables present)
  if (run_check("completeness")) {
    expected_tables <- config$expected_tables %||%
      c("cruise", "site", "tow", "net", "larva", "species")

    missing <- setdiff(expected_tables, tables)
    if (length(missing) > 0) {
      msg <- glue::glue("Missing expected tables: {paste(missing, collapse = ', ')}")
      warnings <- c(warnings, msg)
      results <- dplyr::bind_rows(results, tibble::tibble(
        check = "completeness", table = NA_character_, status = "warning", message = msg))
    }
  }

  # determine pass/fail
  passed <- length(errors) == 0
  if (strict && length(warnings) > 0) {
    passed <- FALSE
  }

  list(
    passed   = passed,
    checks   = results,
    errors   = errors,
    warnings = warnings)
}

#' Freeze a release of the DuckLake
#'
#' Creates an immutable frozen release from the current Working DuckLake state.
#' Strips provenance columns, exports tables to Parquet, creates a catalog,
#' and uploads to \code{gs://calcofi-db/ducklake/releases/{version}/}.
#'
#' @param con DuckDB connection from \code{get_working_ducklake()}
#' @param version Version string in format "vYYYY.MM" (e.g., "v2026.02")
#' @param release_notes Character string with release notes, or path to markdown file
#' @param validate Run validation checks before freezing (default: TRUE)
#' @param tables Character vector of tables to include (default: all non-system tables)
#' @param dry_run If TRUE, simulate without uploading (default: FALSE)
#' @param bucket GCS bucket name (default: "calcofi-db")
#'
#' @return List with:
#'   \itemize{
#'     \item \code{version}: The version string
#'     \item \code{gcs_path}: GCS path to release
#'     \item \code{tables}: Tibble with table names and row counts
#'     \item \code{parquet_files}: Paths to created Parquet files
#'   }
#'
#' @export
#' @concept freeze
#'
#' @examples
#' \dontrun{
#' con <- get_working_ducklake()
#' result <- freeze_release(
#'   con           = con,
#'   version       = "v2026.02",
#'   release_notes = "First release with bottle and larvae data.")
#' }
#' @importFrom DBI dbListTables dbGetQuery dbExecute
#' @importFrom glue glue
#' @importFrom jsonlite write_json
#' @importFrom tibble tibble
#' @importFrom purrr map_chr map_dfr
freeze_release <- function(
    con,
    version,
    release_notes = NULL,
    validate      = TRUE,
    tables        = NULL,
    dry_run       = FALSE,
    bucket        = "calcofi-db") {

  # validate version format
  if (!grepl("^v\\d{4}\\.\\d{2}(\\.\\d+)?$", version)) {
    stop("Version must be in format vYYYY.MM or vYYYY.MM.patch (e.g., 'v2026.02' or 'v2026.02.1')")
  }

  # check if version already exists
  existing <- list_frozen_releases(bucket)
  if (version %in% existing$version) {
    stop(glue::glue("Version {version} already exists. Use a different version or delete the existing one."))
  }

  # run validation if requested
  if (validate) {
    message("Running validation checks...")
    validation <- validate_for_release(con)
    if (!validation$passed) {
      stop(glue::glue(
        "Validation failed with {length(validation$errors)} error(s):\n",
        paste(validation$errors, collapse = "\n")))
    }
    if (length(validation$warnings) > 0) {
      message(glue::glue(
        "Validation passed with {length(validation$warnings)} warning(s):\n",
        paste(validation$warnings, collapse = "\n")))
    }
  }

  # get tables to export
  if (is.null(tables)) {
    tables <- DBI::dbListTables(con)
    tables <- tables[!grepl("^_", tables)]  # exclude system tables
  }

  if (length(tables) == 0) {
    stop("No tables to export")
  }

  message(glue::glue("Freezing {length(tables)} tables to version {version}..."))

  # create temp directory for parquet files
  temp_dir     <- tempfile("freeze_")
  parquet_dir  <- file.path(temp_dir, "parquet")
  dir.create(parquet_dir, recursive = TRUE)

  # export each table to parquet without provenance columns
  table_info <- purrr::map_dfr(tables, function(tbl) {
    message(glue::glue("  Exporting {tbl}..."))

    # get columns (exclude provenance)
    cols <- DBI::dbGetQuery(
      con,
      glue::glue("SELECT column_name FROM information_schema.columns
                  WHERE table_name = '{tbl}'"))$column_name

    provenance_cols <- c("_source_file", "_source_row", "_source_uuid", "_ingested_at")
    export_cols     <- setdiff(cols, provenance_cols)

    if (length(export_cols) == 0) {
      warning(glue::glue("Table '{tbl}' has no columns after removing provenance"))
      return(NULL)
    }

    cols_sql    <- paste(export_cols, collapse = ", ")
    parquet_path <- file.path(parquet_dir, glue::glue("{tbl}.parquet"))

    DBI::dbExecute(con, glue::glue(
      "COPY (SELECT {cols_sql} FROM {tbl}) TO '{parquet_path}'
       (FORMAT PARQUET, COMPRESSION 'snappy')"))

    # get row count
    row_count <- DBI::dbGetQuery(
      con,
      glue::glue("SELECT COUNT(*) as n FROM {tbl}"))$n

    tibble::tibble(
      name       = tbl,
      rows       = row_count,
      columns    = length(export_cols),
      local_path = parquet_path)
  })

  # create catalog.json
  catalog <- list(
    version      = version,
    release_date = as.character(Sys.time()),
    total_rows   = sum(table_info$rows, na.rm = TRUE),
    tables       = table_info |> dplyr::select(name, rows, columns))

  catalog_path <- file.path(temp_dir, "catalog.json")
  jsonlite::write_json(catalog, catalog_path, pretty = TRUE, auto_unbox = TRUE)

  # write release notes
  notes_path <- file.path(temp_dir, "RELEASE_NOTES.md")
  if (!is.null(release_notes)) {
    if (file.exists(release_notes)) {
      file.copy(release_notes, notes_path)
    } else {
      writeLines(release_notes, notes_path)
    }
  } else {
    writeLines(glue::glue("# Release {version}\n\nReleased: {Sys.time()}"), notes_path)
  }

  parquet_files <- table_info$local_path
  gcs_base      <- glue::glue("gs://{bucket}/ducklake/releases/{version}")

  if (dry_run) {
    message("\nDry run - files not uploaded")
    message(glue::glue("Would upload to: {gcs_base}"))
    message(glue::glue("Tables: {paste(tables, collapse = ', ')}"))
  } else {
    message("\nUploading to GCS...")

    # upload catalog
    put_gcs_file(catalog_path, glue::glue("{gcs_base}/catalog.json"))

    # upload release notes
    put_gcs_file(notes_path, glue::glue("{gcs_base}/RELEASE_NOTES.md"))

    # upload parquet files
    for (i in seq_along(parquet_files)) {
      tbl_name <- table_info$name[i]
      put_gcs_file(
        parquet_files[i],
        glue::glue("{gcs_base}/parquet/{tbl_name}.parquet"))
    }

    # update latest symlink (by uploading a pointer file)
    latest_path <- file.path(temp_dir, "latest.txt")
    writeLines(version, latest_path)
    put_gcs_file(latest_path, glue::glue("gs://{bucket}/ducklake/releases/latest.txt"))

    message(glue::glue("\nRelease {version} created at {gcs_base}"))
  }

  # cleanup
  unlink(temp_dir, recursive = TRUE)

  list(
    version       = version,
    gcs_path      = gcs_base,
    tables        = table_info |> dplyr::select(name, rows, columns),
    parquet_files = glue::glue("{gcs_base}/parquet/{table_info$name}.parquet"))
}

#' Compare two frozen releases
#'
#' Generates a diff report between two frozen releases showing added, removed,
#' and modified tables and schema changes.
#'
#' @param v1 First version string (e.g., "v2026.02")
#' @param v2 Second version string (e.g., "v2026.03")
#' @param bucket GCS bucket name (default: "calcofi-db")
#'
#' @return List with:
#'   \itemize{
#'     \item \code{summary}: Tibble with table-level summary (table, rows_v1, rows_v2, change)
#'     \item \code{tables_added}: Tables in v2 but not v1
#'     \item \code{tables_removed}: Tables in v1 but not v2
#'     \item \code{row_changes}: Tibble with row count changes
#'   }
#'
#' @export
#' @concept freeze
#'
#' @examples
#' \dontrun{
#' diff <- compare_releases("v2026.02", "v2026.03")
#' diff$summary
#' diff$tables_added
#' }
#' @importFrom tibble tibble
#' @importFrom dplyr full_join mutate case_when
compare_releases <- function(v1, v2, bucket = "calcofi-db") {
  # get metadata for both versions
  meta1 <- get_release_metadata(v1, bucket)
  meta2 <- get_release_metadata(v2, bucket)

  tables1 <- meta1$tables$name
  tables2 <- meta2$tables$name

  # find added/removed tables
  tables_added   <- setdiff(tables2, tables1)
  tables_removed <- setdiff(tables1, tables2)

  # create summary
  summary <- dplyr::full_join(
    meta1$tables |> dplyr::select(name, rows_v1 = rows),
    meta2$tables |> dplyr::select(name, rows_v2 = rows),
    by = "name") |>
    dplyr::mutate(
      change = dplyr::case_when(
        is.na(rows_v1)       ~ "added",
        is.na(rows_v2)       ~ "removed",
        rows_v2 > rows_v1    ~ "increased",
        rows_v2 < rows_v1    ~ "decreased",
        TRUE                 ~ "unchanged"),
      diff = rows_v2 - rows_v1)

  list(
    summary        = summary,
    tables_added   = tables_added,
    tables_removed = tables_removed,
    v1_metadata    = meta1,
    v2_metadata    = meta2)
}

#' Upload Frozen Release to GCS
#'
#' Uploads a frozen release directory to GCS, creating required metadata files
#' (catalog.json, versions.json, latest.txt) and making files publicly accessible.
#'
#' @param release_dir Path to local release directory (contains parquet/ subdirectory)
#' @param version Version string (e.g., "v2026.02")
#' @param bucket GCS bucket name (default: "calcofi-db")
#' @param set_latest Logical, update latest.txt to point to this version (default: TRUE)
#'
#' @return List with upload statistics (files uploaded, total size)
#'
#' @export
#' @concept freeze
#'
#' @examples
#' \dontrun{
#' upload_frozen_release(
#'   release_dir = "data/releases/v2026.02",
#'   version     = "v2026.02")
#' }
#' @importFrom jsonlite toJSON fromJSON
#' @importFrom glue glue
#' @importFrom tibble tibble
upload_frozen_release <- function(
    release_dir,
    version,
    bucket     = "calcofi-db",
    set_latest = TRUE) {

  stopifnot(
    dir.exists(release_dir),
    grepl("^v\\d{4}\\.\\d{2}$", version))

  parquet_dir <- file.path(release_dir, "parquet")
  stopifnot(dir.exists(parquet_dir))

  gcs_base <- glue::glue("gs://{bucket}/ducklake/releases/{version}")
  message(glue::glue("Uploading frozen release {version} to {gcs_base}..."))

  # read manifest to get table info
  manifest_path <- file.path(parquet_dir, "manifest.json")
  if (!file.exists(manifest_path)) {
    stop("manifest.json not found in parquet directory")
  }
  manifest <- jsonlite::fromJSON(manifest_path)

  # create catalog.json (format expected by cc_get_db())
  tables_df <- tibble::tibble(
    name = manifest$tables,
    rows = as.integer(manifest$files$rows))

  catalog <- list(
    version      = version,
    release_date = as.character(Sys.Date()),
    total_rows   = sum(tables_df$rows),
    total_size   = manifest$total_size_bytes,
    tables       = tables_df)

  catalog_path <- file.path(release_dir, "catalog.json")
  jsonlite::write_json(catalog, catalog_path, auto_unbox = TRUE, pretty = TRUE)
  message(glue::glue("Created catalog.json with {nrow(tables_df)} tables"))

  # upload parquet files
  parquet_files <- list.files(parquet_dir, pattern = "\\.parquet$", full.names = TRUE)
  message(glue::glue("Uploading {length(parquet_files)} parquet files..."))

  for (pf in parquet_files) {
    gcs_path <- glue::glue("{gcs_base}/parquet/{basename(pf)}")
    put_gcs_file(pf, gcs_path)
  }

  # upload catalog.json
  put_gcs_file(catalog_path, glue::glue("{gcs_base}/catalog.json"))

  # upload RELEASE_NOTES.md if exists
  notes_path <- file.path(release_dir, "RELEASE_NOTES.md")
  if (file.exists(notes_path)) {
    put_gcs_file(notes_path, glue::glue("{gcs_base}/RELEASE_NOTES.md"))
    message("Uploaded RELEASE_NOTES.md")
  }

  # update versions.json
  versions_gcs <- glue::glue("gs://{bucket}/ducklake/releases/versions.json")
  versions_local <- tempfile(fileext = ".json")

  existing_versions <- tryCatch({
    get_gcs_file(versions_gcs, versions_local)
    jsonlite::fromJSON(versions_local)
  }, error = function(e) {
    list(versions = list())
  })

  # add/update this version
  new_version <- list(
    version      = version,
    release_date = as.character(Sys.Date()),
    tables       = nrow(tables_df),
    total_rows   = sum(tables_df$rows),
    size_mb      = round(manifest$total_size_bytes / 1024 / 1024, 1))

  # remove existing entry for this version if present
  if (length(existing_versions$versions) > 0) {
    existing_versions$versions <- Filter(
      function(v) v$version != version,
      existing_versions$versions)
  }

  # add new version at the beginning
  existing_versions$versions <- c(list(new_version), existing_versions$versions)

  # write and upload
  jsonlite::write_json(existing_versions, versions_local, auto_unbox = TRUE, pretty = TRUE)
  put_gcs_file(versions_local, versions_gcs)
  message("Updated versions.json")

  # update latest.txt
  if (set_latest) {
    latest_local <- tempfile()
    writeLines(version, latest_local)
    put_gcs_file(latest_local, glue::glue("gs://{bucket}/ducklake/releases/latest.txt"))
    message(glue::glue("Set latest.txt to {version}"))
  }

  message(glue::glue(
    "Release {version} uploaded successfully: ",
    "{length(parquet_files)} files, ",
    "{round(manifest$total_size_bytes / 1024 / 1024, 1)} MB"))

  invisible(list(
    version     = version,
    files       = length(parquet_files),
    total_bytes = manifest$total_size_bytes,
    gcs_path    = gcs_base))
}

# internal helper: null coalescing operator
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}
