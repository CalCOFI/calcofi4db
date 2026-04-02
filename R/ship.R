# ship code reconciliation between datasets

#' Fetch Ship Codes from ICES Reference Codes API
#'
#' Retrieves the full list of ship/platform codes from the ICES Vocabularies
#' API. The default code type GUID corresponds to "SHIPC" (ship codes).
#'
#' @param ices_api character; ICES API base URL
#' @param ices_ship_code_type character; GUID for ship code type
#' @return tibble with columns: ship_nodc, ship_name, remarks, src
#' @export
#' @concept ship
#'
#' @examples
#' \dontrun{
#' d_ices <- fetch_ship_ices()
#' }
#' @importFrom purrr map_dfr
#' @importFrom tibble tibble
#' @importFrom glue glue
#' @importFrom rlang %||%
fetch_ship_ices <- function(
    ices_api            = "https://vocab.ices.dk/services/api",
    ices_ship_code_type = "7f9a91e1-fb57-464a-8eb0-697e4b0235b5") {

  if (!requireNamespace("httr2", quietly = TRUE))
    stop("Package 'httr2' is required for fetch_ship_ices()")

  # fetch paginated results from ICES API
  url_base <- glue::glue("{ices_api}/CodeType/{ices_ship_code_type}/codes")
  page     <- 1
  per_page <- 1000
  all_rows <- list()


  repeat {
    resp <- httr2::request(glue::glue("{url_base}?PageSize={per_page}&Page={page}")) |>
      httr2::req_headers(Accept = "application/json") |>
      httr2::req_perform()

    body <- httr2::resp_body_json(resp)

    if (length(body) == 0) break

    rows <- purrr::map_dfr(body, function(item) {
      tibble::tibble(
        ship_nodc = item$Key    %||% NA_character_,
        ship_name = item$Description %||% NA_character_,
        remarks   = item$LongDescription %||% NA_character_)
    })

    all_rows <- c(all_rows, list(rows))

    if (nrow(rows) < per_page) break
    page <- page + 1
  }

  result <- dplyr::bind_rows(all_rows) |>
    dplyr::mutate(src = "ices")

  message(glue::glue("Fetched {nrow(result)} ship codes from ICES API"))
  result
}

#' Match Ship Codes Across Datasets Using Multi-Source References
#'
#' Reconciles ship codes between datasets using exact matching on ship_nodc
#' and ship_name, followed by word-overlap fuzzy matching against CalCOFI UNOLS,
#' NODC WOD, and optionally ICES reference sources. Manual overrides can be
#' provided via a CSV file.
#'
#' Adapted from the ship reconciliation pattern in
#' `workflows/ingest_calcofi_bottle.qmd` (lines 117-260).
#'
#' @param unmatched_ships tibble with columns: ship_code, ship_name
#'   (ships needing reconciliation from bottle casts)
#' @param reference_ships tibble with columns: ship_key, ship_nodc,
#'   ship_name (known ships from swfsc database)
#' @param ship_renames_csv optional path to manual overrides CSV with columns:
#'   csv_name, csv_code, csv_name_new, csv_code_new
#' @param fetch_ices logical; if TRUE, also query ICES ship API (default TRUE)
#' @return tibble with match results: ship_code, ship_name, match_type,
#'   matched_ship_key, matched_ship_nodc, matched_ship_name, confidence
#' @export
#' @concept ship
#'
#' @examples
#' \dontrun{
#' unmatched <- tibble(ship_code = "325S", ship_name = "RV BELL M SHIMADA")
#' reference <- tibble(ship_key = "3322", ship_nodc = "3322",
#'   ship_name = "BELL M. SHIMADA")
#' matches <- match_ships(unmatched, reference)
#' }
#' @importFrom dplyr mutate filter arrange left_join anti_join bind_rows select
#' @importFrom dplyr distinct pull
#' @importFrom purrr map_chr map_int
#' @importFrom stringr str_split str_to_upper str_trim
#' @importFrom tibble tibble
#' @importFrom readr read_csv
#' @importFrom glue glue
match_ships <- function(
    unmatched_ships,
    reference_ships,
    ship_renames_csv = NULL,
    fetch_ices       = TRUE) {

  stopifnot(
    all(c("ship_code", "ship_name") %in% names(unmatched_ships)),
    all(c("ship_key", "ship_nodc", "ship_name") %in% names(reference_ships)))

  if (nrow(unmatched_ships) == 0) {
    message("No unmatched ships to reconcile")
    return(tibble::tibble(
      ship_code         = character(),
      ship_name         = character(),
      match_type        = character(),
      matched_ship_key  = character(),
      matched_ship_nodc = character(),
      matched_ship_name = character(),
      confidence        = numeric()))
  }

  # rename reference columns to avoid ambiguity
  ref <- reference_ships |>
    dplyr::select(
      ref_key  = ship_key,
      ref_nodc = ship_nodc,
      ref_name = ship_name)

  results <- list()

  # step 1: apply manual overrides ----
  if (!is.null(ship_renames_csv) && file.exists(ship_renames_csv)) {
    renames <- readr::read_csv(ship_renames_csv, show_col_types = FALSE)

    if (all(c("csv_name", "csv_code", "csv_name_new", "csv_code_new") %in%
            names(renames))) {

      manual_matches <- unmatched_ships |>
        dplyr::inner_join(
          renames |> dplyr::select(
            ship_code = csv_code, ship_name = csv_name,
            new_code  = csv_code_new, new_name = csv_name_new),
          by = c("ship_code", "ship_name")) |>
        dplyr::left_join(
          ref, by = c("new_code" = "ref_nodc")) |>
        dplyr::mutate(
          match_type        = "manual_override",
          matched_ship_key  = ref_key,
          matched_ship_nodc = new_code,
          matched_ship_name = dplyr::coalesce(ref_name, new_name),
          confidence        = 1.0) |>
        dplyr::select(
          ship_code, ship_name, match_type,
          matched_ship_key, matched_ship_nodc, matched_ship_name, confidence)

      results <- c(results, list(manual_matches))
      unmatched_ships <- unmatched_ships |>
        dplyr::anti_join(manual_matches, by = c("ship_code", "ship_name"))
    }
  }

  if (nrow(unmatched_ships) == 0) {
    return(dplyr::bind_rows(results))
  }

  # step 2: exact match on ship_nodc ----
  exact_code <- unmatched_ships |>
    dplyr::inner_join(ref, by = c("ship_code" = "ref_nodc")) |>
    dplyr::mutate(
      match_type        = "exact_nodc",
      matched_ship_key  = ref_key,
      matched_ship_nodc = ship_code,
      matched_ship_name = ref_name,
      confidence        = 1.0) |>
    dplyr::select(
      ship_code, ship_name, match_type,
      matched_ship_key, matched_ship_nodc, matched_ship_name, confidence)

  results <- c(results, list(exact_code))
  unmatched_ships <- unmatched_ships |>
    dplyr::anti_join(exact_code, by = "ship_code")

  if (nrow(unmatched_ships) == 0) {
    return(dplyr::bind_rows(results))
  }

  # step 3: exact match on ship_name ----
  exact_name <- unmatched_ships |>
    dplyr::mutate(name_upper = stringr::str_to_upper(stringr::str_trim(ship_name))) |>
    dplyr::inner_join(
      ref |> dplyr::mutate(
        name_upper = stringr::str_to_upper(stringr::str_trim(ref_name))),
      by = "name_upper") |>
    dplyr::mutate(
      match_type        = "exact_name",
      matched_ship_key  = ref_key,
      matched_ship_nodc = ref_nodc,
      matched_ship_name = ref_name,
      confidence        = 1.0) |>
    dplyr::select(
      ship_code, ship_name, match_type,
      matched_ship_key, matched_ship_nodc, matched_ship_name, confidence)

  results <- c(results, list(exact_name))
  unmatched_ships <- unmatched_ships |>
    dplyr::anti_join(exact_name, by = "ship_code")

  if (nrow(unmatched_ships) == 0) {
    return(dplyr::bind_rows(results))
  }

  # step 4: build external reference table ----
  d_ships <- list()

  # fetch CalCOFI UNOLS codes
  tryCatch({
    if (requireNamespace("rvest", quietly = TRUE)) {
      cc_url <- "https://www.calcofi.info/index.php/field-work/calcofi-ships/unols-ship-codes"
      urls <- rvest::read_html(cc_url) |>
        rvest::html_nodes("table a") |>
        rvest::html_attr("href") |>
        purrr::keep(~ grepl("/unols-ship-codes/\\d+-", .x)) |>
        purrr::map_chr(~ paste0("https://www.calcofi.info", .x))

      d_cc <- purrr::map_dfr(urls, function(u) {
        tryCatch({
          tbl <- rvest::read_html(u) |>
            rvest::html_node("table") |>
            rvest::html_table(header = TRUE)
          if (!is.null(tbl) && nrow(tbl) > 0) {
            tbl <- janitor::clean_names(tbl)
            cn <- names(tbl)
            # normalize column names
            code_col <- cn[grepl("code|ship_code", cn)][1]
            name_col <- cn[grepl("ship$|ship_name", cn)][1]
            if (!is.na(code_col) && !is.na(name_col)) {
              tibble::tibble(
                ship_nodc = as.character(tbl[[code_col]]),
                ship_name = as.character(tbl[[name_col]]),
                remarks   = if ("remarks" %in% cn) as.character(tbl[["remarks"]]) else NA_character_,
                src       = "cc")
            }
          }
        }, error = function(e) NULL)
      })
      if (!is.null(d_cc) && nrow(d_cc) > 0)
        d_ships <- c(d_ships, list(d_cc))
      message(glue::glue("Fetched {nrow(d_cc)} CalCOFI UNOLS ship codes"))
    }
  }, error = function(e) {
    message(glue::glue("Could not fetch CalCOFI UNOLS codes: {e$message}"))
  })

  # fetch NODC platform codes
  tryCatch({
    if (requireNamespace("rvest", quietly = TRUE)) {
      nodc_url <- "https://www.nodc.noaa.gov/OC5/WOD/CODES/s_3_platform.html"
      d_nodc <- rvest::read_html(nodc_url) |>
        rvest::html_node("table") |>
        rvest::html_table(header = TRUE) |>
        janitor::clean_names()

      # extract name and remarks from platform_name column
      cn <- names(d_nodc)
      code_col <- cn[grepl("nodc_code|code", cn)][1]
      plat_col <- cn[grepl("platform", cn)][1]

      if (!is.na(code_col) && !is.na(plat_col)) {
        d_nodc <- tibble::tibble(
          ship_nodc = as.character(d_nodc[[code_col]]),
          p         = as.character(d_nodc[[plat_col]])) |>
          dplyr::mutate(
            ship_name = stringr::str_replace(p, "(.*) \\(.*\\)", "\\1"),
            remarks   = purrr::map_chr(p, function(p) {
              if (!stringr::str_detect(p, "\\(")) return(NA_character_)
              stringr::str_replace(p, "(.*) \\((.*)\\)", "\\2")
            }),
            src = "nodc") |>
          dplyr::select(ship_nodc, ship_name, remarks, src)
        d_ships <- c(d_ships, list(d_nodc))
        message(glue::glue("Fetched {nrow(d_nodc)} NODC platform codes"))
      }
    }
  }, error = function(e) {
    message(glue::glue("Could not fetch NODC codes: {e$message}"))
  })

  # fetch ICES codes (optional)
  if (fetch_ices) {
    tryCatch({
      d_ices <- fetch_ship_ices()
      d_ships <- c(d_ships, list(d_ices))
    }, error = function(e) {
      message(glue::glue("Could not fetch ICES codes: {e$message}"))
    })
  }

  # combine all external references
  d_ext <- dplyr::bind_rows(d_ships)

  if (nrow(d_ext) > 0) {
    # step 5: lookup unmatched by code in external references ----
    ext_code <- unmatched_ships |>
      dplyr::inner_join(
        d_ext |> dplyr::select(ship_nodc, ext_name = ship_name, src) |>
          dplyr::distinct(ship_nodc, .keep_all = TRUE),
        by = c("ship_code" = "ship_nodc")) |>
      # now try to match ext_name to reference_ships
      dplyr::mutate(name_upper = stringr::str_to_upper(stringr::str_trim(ext_name))) |>
      dplyr::left_join(
        ref |> dplyr::mutate(
          name_upper = stringr::str_to_upper(stringr::str_trim(ref_name))),
        by = "name_upper") |>
      dplyr::filter(!is.na(ref_key)) |>
      dplyr::mutate(
        match_type        = glue::glue("ext_code_{src}"),
        matched_ship_key  = ref_key,
        matched_ship_nodc = ref_nodc,
        matched_ship_name = ref_name,
        confidence        = 0.9) |>
      dplyr::select(
        ship_code, ship_name, match_type,
        matched_ship_key, matched_ship_nodc, matched_ship_name, confidence)

    results <- c(results, list(ext_code))
    unmatched_ships <- unmatched_ships |>
      dplyr::anti_join(ext_code, by = "ship_code")
  }

  if (nrow(unmatched_ships) == 0) {
    return(dplyr::bind_rows(results))
  }

  # step 6: word-overlap fuzzy matching against reference_ships ----
  fuzzy <- purrr::map_dfr(seq_len(nrow(unmatched_ships)), function(i) {
    row <- unmatched_ships[i, ]
    wds_csv <- stringr::str_split(
      stringr::str_to_upper(row$ship_name), "\\s+")[[1]]
    wds_csv <- wds_csv[nchar(wds_csv) > 1]  # drop single-char words

    if (length(wds_csv) == 0) {
      return(tibble::tibble(
        ship_code = row$ship_code, ship_name = row$ship_name,
        match_type = "unmatched", matched_ship_key = NA_character_,
        matched_ship_nodc = NA_character_, matched_ship_name = NA_character_,
        confidence = 0))
    }

    best <- ref |>
      dplyr::mutate(
        wds_ref     = purrr::map(
          stringr::str_to_upper(ref_name),
          ~ stringr::str_split(.x, "\\s+")[[1]]),
        n_wds_match = purrr::map_int(wds_ref, function(wds) {
          sum(wds_csv %in% wds)
        })) |>
      dplyr::filter(n_wds_match > 1) |>
      dplyr::arrange(dplyr::desc(n_wds_match))

    if (nrow(best) > 0) {
      top <- best[1, ]
      tibble::tibble(
        ship_code         = row$ship_code,
        ship_name         = row$ship_name,
        match_type        = "fuzzy",
        matched_ship_key  = top$ref_key,
        matched_ship_nodc = top$ref_nodc,
        matched_ship_name = top$ref_name,
        confidence        = min(1, top$n_wds_match / length(wds_csv)))
    } else {
      tibble::tibble(
        ship_code         = row$ship_code,
        ship_name         = row$ship_name,
        match_type        = "unmatched",
        matched_ship_key  = NA_character_,
        matched_ship_nodc = NA_character_,
        matched_ship_name = NA_character_,
        confidence        = 0)
    }
  })

  results <- c(results, list(fuzzy))

  result <- dplyr::bind_rows(results) |>
    dplyr::distinct(ship_code, ship_name, .keep_all = TRUE)

  # report summary
  n_matched   <- sum(result$match_type != "unmatched")
  n_unmatched <- sum(result$match_type == "unmatched")
  message(glue::glue(
    "Ship matching: {n_matched} matched, {n_unmatched} unmatched out of ",
    "{nrow(result)} ships"))

  result
}

#' Derive Cruise Key on Bottle Casts via Ship Matching
#'
#' Full pipeline to link bottle \code{casts} to the SWFSC \code{cruise} table
#' by: (1) finding unmatched ship codes, (2) running \code{match_ships()} for
#' fuzzy matching, (3) adding \code{ship_key} and \code{cruise_key} columns
#' to casts, (4) validating against the cruise table.
#'
#' The cruise_key format is YYYY-MM-NODC (4-digit year, 2-digit month,
#' NODC ship code), e.g. "1998-02-33JD".
#'
#' Requires that \code{ship} and \code{cruise} tables are already loaded in
#' the DuckDB connection (e.g., via \code{load_prior_tables()}).
#'
#' @param con DBI connection to DuckDB with casts, ship, and cruise tables
#' @param ship_renames_csv Optional path to manual ship overrides CSV
#' @param fetch_ices Logical; if TRUE, also query ICES ship API (default TRUE)
#'
#' @return List with components:
#'   \itemize{
#'     \item \code{ship_matches}: tibble of ship match results
#'     \item \code{cruise_stats}: tibble of cruise bridge match statistics
#'     \item \code{unmatched_report}: tibble of unmatched ship codes
#'   }
#' @export
#' @concept ship
#'
#' @examples
#' \dontrun{
#' # after loading casts, ship, cruise tables
#' result <- derive_cruise_key_on_casts(
#'   con              = con,
#'   ship_renames_csv = here("metadata/calcofi/bottle/ship_renames.csv"))
#' result$cruise_stats
#' }
#' @importFrom DBI dbExecute dbGetQuery dbReadTable dbListFields
#' @importFrom glue glue
#' @importFrom dplyr filter
derive_cruise_key_on_casts <- function(
    con,
    ship_renames_csv = NULL,
    fetch_ices       = TRUE) {

  # verify required tables exist
  tbls <- DBI::dbListTables(con)
  stopifnot(
    "casts table required"  = "casts"  %in% tbls,
    "ship table required"   = "ship"   %in% tbls,
    "cruise table required" = "cruise" %in% tbls)

  # step 1: find unmatched ships ----
  unmatched <- DBI::dbGetQuery(con, "
    SELECT DISTINCT c.ship_code, c.ship_name
    FROM casts c
    LEFT JOIN ship s ON c.ship_code = s.ship_nodc
    WHERE s.ship_key IS NULL")

  message(glue::glue("{nrow(unmatched)} unmatched ship codes in casts"))

  # step 2: run fuzzy matching ----
  ship_matches <- match_ships(
    unmatched_ships  = unmatched,
    reference_ships  = DBI::dbReadTable(con, "ship"),
    ship_renames_csv = ship_renames_csv,
    fetch_ices       = fetch_ices)

  # step 3: add ship_key to casts ----
  DBI::dbExecute(con, "ALTER TABLE casts ADD COLUMN IF NOT EXISTS ship_key TEXT")

  # exact match: casts.ship_code = ship.ship_nodc
  DBI::dbExecute(con, "
    UPDATE casts SET ship_key = (
      SELECT s.ship_key FROM ship s
      WHERE s.ship_nodc = casts.ship_code
      LIMIT 1)")

  n_exact <- DBI::dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM casts WHERE ship_key IS NOT NULL")$n
  message(glue::glue("Exact ship_nodc match: {n_exact} casts"))

  # apply fuzzy match results
  matched_ships <- ship_matches |>
    dplyr::filter(match_type != "unmatched", !is.na(matched_ship_key))

  if (nrow(matched_ships) > 0) {
    for (i in seq_len(nrow(matched_ships))) {
      m <- matched_ships[i, ]
      DBI::dbExecute(con, glue::glue("
        UPDATE casts SET ship_key = '{m$matched_ship_key}'
        WHERE ship_code = '{m$ship_code}'
          AND ship_key IS NULL"))
    }
    n_fuzzy <- DBI::dbGetQuery(con,
      "SELECT COUNT(*) AS n FROM casts WHERE ship_key IS NOT NULL")$n - n_exact
    message(glue::glue("Fuzzy/manual match: {n_fuzzy} additional casts"))
  }

  # step 4: derive cruise_key as YYYY-MM-NODC ----
  DBI::dbExecute(con, "ALTER TABLE casts ADD COLUMN IF NOT EXISTS cruise_key TEXT")

  DBI::dbExecute(con, "
    UPDATE casts SET cruise_key = CONCAT(
      CAST(EXTRACT(YEAR FROM datetime_utc) AS VARCHAR),
      '-',
      LPAD(CAST(EXTRACT(MONTH FROM datetime_utc) AS VARCHAR), 2, '0'),
      '-',
      (SELECT s.ship_nodc FROM ship s
       WHERE s.ship_key = casts.ship_key LIMIT 1))
    WHERE ship_key IS NOT NULL")

  n_cruise <- DBI::dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM casts WHERE cruise_key IS NOT NULL")$n
  message(glue::glue("Derived cruise_key for {n_cruise} casts"))

  # step 5: validate against cruise table ----
  cruise_stats <- DBI::dbGetQuery(con, "
    SELECT
      CASE
        WHEN c.ship_key IS NULL THEN 'no_ship_match'
        WHEN c.cruise_key IS NULL THEN 'no_cruise_key'
        WHEN cr.cruise_key IS NULL THEN 'no_cruise_match'
        ELSE 'matched'
      END AS status,
      COUNT(*) AS n_casts
    FROM casts c
    LEFT JOIN cruise cr ON c.cruise_key = cr.cruise_key
    GROUP BY status
    ORDER BY status")

  # step 6: report unmatched ships ----
  unmatched_report <- DBI::dbGetQuery(con, "
    SELECT DISTINCT
      c.ship_code, c.ship_name,
      COUNT(*) AS n_casts,
      MIN(c.datetime_utc) AS first_cast,
      MAX(c.datetime_utc) AS last_cast
    FROM casts c
    WHERE c.ship_key IS NULL
    GROUP BY c.ship_code, c.ship_name
    ORDER BY n_casts DESC")

  if (nrow(unmatched_report) > 0) {
    message(glue::glue("{nrow(unmatched_report)} ship codes still unmatched"))
  } else {
    message("All ship codes matched!")
  }

  list(
    ship_matches     = ship_matches,
    cruise_stats     = cruise_stats,
    unmatched_report = unmatched_report)
}

#' Report Ship Matching Status for a Dataset
#'
#' Summarizes ship matching results for a given dataset table: how many rows
#' have matched ship_key, how many are unmatched, and lists any new ships
#' not yet in the reference table.
#'
#' @param con DBI connection to DuckDB
#' @param dataset_label Character label for the dataset (e.g., "ichthyo", "bottle")
#' @param table Name of table to check (default: auto-detect from dataset_label)
#' @param ship_tbl Name of ship reference table (default: "ship")
#'
#' @return Tibble with columns: ship_key, ship_nodc, ship_name, n_rows, status
#' @export
#' @concept ship
#'
#' @examples
#' \dontrun{
#' report_ship_matches(con, "bottle")
#' report_ship_matches(con, "ichthyo", table = "cruise")
#' }
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue
report_ship_matches <- function(
    con,
    dataset_label,
    table    = NULL,
    ship_tbl = "ship") {

  # auto-detect table from dataset label
  if (is.null(table)) {
    table <- switch(dataset_label,
      "ichthyo"  = "cruise",
      "bottle"   = "casts",
      "ctd-cast" = "ctd_cast",
      "dic"      = "dic_sample",
      stop(glue::glue("Unknown dataset_label: {dataset_label}")))
  }

  # check which column links to ship
  cols <- DBI::dbGetQuery(con, glue::glue(
    "SELECT column_name FROM information_schema.columns
     WHERE table_name = '{table}'"))$column_name

  if ("ship_key" %in% cols) {
    report <- DBI::dbGetQuery(con, glue::glue("
      SELECT
        t.ship_key,
        s.ship_nodc,
        s.ship_name,
        COUNT(*) AS n_rows,
        CASE
          WHEN t.ship_key IS NULL THEN 'unmatched'
          WHEN s.ship_key IS NULL THEN 'new_ship'
          ELSE 'matched'
        END AS status
      FROM {table} t
      LEFT JOIN {ship_tbl} s ON t.ship_key = s.ship_key
      GROUP BY t.ship_key, s.ship_nodc, s.ship_name, status
      ORDER BY status, n_rows DESC"))
  } else {
    report <- DBI::dbGetQuery(con, glue::glue("
      SELECT
        'no ship_key column' AS ship_key,
        NULL AS ship_nodc,
        NULL AS ship_name,
        COUNT(*) AS n_rows,
        'no_ship_col' AS status
      FROM {table}"))
  }

  n_matched   <- sum(report$n_rows[report$status == "matched"])
  n_unmatched <- sum(report$n_rows[report$status == "unmatched"])
  n_new       <- sum(report$n_rows[report$status == "new_ship"])

  message(glue::glue(
    "Ship report for {dataset_label} ({table}): ",
    "{n_matched} matched, {n_unmatched} unmatched, {n_new} new ships"))

  report
}

#' Ensure Interim Ship Entries for Unmatched Ships
#'
#' For any ship codes that remain unmatched after [match_ships()], inserts
#' interim placeholder rows into the `ship` table with `ship_nodc = "?SK?"`
#' (where `SK` is the 2-letter ship_key). This allows downstream operations
#' (cruise_key derivation, FK joins) to proceed without errors. Placeholder
#' ships are flagged via the `"?"` markers for later resolution via
#' `metadata/ship_renames.csv`.
#'
#' @param con DBI connection to DuckDB with a `ship` table
#' @param match_result Tibble from [match_ships()] with `match_type` and
#'   `ship_code` columns. Rows with `match_type == "unmatched"` get interim
#'   entries.
#' @param ship_tbl Name of ship table (default: `"ship"`)
#'
#' @return Integer count of interim ships inserted
#' @export
#' @concept ship
#'
#' @examples
#' \dontrun{
#' result <- match_ships(unmatched, reference)
#' n_interim <- ensure_interim_ships(con, result)
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
#' @importFrom dplyr filter distinct pull
ensure_interim_ships <- function(
    con,
    match_result,
    ship_tbl = "ship") {

  unmatched_codes <- match_result |>
    dplyr::filter(match_type == "unmatched") |>
    dplyr::distinct(ship_code) |>
    dplyr::pull(ship_code)

  if (length(unmatched_codes) == 0) {
    message("No unmatched ships — no interim entries needed")
    return(0L)
  }

  # check which codes already have entries (avoid duplicates)
  existing <- DBI::dbGetQuery(con, glue::glue(
    "SELECT ship_key FROM {ship_tbl}
     WHERE ship_key IN ({paste(shQuote(unmatched_codes, type = 'sh'), collapse = ', ')})"))$ship_key

  new_codes <- setdiff(unmatched_codes, existing)

  if (length(new_codes) == 0) {
    message("All unmatched ship codes already have interim entries")
    return(0L)
  }

  for (code in new_codes) {
    placeholder_nodc <- paste0("?", code, "?")
    DBI::dbExecute(con, glue::glue(
      "INSERT INTO {ship_tbl} (ship_key, ship_nodc, ship_name)
       VALUES ('{code}', '{placeholder_nodc}', NULL)"))
  }

  message(glue::glue(
    "Created {length(new_codes)} interim ship(s): ",
    "{paste(new_codes, collapse = ', ')} (ship_nodc = ?XX?)"))

  length(new_codes)
}
