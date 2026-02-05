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
#' `workflows/ingest_calcofi.org_bottle-database_0.qmd` (lines 117-260).
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
