#' Ingest CSV data to database
#'
#' Loads transformed data into a PostgreSQL database with proper metadata.
#'
#' @param con Database connection
#' @param schema Database schema
#' @param transformed_data Transformed data from transform_data()
#' @param d_flds_rd Field redefinition data frame
#' @param d_gdir_data Google Drive metadata (optional)
#' @param workflow_info Workflow information (name, URL, etc.)
#' @param overwrite Whether to overwrite existing tables
#'
#' @return Data frame with ingestion statistics
#' @export
#' @concept ingest
#'
#' @examples
#' \dontrun{
#' stats <- ingest_csv_to_db(
#'   con = db_connection,
#'   schema = "public",
#'   transformed_data = transformed_data,
#'   d_flds_rd = data_info$d_flds_rd,
#'   d_gdir_data = data_info$d_gdir_data,
#'   workflow_info = data_info$workflow_info
#' )
#' }
ingest_csv_to_db <- function(con, schema, transformed_data, d_flds_rd,
                            d_gdir_data = NULL, workflow_info,
                            overwrite = FALSE) {

  # Function to load a table to the database
  tbl_to_db <- function(tbl) {
    message(glue::glue("{schema}.{tbl}  ~ {Sys.time()}"))

    # Check if table exists
    tbl_exists <- DBI::dbExistsTable(
      con, DBI::Id(schema = schema, table = tbl))

    if (!tbl_exists || overwrite) {
      message("  loading table into database")

      # Get transformed data
      d_tbl <- transformed_data |>
        dplyr::filter(tbl_new == tbl) |>
        dplyr::pull(data_new) |>
        purrr::pluck(1)

      # Get field types
      v_fld_types <- d_flds_rd |>
        dplyr::filter(tbl_new == tbl) |>
        dplyr::arrange(order_new) |>
        dplyr::select(fld_new, type_new) |>
        tibble::deframe()

      # Write table to database
      DBI::dbWriteTable(
        con,
        DBI::Id(schema = schema, table = tbl),
        d_tbl,
        field.types = v_fld_types,
        append = FALSE,
        overwrite = TRUE)

      # Add table comment with JSON metadata
      tbl_description <- transformed_data |>
        dplyr::filter(tbl_new == tbl) |>
        dplyr::pull(tbl_description)

      # Get CSV file info
      csv_info <- transformed_data |>
        dplyr::filter(tbl_new == tbl)

      csv <- basename(csv_info$csv[1])

      # Get CSV URL and creation time if available
      csv_url <- NULL
      csv_created <- NULL

      if (!is.null(d_gdir_data)) {
        csv_match <- d_gdir_data |>
          dplyr::filter(name == csv)

        if (nrow(csv_match) > 0) {
          csv_url <- csv_match$web_view_link[1]
          csv_created <- csv_match$created_time[1]
        }
      }

      # Build comment JSON
      comment_data <- list(
        description = tbl_description,
        source = if (!is.null(csv_url)) glue::glue("[{csv}]({csv_url})") else csv,
        source_created = as.character(csv_created),
        workflow = glue::glue("[{workflow_info$workflow}]({workflow_info$workflow_url})"),
        workflow_ingested = as.character(Sys.time())
      )

      comment_json <- jsonlite::toJSON(comment_data, auto_unbox = TRUE)

      # Add comment to table
      DBI::dbExecute(
        con,
        glue::glue("COMMENT ON TABLE {schema}.{tbl} IS '{comment_json}'"))

      # Add comments to fields
      field_descriptions <- d_flds_rd |>
        dplyr::filter(tbl_new == tbl) |>
        dplyr::select(fld_new, fld_description)

      for (i in 1:nrow(field_descriptions)) {
        field <- field_descriptions$fld_new[i]
        description <- field_descriptions$fld_description[i]

        if (!is.na(description) && description != "") {
          DBI::dbExecute(
            con,
            glue::glue("COMMENT ON COLUMN {schema}.{tbl}.{field} IS '{description}'"))
        }
      }
    } else {
      message("  exists, skipping")

      # TODO: Implement table update logic for existing tables
      # This would compare schemas and update data as needed
    }

    # Return summary stats
    d_tbl <- transformed_data |>
      dplyr::filter(tbl_new == tbl) |>
      dplyr::pull(data_new) |>
      purrr::pluck(1)

    return(tibble::tibble(
      tbl = tbl,
      nrow = nrow(d_tbl),
      ncol = ncol(d_tbl),
      dtime_ingested = Sys.time()))
  }

  # Process each table
  tbl_stats <- purrr::map(
    transformed_data$tbl_new |> unique(),
    tbl_to_db) |>
    dplyr::bind_rows()

  return(tbl_stats)
}

#' Ingest a Dataset
#'
#' High-level function to ingest a dataset into the database.
#'
#' @param con Database connection
#' @param provider Data provider name
#' @param dataset Dataset name
#' @param dir_data Base directory for data
#' @param schema Database schema
#' @param dir_googledata Google Drive folder URL (optional)
#' @param email Google Drive authentication email (optional)
#' @param overwrite Whether to overwrite existing tables
#'
#' @return List with ingestion results and statistics
#' @export
#' @concept ingest
#'
#' @examples
#' \dontrun{
#' result <- ingest_dataset(
#'   con = db_connection,
#'   provider = "swfsc.noaa.gov",
#'   dataset = "calcofi-db",
#'   dir_data = "/path/to/data",
#'   schema = "public"
#' )
#' }
ingest_dataset <- function(con, provider, dataset, dir_data,
                          schema = "public", dir_googledata = NULL,
                          email = NULL, overwrite = FALSE) {
  # Load CSV files and metadata
  data_info <- read_csv_files(
    provider = provider,
    dataset = dataset,
    dir_data = dir_data,
    dir_googledata = dir_googledata,
    use_gdrive = !is.null(dir_googledata) && !is.null(email),
    email = email
  )

  # Transform data
  transformed_data <- transform_data(data_info)

  # Detect changes
  changes <- detect_csv_changes(
    con = con,
    schema = schema,
    transformed_data = transformed_data,
    d_flds_rd = data_info$d_flds_rd
  )

  # Ingest data to database
  stats <- ingest_csv_to_db(
    con = con,
    schema = schema,
    transformed_data = transformed_data,
    d_flds_rd = data_info$d_flds_rd,
    d_gdir_data = data_info$d_gdir_data,
    workflow_info = data_info$workflow_info,
    overwrite = overwrite
  )

  # Return results
  list(
    data_info = data_info,
    transformed_data = transformed_data,
    changes = changes,
    stats = stats
  )
}
