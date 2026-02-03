# parquet operations for calcofi data workflow

#' Convert CSV file to Parquet format
#'
#' Reads a CSV file and writes it as Parquet with optional schema enforcement.
#' Parquet provides efficient columnar storage with compression.
#'
#' @param csv_path Path to the CSV file (local or GCS)
#' @param output Path for the output Parquet file. If NULL, uses same name with .parquet extension
#' @param schema_def Optional schema definition (arrow::schema object or list)
#' @param compression Compression codec (default: "snappy")
#' @param col_types Column type specification for readr (optional)
#'
#' @return Path to the created Parquet file
#' @export
#' @concept parquet
#'
#' @examples
#' \dontrun{
#' # basic conversion
#' pqt_file <- csv_to_parquet("data/bottle.csv")
#'
#' # with custom output path
#' pqt_file <- csv_to_parquet(
#'   "data/bottle.csv",
#'   output = "parquet/bottle.parquet")
#'
#' # with explicit schema
#' schema <- arrow::schema(
#'   cruise_id = arrow::int32(),
#'   station   = arrow::string(),
#'   depth     = arrow::float64())
#' pqt_file <- csv_to_parquet("data/bottle.csv", schema_def = schema)
#' }
#' @importFrom glue glue
csv_to_parquet <- function(
    csv_path,
    output      = NULL,
    schema_def  = NULL,
    compression = "snappy",
    col_types   = NULL) {

  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required for Parquet operations. Install with: install.packages('arrow')")
  }

  # handle GCS paths
  if (grepl("^gs://", csv_path)) {
    local_csv <- get_gcs_file(csv_path)
    csv_path  <- local_csv
  }

  stopifnot(file.exists(csv_path))

  # set output path
  if (is.null(output)) {
    output <- sub("\\.csv$", ".parquet", csv_path, ignore.case = TRUE)
  }

  # ensure output directory exists
  output_dir <- dirname(output)
  if (!dir.exists(output_dir) && output_dir != ".") {
    dir.create(output_dir, recursive = TRUE)
  }

  # read CSV
  if (!is.null(col_types)) {
    data <- readr::read_csv(csv_path, col_types = col_types, show_col_types = FALSE)
  } else {
    data <- readr::read_csv(csv_path, show_col_types = FALSE)
  }

  # write to Parquet
  if (!is.null(schema_def)) {
    arrow::write_parquet(
      data,
      sink        = output,
      compression = compression,
      schema      = schema_def)
  } else {
    arrow::write_parquet(
      data,
      sink        = output,
      compression = compression)
  }

  message(glue::glue("Created Parquet file: {output} ({nrow(data)} rows)"))
  return(output)
}

#' Read a Parquet table
#'
#' Reads a Parquet file into a data frame or as a lazy Arrow Table.
#'
#' @param path Path to Parquet file (local or GCS)
#' @param lazy If TRUE, returns an Arrow Table for lazy evaluation (default: FALSE)
#' @param columns Optional vector of column names to read (default: all)
#'
#' @return Data frame or Arrow Table
#' @export
#' @concept parquet
#'
#' @examples
#' \dontrun{
#' # read as data frame
#' df <- read_parquet_table("parquet/bottle.parquet")
#'
#' # read as lazy Arrow table for large files
#' tbl <- read_parquet_table("parquet/bottle.parquet", lazy = TRUE)
#'
#' # read only specific columns
#' df <- read_parquet_table("parquet/bottle.parquet", columns = c("cruise_id", "depth"))
#' }
read_parquet_table <- function(path, lazy = FALSE, columns = NULL) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required for Parquet operations. Install with: install.packages('arrow')")
  }

  # handle GCS paths
  if (grepl("^gs://", path)) {
    local_path <- get_gcs_file(path)
    path       <- local_path
  }

  stopifnot(file.exists(path))

  if (lazy) {
    tbl <- arrow::read_parquet(path, as_data_frame = FALSE)
    if (!is.null(columns)) {
      tbl <- tbl$Select(columns)
    }
    return(tbl)
  }

  arrow::read_parquet(path, col_select = columns)
}

#' Write data to Parquet format
#'
#' Writes a data frame to Parquet, optionally with partitioning.
#'
#' @param data Data frame to write
#' @param path Output path for Parquet file or directory (for partitioned)
#' @param partitions Optional character vector of columns to partition by
#' @param compression Compression codec (default: "snappy")
#'
#' @return Path to the created Parquet file/directory
#' @export
#' @concept parquet
#'
#' @examples
#' \dontrun{
#' # simple write
#' write_parquet_table(df, "output/data.parquet")
#'
#' # partitioned by year and month
#' write_parquet_table(
#'   df,
#'   "output/data/",
#'   partitions = c("year", "month"))
#' }
#' @importFrom glue glue
write_parquet_table <- function(
    data,
    path,
    partitions  = NULL,
    compression = "snappy") {

  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required for Parquet operations. Install with: install.packages('arrow')")
  }

  # ensure output directory exists
  output_dir <- if (is.null(partitions)) dirname(path) else path
  if (!dir.exists(output_dir) && output_dir != ".") {
    dir.create(output_dir, recursive = TRUE)
  }

  if (!is.null(partitions)) {
    # write partitioned dataset
    arrow::write_dataset(
      data,
      path        = path,
      format      = "parquet",
      partitioning = partitions,
      compression = compression)
    message(glue::glue("Created partitioned Parquet dataset: {path}"))
  } else {
    # write single file
    arrow::write_parquet(
      data,
      sink        = path,
      compression = compression)
    message(glue::glue("Created Parquet file: {path}"))
  }

  return(path)
}

#' Get Parquet file metadata
#'
#' Retrieves schema and metadata from a Parquet file.
#'
#' @param path Path to Parquet file
#'
#' @return List with schema, num_rows, num_columns, and file_size
#' @export
#' @concept parquet
#'
#' @examples
#' \dontrun{
#' meta <- get_parquet_metadata("parquet/bottle.parquet")
#' meta$schema
#' meta$num_rows
#' }
get_parquet_metadata <- function(path) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required for Parquet operations. Install with: install.packages('arrow')")
  }

  # handle GCS paths
  if (grepl("^gs://", path)) {
    local_path <- get_gcs_file(path)
    path       <- local_path
  }

  stopifnot(file.exists(path))

  pf <- arrow::read_parquet(path, as_data_frame = FALSE)

  list(
    schema      = pf$schema,
    num_rows    = pf$num_rows,
    num_columns = length(pf$schema),
    file_size   = file.info(path)$size,
    column_names = names(pf$schema))
}

#' Add metadata to Parquet file
#'
#' Reads a Parquet file, adds custom metadata, and rewrites it.
#'
#' @param path Path to Parquet file
#' @param metadata Named list of metadata key-value pairs
#' @param output Output path (default: overwrites input file)
#'
#' @return Path to the modified Parquet file
#' @export
#' @concept parquet
#'
#' @examples
#' \dontrun{
#' add_parquet_metadata(
#'   "parquet/bottle.parquet",
#'   metadata = list(
#'     source     = "calcofi.org",
#'     version    = "2026.01.31",
#'     created_by = "calcofi4db"))
#' }
add_parquet_metadata <- function(path, metadata, output = path) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required for Parquet operations. Install with: install.packages('arrow')")
  }

  stopifnot(file.exists(path))

  # read table
  tbl <- arrow::read_parquet(path, as_data_frame = FALSE)

  # add metadata to schema
  new_metadata <- c(tbl$schema$metadata, metadata)
  new_schema   <- tbl$schema$WithMetadata(new_metadata)

  # rewrite with new metadata
  arrow::write_parquet(
    tbl$cast(new_schema),
    sink = output)

  message(glue::glue("Added metadata to: {output}"))
  return(output)
}

#' Upload Parquet file to GCS
#'
#' Convenience function to upload a Parquet file to the calcofi-db bucket.
#'
#' @param local_path Path to local Parquet file
#' @param gcs_path Relative path in calcofi-db bucket (e.g., "parquet/bottle.parquet")
#' @param bucket GCS bucket name (default: "calcofi-db")
#'
#' @return GCS URI of uploaded file
#' @export
#' @concept parquet
#'
#' @examples
#' \dontrun{
#' upload_parquet("local/bottle.parquet", "parquet/bottle.parquet")
#' }
upload_parquet <- function(
    local_path,
    gcs_path,
    bucket = "calcofi-db") {

  stopifnot(file.exists(local_path))

  put_gcs_file(local_path, gcs_path, bucket = bucket)
}
