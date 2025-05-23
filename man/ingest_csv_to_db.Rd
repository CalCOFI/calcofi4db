% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ingest.R
\name{ingest_csv_to_db}
\alias{ingest_csv_to_db}
\title{Ingest CSV data to database}
\usage{
ingest_csv_to_db(
  con,
  schema,
  transformed_data,
  d_flds_rd,
  d_gdir_data = NULL,
  workflow_info,
  overwrite = FALSE
)
}
\arguments{
\item{con}{Database connection}

\item{schema}{Database schema}

\item{transformed_data}{Transformed data from transform_data()}

\item{d_flds_rd}{Field redefinition data frame}

\item{d_gdir_data}{Google Drive metadata (optional)}

\item{workflow_info}{Workflow information (name, URL, etc.)}

\item{overwrite}{Whether to overwrite existing tables}
}
\value{
Data frame with ingestion statistics
}
\description{
Loads transformed data into a PostgreSQL database with proper metadata.
}
\examples{
\dontrun{
stats <- ingest_csv_to_db(
  con = db_connection,
  schema = "public",
  transformed_data = transformed_data,
  d_flds_rd = data_info$d_flds_rd,
  d_gdir_data = data_info$d_gdir_data,
  workflow_info = data_info$workflow_info
)
}
}
\concept{ingest}
