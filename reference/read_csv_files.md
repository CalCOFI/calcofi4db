# Read CSV Files and Their Metadata

Reads CSV files from a directory or GCS archive and prepares them for
ingestion into a database. This function is the primary entry point for
the CalCOFI data ingestion workflow. It performs the following steps:

## Usage

``` r
read_csv_files(
  provider,
  dataset,
  subdir = NULL,
  dir_data = NULL,
  metadata_dir = NULL,
  gcs_archive = NULL,
  gcs_bucket = "calcofi-files-public",
  archive_prefix = "archive",
  sync_archive = TRUE,
  verbose = FALSE,
  field_descriptions = NULL
)
```

## Arguments

- provider:

  Data provider (e.g., "swfsc")

- dataset:

  Dataset name (e.g., "ichthyo")

- subdir:

  Optional subdirectory (i.e., dir_data/provider/dataset/subdir) for CSV
  files. Use for datasets organized with `raw/` or `derived/`
  subdirectories.

- dir_data:

  Directory path of CalCOFI base data folder available locally, with
  CSVs under provider/dataset directory. If NULL and gcs_archive is also
  NULL, will error. Set to NULL to use gcs_archive instead.

- metadata_dir:

  Directory containing redefinition metadata files (tbls_redefine.csv,
  flds_redefine.csv). The directory should be structured as
  metadata_dir/provider/dataset/. If NULL, falls back to the legacy
  location in calcofi4db/inst/ingest/ (deprecated).

- gcs_archive:

  GCS archive path to read from (for reproducibility). Can be either a
  timestamp (e.g., "2026-02-02_121557") or full path (e.g.,
  "gs://calcofi-files-public/archive/2026-02-02_121557"). If provided,
  downloads from archive instead of using local files.

- gcs_bucket:

  GCS bucket for archives (default: "calcofi-files-public")

- archive_prefix:

  Prefix for archive folder (default: "archive")

- sync_archive:

  Whether to sync local files to GCS archive (default: TRUE). Only
  applies when using dir_data (local files).

- verbose:

  Print detailed messages. Default: FALSE

- field_descriptions:

  Named list of CSV file paths containing field metadata for
  auto-populating descriptions and units in the generated
  `flds_redefine.csv`. See
  [`create_redefinition_files()`](https://calcofi.io/calcofi4db/reference/create_redefinition_files.md)
  for details. Default: NULL.

## Value

A list containing:

- d_csv:

  List with CSV data including: - data: tibble with columns (tbl, csv,
  file_size, last_modified, data, nrow, ncol, flds, gcs_path) - tables:
  summary of tables (tbl, nrow, ncol) - fields: summary of fields (tbl,
  fld, type)

- source_files:

  Data frame for provenance tracking with columns: table, local_path,
  gcs_path, file_size, last_modified, nrow, ncol

- d_tbls_rd:

  Table redefinition data frame with columns: tbl_old, tbl_new,
  tbl_description

- d_flds_rd:

  Field redefinition data frame with columns: tbl_old, tbl_new, fld_old,
  fld_new, order_old, order_new, type_old, type_new, fld_description,
  notes, mutation

- paths:

  List of file paths used in the workflow

## Details

1.  Reads CSV files from local directory or downloads from GCS archive

2.  If using local files, syncs to GCS archive for immutable provenance

3.  Extracts metadata about tables and fields from the CSV files

4.  Creates or reads redefinition files for table and field
    transformations

The function returns a comprehensive data structure containing:

- Raw CSV data and metadata (d_csv)

- Source files with provenance tracking (source_files)

- Table redefinitions (d_tbls_rd) for renaming/describing tables

- Field redefinitions (d_flds_rd) for renaming/typing/transforming
  fields

- File paths used in the workflow

## Examples

``` r
if (FALSE) { # \dontrun{
# Read from local Google Drive mount (syncs to GCS archive)
d <- read_csv_files(
  provider     = "swfsc",
  dataset      = "ichthyo",
  dir_data     = "~/My Drive/projects/calcofi/data-public",
  metadata_dir = "metadata")

# Read from specific GCS archive (for reproducibility)
d <- read_csv_files(
  provider     = "swfsc",
  dataset      = "ichthyo",
  gcs_archive  = "2026-02-02_121557",
  metadata_dir = "metadata")

# Access the raw CSV data
d$d_csv$data

# Check source file provenance
d$source_files
} # }
```
