# Load Tables from a Prior Ingest's Parquet Directory

Reads parquet files from another ingest workflow's output directory into
the current wrangling DuckDB. Handles WKB-\>GEOMETRY conversion for
spatial tables and hive-partitioned subdirectories automatically.

## Usage

``` r
load_prior_tables(
  con,
  parquet_dir,
  tables = NULL,
  gcs_prefix = NULL,
  geom_tables = c("grid", "site", "segment"),
  overwrite = TRUE,
  include_supplemental = FALSE,
  as_view = FALSE
)
```

## Arguments

- con:

  DBI connection to DuckDB

- parquet_dir:

  Path to directory containing parquet files

- tables:

  Character vector of table names to load (without .parquet extension).
  If NULL (default), auto-discovers all parquet in the directory.

- gcs_prefix:

  Optional GCS prefix (e.g., "ingest/swfsc_ichthyo") to use as fallback
  when `parquet_dir` doesn't exist or is empty. Reads from
  `https://storage.googleapis.com/calcofi-db/{gcs_prefix}/`. Requires
  the httpfs DuckDB extension.

- geom_tables:

  Character vector of table names that contain WKB geometry columns
  needing conversion (default: c("grid", "site", "segment"))

- overwrite:

  If TRUE, replace existing tables (default: TRUE)

- include_supplemental:

  If FALSE (default), tables listed under `"supplemental"` in the
  directory's manifest.json are excluded from auto-discovery. Set to
  TRUE to load all tables including supplemental (e.g. wide-format
  ERDDAP outputs).

- as_view:

  If TRUE, create VIEWs instead of TABLEs (zero-copy, reads directly
  from parquet on disk). Use for dependency tables that don't need to be
  modified or re-exported. Default FALSE for backward compatibility.

## Value

Tibble with columns: table, rows, has_geom

## Details

When `tables = NULL` (default), auto-discovers all parquet sources in
the directory: single `.parquet` files and subdirectories containing
`.parquet` files (hive-partitioned tables).

## Examples

``` r
if (FALSE) { # \dontrun{
# load grid from ichthyo workflow
load_prior_tables(
  con         = con,
  tables      = "grid",
  parquet_dir = here("data/parquet/swfsc_ichthyo"))

# load all tables from a dataset (auto-discover)
load_prior_tables(
  con         = con,
  parquet_dir = here("data/parquet/swfsc_ichthyo"))

# fallback to GCS if local dir doesn't exist
load_prior_tables(
  con         = con,
  parquet_dir = here("data/parquet/swfsc_ichthyo"),
  gcs_prefix  = "ingest/swfsc_ichthyo")
} # }
```
