# Get Working DuckLake connection

Connects to the internal Working DuckLake database used by ingestion
workflows. This database includes full provenance tracking columns
(`_source_file`, `_source_row`, `_source_uuid`, `_ingested_at`).

## Usage

``` r
get_working_ducklake(
  local_path = NULL,
  read_only = FALSE,
  refresh = FALSE,
  gcs_path = "gs://calcofi-db/ducklake/working/calcofi.duckdb"
)
```

## Arguments

- local_path:

  Path to local DuckDB file for caching. If NULL, uses a temp directory
  path derived from the GCS location.

- read_only:

  Open in read-only mode (default: FALSE)

- refresh:

  Force re-download from GCS even if local cache exists (default: FALSE)

- gcs_path:

  GCS path to Working DuckLake (default:
  "gs://calcofi-db/ducklake/working/calcofi.duckdb")

## Value

DuckDB connection object

## Details

The Working DuckLake is stored at `gs://calcofi-db/ducklake/working/`.
It includes provenance columns and supports time travel queries via
DuckLake.

For read-only access to stable data, use `cc_get_db()` from the
`calcofi4r` package to access frozen releases instead.

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_working_ducklake()
DBI::dbListTables(con)

# read-only access
con <- get_working_ducklake(read_only = TRUE)
} # }
```
