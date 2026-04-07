# Write Tables to Parquet Files

Exports all tables from a DuckDB connection to parquet files. Optionally
strips provenance columns for public releases.

## Usage

``` r
write_parquet_outputs(
  con,
  output_dir,
  tables = NULL,
  partition_by = NULL,
  sort_by = NULL,
  strip_provenance = TRUE,
  compression = "snappy",
  mismatches = NULL,
  supplemental = NULL
)
```

## Arguments

- con:

  DuckDB connection

- output_dir:

  Directory for parquet files

- tables:

  Optional vector of table names to export. If NULL, exports all tables.

- partition_by:

  Named list mapping table names to partition column(s), e.g.
  `list(ctd_measurement = "cruise_key")`. Creates hive-partitioned
  subdirectories.

- sort_by:

  Named list mapping table names to sort column(s) for optimized row
  group statistics. For Hilbert-curve spatial sorting, use
  `"hilbert:lon_col,lat_col"`. Example:
  `list(ctd_measurement = c("measurement_type", "depth_m"), site = "hilbert:longitude,latitude")`.

- strip_provenance:

  Remove provenance columns (*source*\*, \_ingested_at) (default: TRUE)

- compression:

  Parquet compression codec (default: "snappy")

- mismatches:

  Optional named list of mismatch tibbles to include in manifest.json.
  Expected names: `ships`, `measurement_types`, `cruise_keys`. Each
  element should be a tibble (or data frame) describing unresolved
  entities. Zero-row tibbles are recorded as empty arrays.

- supplemental:

  Character vector of table names that are supplemental outputs (e.g.
  wide-format tables for ERDDAP). These are written to parquet and
  listed in manifest.json under `"supplemental"`, but are excluded by
  default from downstream database loading via
  [`load_prior_tables()`](https://calcofi.io/calcofi4db/reference/load_prior_tables.md).

## Value

Tibble with export statistics (table, rows, file_size, path)

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_duckdb_con("calcofi.duckdb")
stats <- write_parquet_outputs(con, "output/parquet")
} # }
```
