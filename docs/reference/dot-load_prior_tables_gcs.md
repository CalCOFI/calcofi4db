# Load prior tables from GCS (fallback when local parquet missing)

Load prior tables from GCS (fallback when local parquet missing)

## Usage

``` r
.load_prior_tables_gcs(
  con,
  parquet_dir,
  gcs_prefix,
  tables = NULL,
  geom_tables = c("grid", "site", "segment"),
  overwrite = TRUE
)
```

## Arguments

- con:

  DBI connection

- parquet_dir:

  Local parquet dir (used to find manifest.json)

- gcs_prefix:

  GCS prefix under calcofi-db bucket

- tables:

  Optional table filter

- geom_tables:

  Spatial table names

- overwrite:

  Replace existing tables

## Value

Tibble with table, rows, has_geom
