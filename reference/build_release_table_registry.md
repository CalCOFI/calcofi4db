# Build Release Table Registry from Ingest Manifests

Reads all ingest `manifest.json` files to build a registry of which
tables come from which ingests. For tables appearing in multiple
ingests, marks the first occurrence as canonical. Excludes supplemental
tables. Used by `release_database.qmd` to auto-discover table sources
for GCS server-side copy.

## Usage

``` r
build_release_table_registry(workflows_dir = here::here())
```

## Arguments

- workflows_dir:

  Path to the workflows directory (default:
  [`here::here()`](https://here.r-lib.org/reference/here.html))

## Value

Tibble with columns: table, ingest, parquet_dir, gcs_prefix,
partitioned, supplemental, canonical

## Examples

``` r
if (FALSE) { # \dontrun{
registry <- build_release_table_registry()
# canonical single-source tables for GCS copy
registry |> filter(canonical, !supplemental)
} # }
```
