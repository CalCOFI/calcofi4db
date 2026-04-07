# Finalize Ingest — Push Parquet Tables to Working DuckLake

High-level function each ingest notebook calls at the end to push all
parquet tables to the Working DuckLake. Downloads (or creates) the
Working DuckLake, ingests each parquet file with provenance, saves back
to GCS, and returns ingestion statistics.

## Usage

``` r
finalize_ingest(
  parquet_dir,
  source_label = basename(parquet_dir),
  tables = NULL,
  geom_tables = c("grid", "site", "segment"),
  include_supplemental = FALSE
)
```

## Arguments

- parquet_dir:

  Path to directory containing parquet files to ingest

- source_label:

  Label for provenance tracking (default: basename of parquet_dir)

- tables:

  Optional character vector of table names to ingest. If NULL, all
  .parquet files in the directory are ingested.

- geom_tables:

  Character vector of table names with WKB geometry columns (default:
  c("grid", "site", "segment")). These are skipped during
  ingest_to_working() since geometry columns don't survive
  dbWriteTable(). Instead they are loaded directly from parquet.

- include_supplemental:

  If FALSE (default), tables listed under `"supplemental"` in
  manifest.json are excluded. Set to TRUE to push all tables including
  supplemental outputs (e.g. wide-format for ERDDAP).

## Value

Tibble with ingestion statistics per table

## Examples

``` r
if (FALSE) { # \dontrun{
# at the end of ingest_swfsc_ichthyo.qmd
finalize_ingest(
  parquet_dir  = here("data/parquet/swfsc_ichthyo"),
  source_label = "swfsc_ichthyo")
} # }
```
