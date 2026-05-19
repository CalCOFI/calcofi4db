# Merge Per-Ingest metadata.json into a Release-Level Sidecar

Combines per-ingest `metadata.json` files (produced by
[`build_metadata_json()`](https://calcofi.io/calcofi4db/reference/build_metadata_json.md))
into a single release-level `metadata.json`. Adds release-only tables
and columns from CSV registries plus optional `dataset.csv` and
`measurement_type.csv` blocks. Emits schema version `"1.1"` alongside
`catalog.json` and `relationships.json` in a frozen release directory.

## Usage

``` r
merge_metadata_json(
  paths,
  output_path,
  release_version = NULL,
  release_tables_csv = NULL,
  release_columns_csv = NULL,
  measurement_type_csv = NULL,
  dataset_csv = NULL
)
```

## Arguments

- paths:

  Character vector of paths to per-ingest `metadata.json` files.

- output_path:

  Path for the merged output file.

- release_version:

  Optional release version string (e.g. `"v2026.05.14"`) written to the
  top-level `release_version` field.

- release_tables_csv:

  Optional path to a CSV with columns
  `table, name_long, description_md, provider, dataset` describing
  tables built inside `release_database.qmd` that have no per-ingest
  metadata.json (e.g. `cruise_summary`, `_spatial`).

- release_columns_csv:

  Optional path to a CSV with columns
  `table, column, name_long, units, description_md` for release-only
  columns.

- measurement_type_csv:

  Optional path to `metadata/measurement_type.csv`. When supplied,
  populates the `measurement_types` block with one entry per canonical
  type.

- dataset_csv:

  Optional path to `metadata/dataset.csv`. When supplied, populates the
  `datasets` block keyed by `"\{provider\}_\{dataset\}"`.

## Value

Path to the merged `metadata.json` file (invisibly returns
`output_path`).

## Details

Conflict rule: when the same `table` or `table.column` key appears in
multiple per-ingest files, the last path wins, but a warning lists the
duplicates so genuine drift between ingests is surfaced.

## Examples

``` r
if (FALSE) { # \dontrun{
merge_metadata_json(
  paths = c(
    "data/parquet/swfsc_ichthyo/metadata.json",
    "data/parquet/calcofi_bottle/metadata.json",
    "data/parquet/calcofi_ctd-cast/metadata.json",
    "data/parquet/calcofi_dic/metadata.json"),
  output_path          = "data/releases/v2026.05.14/metadata.json",
  release_version      = "v2026.05.14",
  release_tables_csv   = "metadata/release_tables.csv",
  release_columns_csv  = "metadata/release_columns.csv",
  measurement_type_csv = "metadata/measurement_type.csv",
  dataset_csv          = "metadata/dataset.csv")
} # }
```
