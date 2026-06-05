# Build Metadata JSON for Parquet Outputs

Creates a sidecar `metadata.json` file alongside parquet outputs that
documents every table and column. DuckDB `COMMENT ON` does not propagate
to parquet via `COPY TO`, so this provides the metadata externally.

## Usage

``` r
build_metadata_json(
  con,
  d_tbls_rd,
  d_flds_rd,
  metadata_derived_csv = NULL,
  output_dir,
  tables = NULL,
  set_comments = TRUE,
  provider = NULL,
  dataset = NULL,
  workflow_url = NULL,
  tables_owned = NULL
)
```

## Arguments

- con:

  DuckDB connection

- d_tbls_rd:

  Table redefinition data frame (with `tbl_new`, `tbl_description`)

- d_flds_rd:

  Field redefinition data frame (with `tbl_new`, `fld_new`,
  `fld_description`, `units`)

- metadata_derived_csv:

  Path to CSV with derived table/column metadata (columns: table,
  column, name_long, units, description_md)

- output_dir:

  Directory to write `metadata.json`

- tables:

  Character vector of table names to include. If NULL, uses all tables
  from DuckDB.

- set_comments:

  If TRUE, also sets DuckDB `COMMENT ON` for tables/columns

- provider:

  Data provider identifier (e.g. "swfsc")

- dataset:

  Dataset identifier (e.g. "ichthyo")

- workflow_url:

  URL to the rendered workflow page

- tables_owned:

  Optional list describing the tables this ingest owns, as parsed from
  the `calcofi$tables_owned` YAML block: a list of entries each with
  `table` and optional `shared` (logical) / `note`. When supplied, a
  `contributions` block (per-table row counts) is emitted for these
  tables only, so reference tables loaded from prior ingests are not
  mis-attributed.

## Value

Path to the created `metadata.json` file

## Details

Metadata is assembled from three sources:

1.  Table/field redefinition files (`d_tbls_rd`, `d_flds_rd`)

2.  A derived metadata CSV for workflow-created tables/columns

3.  Auto-generated stubs for any remaining undocumented columns

Optionally sets DuckDB `COMMENT ON` for tables and columns.

## Examples

``` r
if (FALSE) { # \dontrun{
build_metadata_json(
  con                  = con,
  d_tbls_rd            = d$d_tbls_rd,
  d_flds_rd            = d$d_flds_rd,
  metadata_derived_csv = "metadata/swfsc/ichthyo/metadata_derived.csv",
  output_dir           = "data/parquet/swfsc_ichthyo",
  tables               = DBI::dbListTables(con),
  provider             = "swfsc",
  dataset              = "ichthyo",
  workflow_url         = "https://calcofi.io/workflows/ingest_swfsc_ichthyo.html")
} # }
```
