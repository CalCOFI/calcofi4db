# Create Redefinition Files for Tables and Fields

Creates redefinition files for tables and fields if they don't exist.

## Usage

``` r
create_redefinition_files(
  d_tbls_in,
  d_flds_in,
  d,
  tbls_rd_csv,
  flds_rd_csv,
  field_descriptions = NULL
)
```

## Arguments

- d_tbls_in:

  Data frame with table metadata

- d_flds_in:

  Data frame with field metadata

- d:

  Data frame with raw data

- tbls_rd_csv:

  Path to table redefinition CSV file

- flds_rd_csv:

  Path to field redefinition CSV file

- field_descriptions:

  Named list of CSV file paths containing field metadata. Names should
  match table names in the source data (e.g.
  `list("194903-202105_Cast" = "path/to/Cast Field Descriptions.csv")`).
  Each CSV must have columns: `Field Name`, `Units`, `Description`. When
  provided, auto-populates `fld_description` and `units` in the
  generated redefinition file. Default: NULL (empty descriptions).

## Value

Invisible NULL (files are written to disk)

## Examples

``` r
if (FALSE) { # \dontrun{
create_redefinition_files(
  d_tbls_in = tables_metadata,
  d_flds_in = fields_metadata,
  d = raw_data,
  tbls_rd_csv = "path/to/tbls_redefine.csv",
  flds_rd_csv = "path/to/flds_redefine.csv"
)
} # }
```
