# Build Relationships JSON from dm Object

Extracts primary keys and foreign keys from a `dm` object and writes a
`relationships.json` sidecar file alongside parquet outputs. Since
parquet files cannot store table relationships natively, this provides a
machine-readable source of truth for PKs and FKs.

## Usage

``` r
build_relationships_json(
  dm = NULL,
  rels = NULL,
  output_dir,
  provider = NULL,
  dataset = NULL
)
```

## Arguments

- dm:

  A `dm` object with PKs and FKs defined. Deprecated in favour of
  `rels`; kept for backward compatibility.

- rels:

  A list with `primary_keys` (named list: table → column) and
  `foreign_keys` (list of lists with `table`, `column`, `ref_table`,
  `ref_column`). Takes precedence over `dm`.

- output_dir:

  Directory to write `relationships.json`

- provider:

  Data provider identifier (e.g. "swfsc")

- dataset:

  Dataset identifier (e.g. "ichthyo")

## Value

Path to the created `relationships.json` file

## Examples

``` r
if (FALSE) { # \dontrun{
# preferred: pass relationships as a list
rels <- list(
  primary_keys = list(cruise = "cruise_key", ship = "ship_key"),
  foreign_keys = list(
    list(table = "cruise", column = "ship_key",
         ref_table = "ship", ref_column = "ship_key")))
build_relationships_json(
  rels       = rels,
  output_dir = "data/parquet/swfsc_ichthyo",
  provider   = "swfsc",
  dataset    = "ichthyo")
} # }
```
