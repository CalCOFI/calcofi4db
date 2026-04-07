# Strip provenance columns from data

Removes the provenance tracking columns (`_source_file`, `_source_row`,
`_source_uuid`, `_ingested_at`) from a data frame. Used when preparing
data for frozen releases.

## Usage

``` r
strip_provenance_columns(data)
```

## Arguments

- data:

  Data frame with provenance columns

## Value

Data frame without provenance columns

## Examples

``` r
if (FALSE) { # \dontrun{
# strip provenance for public release
clean_data <- strip_provenance_columns(data_with_prov)
} # }
```
