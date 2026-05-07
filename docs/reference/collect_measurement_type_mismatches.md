# Collect Measurement Type Mismatches

Finds measurement types present in the DuckDB `measurement_type` table
but not registered in the central `measurement_type.csv` registry. Used
to populate the `mismatches$measurement_types` section of
`manifest.json`.

## Usage

``` r
collect_measurement_type_mismatches(con, measurement_type_csv)
```

## Arguments

- con:

  DBI connection to DuckDB with a `measurement_type` table

- measurement_type_csv:

  Path to `metadata/measurement_type.csv`

## Value

Tibble with columns: measurement_type, source (value: "db_only")

## Examples

``` r
if (FALSE) { # \dontrun{
collect_measurement_type_mismatches(con, "metadata/measurement_type.csv")
} # }
```
