# Validate Lookup Values Exist

Checks that all values in a column exist in the lookup table for a given
lookup type. Returns a tibble of rows with invalid lookup values.

## Usage

``` r
validate_lookup_values(
  con,
  data_tbl,
  value_col,
  lookup_type,
  lookup_tbl = "lookup",
  lookup_type_col = "lookup_type",
  lookup_num_col = "lookup_num"
)
```

## Arguments

- con:

  DuckDB connection

- data_tbl:

  Table name containing values to validate

- value_col:

  Name of the column containing values to check

- lookup_type:

  Type of lookup to validate against (e.g., "egg_stage", "larva_stage")

- lookup_tbl:

  Name of the lookup table (default: "lookup")

- lookup_type_col:

  Column in lookup table containing the type (default: "lookup_type")

- lookup_num_col:

  Column in lookup table containing the numeric key values (default:
  "lookup_num")

## Value

Tibble with rows containing invalid lookup values

## Examples

``` r
if (FALSE) { # \dontrun{
invalid_stages <- validate_lookup_values(
  con         = con,
  data_tbl    = "ichthyo",
  value_col   = "measurement_value",
  lookup_type = "egg_stage")
} # }
```
