# Validate Egg Stage Values

Specific validation for egg stages - checks that values are in valid
range (1-11 per Moser & Ahlstrom 1985). Values 12-15 are invalid.

## Usage

``` r
validate_egg_stages(con, table_name, stage_col = "stage")
```

## Arguments

- con:

  DuckDB connection

- table_name:

  Table containing egg stage data

- stage_col:

  Column containing stage values (default: "stage")

## Value

Tibble of rows with invalid egg stage values (\>11)

## Examples

``` r
if (FALSE) { # \dontrun{
invalid_stages <- validate_egg_stages(con, "eggstage", "stage")
} # }
```
