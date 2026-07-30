# Reject sentinel strings that should have been empty cells

Guards a metadata registry against the `write_csv(na = "NA")` round-trip
described in `R/registry.R`. Errors listing the offending columns and
rows rather than returning quietly, because the whole failure mode is
silence.

## Usage

``` r
check_registry_na_strings(
  df,
  path = NULL,
  sentinels = c("NA", "NaN", "NULL", "N/A", "na"),
  cols = NULL
)
```

## Arguments

- df:

  a data.frame read from a registry CSV

- path:

  optional source path, used in the error message

- sentinels:

  character strings that must never appear as literal values (default
  `"NA"`, `"NaN"`, `"NULL"`, `"N/A"`, `"na"`)

- cols:

  character columns to check (default: all character columns)

## Value

`df`, invisibly and unchanged, when clean

## Examples

``` r
if (FALSE) { # \dontrun{
check_registry_na_strings(readr::read_csv("metadata/measurement_type.csv"))
} # }
```
