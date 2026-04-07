# Determine Field Types for Database

Determines appropriate PostgreSQL data types for fields based on data
values.

## Usage

``` r
determine_field_types(d, tbl, fld)
```

## Arguments

- d:

  Data frame with raw data

- tbl:

  Table name

- fld:

  Field name

## Value

Character vector of PostgreSQL data types

## Examples

``` r
if (FALSE) { # \dontrun{
determine_field_types(raw_data, "species", "species_name")
} # }
```
