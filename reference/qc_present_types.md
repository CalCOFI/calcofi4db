# Which measurement types actually exist for a dataset

Computed once and passed to every rule: one `DISTINCT` scan instead of
one presence query per rule.

## Usage

``` r
qc_present_types(con, dataset_key = "calcofi_ctd-cast")
```

## Arguments

- con:

  a DBI connection carrying an `obs` table or view

- dataset_key:

  dataset to restrict to

## Value

character vector of `measurement_type` values present
