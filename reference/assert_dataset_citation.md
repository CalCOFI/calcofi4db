# Stop on any non-exempt error finding from [`check_dataset_citation()`](https://calcofi.io/calcofi4db/reference/check_dataset_citation.md)

The one place the failure is formatted, shared by
`build_workflows_index.R` and `release_database.qmd`. Warn-level
findings (drift, an unreachable authority) are reported as messages and
never stop.

## Usage

``` r
assert_dataset_citation(d, quiet = FALSE)
```

## Arguments

- d:

  the table from
  [`check_dataset_citation()`](https://calcofi.io/calcofi4db/reference/check_dataset_citation.md)

- quiet:

  suppress the messages for warn-level and exempt rows

## Value

`d`, invisibly, when nothing blocks.
