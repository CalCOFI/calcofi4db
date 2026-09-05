# Parse one `publish_*` cell of `dataset_status.csv`

`done` → `published`; `n/a` (or empty) → `n/a`; anything naming
`planned` → `planned`; the `#NN` tokens become workflows issue URLs.

## Usage

``` r
parse_registration(x)
```

## Arguments

- x:

  one cell

## Value

`list(status, issues)` — `issues` a character vector of URLs.
