# Fail unless every non-NULL `site_key` is in its canonical spelling

The release gate for
[`site_key_sql()`](https://calcofi.io/calcofi4db/reference/site_key_sql.md):
a `site_key` that is not equal to its own normalisation would key a
section or a climatology cell onto a station that does not exist. NULL
is allowed (underway, transect and region-pooled samples have no
station). Reports per dataset so the offending ingest is named.

## Usage

``` r
check_site_key_format(con, sample_tbl = "sample")
```

## Arguments

- con:

  DuckDB connection holding the sample table.

- sample_tbl:

  the sample table (default `"sample"`).

## Value

invisibly, a data.frame `dataset_key`, `n_rows`, `n_site`, `n_bad`,
`example` (one offending value, or NA).
