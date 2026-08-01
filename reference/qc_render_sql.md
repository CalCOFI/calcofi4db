# Substitute `{{param}}` placeholders into a rule's SQL

Errors on a placeholder with no matching param. Silently leaving
`{{threshold}}` in the query would produce a DuckDB parse error far from
its cause, and worse, a *missing* threshold could otherwise render as an
empty string and quietly change the rule's meaning rather than failing.

## Usage

``` r
qc_render_sql(sql, params)
```

## Arguments

- sql:

  rule SQL text

- params:

  named list, e.g. from
  [`qc_parse_params()`](https://calcofi.io/calcofi4db/reference/qc_parse_params.md)

## Value

`sql` with every placeholder substituted

## Examples

``` r
qc_render_sql("SELECT * FROM obs WHERE v > {{threshold}}", list(threshold = "3"))
#> [1] "SELECT * FROM obs WHERE v > 3"
```
