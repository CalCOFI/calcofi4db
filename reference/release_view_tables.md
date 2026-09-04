# The tables a catalog view reads, and the SQL with them resolved

A view in `catalog.json` names its source tables as `{{table}}` tokens
so that the SQL is storage-agnostic. `release_view_tables()` lists them;
`substitute_view_tables()` replaces each with `rp(table)` — a quoted
identifier by default, or whatever the caller reads a table through.

## Usage

``` r
release_view_tables(sql)

substitute_view_tables(sql, rp = function(table) paste0("\"", table, "\""))
```

## Arguments

- sql:

  a view's SQL carrying `{{table}}` tokens.

- rp:

  `function(table) -> character(1)`; default quotes the name.

## Value

`release_view_tables()`: the distinct table names, in order of first
appearance; `substitute_view_tables()`: the SQL with every token
replaced.

## Examples

``` r
release_view_tables(obs_view_sql())
#> [1] "obs_bio" "obs_env"
```
