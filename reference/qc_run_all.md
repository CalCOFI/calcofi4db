# Run every rule in a registry, one at a time

Sequential on purpose: these are multi-GB scans and running them
concurrently against one DuckDB just contends for the same buffer pool.

## Usage

``` r
qc_run_all(
  con,
  rules,
  limit = 500L,
  on_progress = NULL,
  present_types = qc_present_types(con),
  scope_values = list()
)
```

## Arguments

- con:

  a DBI connection carrying the tables the rule targets

- rules:

  a rule registry from
  [`qc_read_rules()`](https://calcofi.io/calcofi4db/reference/qc_read_rules.md)

- limit:

  cap on rows returned. The `COUNT` is always computed over the full
  result, so a truncated display never understates the problem — a rule
  that silently showed 500 of 40,000 hits would read as "minor".

- on_progress:

  optional `function(i, n, rule_key)` callback

- present_types:

  output of
  [`qc_present_types()`](https://calcofi.io/calcofi4db/reference/qc_present_types.md);
  `NULL` disables the check

- scope_values:

  named list supplying scope parameters, e.g.
  `list(cruise_key = "2023-11-33P4")`. A rule with `scope = "cruise"`
  reads the full-resolution `obs_ctd_full` and is meaningless unscoped,
  so it SKIPS rather than silently scanning everything.

## Value

list of
[`qc_run_rule()`](https://calcofi.io/calcofi4db/reference/qc_run_rule.md)
results, in registry order
