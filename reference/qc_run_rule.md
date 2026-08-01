# Execute one rule, returning its findings

PRECONDITIONS ARE CHECKED FIRST, and this is not a nicety. A rule whose
input measurement type is absent returns zero rows, which is
indistinguishable from "the data is clean" — a false pass. The three
bottle-vs-sensor calibration rules did exactly that against release
`v2026.07.30`, which carries only `btl_ammonium` because it predates the
change making the other bottle-reference types canonical. A QA/QC tool
that reports green without having checked anything is worse than no
tool, so an unmet precondition is `skip`, never `pass`.

## Usage

``` r
qc_run_rule(
  con,
  rule,
  limit = 500L,
  present_types = NULL,
  scope_values = list()
)
```

## Arguments

- con:

  a DBI connection carrying the tables the rule targets

- rule:

  one row of
  [`qc_read_rules()`](https://calcofi.io/calcofi4db/reference/qc_read_rules.md)

- limit:

  cap on rows returned. The `COUNT` is always computed over the full
  result, so a truncated display never understates the problem — a rule
  that silently showed 500 of 40,000 hits would read as "minor".

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

list with `rule_key`, `n`, `findings`, `elapsed_s`, `error`, `skipped`,
`skip_reason`
