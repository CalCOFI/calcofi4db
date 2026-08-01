# Collapse rule results into one row per rule

`skip` is deliberately its own status rather than folded into `pass`:
they mean opposite things about how much you should trust the run.

## Usage

``` r
qc_summarize(results, rules)
```

## Arguments

- results:

  list from
  [`qc_run_all()`](https://calcofi.io/calcofi4db/reference/qc_run_all.md)

- rules:

  the registry those results came from

## Value

a tibble, one row per rule, with a `status` of `pass` / `flag` / `FAIL`
/ `ERROR` / `skip`
