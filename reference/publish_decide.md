# Reuse a dataset's previous build, or rebuild it?

Reuse a dataset's previous build, or rebuild it?

## Usage

``` r
publish_decide(fp, prior = NULL, outputs = character())
```

## Arguments

- fp:

  this run's fingerprint, from
  [`publish_fingerprint()`](https://calcofi.io/calcofi4db/reference/publish_fingerprint.md)

- prior:

  the fingerprint recorded with the previous build — a list with `hash`
  and `parts` (as stored in a manifest), or NULL when there is none

- outputs:

  paths the previous build must still have on disk to be reused

## Value

A list: `action` (`"reuse"` \| `"build"`), `changed` (the input names
that differ, via
[`changed_inputs()`](https://calcofi.io/calcofi4db/reference/changed_inputs.md);
every input when there is no prior), and `reason` (one line for the
notebook to print).
