# Read the calcofi YAML block from a single workflow file

Read the calcofi YAML block from a single workflow file

## Usage

``` r
read_calcofi_meta(qmd_path)
```

## Arguments

- qmd_path:

  Path to one `ingest_*.qmd` (or any .qmd with a `calcofi:` YAML block).

## Value

The parsed `calcofi` block as a list, augmented with `provider_dataset`
and `qmd`, or `NULL` if absent. Use this in an ingest's setup chunk to
read its own `provider`/`dataset`/ `tables_owned` from the authoritative
YAML rather than hard-coding.
