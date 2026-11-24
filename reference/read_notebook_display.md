# Read the display keys each ingest notebook declares

The three structural display keys of schema 1.2 (`dataset_name_plain`,
`collection_method`, `browse_group`) are authored in an ingest
notebook's `calcofi.dataset_meta`. They never enter the release
`dataset` table, so the record builder reads them from the notebooks'
YAML directly.

## Usage

``` r
read_notebook_display(workflow_dir, pattern = "^ingest_.*\\.qmd$")
```

## Arguments

- workflow_dir:

  the directory holding `ingest_*.qmd`

- pattern:

  which notebooks

## Value

A named list keyed by `dataset_key`; each element a list of the three
keys (absent ones `NULL`). Empty when the directory holds no notebook.
