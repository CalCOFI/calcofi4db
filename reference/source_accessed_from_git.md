# When was a dataset's source last read? Measured from git

The last commit date of
`data/parquet/{provider}_{dataset}/manifest.json` — the sidecar every
ingest rewrites when it runs — is the best available record of when the
source was read, and it costs no ingest re-run. Method `sidecar_commit`.
An untracked sidecar, or a directory outside a repository, yields `NA`.

## Usage

``` r
source_accessed_from_git(dir_parquet, file = "manifest.json")
```

## Arguments

- dir_parquet:

  one or more sidecar directories (`data/parquet/{provider}_{dataset}`)

- file:

  the sidecar whose history is read (default `manifest.json`)

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html):
`dataset_key` (the directory name), `source_accessed` (Date),
`source_accessed_method`, `source_accessed_ref` (the commit).

## Details

Prefer
[`resolve_source_accessed()`](https://calcofi.io/calcofi4db/reference/resolve_source_accessed.md),
which takes an ingest's own
[`stamp_source_access()`](https://calcofi.io/calcofi4db/reference/stamp_source_access.md)
record from `metadata.json` when there is one.
