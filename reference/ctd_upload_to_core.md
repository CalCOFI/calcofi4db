# Project an uploaded file into the core `obs` / `sample` shape

Once this has run, every QC rule in `metadata/qc_rules/` applies
unchanged.

## Usage

``` r
ctd_upload_to_core(
  d,
  mapping,
  header = list(),
  dataset_key = "upload",
  cruise_key = NULL,
  site_key = NULL,
  cast_id = NULL
)
```

## Arguments

- d:

  the parsed upload

- mapping:

  output of
  [`ctd_map_columns()`](https://calcofi.io/calcofi4db/reference/ctd_map_columns.md)

- header:

  the Sea-Bird header list, if any

- dataset_key:

  stamped on every row

- cruise_key, site_key, cast_id:

  override the header-derived values

## Value

list with `sample`, `obs` (both data frames) and `n_sentinel`

## Details

The two source-specific repairs the pipeline already knows about are
applied here too, because a new file is exactly where they arrive: the
`-99` sentinel (and `-9.99e-29`) are deleted rather than carried as
readings, and quality codes stored as `"9.0"` by a double-to-string cast
are stripped textually — not via an integer cast, which would round an
unexpected `"9.5"`.
