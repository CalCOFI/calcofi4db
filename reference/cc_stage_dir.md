# Local Staging Root for Bulk Data

Directory where bulk parquet is written on its way to GCS. Read from the
`CALCOFI_STAGE_DIR` environment variable, falling back to
`~/_big/calcofi`.

## Usage

``` r
cc_stage_dir()
```

## Value

Absolute path, with `~` expanded. Not created.

## Details

Set this in `~/.Renviron` to a path that is **neither a git working tree
nor a synced cloud folder** — a 24 GB tree inside either one is a
problem for that tool, not a feature:

    CALCOFI_STAGE_DIR=/Users/bbest/_big/calcofi

The fallback keeps a fresh clone working without setup. It is
deliberately outside the repo, so the failure mode of forgetting to set
the variable is "writes to an unexpected but harmless place", never
"writes 24 GB back into git".

## Examples

``` r
cc_stage_dir()
#> [1] "/home/runner/_big/calcofi"
```
