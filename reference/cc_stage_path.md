# Path Within the Local Staging Root

Joins path components onto
[`cc_stage_dir()`](https://calcofi.io/calcofi4db/reference/cc_stage_dir.md).

## Usage

``` r
cc_stage_path(..., create = FALSE)
```

## Arguments

- ...:

  Path components, e.g. `"parquet", "calcofi_dic"`.

- create:

  If `TRUE`, create the directory (recursively) if absent. Use on a
  directory you are about to write into.

## Value

Absolute path.

## Examples

``` r
cc_stage_path("parquet", "calcofi_dic")
#> [1] "/home/runner/_big/calcofi/parquet/calcofi_dic"
```
