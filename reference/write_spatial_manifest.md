# Write Spatial Manifest

Generates a `manifest.json` for spatial parquet outputs that do not use
[`write_parquet_outputs()`](https://calcofi.io/calcofi4db/reference/write_parquet_outputs.md).
Inventories all `.parquet` files in a directory, reads row counts via a
transient DuckDB connection, and writes a manifest in the same format as
[`write_parquet_outputs()`](https://calcofi.io/calcofi4db/reference/write_parquet_outputs.md).

## Usage

``` r
write_spatial_manifest(parquet_dir)
```

## Arguments

- parquet_dir:

  Directory containing `.parquet` files

## Value

Invisible path to the written `manifest.json`

## Examples

``` r
if (FALSE) { # \dontrun{
write_spatial_manifest("data/parquet/spatial")
} # }
```
