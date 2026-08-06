# Write Spatial Manifest

Generates a `manifest.json` for spatial parquet outputs that do not use
[`write_parquet_outputs()`](https://calcofi.io/calcofi4db/reference/write_parquet_outputs.md).
Inventories all `.parquet` files in a directory, reads row counts via a
transient DuckDB connection, and writes a manifest in the same format as
[`write_parquet_outputs()`](https://calcofi.io/calcofi4db/reference/write_parquet_outputs.md).

## Usage

``` r
write_spatial_manifest(parquet_dir, output_dir = parquet_dir)
```

## Arguments

- parquet_dir:

  Directory containing `.parquet` files

- output_dir:

  Directory to write `manifest.json` into. Defaults to `parquet_dir`;
  pass the repo-side sidecar directory when the bytes are staged outside
  the repo (see
  [`cc_stage_dir()`](https://calcofi.io/calcofi4db/reference/cc_stage_dir.md)).

## Value

Invisible path to the written `manifest.json`

## Examples

``` r
if (FALSE) { # \dontrun{
write_spatial_manifest(
  parquet_dir = cc_stage_path("parquet", "spatial"),
  output_dir  = "data/parquet/spatial")
} # }
```
