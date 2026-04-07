# Add metadata to Parquet file

Reads a Parquet file, adds custom metadata, and rewrites it.

## Usage

``` r
add_parquet_metadata(path, metadata, output = path)
```

## Arguments

- path:

  Path to Parquet file

- metadata:

  Named list of metadata key-value pairs

- output:

  Output path (default: overwrites input file)

## Value

Path to the modified Parquet file

## Examples

``` r
if (FALSE) { # \dontrun{
add_parquet_metadata(
  "parquet/bottle.parquet",
  metadata = list(
    source     = "calcofi",
    version    = "2026.01.31",
    created_by = "calcofi4db"))
} # }
```
