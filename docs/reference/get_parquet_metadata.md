# Get Parquet file metadata

Retrieves schema and metadata from a Parquet file.

## Usage

``` r
get_parquet_metadata(path)
```

## Arguments

- path:

  Path to Parquet file

## Value

List with schema, num_rows, num_columns, and file_size

## Examples

``` r
if (FALSE) { # \dontrun{
meta <- get_parquet_metadata("parquet/bottle.parquet")
meta$schema
meta$num_rows
} # }
```
