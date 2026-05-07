# Get local file manifest

Retrieves metadata (size, mtime, md5, path) for all CSV files in a local
directory. Uses [`tools::md5sum()`](https://rdrr.io/r/tools/md5sum.html)
for content hashing and
[`file.mtime()`](https://rdrr.io/r/base/file.info.html) for modification
timestamps.

## Usage

``` r
get_local_manifest(dir_csv)
```

## Arguments

- dir_csv:

  Local directory containing CSV files

## Value

Tibble with columns: name, size, mtime, md5, local_path

## Examples

``` r
if (FALSE) { # \dontrun{
manifest <- get_local_manifest("/path/to/csv/files")
} # }
```
