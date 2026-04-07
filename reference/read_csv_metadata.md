# Read CSV Files and Extract Metadata

Reads all CSV files in a directory, extracts metadata about tables and
fields, and writes this metadata to files for further processing.

## Usage

``` r
read_csv_metadata(dir_csv, dir_ingest, create_dirs = TRUE)
```

## Arguments

- dir_csv:

  Directory containing CSV files

- dir_ingest:

  Directory to store metadata files

- create_dirs:

  Whether to create directories if they don't exist

## Value

A list with two data frames: tables and fields metadata

## Examples

``` r
if (FALSE) { # \dontrun{
csv_metadata <- read_csv_metadata(
  dir_csv = "/path/to/data/provider/dataset",
  dir_ingest = "/path/to/ingest/provider/dataset")
} # }
```
