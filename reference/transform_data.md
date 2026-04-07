# Transform Data for Database Ingestion

Applies transformations to raw data based on redefinition files.

## Usage

``` r
transform_data(data_info, verbose = FALSE)
```

## Arguments

- data_info:

  List containing data and metadata from
  [`read_csv_files()`](https://calcofi.io/calcofi4db/reference/read_csv_files.md)

- verbose:

  Print progress messages. Default: FALSE

## Value

List with transformed data ready for database ingestion

## Examples

``` r
if (FALSE) { # \dontrun{
transformed_data <- transform_data(data_info, verbose = TRUE)
} # }
```
