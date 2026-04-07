# Read a Parquet table

Reads a Parquet file into a data frame or as a lazy Arrow Table.

## Usage

``` r
read_parquet_table(path, lazy = FALSE, columns = NULL)
```

## Arguments

- path:

  Path to Parquet file (local or GCS)

- lazy:

  If TRUE, returns an Arrow Table for lazy evaluation (default: FALSE)

- columns:

  Optional vector of column names to read (default: all)

## Value

Data frame or Arrow Table

## Examples

``` r
if (FALSE) { # \dontrun{
# read as data frame
df <- read_parquet_table("parquet/bottle.parquet")

# read as lazy Arrow table for large files
tbl <- read_parquet_table("parquet/bottle.parquet", lazy = TRUE)

# read only specific columns
df <- read_parquet_table("parquet/bottle.parquet", columns = c("cruise_id", "depth"))
} # }
```
