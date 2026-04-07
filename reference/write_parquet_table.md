# Write data to Parquet format

Writes a data frame to Parquet, optionally with partitioning.

## Usage

``` r
write_parquet_table(data, path, partitions = NULL, compression = "snappy")
```

## Arguments

- data:

  Data frame to write

- path:

  Output path for Parquet file or directory (for partitioned)

- partitions:

  Optional character vector of columns to partition by

- compression:

  Compression codec (default: "snappy")

## Value

Path to the created Parquet file/directory

## Examples

``` r
if (FALSE) { # \dontrun{
# simple write
write_parquet_table(df, "output/data.parquet")

# partitioned by year and month
write_parquet_table(
  df,
  "output/data/",
  partitions = c("year", "month"))
} # }
```
