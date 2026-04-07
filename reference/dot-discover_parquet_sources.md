# Discover parquet sources in a directory

Discover parquet sources in a directory

## Usage

``` r
.discover_parquet_sources(parquet_dir, tables = NULL)
```

## Arguments

- parquet_dir:

  Path to directory

- tables:

  Optional character vector to filter by table name

## Value

List of list(name, path, partitioned)
