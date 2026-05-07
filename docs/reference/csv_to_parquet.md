# Convert CSV file to Parquet format

Reads a CSV file and writes it as Parquet with optional schema
enforcement. Parquet provides efficient columnar storage with
compression.

## Usage

``` r
csv_to_parquet(
  csv_path,
  output = NULL,
  schema_def = NULL,
  compression = "snappy",
  col_types = NULL
)
```

## Arguments

- csv_path:

  Path to the CSV file (local or GCS)

- output:

  Path for the output Parquet file. If NULL, uses same name with
  .parquet extension

- schema_def:

  Optional schema definition (arrow::schema object or list)

- compression:

  Compression codec (default: "snappy")

- col_types:

  Column type specification for readr (optional)

## Value

Path to the created Parquet file

## Examples

``` r
if (FALSE) { # \dontrun{
# basic conversion
pqt_file <- csv_to_parquet("data/bottle.csv")

# with custom output path
pqt_file <- csv_to_parquet(
  "data/bottle.csv",
  output = "parquet/bottle.parquet")

# with explicit schema
schema <- arrow::schema(
  cruise_id = arrow::int32(),
  station   = arrow::string(),
  depth     = arrow::float64())
pqt_file <- csv_to_parquet("data/bottle.csv", schema_def = schema)
} # }
```
