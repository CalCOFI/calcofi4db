# Get historical file from a specific date

Retrieves a file as it existed on a specific date from the archive.

## Usage

``` r
get_historical_file(path, date, bucket = "calcofi-files", local_path = NULL)
```

## Arguments

- path:

  Relative path to the file

- date:

  Date to retrieve (character "YYYY-MM-DD" or Date object)

- bucket:

  GCS bucket name (default: "calcofi-files")

- local_path:

  Local path to save file (default: temp file)

## Value

Path to the downloaded local file

## Examples

``` r
if (FALSE) { # \dontrun{
# get bottle.csv as it was on 2026-01-15
file <- get_historical_file(
  "calcofi/bottle/bottle.csv",
  date = "2026-01-15")
} # }
```
