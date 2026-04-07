# Get a CalCOFI file from the immutable archive

Downloads a file from the immutable archive snapshot. This ensures
reproducible data access by referencing specific archive timestamps.

## Usage

``` r
get_calcofi_file(path, date = "latest", bucket = "public", local_path = NULL)
```

## Arguments

- path:

  Relative path within archive (e.g., "swfsc/ichthyo/cruise.csv")

- date:

  Date string in format "YYYY-MM-DD_HHMMSS" or "latest" (default)

- bucket:

  Bucket type: "public" or "private" (default: "public")

- local_path:

  Local path to save file (default: temp file)

## Value

Path to the downloaded local file

## Examples

``` r
if (FALSE) { # \dontrun{
# get latest version
cruise_csv <- get_calcofi_file("swfsc/ichthyo/cruise.csv")

# get specific version for reproducibility
cruise_csv <- get_calcofi_file(
  "swfsc/ichthyo/cruise.csv",
  date = "2026-02-01_143059")
} # }
```
