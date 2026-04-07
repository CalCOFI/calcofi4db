# List CalCOFI files from manifest

Lists files available in a specific archive snapshot using the manifest.
This provides immutable, reproducible file references.

## Usage

``` r
list_calcofi_files(date = "latest", bucket = "public", path = NULL)
```

## Arguments

- date:

  Date string in format "YYYY-MM-DD_HHMMSS" or "latest" (default)

- bucket:

  Bucket type: "public" or "private" (default: "public")

- path:

  Optional path filter (e.g., "swfsc/ichthyo")

## Value

Data frame of files from the manifest

## Examples

``` r
if (FALSE) { # \dontrun{
files <- list_calcofi_files()
files <- list_calcofi_files(path = "swfsc/ichthyo")
files <- list_calcofi_files("2026-02-01_143059", "public")
} # }
```
