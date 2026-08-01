# Fingerprint the inputs an ingest's outputs depend on

Fingerprint the inputs an ingest's outputs depend on

## Usage

``` r
input_fingerprint(files = character(), values = character())
```

## Arguments

- files:

  paths to hash by content (missing paths are recorded as `"<missing>"`,
  which still changes the fingerprint)

- values:

  additional values to fold in — a scraped URL list, a package version,
  anything not on disk. Coerced to character and hashed in order, so
  sort first if the order is not meaningful.

## Value

list with `hash` (a single string) and `parts` (named character vector,
one entry per input, for reporting *what* changed)

## Examples

``` r
if (FALSE) { # \dontrun{
input_fingerprint(
  files  = c("metadata/measurement_type.csv", "metadata/measurement_qual.csv"),
  values = sort(d_zips$url))
} # }
```
