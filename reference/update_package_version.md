# Synchronized Version Management for Package and Database

These functions manage synchronized versioning between the R package,
database schema, NEWS.md file, and git commits with GitHub permalinks.
Update Package Version and NEWS

## Usage

``` r
update_package_version(version, description, news_items = NULL)
```

## Arguments

- version:

  New semantic version number (e.g., "1.0.0")

- description:

  Description of changes for NEWS.md

- news_items:

  Character vector of bullet points for NEWS.md (optional)

## Value

List with updated version info

## Details

Updates the package DESCRIPTION version and prepends a new entry to
NEWS.md. This should be called before committing and recording the
schema version.

## Examples

``` r
if (FALSE) { # \dontrun{
update_package_version(
  version = "1.0.0",
  description = "Initial production release",
  news_items = c(
    "Complete NOAA CalCOFI Database ingestion",
    "Add schema versioning system",
    "Create master ingestion workflow"
  )
)
} # }
```
