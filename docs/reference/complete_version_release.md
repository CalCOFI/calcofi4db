# Complete Version Release Workflow

Performs the complete version release workflow:

1.  Updates package DESCRIPTION and NEWS.md

2.  Commits changes to git

3.  Records schema version in database

4.  Updates schema_version.csv

## Usage

``` r
complete_version_release(
  con,
  schema = "dev",
  version,
  description,
  news_items = NULL,
  additional_files = NULL,
  commit = FALSE,
  push = FALSE,
  repo_owner = "CalCOFI",
  repo_name = "calcofi4db"
)
```

## Arguments

- con:

  Database connection

- schema:

  Database schema (default: "dev")

- version:

  New version number

- description:

  Version description

- news_items:

  News items for NEWS.md (optional)

- additional_files:

  Additional files to commit (optional)

- commit:

  Logical, whether to commit (default: FALSE)

- push:

  Logical, whether to push (default: FALSE)

- repo_owner:

  GitHub owner (default: "CalCOFI")

- repo_name:

  GitHub repo (default: "calcofi4db")

## Value

List with version info, commit details, and database record

## Examples

``` r
if (FALSE) { # \dontrun{
# Complete release workflow
release_result <- complete_version_release(
  con = con_dev,
  schema = "dev",
  version = "1.0.0",
  description = "Initial production release with NOAA CalCOFI Database",
  news_items = c(
    "Complete NOAA CalCOFI Database ingestion",
    "Add synchronized versioning system",
    "Create master ingestion workflow"
  ),
  additional_files = c("inst/create_db.qmd", "inst/schema_version.csv"),
  commit = TRUE,
  push = TRUE
)
} # }
```
