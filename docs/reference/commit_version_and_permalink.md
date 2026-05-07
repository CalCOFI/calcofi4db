# Commit Version Changes and Get Permalink

Commits DESCRIPTION, NEWS.md, and specified files to git, then generates
a GitHub permalink to the ingestion script at the committed version.

## Usage

``` r
commit_version_and_permalink(
  version,
  files = NULL,
  repo_owner = "CalCOFI",
  repo_name = "calcofi4db",
  script_path = "inst/create_db.qmd",
  commit = FALSE,
  push = FALSE
)
```

## Arguments

- version:

  Version number for commit message

- files:

  Additional files to commit (e.g., "inst/create_db.qmd")

- repo_owner:

  GitHub repository owner (default: "CalCOFI")

- repo_name:

  GitHub repository name (default: "calcofi4db")

- script_path:

  Path to script for permalink (default: "inst/create_db.qmd")

- commit:

  Logical, whether to actually commit (default: FALSE for safety)

- push:

  Logical, whether to push to remote (default: FALSE for safety)

## Value

List with commit hash and permalink

## Examples

``` r
if (FALSE) { # \dontrun{
# Dry run (no commit)
result <- commit_version_and_permalink(
  version = "1.0.0",
  files = c("inst/create_db.qmd", "inst/schema_version.csv")
)

# Actually commit and push
result <- commit_version_and_permalink(
  version = "1.0.0",
  files = c("inst/create_db.qmd", "inst/schema_version.csv"),
  commit = TRUE,
  push = TRUE
)
} # }
```
