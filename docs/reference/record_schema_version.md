# Record Schema Version

Records a new schema version in the schema_version table with metadata
about the ingestion. This function should be called after successful
completion of all dataset ingestions and relationship creation.

## Usage

``` r
record_schema_version(
  con,
  schema = "dev",
  version,
  description,
  script_permalink
)
```

## Arguments

- con:

  Database connection object

- schema:

  Schema name (default: "dev")

- version:

  Semantic version number (e.g., "1.0.0", "1.1.0")

- description:

  Description of changes in this version

- script_permalink:

  GitHub permalink to the versioned ingestion script

## Value

Invisible NULL (version is recorded in database and CSV file)

## Details

The function:

1.  Creates the schema_version table if it doesn't exist

2.  Reads any existing versions from inst/schema_version.csv

3.  Inserts the new version record into the database

4.  Appends the new version to inst/schema_version.csv

## Examples

``` r
if (FALSE) { # \dontrun{
record_schema_version(
  con = con,
  schema = "dev",
  version = "1.0.0",
  description = "Initial ingestion of NOAA CalCOFI Database",
  script_permalink = "https://github.com/CalCOFI/calcofi4db/blob/abc123/inst/create_db.qmd"
)
} # }
```
