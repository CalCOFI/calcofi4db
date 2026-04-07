# Get Schema Version History

Retrieves the schema version history from either the database or CSV
file.

## Usage

``` r
get_schema_versions(con = NULL, schema = "dev", from_csv = FALSE)
```

## Arguments

- con:

  Database connection object (optional if from_csv = TRUE)

- schema:

  Schema name (default: "dev")

- from_csv:

  Logical, read from CSV file instead of database (default: FALSE)

## Value

A tibble with version history

## Examples

``` r
if (FALSE) { # \dontrun{
# Get from database
get_schema_versions(con, schema = "dev")

# Get from CSV file
get_schema_versions(from_csv = TRUE)
} # }
```
