# Copy Database Schema

Copies a database schema with all tables, data, sequences, and
constraints to a new schema. Useful for promoting dev to prod.

## Usage

``` r
copy_schema(
  con,
  from_schema = "dev",
  to_schema = "prod",
  drop_existing = TRUE,
  copy_fk = TRUE,
  verbose = TRUE
)
```

## Arguments

- con:

  Database connection

- from_schema:

  Source schema name (default: "dev")

- to_schema:

  Destination schema name (default: "prod")

- drop_existing:

  Logical, drop existing destination schema (default: TRUE)

- copy_fk:

  Logical, copy foreign key constraints (default: TRUE)

- verbose:

  Logical, print progress messages (default: TRUE)

## Value

List with copy summary statistics

## Examples

``` r
if (FALSE) { # \dontrun{
# Copy dev to prod
result <- copy_schema(con, from_schema = "dev", to_schema = "prod")

# Check results
result$summary
} # }
```
