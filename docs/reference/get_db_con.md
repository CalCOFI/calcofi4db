# Get a database connection to the CalCOFI PostgreSQL database (DEPRECATED)

**\[deprecated\]**

This function is deprecated. Please use one of the following
alternatives:

- [`get_working_ducklake`](https://calcofi.io/calcofi4db/reference/get_working_ducklake.md):
  For admin/ingest work with the Working DuckLake

- [`get_duckdb_con`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md):
  For DuckDB connections to frozen releases

- [`calcofi4r::cc_get_db`](https://calcofi.github.io/calcofi4r/reference/cc_get_db.html):
  For end-users accessing frozen CalCOFI data

## Usage

``` r
get_db_con(
  schemas = "public",
  host = NULL,
  port = 5432,
  dbname = "gis",
  user = "admin",
  password_file = NULL
)
```

## Arguments

- schemas:

  Character vector of schema search paths (default: "public")

- host:

  Database host (default: automatically detected based on system)

- port:

  Database port (default: 5432)

- dbname:

  Database name (default: "gis")

- user:

  Database user (default: "admin")

- password_file:

  Path to file containing database password (default: system-dependent
  location)

## Value

A database connection object
