# Delete Flagged Rows from Database

Removes rows that were flagged during validation from the database
tables. Should be called after reviewing flagged files to clean up
invalid data.

## Usage

``` r
delete_flagged_rows(
  con,
  validation_results,
  tables_to_clean = NULL,
  dry_run = TRUE
)
```

## Arguments

- con:

  DuckDB connection

- validation_results:

  Results from
  [`validate_dataset()`](https://calcofi.io/calcofi4db/reference/validate_dataset.md)

- tables_to_clean:

  Optional character vector of table names to clean. If NULL (default),
  cleans all tables with flagged rows.

- dry_run:

  If TRUE (default), only reports what would be deleted without actually
  deleting. Set to FALSE to perform deletion.

## Value

Tibble with deletion statistics per table

## Examples

``` r
if (FALSE) { # \dontrun{
# first, run validation
results <- validate_dataset(con, validations)

# dry run to see what would be deleted
delete_flagged_rows(con, results, dry_run = TRUE)

# actually delete
delete_flagged_rows(con, results, dry_run = FALSE)
} # }
```
