# Flag and Export Invalid Rows

Writes invalid rows to a CSV file for manual review. Creates the output
directory if it doesn't exist. Returns the path to the created file.

## Usage

``` r
flag_invalid_rows(invalid_rows, output_path, description, append = FALSE)
```

## Arguments

- invalid_rows:

  Tibble of invalid rows to export

- output_path:

  Path for output CSV file

- description:

  Description of the validation failure (for logging)

- append:

  If TRUE, append to existing file; if FALSE (default), overwrite

## Value

Path to the created/updated CSV file, or NULL if no rows to flag

## Examples

``` r
if (FALSE) { # \dontrun{
orphan_species <- validate_fk_references(con, "ichthyo", "species_id", "species", "species_id")
if (nrow(orphan_species) > 0) {
  flag_invalid_rows(
    invalid_rows = orphan_species,
    output_path  = "data/flagged/orphan_species.csv",
    description  = "Species IDs not found in species table")
}
} # }
```
