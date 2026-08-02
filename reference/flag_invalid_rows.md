# Flag and Export Invalid Rows

Writes invalid rows to a CSV file for manual review. Creates the output
directory if it doesn't exist. Returns the path to the created file.

## Usage

``` r
flag_invalid_rows(
  invalid_rows,
  output_path,
  description,
  append = FALSE,
  volatile_cols = "_ingested_at"
)
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

- volatile_cols:

  Columns to ignore when deciding whether the file changed. Defaults to
  `"_ingested_at"`. Set to
  [`character()`](https://rdrr.io/r/base/character.html) to always
  rewrite.

## Value

Path to the created/updated CSV file, or NULL if no rows to flag

## Details

These files are committed and reviewed in diffs, so the write is
**idempotent** with respect to columns that change on every run for no
reason. `_ingested_at` is stamped per row at read time, so re-running an
ingest over unchanged source data rewrote the whole file with a new
timestamp on every row: `data/flagged/invalid_egg_stages.csv` churned
790 rows — the same 790 rows — each time, which is noise that hides the
diff that would matter. When the new rows match the file on disk apart
from `volatile_cols`, the file is left alone.

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
