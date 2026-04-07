# Run All Validations for a Dataset

Runs a set of validation checks on the database and exports flagged
rows. Returns a summary of all validation results.

## Usage

``` r
validate_dataset(con, validations, output_dir = "data/flagged")
```

## Arguments

- con:

  DuckDB connection

- validations:

  List of validation definitions, each containing:

  - `type`: "fk" (foreign key) or "lookup" (lookup value)

  - `data_tbl`: Table to validate

  - `col`: Column to check

  - `ref_tbl`: Reference table (for fk) or lookup_type (for lookup)

  - `ref_col`: Reference column (for fk only)

  - `output_file`: Filename for flagged rows (placed in output_dir)

  - `description`: Human-readable description

- output_dir:

  Directory for flagged CSV files (default: "data/flagged")

## Value

List containing:

- `checks`: Tibble summarizing each validation check

- `total_flagged`: Total number of flagged rows

- `flagged_files`: Vector of created flagged file paths

- `invalid_data`: List of invalid data tibbles by check name

## Examples

``` r
if (FALSE) { # \dontrun{
validations <- list(
  list(
    type        = "fk",
    data_tbl    = "ichthyo",
    col         = "species_id",
    ref_tbl     = "species",
    ref_col     = "species_id",
    output_file = "orphan_species.csv",
    description = "Species IDs not found in species table"),
  list(
    type        = "fk",
    data_tbl    = "ichthyo",
    col         = "net_id",
    ref_tbl     = "net",
    ref_col     = "net_id",
    output_file = "orphan_nets.csv",
    description = "Net IDs not found in net table"),
  list(
    type        = "lookup",
    data_tbl    = "ichthyo",
    col         = "measurement_value",
    ref_tbl     = "egg_stage",
    output_file = "invalid_egg_stages.csv",
    description = "Egg stage values not in vocabulary"))

results <- validate_dataset(con, validations, output_dir = "data/flagged")
results$checks |> print()
} # }
```
