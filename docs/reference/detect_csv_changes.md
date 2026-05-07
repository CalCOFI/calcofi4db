# Detect Changes in CSV Files

Compares latest CSV files with redefinition metadata to detect changes
in tables, fields, and data types. This function compares the raw CSV
data (d\$d_csv) with the table and field redefinitions (d\$d_tbls_rd and
d\$d_flds_rd) to identify mismatches before database ingestion.

## Usage

``` r
detect_csv_changes(d)
```

## Arguments

- d:

  List output from read_csv_files() containing:

  - d_csv: CSV metadata with tables, fields, and data

  - d_tbls_rd: Table redefinition data frame

  - d_flds_rd: Field redefinition data frame

## Value

A list containing detected changes:

- tables_added: Tables in CSV but not in redefinitions

- tables_removed: Tables in redefinitions but not in CSV

- fields_added: Fields in CSV but not in redefinitions (by table)

- fields_removed: Fields in redefinitions but not in CSV (by table)

- type_mismatches: Field type differences between CSV and redefinitions

- summary: Data frame summarizing all changes for display

## Examples

``` r
if (FALSE) { # \dontrun{
# Read CSV files and metadata
d <- read_csv_files("swfsc", "ichthyo")

# Detect changes between CSV files and redefinitions
changes <- detect_csv_changes(d)

# Display summary of changes
print(changes$summary)
} # }
```
