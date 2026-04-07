# Check Data Integrity for Ingestion

Validates that CSV files match their redefinition metadata before
database ingestion. This function is designed to be called from Quarto
notebooks and will stop notebook execution if mismatches are detected.

## Usage

``` r
check_data_integrity(
  d,
  dataset_name = "Dataset",
  halt_on_fail = TRUE,
  type_exceptions = NULL,
  display_format = "DT",
  verbose = TRUE,
  header_level = 3
)
```

## Arguments

- d:

  List output from read_csv_files() containing CSV and redefinition data

- dataset_name:

  Name of dataset for display purposes (e.g., "NOAA CalCOFI Database")

- halt_on_fail:

  Logical, whether to set knitr eval=FALSE on failure (default: TRUE)

- type_exceptions:

  Character vector of known acceptable type mismatches. Use `"all"` to
  accept all type mismatches, or specific `"table.field"` patterns
  (e.g., `c("casts.time", "bottle.t_qual")`). Default: NULL (no
  exceptions).

- display_format:

  Format for displaying changes: "DT" (DataTable), "kable", or "print"
  (default: "DT")

- verbose:

  Logical, print detailed messages (default: TRUE)

- header_level:

  Integer, markdown header level for output messages (default: 3).
  Controls the top-level header depth; sub-headers use header_level + 1.
  Set to match the parent section level in your Quarto document to keep
  the Table of Contents hierarchy correct.

## Value

List with:

- passed: Logical indicating if integrity check passed

- changes: Full changes object from detect_csv_changes()

- n_changes: Number of changes detected (after filtering exceptions)

- n_exceptions: Number of type mismatches accepted as exceptions

- message: Character string with markdown-formatted message

## Details

The function:

1.  Detects changes between CSV files and redefinitions using
    detect_csv_changes()

2.  Optionally filters out known acceptable type mismatches via
    `type_exceptions`

3.  Prints summary statistics of detected changes

4.  Displays interactive table of changes if any exist

5.  Returns appropriate status for notebook control flow

When called from a Quarto notebook in an output: asis chunk, this
function will render markdown messages and can control chunk evaluation
via knitr options.

## Examples

``` r
if (FALSE) { # \dontrun{
# strict check — halt on any mismatch
integrity <- check_data_integrity(d, "NOAA CalCOFI Database")

# accept all type mismatches (e.g., readr infers types differently)
integrity <- check_data_integrity(
  d               = d,
  dataset_name    = "CalCOFI Bottle Database",
  halt_on_fail    = FALSE,
  type_exceptions = "all")

# use header_level = 2 for top-level sections
integrity <- check_data_integrity(
  d            = d,
  dataset_name = "NOAA CalCOFI Database",
  header_level = 2)
} # }
```
