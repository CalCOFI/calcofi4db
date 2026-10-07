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
  header_level = 3,
  stop_on_fail = !rlang::is_interactive()
)
```

## Arguments

- d:

  List output from read_csv_files() containing CSV and redefinition data

- dataset_name:

  Name of dataset for display purposes (e.g., "NOAA CalCOFI Database")

- halt_on_fail:

  Logical, whether to halt the notebook on failure (default: TRUE). How
  it halts depends on `stop_on_fail`.

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

- stop_on_fail:

  Logical, whether a halting failure raises an error
  ([`stop()`](https://rdrr.io/r/base/stop.html)) instead of only setting
  knitr `eval = FALSE` on the remaining chunks. Default:
  `!rlang::is_interactive()`, so a non-interactive render
  (`quarto render`,
  [`targets::tar_make()`](https://docs.ropensci.org/targets/reference/tar_make.html))
  FAILS – the render exits non-zero and its target errors – rather than
  finishing "successfully" having written nothing. Until calcofi4db
  4.20.0 a failed check only disabled the remaining chunks, so
  `ingest_swfsc_ichthyo.qmd` halted at this checkpoint from 2026-09 to
  2026-10-04 while every
  [`tar_make()`](https://docs.ropensci.org/targets/reference/tar_make.html)
  reported it completed and the release shipped its stale shard. Pass
  `FALSE` only to render the failure report on purpose, never inside the
  pipeline.

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
