# Check Multiple Datasets for Integrity

Convenience function to check integrity of multiple datasets and halt if
any fail. Useful for master ingestion scripts with multiple datasets.

## Usage

``` r
check_multiple_datasets(
  datasets,
  halt_on_first_fail = FALSE,
  display_format = "DT"
)
```

## Arguments

- datasets:

  Named list where names are dataset labels and values are outputs from
  read_csv_files()

- halt_on_first_fail:

  Logical, stop checking after first failure (default: FALSE)

- display_format:

  Format for displaying changes (default: "DT")

## Value

List with:

- all_passed: Logical indicating if all checks passed

- results: Named list of individual check results

- n_failed: Number of datasets that failed

- failed_datasets: Character vector of failed dataset names

## Examples

``` r
if (FALSE) { # \dontrun{
datasets <- list(
  "NOAA CalCOFI DB" = d_noaa,
  "Bottle Database" = d_bottle
)
check_results <- check_multiple_datasets(datasets)

if (!check_results$all_passed) {
  stop("One or more datasets failed integrity checks")
}
} # }
```
