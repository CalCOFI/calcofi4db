# Display CSV Changes in a Formatted Table

Creates a formatted table showing changes detected between CSV files and
redefinitions. Changes are color-coded: additions in green, removals in
red, and type mismatches in orange.

## Usage

``` r
display_csv_changes(changes, format = "DT", title = NULL)
```

## Arguments

- changes:

  List output from detect_csv_changes() containing change information

- format:

  Output format: "DT" for interactive DataTable (default), "kable" for
  static knitr table, or "tibble" for raw data frame

- title:

  Optional title for the table

## Value

Formatted table object (DT, kable, or tibble) showing changes

## Examples

``` r
if (FALSE) { # \dontrun{
# Read CSV files and detect changes
d <- read_csv_files("swfsc", "ichthyo")
changes <- detect_csv_changes(d)

# Display interactive table
display_csv_changes(changes)

# Display static table for reports
display_csv_changes(changes, format = "kable")
} # }
```
