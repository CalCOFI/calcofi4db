# Print CSV Change Statistics

Prints a summary of change statistics from detect_csv_changes() output.

## Usage

``` r
print_csv_change_stats(changes, verbose = TRUE)
```

## Arguments

- changes:

  List output from detect_csv_changes()

- verbose:

  Logical, whether to print detailed information

## Value

Invisible NULL (prints to console)

## Examples

``` r
if (FALSE) { # \dontrun{
d <- read_csv_files("swfsc", "ichthyo")
changes <- detect_csv_changes(d)
print_csv_change_stats(changes)
} # }
```
