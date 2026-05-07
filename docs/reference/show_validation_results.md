# Show Validation Results with GitHub Links

Displays validation results from
[`validate_dataset()`](https://calcofi.io/calcofi4db/reference/validate_dataset.md)
as a datatable with output_file paths converted to clickable GitHub
links.

## Usage

``` r
show_validation_results(
  validation_results,
  caption = "Validation Results",
  repo = "CalCOFI/workflows",
  branch = "main"
)
```

## Arguments

- validation_results:

  Results from
  [`validate_dataset()`](https://calcofi.io/calcofi4db/reference/validate_dataset.md)
  containing `$checks` tibble

- caption:

  Table caption (default: "Validation Results")

- repo:

  GitHub repository (default: "CalCOFI/workflows")

- branch:

  Git branch (default: "main")

## Value

DT datatable object

## Examples

``` r
if (FALSE) { # \dontrun{
validation_results <- validate_dataset(con, validations, output_dir)
show_validation_results(validation_results)
} # }
```
