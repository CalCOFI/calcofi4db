# Show Flagged File Result

Displays the result of
[`flag_invalid_rows()`](https://calcofi.io/calcofi4db/reference/flag_invalid_rows.md)
with a GitHub link to the flagged file and summary statistics.

## Usage

``` r
show_flagged_file(
  invalid_rows,
  output_path,
  description,
  repo = "CalCOFI/workflows",
  branch = "main"
)
```

## Arguments

- invalid_rows:

  Tibble of invalid rows that were flagged

- output_path:

  Path to the flagged CSV file

- description:

  Description of what was flagged

- repo:

  GitHub repository (default: "CalCOFI/workflows")

- branch:

  Git branch (default: "main")

## Value

HTML string with flagged file summary and link

## Examples

``` r
if (FALSE) { # \dontrun{
invalid_stages <- validate_egg_stages(con, "egg_stage", "stage")
if (nrow(invalid_stages) > 0) {
  output_path <- flag_invalid_rows(invalid_stages, "data/flagged/invalid_egg_stages.csv", "Invalid egg stages")
  show_flagged_file(invalid_stages, output_path, "Invalid egg stages")
}
} # }
```
