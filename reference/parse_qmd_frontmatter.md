# Parse YAML Frontmatter from Quarto Notebooks

Reads each `.qmd` file in a directory and extracts the `calcofi:` block
from the YAML frontmatter. Returns a tibble describing each workflow's
target name, type, dependencies, and output path.

## Usage

``` r
parse_qmd_frontmatter(workflows_dir = here::here(), pattern = "*.qmd")
```

## Arguments

- workflows_dir:

  Path to the directory containing `.qmd` files (default: current
  directory via
  [`here::here()`](https://here.r-lib.org/reference/here.html))

- pattern:

  Glob pattern for `.qmd` files (default: `"*.qmd"`)

## Value

Tibble with columns: `qmd_file`, `target_name`, `workflow_type`,
`dependency` (list column), `output`, `modifies` (list column of
dependency table names this ingest inserts/modifies)

## Examples

``` r
if (FALSE) { # \dontrun{
wf <- parse_qmd_frontmatter("workflows/")
wf |> dplyr::filter(workflow_type == "ingest")
} # }
```
