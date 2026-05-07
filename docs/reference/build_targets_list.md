# Build Targets List from Quarto Frontmatter

Reads `calcofi:` YAML frontmatter from all `.qmd` files in the workflows
directory and returns a [`list()`](https://rdrr.io/r/base/list.html) of
[`targets::tar_target()`](https://docs.ropensci.org/targets/reference/tar_target.html)
calls suitable for use as the body of `_targets.R`. Includes a
`corrections_csv` target that tracks `metadata/ship_renames.csv` and
`metadata/measurement_type.csv`, so any edit to those files forces
re-run of dependent ingest targets.

## Usage

``` r
build_targets_list(
  workflows_dir = here::here(),
  corrections = c("metadata/ship_renames.csv", "metadata/measurement_type.csv"),
  exclude = NULL,
  verbose = TRUE
)
```

## Arguments

- workflows_dir:

  Path to workflows directory (default:
  [`here::here()`](https://here.r-lib.org/reference/here.html))

- corrections:

  Character vector of correction file paths relative to `workflows_dir`
  (default:
  `c("metadata/ship_renames.csv", "metadata/measurement_type.csv")`)

- exclude:

  Character vector of target names to exclude from the pipeline
  (default: NULL). Excluded targets are also removed from other targets'
  dependency lists. Useful for skipping large workflows like
  `"ingest_calcofi_ctd-cast"`.

- verbose:

  Print parsed workflow table (default: TRUE)

## Value

A [`list()`](https://rdrr.io/r/base/list.html) of
[`tar_target()`](https://docs.ropensci.org/targets/reference/tar_target.html)
objects ready for `_targets.R`

## Details

Workflows with `dependency: [auto]` (typically `release_database`) auto-
depend on all targets whose `workflow_type` is `"ingest"` or
`"spatial"`.

## Examples

``` r
if (FALSE) { # \dontrun{
# in _targets.R:
library(targets)
devtools::load_all(here::here("../calcofi4db"))
build_targets_list()

# skip CTD ingest:
build_targets_list(exclude = "ingest_calcofi_ctd-cast")
} # }
```
