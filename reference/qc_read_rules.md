# Read the rule registry, attaching SQL text and parsed params

Read the rule registry, attaching SQL text and parsed params

## Usage

``` r
qc_read_rules(dir, active_only = TRUE)
```

## Arguments

- dir:

  directory holding `rules.csv` and `sql/`

- active_only:

  drop rules parked for a later phase (`active = FALSE`)

## Value

a tibble, one row per rule, with `sql` (character) and `params`
(list-column) added

## Examples

``` r
if (FALSE) { # \dontrun{
qc_read_rules(here::here("metadata/qc_rules"))
} # }
```
