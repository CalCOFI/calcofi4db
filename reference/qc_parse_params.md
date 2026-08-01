# Parse a rule's `params` cell into a named list

Format is `k=v;k=v` — deliberately flat. Anything needing more structure
than that is a sign the logic belongs in the rule's SQL file, not in the
index.

## Usage

``` r
qc_parse_params(x)
```

## Arguments

- x:

  a single `params` cell (character; `NA` or empty gives an empty list)

## Value

a named list of character values

## Examples

``` r
qc_parse_params("threshold=0.5;units=degC")
#> $threshold
#> [1] "0.5"
#> 
#> $units
#> [1] "degC"
#> 
```
