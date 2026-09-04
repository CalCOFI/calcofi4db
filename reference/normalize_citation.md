# Normalize a citation string for comparison

Lower-case, markup and HTML entities removed, then everything but
letters and digits dropped — so a trailing period, a re-flowed line,
`<i>` around a title or an upper-cased DOI (doi.org content negotiation
returns `10.25921/3W9F-JD72`) is not drift. Author-name abbreviation IS
drift, on purpose: `Keeling, C.D.` and `Keeling, Charles D.` are
different strings.

## Usage

``` r
normalize_citation(x)
```

## Arguments

- x:

  character

## Value

character of the same length
