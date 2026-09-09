# Normalize `site_key` strings in R

The vector twin of
[`site_key_sql()`](https://calcofi.io/calcofi4db/reference/site_key_sql.md):
the first two signed decimal numbers in each string, formatted
`sprintf("%05.1f %05.1f", line, station)`; `NA` where there are fewer
than two. Use it in an ingest that builds `site_key` in R before the arm
reaches
[`append_sample()`](https://calcofi.io/calcofi4db/reference/append_sample.md)
(which normalises again, harmlessly).

## Usage

``` r
normalize_site_key(x)
```

## Arguments

- x:

  character vector of station strings in any spelling.

## Value

character vector, same length, canonical or `NA_character_`.

## Examples

``` r
normalize_site_key(c("93.3    26.4", "0093. 060.0", "090.0 27.76", "88.50 030.1",
                     "093.3 026.4", "011.7 -02.6", "93.3,26.4", "line 90", NA))
#> [1] "093.3 026.4" "093.0 060.0" "090.0 027.8" "088.5 030.1" "093.3 026.4"
#> [6] "011.7 -02.6" "093.3 026.4" NA            NA           
```
