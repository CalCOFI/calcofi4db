# The canonical `site_key` spelling, as a SQL expression

Returns a DuckDB expression that rewrites any spelling of a CalCOFI
line/station pair into the one form the rest of the pipeline uses,
`printf('%05.1f %05.1f', line, station)` — the form
[`standardize_site_key()`](https://calcofi.io/calcofi4db/reference/standardize_site_key.md)
writes from numeric columns. The first two signed decimal numbers in the
string are the line and the station, whatever separates them (a space,
several, a comma) and however they are padded; a string with fewer than
two numbers becomes `NULL`. A negative station (ichthyo's
`"011.7 -02.6"`, inshore of the line origin) keeps its sign; the width-5
format makes it `-02.6`, which is what `%05.1f` already produced for
those rows.

## Usage

``` r
site_key_sql(expr = "site_key")
```

## Arguments

- expr:

  a SQL expression or column name holding the string (default
  `"site_key"`).

## Value

a length-one character SQL expression.

## Details

Rounding is to one decimal, the grid's own resolution (0.1 station unit
is 0.74 km along a line): the one source form with two decimals,
`090.0 27.76`, becomes `090.0 027.8`.

## See also

[`normalize_site_key()`](https://calcofi.io/calcofi4db/reference/normalize_site_key.md)
for the R vector form,
[`check_site_key_format()`](https://calcofi.io/calcofi4db/reference/check_site_key_format.md)
for the release gate,
[`append_sample()`](https://calcofi.io/calcofi4db/reference/append_sample.md)
which applies this to every ingest.

## Examples

``` r
site_key_sql("site_key")
#> [1] "CASE WHEN site_key IS NULL THEN NULL\nWHEN len(regexp_extract_all(CAST(site_key AS VARCHAR), '-?[0-9]+(?:\\.[0-9]+)?')) < 2 THEN NULL\nELSE printf('%05.1f %05.1f',\n            CAST(regexp_extract_all(CAST(site_key AS VARCHAR), '-?[0-9]+(?:\\.[0-9]+)?')[1] AS DOUBLE), CAST(regexp_extract_all(CAST(site_key AS VARCHAR), '-?[0-9]+(?:\\.[0-9]+)?')[2] AS DOUBLE)) END"
```
