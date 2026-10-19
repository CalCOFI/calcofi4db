# The `# ...` line written at the top of `metadata/variable.csv`

Same pattern as
[`write_holdings_csv()`](https://calcofi.io/calcofi4db/reference/holdings_from_sidecars.md)'s:
a single `#`-prefixed line ahead of the real header, saying what the
file is and who appends to it, so a person opening the CSV cold does not
have to find this file to learn that.
[`read_variable()`](https://calcofi.io/calcofi4db/reference/read_variable.md)
skips it with
[`.registry_csv_text()`](https://calcofi.io/calcofi4db/reference/dot-registry_csv_text.md)
(the leading `#` block only).

## Usage

``` r
variable_csv_header_comment()
```

## Value

length-1 character
