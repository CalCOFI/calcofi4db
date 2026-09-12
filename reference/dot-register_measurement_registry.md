# Append-or-update rows in a measurement-face registry by natural key

Unlike
[`register_variables()`](https://calcofi.io/calcofi4db/reference/register_variables.md)
(append-only, refuses a duplicate), the five measurement-face registries
are filled incrementally by hand and by a workstream that may be re-run,
so a row whose natural key (`key_cols`) matches an existing row
**replaces** it; a genuinely new key is appended. Always writes
`na = ""`.

## Usage

``` r
.register_measurement_registry(
  new_rows,
  path,
  cols,
  key_cols,
  header_comment,
  label,
  quiet = FALSE
)
```
