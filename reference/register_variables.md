# Append new rows to `metadata/variable.csv`, safely

Mirrors
[`register_measurement_types()`](https://calcofi.io/calcofi4db/reference/register_measurement_types.md):
append-only (edit the CSV by hand to change an existing row), refuses a
duplicate `variable` key, writes with `na = ""`.

## Usage

``` r
register_variables(new_vars, path, measurement_type_path = NULL, quiet = FALSE)
```

## Arguments

- new_vars:

  data.frame with a `variable` column and any of `label`, `description`,
  `units`, `nerc_p01`, `category`, `is_unified`. Columns absent from the
  registry are dropped with a warning.

- path:

  path to `metadata/variable.csv`

- measurement_type_path:

  path to `metadata/measurement_type.csv`, used only to check `nerc_p01`
  agreement against member series (rows whose `variable` equals the new
  row's `variable`). `NULL` skips the check (not recommended — nothing
  else catches this).

- quiet:

  suppress the "added N variable(s)" message

## Value

The full updated registry.

## Details

It additionally refuses a row whose `nerc_p01` is set but disagrees with
a member series' own `nerc_p01` in `measurement_type.csv` — the whole
point of the crosswalk key is that agreement can be asserted once, and a
silent mismatch would ship the wrong concept URI to an OBIS/DwC export
for every series sharing the key but one. It does **not** refuse a key
whose member series disagree with each OTHER (two series under the same
`variable` that carry two different NERC concepts) — that is a real fact
about the data (`sigma_theta`: the bottle's own computation is
`SIGTEQ01`, the CTD's is `SIGTPR01` — different P01 concepts, found
WS-M1 2026-09-10), and the correct way to record it is to leave the
row's own `nerc_p01` empty, exactly as an id is left empty anywhere else
in these registries when no single concept says exactly what a row is
(`metadata-registries` skill). Passing a non-empty `nerc_p01` for such a
key is refused, because it would silently pick a side.

## See also

[`check_variable_registry()`](https://calcofi.io/calcofi4db/reference/check_variable_registry.md),
the standing invariant this enforces at write time.

## Examples

``` r
if (FALSE) { # \dontrun{
register_variables(
  data.frame(variable = "temperature", label = "Temperature",
             units = "degC",
             nerc_p01 = "http://vocab.nerc.ac.uk/collection/P01/current/TEMPPR01/",
             category = "Physical Oceanography", is_unified = TRUE),
  here::here("metadata/variable.csv"),
  here::here("metadata/measurement_type.csv"))
} # }
```
