# Check `metadata/variable.csv` against `metadata/measurement_type.csv`

Three standing invariants a measurement page (D6) needs to hold, checked
together so a single run reports every violation rather than stopping at
the first:

## Usage

``` r
check_variable_registry(variable_csv, measurement_type_csv)
```

## Arguments

- variable_csv:

  path to `metadata/variable.csv`

- measurement_type_csv:

  path to `metadata/measurement_type.csv`

## Value

`TRUE`, invisibly, on success.

## Details

- every non-`NA` `measurement_type.variable` value has **exactly one**
  `variable.csv` row — no crosswalk key is orphaned

- every `variable.csv` row has **\>= 1 member series** in
  `measurement_type.csv` — no unused key ships

- a row's `nerc_p01`, when set, equals **every** member series' own
  `nerc_p01` (an empty row value is always allowed — see
  [`register_variables()`](https://calcofi.io/calcofi4db/reference/register_variables.md)
  for why a genuine member disagreement is recorded that way, not as an
  error)

- a key's member series carry the **same units**, or a known-equivalent
  spelling (`PSS-78` / `PSU` — the same practical salinity scale under
  two names; the CalCOFI bottle and the CTD's own processing software
  write it differently)

## See also

[`register_variables()`](https://calcofi.io/calcofi4db/reference/register_variables.md),
which enforces the `nerc_p01` half of this at write time.
