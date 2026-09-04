# The columns [`declare_measurement_fields()`](https://calcofi.io/calcofi4db/reference/declare_measurement_fields.md) is allowed to touch

`category` / `variable` were the original two (the Explorer's *Browse*
tab); `derivation` / `is_canonical` were added in calcofi4db 3.29.0 so
the bottle `r_*` pre-QC types could record "interpolated to standard
depth, not an input for further interpolation" and flip `is_canonical`
to FALSE without a bare `write_csv()` (WS-G, 2026-09-03). Both new
columns are treated as character — `is_canonical` is stored in the CSV
as the literal string `"TRUE"`/`"FALSE"`, matching how
[`read_measurement_type()`](https://calcofi.io/calcofi4db/reference/read_measurement_type.md)
reads it, so no type coercion happens here that the registry's own round
trip does not already do.

## Usage

``` r
declarable_measurement_fields()
```

## Value

Character vector of the allowed field names.

## Details

`nerc_p01` / `units_nerc_p06` followed in 3.32.0 (WS-H2, pre-release
decision D-S2): the controlled-vocabulary ids a portal export needs —
OBIS/DwC eMoF's `measurementTypeID` (NERC BODC Parameter Usage
Vocabulary P01) and `measurementUnitID` (NERC P06). Both hold the **full
concept URI**, and both are filled only on an exact vocabulary match, so
an empty cell means "no concept states exactly what this type is", never
"not looked at".
