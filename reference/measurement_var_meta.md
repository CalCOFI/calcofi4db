# CF variable metadata from the `measurement_type` registry

Turns the canonical measurement registry into the per-variable lookup
the netCDF writers use: a variable needs one `units`, one `long_name`
and optionally one `standard_name`. This is the widening half of
publishing — the database stores every quantity in a single
`measurement_value` column, so the units live in the registry rather
than on the value.

## Usage

``` r
measurement_var_meta(mt)
```

## Arguments

- mt:

  data.frame from
  [`read_measurement_type()`](https://calcofi.io/calcofi4db/reference/read_measurement_type.md).
  Only `measurement_type` is required; `units`, `description`,
  `standard_name`, `is_canonical`, `valid_min` and `valid_max` are used
  when present.

## Value

Named list keyed by `measurement_type`, each element
`list(units, long_name, standard_name, canonical, valid_min, valid_max)`.
An empty registry cell becomes `""` for `units` (never the string
`"NA"`) and falls back to the type name for `long_name`, because a CF
variable with `long_name = "NA"` is worse than one with no `long_name`.

## Examples

``` r
mt <- data.frame(measurement_type = "temperature_ave", units = "degree_C",
                 description = "average temperature", stringsAsFactors = FALSE)
measurement_var_meta(mt)$temperature_ave$units
#> [1] "degree_C"
```
