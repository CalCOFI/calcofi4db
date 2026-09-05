# Read the three registries a Darwin Core Archive needs

A convenience over
[`read_gear_registry()`](https://calcofi.io/calcofi4db/reference/read_gear_registry.md),
[`read_life_stage_registry()`](https://calcofi.io/calcofi4db/reference/read_life_stage_registry.md)
and
[`read_measurement_type()`](https://calcofi.io/calcofi4db/reference/read_measurement_type.md)
so a notebook names the metadata directory once.

## Usage

``` r
dwc_registries(dir)
```

## Arguments

- dir:

  the `metadata/` directory

## Value

A named list: `gear`, `life_stage`, `measurement_type`.
