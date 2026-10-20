# Declare that a dataset also publishes measurement types another dataset defined

The registry-file counterpart of
[`merge_source_datasets()`](https://calcofi.io/calcofi4db/reference/merge_source_datasets.md):
reads `measurement_type.csv`, adds `dataset_key` to `_source_datasets`
on the named types (keeping every dataset already listed) and writes
with `na = ""`. Nothing else on the row changes — the description,
units, derivation and bounds stay the defining ingest's. Like
[`declare_measurement_bounds()`](https://calcofi.io/calcofi4db/reference/declare_measurement_bounds.md)
it refuses an unknown `measurement_type` (register a new type with
[`register_measurement_types()`](https://calcofi.io/calcofi4db/reference/register_measurement_types.md)
first) and writes nothing when every type already lists the dataset.

## Usage

``` r
add_measurement_source_dataset(types, dataset_key, path, quiet = FALSE)
```

## Arguments

- types:

  character: measurement types the dataset publishes.

- dataset_key:

  the publishing dataset (`provider_dataset`).

- path:

  path to `metadata/measurement_type.csv`.

- quiet:

  suppress the summary message.

## Value

The full registry (invisibly if nothing changed).

## Examples

``` r
if (FALSE) { # \dontrun{
add_measurement_source_dataset("spiciness0", "calcofi_bottle-derived",
                               here::here("metadata/measurement_type.csv"))
} # }
```
