# Add a dataset to a measurement type's `_source_datasets`, keeping the others

`_source_datasets` lists every dataset that publishes rows of a type,
separated by `;` (`"calcofi_bottle;calcofi_2022-edna"`). A type two
ingests share (`spiciness0`: `calcofi_ctd-derived` per CTD bin,
`calcofi_bottle-derived` per bottle) must not be narrowed to one of them
when the other re-renders and writes its literal, so each ingest merges
its own key into what the registry already holds. The existing order is
kept, the new key goes last, a key is never listed twice, and an empty
or `NA` cell becomes the key alone.

## Usage

``` r
merge_source_datasets(current, dataset_key)
```

## Arguments

- current:

  character: the registry's `_source_datasets` values (`NA` / `""`
  allowed).

- dataset_key:

  the dataset key to add (one string).

## Value

character, the same length as `current`.

## Examples

``` r
merge_source_datasets(c(NA, "calcofi_ctd-derived", "a;calcofi_ctd-derived"),
                      "calcofi_ctd-derived")
#> [1] "calcofi_ctd-derived"   "calcofi_ctd-derived"   "a;calcofi_ctd-derived"
# "calcofi_ctd-derived" "calcofi_ctd-derived" "a;calcofi_ctd-derived"
```
