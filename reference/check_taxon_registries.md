# Every dataset a taxon registry names must be one some dataset supplies

`metadata/taxon_override.csv` and `metadata/taxon_group.csv` are read
whole by every ingest while each loads only its own vocabulary, so a row
for another dataset is normal there and cannot be validated. This is the
check for the place where every dataset IS present — the release, after
[`assemble_core()`](https://calcofi.io/calcofi4db/reference/assemble_core.md)
— replacing the hard-coded list of dataset names the package used to
validate against (taxon plan D5). The allowed set is the `dataset_key`s
present in `dataset_taxon` ∪ `measurement_taxon`; a row naming anything
else (a typo, a retired dataset) errors, because a registry row that
matches nothing is how a missing id hides.

## Usage

``` r
check_taxon_registries(
  con,
  overrides = NULL,
  group_rules = NULL,
  measurement_taxon = NULL,
  exclude = character(0),
  halt = TRUE
)
```

## Arguments

- con:

  a DBI connection holding the assembled `dataset_taxon`

- overrides:

  the override registry (`metadata/taxon_override.csv`), or NULL

- group_rules:

  the group registry
  ([`read_taxon_group_rules()`](https://calcofi.io/calcofi4db/reference/read_taxon_group_rules.md)),
  or NULL

- measurement_taxon:

  the composite crosswalk (`metadata/measurement_taxon.csv`), whose
  `dataset_key`s count as supplied, or NULL

- exclude:

  character vector of `dataset_key`s staged but held out of this release
  (`calcofi.in_release: false`), whose registry rows are therefore
  expected to match nothing in `con`. Pass
  [`release_excluded_datasets()`](https://calcofi.io/calcofi4db/reference/release_excluded_datasets.md),
  which returns the same `{provider}_{dataset}` labels a `dataset_key`
  uses. A held-out dataset's rows are skipped, not validated; a typo
  outside this list still fails. Default `character(0)`.

- halt:

  logical; [`stop()`](https://rdrr.io/r/base/stop.html) on an orphan
  (default `TRUE`)

## Value

(invisibly) a named list of the orphan `dataset_key`s per registry
