# Build the `taxon_group` grouping table (many taxa per group) from the registry

Groups come from `metadata/taxon_group.csv` (taxon plan D4), not from
code:

## Usage

``` r
build_taxon_group(con, rules = NULL, tbl = "taxon_group", ...)
```

## Arguments

- con:

  a DuckDB connection holding `taxon` and `dataset_taxon`

- rules:

  the registry as read by
  [`read_taxon_group_rules()`](https://calcofi.io/calcofi4db/reference/read_taxon_group_rules.md),
  or a path to it. `NULL` looks for `metadata/taxon_group.csv` under
  [`here::here()`](https://here.r-lib.org/reference/here.html).

- tbl:

  target table name (default `"taxon_group"`)

- ...:

  the pre-3.29 `measurement_taxon` / `overrides` arguments, accepted
  with a deprecation warning and ignored (the groups are not derived
  from them any more)

## Value

(invisibly) the row count written

## Details

- **`class`** — every vocabulary taxon whose released `class` equals
  `rule_value`: `calcofi:seabirds` = Aves, `calcofi:marine_mammals` =
  Mammalia. Cross-dataset by construction; no dataset column. Scoped to
  taxa some dataset actually observes (present in `dataset_taxon`),
  never to a bare lineage ancestor — a group selects observed taxa, and
  the ancestors are reachable through `parent_taxon_key` anyway.

- **`dataset_taxon`** — by `(dataset_key, match_column, match_value)`
  against `dataset_taxon`, the same matcher `taxon_override.csv` uses:
  the phytoplankton functional groups on `ds_common_name`.

A rule naming a `match_column` the vocabulary lacks errors; a rule for a
dataset absent from this connection is skipped (it is another ingest's),
and
[`check_taxon_registries()`](https://calcofi.io/calcofi4db/reference/check_taxon_registries.md)
catches one nobody supplies at the release. Needs `taxon` and
`dataset_taxon` in `con`, i.e. call it after
[`build_taxon_reference()`](https://calcofi.io/calcofi4db/reference/build_taxon_reference.md)
and
[`resolve_dataset_taxon()`](https://calcofi.io/calcofi4db/reference/resolve_dataset_taxon.md).
