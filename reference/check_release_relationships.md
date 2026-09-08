# Measure every declared primary and foreign key on the frozen tables

The release has always *declared* its keys (`relationships.json`, from
[`core_relationships()`](https://calcofi.io/calcofi4db/reference/core_relationships.md)
and `metadata/relationships_cross.csv`) and gated a few of them —
[`check_core_pk_unique()`](https://calcofi.io/calcofi4db/reference/check_core_pk_unique.md)
on the core primary keys,
[`check_cruise_key_integrity()`](https://calcofi.io/calcofi4db/reference/check_cruise_key_integrity.md)
on the `cruise_key` edges — while the other ~50 declared foreign keys
were true by construction and never measured. This walks the whole
declaration on the assembled tables and writes the result as the
`integrity.json` sidecar beside `catalog.json`, so a consumer (the
schema browser, the docs) can show "unique, measured" and "0 orphans"
per key instead of trusting the diagram.

## Usage

``` r
check_release_relationships(
  con,
  rels,
  path = NULL,
  version = NULL,
  halt = TRUE
)
```

## Arguments

- con:

  DBI connection holding the frozen tables.

- rels:

  Path to a `relationships.json`, or the list it parses to
  (`primary_keys`, `foreign_keys`).

- path:

  Optional path to write `integrity.json` to.

- version:

  Release version stamped into the sidecar (no wall clock: the file is
  deterministic for unchanged inputs, like the release stamp).

- halt:

  [`stop()`](https://rdrr.io/r/base/stop.html) on any duplicate or
  `NULL` primary key, or any foreign-key orphan (default `TRUE`). A
  skipped key never halts.

## Value

Invisibly, a list: `primary_keys` and `foreign_keys` data frames, `ok`,
and `n_skipped`.

## Details

For each primary key: rows, distinct key values, duplicates, rows with a
`NULL` in any key column. For each foreign key: rows, rows whose key is
`NULL` (permitted — a nullable edge is one where the source has nothing
to point at, e.g. `obs.taxon_key` on an env row), and **orphans**:
non-`NULL` values with no match in the referenced column. A key whose
table or column is not in `con` is reported as `skipped`, never silently
dropped.
