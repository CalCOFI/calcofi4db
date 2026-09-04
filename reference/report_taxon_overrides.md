# What each `taxon_override.csv` row matched, applied to and skipped

Recomputes the override rule (see
[`resolve_dataset_taxon()`](https://calcofi.io/calcofi4db/reference/resolve_dataset_taxon.md))
from `dataset_taxon` alone — the dataset's vocabulary, its
`ds_source_json` (what the source supplied) and the registry — so the
same report is available in an ingest after the resolve and in
`release_database.qmd`, where every dataset is present and only
`dataset_taxon` is. One row per override row:

## Usage

``` r
report_taxon_overrides(con, overrides, tbl = "dataset_taxon", verbose = TRUE)
```

## Arguments

- con:

  a DBI connection holding `dataset_taxon`

- overrides:

  the override registry (`metadata/taxon_override.csv`)

- tbl:

  crosswalk table name (default `"dataset_taxon"`)

- verbose:

  logical; message the per-dataset summary

## Value

a data.frame (`dataset_key`, `match_column`, `match_value`,
`override_key`, `n_matched`, `n_applied`, `n_skipped`, `skipped_codes`,
`source_json_known`), one row per override row, invisibly when
`verbose = FALSE`

## Details

- `n_matched` — vocabulary rows whose `match_column` equals
  `match_value`;

- `n_applied` — of those, the rows the override keyed (a code match, or
  a row whose source supplied no id);

- `n_skipped` — of those, the rows that kept the source's own id, with
  the first of their codes in `skipped_codes`. `NA` when
  `source_json_known` is `FALSE`: no row of that dataset carries
  `ds_source_json`, so the shard either predates calcofi4db 3.29.0 or
  its source supplied no ids at all, and the release cannot tell which —
  the ingest's own message can.

`match_column` is one of `dataset_taxon`'s `ds_taxa_code` /
`ds_scientific_name` / `ds_common_name`; anything else errors, as it
does at ingest (the transitional aliases for the arms' own column names
went with the arms in 4.0.0). A row that matches nothing is reported
with zeros, not dropped —
[`check_taxon_registries()`](https://calcofi.io/calcofi4db/reference/check_taxon_registries.md)
is where a dataset nobody supplies fails.
