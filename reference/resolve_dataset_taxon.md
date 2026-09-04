# Fill `taxon_key` on the `dataset_taxon` crosswalk (per-dataset vocabulary -\> `taxon`)

One row per (dataset, local taxon): the dataset's own `ds_taxon_key`
(`"<dataset-or-known-list>:<local id>"`, all lowercase — e.g.
`calcofi:19` for the shared CalCOFI species list, `cce-lter_zoodb:3`
otherwise), its `ds_scientific_name` / `ds_common_name` /
`ds_taxa_code`, the source's own claims as `ds_source_json`, and the
global `taxon_key` it resolves to. Deduped on `ds_taxon_key`.

## Usage

``` r
resolve_dataset_taxon(
  con,
  measurement_taxon = NULL,
  overrides = NULL,
  tbl = "dataset_taxon",
  verbose = TRUE
)

build_dataset_taxon(
  con,
  measurement_taxon = NULL,
  overrides = NULL,
  tbl = "dataset_taxon"
)
```

## Arguments

- con:

  a DuckDB connection with the staged vocabulary loaded

- measurement_taxon:

  optional data.frame of the composite-type crosswalk
  (`metadata/measurement_taxon.csv`) so cufes/phyllosoma/crab taxa,
  which live in `measurement_type` names not a taxon table, are included

- overrides:

  optional data.frame of manual id resolution
  (`metadata/taxon_override.csv`) for coarse taxa (phyto groups,
  mammals)

- tbl:

  target table name (default `"dataset_taxon"`)

- verbose:

  logical; message what the overrides applied to and skipped

## Value

(invisibly) the row count written

## Details

Rows staged by
[`append_dataset_taxon()`](https://calcofi.io/calcofi4db/reference/append_dataset_taxon.md)
are **filled in place**: every column but `taxon_key` comes back
byte-identical, so a re-run over unchanged inputs is a no-op. Since
4.0.0 a dataset that has **not** staged is an error naming the working
table the notebook left behind — the seven per-dataset arms are gone,
and the composite-measurement crosswalk (`measurement_taxon`) is the
only other source. The key is minted by
[`taxon_key_of()`](https://calcofi.io/calcofi4db/reference/taxon_key_of.md)
from the resolved ids and the class the staged lineage supplies, so call
[`ensure_taxon_xref()`](https://calcofi.io/calcofi4db/reference/ensure_taxon_xref.md)
then
[`ensure_taxon_lineage()`](https://calcofi.io/calcofi4db/reference/ensure_taxon_lineage.md)
first.

**The override rule** (Ben, 2026-09-04): a `taxon_override.csv` row
**never replaces an id the source supplied, unless it names the row by
the dataset's own code**. A row matched on a non-code column
(`ds_common_name`, `ds_scientific_name`) applies only to vocabulary rows
whose source supplied no `worms_id` / `itis_id` (nothing in
`ds_source_json`); a row matched on `ds_taxa_code` applies always, and
wins over a non-code row on the same vocabulary row whatever order the
registry lists them in. The rows an override was *skipped* for are
counted per registry row and staged as `_taxon_override_report` (see
[`report_taxon_overrides()`](https://calcofi.io/calcofi4db/reference/report_taxon_overrides.md)),
and summarised in a message, so the notebook shows what the rule kept.
v2026.08.25 released 22 phytoplankton keys for 393 codes because six
`taxa`-matched functional-group rows replaced 287 species AphiaIDs the
source had resolved.

Renamed from `build_dataset_taxon()` in 3.29.0, which remains as a
deprecated alias: that name described a rebuild from the arms, which is
exactly what an ingest could not stage against.
