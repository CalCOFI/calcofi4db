# Project one dataset into the consolidated core tables

The per-ingest entry point, and the authoritative projection: after an
ingest has built its per-dataset tables, `emit_core_tables()` turns them
into that dataset's slice of the shared core family — `sample` (via
[`build_sample_reference()`](https://calcofi.io/calcofi4db/reference/build_sample_reference.md),
which auto-detects the dataset's event tables present in `con`), its
`obs` occurrence headline, `obs_attribute` sub-occurrence detail,
`sample_measurement` event-level effort, and its slice of the taxa
references (`taxon` / `dataset_taxon` / `taxon_group`). These shards ARE
the ingest's parquet output; `release_database.qmd` concatenates them
rather than re-deriving the core from per-dataset tables (which is how
the two projections drifted apart). `pic_zooplankton` (no measurements)
contributes `sample` only.

## Usage

``` r
emit_core_tables(
  con,
  dataset_key,
  sample = TRUE,
  measurement_taxon = NULL,
  overrides = NULL,
  taxa = TRUE
)
```

## Arguments

- con:

  a DuckDB connection holding this dataset's per-dataset tables

- dataset_key:

  provider_dataset (e.g. `"swfsc_ichthyo"`, `"calcofi_bottle"`)

- sample:

  logical; also (re)build `sample` from the present event tables
  (default TRUE)

- measurement_taxon:

  optional data.frame of the composite-type crosswalk
  (`metadata/measurement_taxon.csv`); required for `swfsc_cufes` /
  `calcofi_phyllosoma`, ignored by every other dataset

- overrides:

  optional data.frame of manual id resolution
  (`metadata/taxon_override.csv`) for coarse taxa (phyto groups,
  mammals)

- taxa:

  logical; also build this dataset's `taxon` / `dataset_taxon` /
  `taxon_group` slices (default TRUE). Set FALSE to project against taxa
  references already present in `con`.

## Value

(invisibly) a named list of row counts for the core tables written

## Details

`taxon_key` is resolved here, at ingest time. Datasets whose taxon lives
in a vocabulary table (ichthyo `species`, `zoodb_taxon`,
`bird_mammal_species`, …) resolve through `dataset_taxon`; datasets that
bake the taxon into the measurement type name (cufes `sardine_eggs`,
phyllosoma `phyllosoma_stage_3`) resolve through `measurement_taxon` —
pass `metadata/measurement_taxon.csv` and `metadata/taxon_override.csv`,
or those arms project zero rows.

## Examples

``` r
if (FALSE) { # \dontrun{
mt <- readr::read_csv(here::here("metadata/measurement_taxon.csv"))
ov <- readr::read_csv(here::here("metadata/taxon_override.csv"))
core <- emit_core_tables(con, "swfsc_cufes", measurement_taxon = mt, overrides = ov)
} # }
```
