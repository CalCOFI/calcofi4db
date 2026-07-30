# Stage the `_measurement_taxon` crosswalk in a connection

Materializes `metadata/measurement_taxon.csv` as the
`_measurement_taxon` table an `obs`/`obs_attribute` projection INNER
JOINs to split a taxon-bearing measurement_type name (`sardine_eggs`,
`phyllosoma_stage_3`) into
`(taxon_key, canonical type, life_stage, bin_value)`.

## Usage

``` r
ensure_measurement_taxon(
  con,
  measurement_taxon = NULL,
  dataset_key = NULL,
  tbl = "_measurement_taxon"
)
```

## Arguments

- con:

  a DuckDB connection

- measurement_taxon:

  the crosswalk data.frame (or NULL for an empty table)

- dataset_key:

  restrict to this dataset, so an ingest never stages another dataset's
  rows

- tbl:

  target table name

## Value

(invisibly) `tbl`

## Details

Exported because a dataset's projection lives in its own ingest
notebook, and the derived `taxon_key` is the part you must not
hand-roll: it is
[`taxon_key_of()`](https://calcofi.io/calcofi4db/reference/taxon_key_of.md)
over `worms_id`/`itis_id`, so a `'worms:' || worms_id` string built
inline silently mis-keys any ITIS-resolved taxon.
