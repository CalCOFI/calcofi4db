# Scan a metadata sidecar for missing documentation

Empty table/column descriptions and missing units are invisible until a
consumer hits them: they travel from an ingest's `metadata.json` into
the release sidecar and out through calcofi4r `cc_describe_table()` /
`cc_db_catalog()`, where they render as blank documentation.
[`build_metadata_json()`](https://calcofi.io/calcofi4db/reference/build_metadata_json.md)
calls this on every write.

## Usage

``` r
scan_metadata_gaps(
  metadata,
  unit_exempt =
    paste0("(_key|_id|_uuid|_seq|_md|_url|_qual|_flag|_code|_name|_type|_status)$",
    "|^(latitude|longitude|geom|datetime|date|time|notes|comments|description)$",
    "|^(measurement_value|measurement_prec|bin_value|condition_value)$",
    "|^_|^(realm|grain|units|is_canonical|life_stage|stage|order_occ|rank",
    "|rank_order|count|n|sample_type|taxonomic_status|provider|dataset)$")
)
```

## Arguments

- metadata:

  Either the in-memory metadata list, or a path to a `metadata.json`
  file.

- unit_exempt:

  Regular expression matched against the **bare** column name (table
  prefix stripped) for columns where a missing `units` is expected
  rather than a gap — keys, identifiers, names, flags, timestamps, free
  text and the long-format value columns are not unit-bearing
  measurements.

## Value

An object of class `cc_metadata_gaps`: a list with `n_tables`,
`n_columns`, and the character vectors `tables_no_desc`,
`columns_no_desc` and `columns_no_units`. Has a
[`print()`](https://rdrr.io/r/base/print.html) method, so a notebook
chunk can just call it.

## Details

`units` is only meaningful for a measured quantity, so reporting every
`*_key`, name and timestamp as unit-less would bury the real gaps in
noise — which is why `unit_exempt` exists rather than a flat count. The
exemption is about *reporting*: nothing here changes what is written.

## Examples

``` r
md <- list(
  tables  = list(obs = list(description_md = "")),
  columns = list(`obs.temperature` = list(description_md = "temp", units = NULL),
                 `obs.sample_key`  = list(description_md = "",     units = NULL)))
scan_metadata_gaps(md)
#> metadata.json documentation gaps (1 tables, 2 columns) — these render blank in cc_describe_table() / cc_db_catalog():
#> tables with no description_md: 1    obs
#> columns with no description_md: 1    obs.sample_key
#> measurement columns with no units: 1    obs.temperature
#>   backfill via metadata/{provider}/{dataset}/flds_redefine.csv, then re-run
```
