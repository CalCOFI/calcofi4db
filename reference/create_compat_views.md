# Recreate per-dataset tables as VIEWs over the consolidated core

Once an ingest publishes the core, the per-dataset event and measurement
tables it used to publish can be served as VIEWs instead of stored
bytes: the source id is recovered from the namespaced `sample_key`, the
containment FK from `parent_sample_key`, event-level effort by pivoting
`sample_measurement` back out of long form, and the measurement triples
straight from `obs`.

## Usage

``` r
create_compat_views(con, dataset_key, replace = TRUE, sample_tbl = "sample")
```

## Arguments

- con:

  a DuckDB connection holding the core tables

- dataset_key:

  provider_dataset to rebuild views for

- replace:

  logical; drop an existing table/view of the same name first (default
  TRUE — the ingest still has the real tables in scope)

- sample_tbl:

  name of the core `sample` table to read. Override when a downstream
  ingest loads ANOTHER dataset's shard as a reference — e.g. dic loads
  bottle's `sample` as `_bottle_sample` so rebuilding `casts`/`bottle`
  does not collide with the `sample` dic builds for itself.

## Value

(invisibly) character vector of view names created

## Details

**This is exact for the columns the core models and lossy for the
rest.** Verified against the shipped data, `net` and `tow` round-trip
identically (76,512 / 75,506 rows, every value equal). What does NOT
come back is the columns the consolidated model never carried —
`net.side`, `tow.tow_number`,
`site.order_occ`/`line`/`station`/`site_key`, most of the 33 legacy
`casts` columns (`rpt_line`, `ac_sta`, `int_chl`, …),
`bottle.btl_num`/`depth_qual`, and the CTD scan-grain columns
(`ctd_cast_uuid`, `cast_dir`, `data_stage`), since `sample` holds one
row per physical cast. Those are dropped from the release by `core_keep`
regardless, so the VIEW is no thinner than what consumers already get —
but do not treat it as a lossless archive of the source. Use
[`core_output_tables()`](https://calcofi.io/calcofi4db/reference/core_output_tables.md)
to publish; use this to keep in-notebook consumers and ad-hoc queries
working against the old names.
