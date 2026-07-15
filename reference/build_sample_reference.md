# Build the shared `sample` event dimension from the per-dataset event tables

Materializes the adjacency-list `sample` dimension (one row per physical
sampling event, at its native grain) from whichever per-dataset event
tables are present in `con` — subsuming
`site`/`tow`/`net`/`casts`/`ctd_cast`/`dic_sample`/
`cufes_sample`/`*_tow`/`*_sample`/`bird_mammal_transect`/`phyto_sample`
into one table. Every `sample_key` is namespaced
`"<dataset_key>:<sample_type>:<id>"`;
`parent_sample_key`/`root_sample_key` encode the `site->tow->net` and
`cast->bottle` hierarchies (a flat adjacency list with no attribute
inheritance). `geom` is minted from `latitude`/`longitude`. Only arms
whose source tables exist are included. Errors if the resulting
`sample_key` is not unique.

## Usage

``` r
build_sample_reference(con, sample_tbl = "sample")
```

## Arguments

- con:

  a DuckDB connection with the per-dataset event tables loaded

- sample_tbl:

  target table name (default `"sample"`)

## Value

(invisibly) the row count of the built `sample` table
