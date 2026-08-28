# Exact polygon membership of every root sample, one layer at a time

`ST_Intersects` between the root samples' points and each layer's
polygons, chunked per layer so the join never holds more than one layer
in memory (the spatial join that OOM-ed the 16 GB server when an app ran
it over every layer at once). CRS tags are stripped on both sides
through WKB (`ST_Point` tags `OGC:CRS84`, `ST_Read` tags `EPSG:4326`,
and DuckDB refuses to intersect across them). Only polygon geometries
take part: the maritime-limit layers are boundary *lines* and the ports
are points, and a point never intersects either — a layer with no
polygons is skipped and reported with `n_polys = 0`. Asserts per layer
that no `(root_id, spatial_key)` pair repeats.

## Usage

``` r
build_sample_spatial(con, layers = NULL, tbl = "sample_spatial")
```

## Arguments

- con:

  DuckDB connection with the spatial extension, `sample_root` and
  `spatial`.

- layers:

  layers to compute (default: every layer in `spatial`).

- tbl:

  output table.

## Value

A tibble with one row per layer: `layer`, `n_polys`, `n_roots`,
`n_memberships`.
