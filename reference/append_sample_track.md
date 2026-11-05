# Append path rows into the core `sample_track` table

`sample_track` holds one row per sample whose event is a path on the sea
surface rather than a point: where it began and ended in x, y and t, its
middle, its length and a `LINESTRING` from beginning to end. The sample
keeps its single point in `sample` (the caller decides which: a
segment's midpoint, a sighting's animal position);
`sample_track.sample_key` is a foreign key to it and the table's primary
key, so a sample has at most one path.

## Usage

``` r
append_sample_track(con, select_sql, track_tbl = "sample_track")
```

## Arguments

- con:

  a DuckDB connection (open via
  [`get_duckdb_con()`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md))

- select_sql:

  a SELECT producing the canonical `obs` columns by name

- track_tbl:

  target table (default `"sample_track"`)

## Value

(invisibly) the total row count of `track_tbl` after the append

## Details

`select_sql` is bound **positionally** and must yield 10 columns:
`sample_key`, `dataset_key`, `track_type`, `datetime_beg`,
`datetime_end`, `longitude_beg`, `latitude_beg`, `longitude_end`,
`latitude_end`, `length_km`. The middle is computed here, never
supplied: `datetime_mid` is halfway in time and `longitude_mid` /
`latitude_mid` the great-circle midpoint. A NULL `length_km` is filled
with the great-circle distance; a supplied one (the provider's own) is
kept. `track_type` names what the path is (`effort_segment`,
`sight_line`, …). Times may be NULL (a sight line is instantaneous: pass
the same time for both ends).

Non-finite coordinates (`NaN`, `Inf`) become NULL, as in
[`append_sample()`](https://calcofi.io/calcofi4db/reference/append_sample.md),
and a row with any NULL end coordinate gets no `geom`. The line is
minted with `ST_SetCRS(…, 'EPSG:4326')` as
[`append_sample()`](https://calcofi.io/calcofi4db/reference/append_sample.md)
mints its point; the release tags every geometry column `EPSG:4326`.
