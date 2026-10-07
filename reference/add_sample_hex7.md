# Stamp `hex7` on `sample` (or any table with a position)

Rebuilds `tbl` with a trailing `hex7` column: the resolution-7 H3 cell
(`UBIGINT`) that is the **parent of the resolution-10 cell** of the
row's `latitude` / `longitude` — the same two SQL fragments that give an
observation its `hex_id`
([`append_obs()`](https://calcofi.io/calcofi4db/reference/append_obs.md))
and `obs_bio` / `obs_env` their `hex7`
([`build_obs_slim()`](https://calcofi.io/calcofi4db/reference/build_obs_slim.md)),
so a sampling event and an observation at the same position are in the
same hexagon by construction. It is deliberately **not** the
resolution-7 cell the position falls in: H3 cells do not nest exactly,
and the two disagree near every cell edge.

## Usage

``` r
add_sample_hex7(
  con,
  tbl = "sample",
  lat = "latitude",
  lng = "longitude",
  res_max = CC_H3_RES_MAX
)
```

## Arguments

- con:

  DuckDB connection holding `tbl`; the community `h3` extension is
  loaded.

- tbl:

  table (or view) to stamp, with `lat` and `lng` columns.

- lat, lng:

  names of the position columns.

- res_max:

  the resolution of the cell `hex7` is the parent of (the resolution of
  `obs.hex_id`); change it only together with
  [`append_obs()`](https://calcofi.io/calcofi4db/reference/append_obs.md)'s.

## Value

Invisibly, the number of rows carrying a `hex7`.

## Details

`hex7` is `NULL` where the row has no finite position — either
coordinate `NULL`, `NaN` or infinite. The coordinates themselves are
left as they are: this function stamps, it does not clean.

The table is recreated by `SELECT` rather than `UPDATE`d or `ALTER`ed,
because DuckDB cannot update a table carrying a CRS-tagged `GEOMETRY`
column (the `geom` on `sample`); every other column keeps its name,
position and type, and the row count is asserted unchanged. A `hex7`
already on the table is recomputed, never trusted. A view becomes a
table.

Why the release needs it: a per-sample value in `sample_measurement` (a
cast's mixed-layer depth) has no observation row to borrow a cell from,
so without `hex7` on its sample it cannot be drawn in a hexagon lens.
[`build_sample_root()`](https://calcofi.io/calcofi4db/reference/build_sample_root.md)
carries the column onto `sample_root`, and
[`check_sample_hex7()`](https://calcofi.io/calcofi4db/reference/check_sample_hex7.md)
is the gate.

## See also

[`h3_parent_sql()`](https://calcofi.io/calcofi4db/reference/h3_parent_sql.md)
for the coarser parents of a `hex7`.
