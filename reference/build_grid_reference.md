# Build the shared `grid` reference table (deterministic, dataset-independent)

Materializes the CalCOFI station grid from
[`calcofi4r::cc_grid`](https://calcofi.io/calcofi4r/reference/cc_grid.html) +
[`calcofi4r::cc_grid_ctrs`](https://calcofi.io/calcofi4r/reference/cc_grid_ctrs.html)
— the exact build embedded in `ingest_swfsc_ichthyo.qmd` (`mk_grid_v2` +
`grid_to_db`), which is what writes the released `grid`. Because it is a
pure deterministic function of the bundled `cc_grid`/`cc_grid_ctrs`,
`grid_key` values are byte-identical wherever it runs. Requires the
DuckDB connection to allow native GEOMETRY (open via
[`get_duckdb_con()`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md),
which sets `storage_compatibility_version = 'latest'`).

## Usage

``` r
build_grid_reference(
  con,
  grid_tbl = "grid",
  cc_grid = calcofi4r::cc_grid,
  cc_grid_ctrs = calcofi4r::cc_grid_ctrs
)
```

## Arguments

- con:

  a DuckDB connection

- grid_tbl:

  target table name (default `"grid"`)

- cc_grid, cc_grid_ctrs:

  the cells and their sites (defaults: the datasets bundled in
  calcofi4r); arguments so a test or a variant can pass its own

## Value

(invisibly) the row count of the created `grid` table

## Details

The grid is whatever `cc_grid` is: from calcofi4r 1.25.0, one cell per
official station (a Voronoi tessellation of the official station
positions, confined to the previous cells it replaces) beside the
previous cells beyond the official pattern, kept as they were
(CalCOFI/workflows#130); before it, the idealized lattice
(`cc_grid_v1`). The columns are the same either way: `grid_key`
(`st{station}-ln{line}`, `_hist` for the historical pattern), `station`,
`line`, `shore`, `pattern`, `spacing`, `zone`, `area_km2`, `geom` (a
polygon, or a multipolygon for a cell in several pieces) and `geom_ctr`
(the cell's site: the station itself in the rebuilt grid, the centroid
of the cell's largest polygon before). Where `cc_grid` carries its own
`grid_key`, the key derived here must equal it, and the build stops
otherwise. The two grids share key names whose polygons differ;
[`build_grid_crosswalk()`](https://calcofi.io/calcofi4db/reference/build_grid_crosswalk.md)
maps one to the other.
