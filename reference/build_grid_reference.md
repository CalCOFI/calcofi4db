# Build the shared `grid` reference table (deterministic, dataset-independent)

Materializes the CalCOFI station grid from `calcofi4r::cc_grid` +
`calcofi4r::cc_grid_ctrs` — the exact build previously embedded in
`ingest_swfsc_ichthyo.qmd` (`mk_grid_v2` + `grid_to_db`). Because it is
a pure deterministic function of the bundled `cc_grid`/`cc_grid_ctrs`,
`grid_key` values are byte-identical wherever it runs, so promoting the
build out of the ichthyo ingest into a shared reference is
non-destructive. Requires the DuckDB connection to allow native GEOMETRY
(open via
[`get_duckdb_con()`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md),
which sets `storage_compatibility_version = 'latest'`).

## Usage

``` r
build_grid_reference(con, grid_tbl = "grid")
```

## Arguments

- con:

  a DuckDB connection

- grid_tbl:

  target table name (default `"grid"`)

## Value

(invisibly) the row count of the created `grid` table
