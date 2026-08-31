# Stamp `seafloor_depth_m` onto the sample table

Rebuilds `sample_tbl` with a trailing `seafloor_depth_m` column
(bilinear GEBCO depth, see
[`sample_seafloor()`](https://calcofi.io/calcofi4db/reference/sample_seafloor.md)).
The table is recreated rather than `UPDATE`d because DuckDB cannot
update a table carrying a CRS-tagged `GEOMETRY` column (the `geom` on
`sample`).

## Usage

``` r
add_sample_seafloor(
  con,
  gebco_tif,
  sample_tbl = "sample",
  key_col = "sample_key",
  seafloor = NULL
)
```

## Arguments

- con:

  DBI connection holding `sample_tbl`.

- gebco_tif:

  Path to a GEBCO GeoTIFF (elevation, metres, negative below sea level).
  Any extent works; positions outside it return NA. A `/vsicurl/...` (or
  `http(s)://`) source streams over GDAL's range reads — the release's
  fallback when no local tile is present (D29).

- seafloor:

  Optional result of
  [`sample_seafloor()`](https://calcofi.io/calcofi4db/reference/sample_seafloor.md)
  to reuse instead of extracting again.

## Value

Invisibly, the
[`sample_seafloor()`](https://calcofi.io/calcofi4db/reference/sample_seafloor.md)
data.frame used.
