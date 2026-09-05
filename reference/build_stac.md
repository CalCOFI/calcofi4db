# Build the static STAC catalog of a release

Writes STAC 1.0.0 into `dir`: a root `catalog.json`, one
`collections/{dataset_key}/collection.json` per **public** dataset with
its extent, licence, providers, keywords, `table:tables` and `sci:doi` /
`sci:citation`, one Item per release at
`collections/{dataset_key}/items/{version}.json` whose assets are the
dataset's parquet objects (`application/x-parquet`, `roles: [data]`,
with `table:columns` from `metadata.json` and `file:size` /
`file:checksum`), its CF netCDF, the ERDDAP pages (`roles: [overview]`)
and the ISO 19115 record (`roles: [metadata]`); and one
`collections/layer_{key}/collection.json` per spatial layer with its
PMTiles asset. Everything is read from the record — no service is asked
anything here.

## Usage

``` r
build_stac(
  record,
  catalog = NULL,
  spatial_layers = NULL,
  dir,
  base_url = CC_STAC_HTTPS,
  metadata = NULL,
  include_layers = TRUE
)
```

## Arguments

- record:

  `datasets.json`: a path/URL or the parsed list

- catalog:

  the release `catalog.json` (path/URL or list), or NULL — used only for
  the release version/date when `record$release` lacks them

- spatial_layers:

  `spatial_layers.json` (path/URL or list), or NULL

- dir:

  the directory to write into (created)

- base_url:

  the HTTPS root the catalog will be served from; the `self`, `root`,
  `parent`, `child` and `item` links are absolute against it. Pass the
  staging root for a staging run.

- metadata:

  `metadata.json` (path/URL or list), or NULL — the source of the column
  and table descriptions

- include_layers:

  write the spatial-layer collections (default TRUE)

## Value

The paths written, invisibly, `catalog.json` first.
