# Build the dataset catalog record — `datasets.json`

One record per `dataset_key` (plan § D-1, Appendix A), joined from the
release sidecars, the registries and the measured endpoints:

## Usage

``` r
build_dataset_catalog(
  meta,
  coverage,
  catalog,
  registries,
  version = NULL,
  erddap = NULL,
  netcdf = NULL,
  since = NULL,
  source_accessed = NULL,
  spatial_layers = NULL,
  bathymetry = NULL,
  workflows_base = "https://calcofi.io/workflows/",
  release_prefix = "ducklake/releases"
)
```

## Arguments

- meta:

  the release `metadata.json` (path or parsed list)

- coverage:

  the release `coverage.json`

- catalog:

  the release `catalog.json`

- registries:

  from
  [`read_catalog_registries()`](https://calcofi.io/calcofi4db/reference/read_catalog_registries.md)

- version:

  the release version (default: the catalog's)

- erddap:

  the table from
  [`fetch_erddap_datasets()`](https://calcofi.io/calcofi4db/reference/fetch_erddap_datasets.md)
  (NULL: no ERDDAP rows)

- netcdf:

  the list from
  [`fetch_netcdf_manifests()`](https://calcofi.io/calcofi4db/reference/fetch_netcdf_manifests.md)

- since:

  a named character vector `dataset_key -> first version`
  ([`dataset_since_versions()`](https://calcofi.io/calcofi4db/reference/dataset_since_versions.md))

- source_accessed:

  a named character vector `dataset_key -> YYYY-MM-DD` (the release
  `dataset` table's measured column)

- spatial_layers:

  the release `spatial_layers.json` (for `reference[]`)

- bathymetry:

  the `bathymetry/gebco_2025.json` manifest (for `reference[]`)

- workflows_base:

  the URL prefix of the rendered notebooks

- release_prefix:

  the bucket-relative releases prefix the run writes to
  (`ducklake/releases`, or the staging prefix) — `release.url` follows
  it

## Value

A list ready for
[`write_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/write_dataset_catalog.md)
/ `jsonlite::write_json(auto_unbox = TRUE)`.

## Details

- identity, description, attribution, links and `tables[]` from the
  `metadata.json` dataset block, with `provider` and `category` expanded
  from their registries and the descriptive sidecar's `keywords`,
  `creators[]`, `funding` and `visibility`;

- `coverage` rolled up from `coverage.json`: `years[]` (the sparkline),
  `n_stations`, `n_variables`, `n_taxa`, the depth span, `variables[]`,
  `life_stages[]` (when the coverage carries them) and
  `contributes_to[]` (env-realm variables homed in another category);

- `objects[]` from `catalog.json`: the dataset's `dataset_key=`
  partitions and the whole tables attributed to it, each with `bytes`,
  `sha256`, `since` and an absolute URL; `since_version` from `since`;

- `distributions[]`
  ([`dataset_distributions()`](https://calcofi.io/calcofi4db/reference/dataset_distributions.md)):
  parquet, netCDF, the ERDDAP ids that exist now, the notebook, the
  calcofi.org page, the source and the curated `distribution.csv` rows;

- `registrations[]` from `dataset_status.csv`'s `publish_*` columns —
  with ERDDAP and OBIS *measured* (a served id / a curated OBIS row wins
  over the registry cell) and Zenodo from the release DOI;

- `status` from `dataset_status.csv` plus the open/proposed questions.

`holdings[]` are the sidecars with
`status: planned | external | archived` (a dataset without a release, §
D-11), and `reference[]` the cruise, ship, grid and spatial tables, the
boundary layers and the bathymetry (Decision 20). Deterministic: no wall
clock, sorted by key.

## See also

[`check_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/check_dataset_catalog.md),
[`write_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/write_dataset_catalog.md)
