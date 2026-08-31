# The explorer's boundary-layer sidecar: the registry joined with the release's `spatial` table

`metadata/spatial_layers.csv` is the registry of the boundary layers
(Erin's sheet: one row per drawable layer with its PMTiles group,
default symbology, filter expression and provenance), and the archives
at `{pmtiles_base}{dataset_group}.pmtiles` carry the features. The
explorer must not hard-code that list nor fetch the CSV from GitHub at
runtime (plan 2026-08-31 D23), so each release ships
`spatial_layers.json`: the registry verbatim **plus what only the
release knows** — each layer's feature count, bbox, its sorted distinct
`name`s when there are at most `names_max` (the by-name palette, D24;
`NULL` above that, and the app falls back to an id-hash palette), and
`n_memberships` (distinct root samples in `sample_spatial`, so the
Regions lens can list exactly the layers that can summarize something).

## Usage

``` r
build_spatial_layers(
  con,
  registry_csv,
  version,
  pmtiles_base,
  built = NULL,
  names_max = 200
)
```

## Arguments

- con:

  DuckDB connection holding `spatial` (and, if built, `sample_spatial`).

- registry_csv:

  Path to `metadata/spatial_layers.csv`.

- version:

  Release version string, stamped into the sidecar.

- pmtiles_base:

  URL prefix of the PMTiles archives (source-layer = `dataset_group`).

- built:

  When the archives were last built (the `ingest_spatial` manifest's
  mtime) — version skew between releases and archives is accepted but
  must be visible.

- names_max:

  Above this many distinct names a layer's `names` is `NULL`.

## Value

A list ready for `jsonlite::write_json(auto_unbox = TRUE)`: `version`,
`pmtiles_base`, `built`, and `layers[]` with `id` (the registry
`dataset_id`), `group`, `name` (the human layer name), `source`, `geom`,
`filter` (the registry expression verbatim, as parsed JSON), the
symbology defaults, `name_field`, `description`, `attribution`,
`n_features`, `bbox`, `names`, `n_memberships`.
