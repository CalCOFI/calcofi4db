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
  names_max = 200,
  reference_json = NULL,
  gazetteer_manifest = "https://storage.oceanmetrics.io/gazetteer/index/layers.json"
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

- reference_json:

  Path to the reference-layer manifest (`layers.<dataset_id>.n_features`
  / `.bbox`); `NULL` (default) leaves a reference row at zero features.

- gazetteer_manifest:

  URL or local path of the gazetteer's `index/layers.json` (`layers[]`
  with `pmtiles`, `n`, `bbox`); read only when the registry has a
  `pmtiles` row with a `source_url`. `NULL` skips it. Unreachable or
  unparseable: a message, and those rows keep zero features.

## Value

A list ready for `jsonlite::write_json(auto_unbox = TRUE)`: `version`,
`pmtiles_base`, `built`, and `layers[]` with `id` (the registry
`dataset_id`), `group`, `name` (the human layer name), `source`, `geom`,
`role`, `source_type`, `source_url`, `filter` (the registry expression
verbatim, as parsed JSON), the symbology defaults, `name_field`,
`description`, `attribution`, `source_layer` and `popup_fields`
(gazetteer rows; `NULL` otherwise), `n_features`, `bbox`, `names`,
`n_memberships`.

## Details

**Reference layers** (plan 2026-09-09 D52): a registry row with
`role = reference` (the OSM land mask, the GEBCO gazetteer labels,
Esri's raster reference) is not a region — it has no rows in `spatial`,
no memberships and no names — so its `n_features` and `bbox` come from
`reference_json` (`data/parquet/spatial/reference_layers.json`, written
by `ingest_spatial.qmd`'s Reference layers section) and its absence from
`spatial` is not a warning. The optional registry columns `role`
(default `boundary`), `source_type` (`pmtiles` \| `raster`, default
`pmtiles`) and `source_url` (raster tiles only) ride through to the
sidecar; `geom_type` may also be `label` (a symbol layer) or `raster`.

**Gazetteer rows** (oceanmetrics gazetteer, explore PR \#16): a
`pmtiles` row that carries its own `source_url` (absolute PMTiles URL)
is served from that archive, not from `pmtiles_base`, so it has no rows
in `spatial` either and its absence is not a warning. Two more optional
registry columns ride through for such rows: `source_layer` (the vector
layer name inside the archive) and `popup_fields` (`|`-separated
property names for the popup, emitted as a JSON array). Their
`n_features` and `bbox` come from the gazetteer manifest
(`gazetteer_manifest`, rows matched on `pmtiles` = `source_url`, field
`n`); when the manifest is unreachable (the bucket has answered 403)
they stay `0` / `NULL` with a message, never an error.
