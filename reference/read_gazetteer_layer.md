# Read one gazetteer layer, pinned and verified

Downloads a collection's `places.parquet` from the oceanmetrics
gazetteer and returns it as an `sf`. The read is **pinned**: the
manifest row (`index/layers.json`, matched on `slug`) names the
`version` and the parquet's `parquet_sha256`, and the downloaded bytes
must hash to it, else an error (a silently changed layer must never
reach a release). The manifest's newer fields are optional, so this
works against older manifests:

## Usage

``` r
read_gazetteer_layer(
  slug,
  version = NULL,
  manifest = GAZETTEER_MANIFEST,
  cache_dir = tools::R_user_dir("calcofi4db", "cache")
)
```

## Arguments

- slug:

  the gazetteer collection, e.g. `"noaa_aoa_socal"`.

- version:

  the collection version to read; `NULL` (default) = the manifest's
  `version`.

- manifest:

  URL or local path of the gazetteer's `index/layers.json`.

- cache_dir:

  directory for verified downloads (default: the user cache of
  calcofi4db).

## Value

An `sf` in EPSG:4326 with geometry column `geometry`, the columns
`place_id`, `name`, `status`, `status_date`, `status_source`,
`status_upstream`, `status_history` first (NA when the parquet lacks
one), then the native attributes (the GeoParquet `bbox` covering column
dropped). Attribute `gazetteer` = `list(slug, version, sha256, url)`.

## Details

- no `parquet`: the URL is `<base><slug>/places.parquet` (`base` from
  the manifest, default `https://storage.oceanmetrics.io/gazetteer/`);

- no `parquet_sha256`, or a `version` other than the manifest's: the
  hash comes from `<base><slug>/versions.json` (`versions[]` entry for
  `version`, `assets["places.parquet"].sha256`, and its `href` when
  given).

A verified copy is kept in `cache_dir/<slug>/<version>/places.parquet`
and reused while its hash still matches. `manifest`, `base`, `parquet`
and `versions.json` may also be local paths.

## Examples

``` r
if (FALSE) { # \dontrun{
aoa <- read_gazetteer_layer("noaa_aoa_socal")
attr(aoa, "gazetteer")
} # }
```
