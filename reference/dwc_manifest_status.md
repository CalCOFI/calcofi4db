# Read a Darwin Core Archive manifest and say whether the OBIS copy is current

Feeds `registrations[]` (D-8): `published (vX)` when the uploaded bytes
are these bytes, `stale — data changed in vY` when they are not,
`built, not uploaded` when nothing was ever uploaded.

## Usage

``` r
dwc_manifest_status(path)
```

## Arguments

- path:

  a `{dataset_key}_manifest.json`

## Value

A one-row data frame: `dataset_key`, `version`, `content_hash`,
`ipt_resource`, `obis_dataset_id`, `uploaded_utc`, `status`.
