# Build the CF/ACDD global attributes for a published dataset

Derives the file's self-description from the ingest notebook's
`calcofi.dataset_meta` YAML — the same block that feeds the release
`dataset` table — so the netCDF, the database and the schema site cannot
describe the same dataset differently. Per-file overrides go in
`calcofi.netcdf`.

## Usage

``` r
nc_global_atts(
  dataset_key,
  dataset_meta = list(),
  release,
  shape = c("profile", "groups"),
  cf_scope = NULL,
  workflow_url = NULL,
  date_created = NULL,
  extra = list()
)
```

## Arguments

- dataset_key:

  Provenance stamp, e.g. `"calcofi_ctd-cast"`.

- dataset_meta:

  The ingest's `dataset_meta` list (`dataset_name`, `description`,
  `citation_main`, `coverage_temporal`, `coverage_spatial`, `license`,
  …). Missing keys simply omit their attribute. A `title` or
  `description` here wins, so a caller overrides the derived text by
  merging its `calcofi.netcdf` block over `dataset_meta` before passing
  it.

- release:

  Database release the file was built from, e.g. `"v2026.07.30"`.

- shape:

  `"profile"` or `"groups"`, from
  [`plan_dataset_netcdf()`](https://calcofi.io/calcofi4db/reference/plan_dataset_netcdf.md).

- cf_scope:

  Override for the honesty statement about CF coverage; defaults to text
  appropriate to `shape`.

- workflow_url:

  Rendered notebook URL, written as `references`.

- date_created:

  ACDD creation date. Defaults to the **release** date parsed from
  `release`, deliberately: see Details.

- extra:

  Named list of additional or overriding global attributes.

## Value

Named list of global attributes, ready for `ncdf4::ncatt_put(nc, 0, …)`.

## Details

**Why `date_created` is the release date, not
[`Sys.time()`](https://rdrr.io/r/base/Sys.time.html).** The publisher
skips re-uploading a file whose sha256 matches an earlier release
(release-named paths, bytes written once). A wall-clock `date_created`
puts a fresh timestamp inside every build, so no rebuild is ever
byte-identical and that check can never fire — it silently degrades to
"always re-upload". Tying the attribute to the release makes a rebuild
of the same release reproducible, which is also the more useful claim:
it dates the data product, not the run.
