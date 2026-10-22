# Keep every source file an ingest reads in an immutable GCS archive

A file fetched from the web lives only where the render ran until
something copies it off the machine; on the Mac mini, `dir_data` is a
plain folder that nothing syncs back to GCS, so a provider that later
withdraws a file takes the only copy with it (calcofi.org's 1995-98
FinalQC zips, 2026-10-20). Call this after the downloads, on the files
the ingest reads. Each file lands once at
[`source_archive_path()`](https://calcofi.io/calcofi4db/reference/source_archive_path.md):
an object already there costs nothing, one the `_sync/` mirror holds is
copied server side, anything else is uploaded. A failed copy or upload
stops the render.

## Usage

``` r
archive_sources(
  files,
  provider,
  dataset,
  urls = NA_character_,
  bucket = "calcofi-files-public",
  prefix = "archive/sources",
  mirror_prefix = NULL,
  dry_run = FALSE
)
```

## Arguments

- files:

  local source files

- provider, dataset:

  the ingest's provider and dataset

- urls:

  where each file was fetched from (`NA` for a file that arrived by hand
  or from Drive); recycled to `files`

- bucket:

  GCS bucket (default `"calcofi-files-public"`; an `in_release: false`
  dataset goes to `calcofi-files-private`, see
  [`gcs_bucket_for_dataset()`](https://calcofi.io/calcofi4db/reference/gcs_bucket_for_dataset.md))

- prefix:

  archive prefix (default `"archive/sources"`)

- mirror_prefix:

  a mirror to copy from server side, e.g.
  `"_sync/calcofi/ctd-cast/download"`; `NULL` for none

- dry_run:

  if `TRUE`, plan only: nothing is copied or uploaded

## Value

A tibble: `source` (base name), `method` (`file_mtime`), `accessed`,
`bytes`, `md5`, `url`, `archive` (`gs://` URI) and `action`.

## Details

The result is a
[`stamp_source_access()`](https://calcofi.io/calcofi4db/reference/stamp_source_access.md)
table with the archive columns added, ready for
[`build_metadata_json()`](https://calcofi.io/calcofi4db/reference/build_metadata_json.md)'s
`sources` argument, so `metadata.json` `sources[]` says which bytes were
read (`md5`), from where (`url`) and where the copy is kept (`archive`).
