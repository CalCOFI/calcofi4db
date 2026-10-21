# Where a source file is archived on GCS

Sources are kept by content, not by date:
`{prefix}/{provider}/{dataset}/{md5}/{file}`. A provider that
re-publishes a corrected file under the same name gets a second object
beside the first, never an overwrite, and a file read by many renders is
stored once.

## Usage

``` r
source_archive_path(md5, file, provider, dataset, prefix = "archive/sources")
```

## Arguments

- md5:

  hexadecimal MD5 of the file

- file:

  the file's base name

- provider, dataset:

  the ingest's provider and dataset

- prefix:

  archive prefix within the bucket (default `"archive/sources"`)

## Value

object paths, relative to the bucket
