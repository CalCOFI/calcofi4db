# The bucket an upload of one dataset may go to

A dataset whose ingest says `in_release: false` has not been confirmed
by its provider, so neither its sources nor its staged parquet may reach
a bucket anyone can read. A private bucket, or a released dataset, keeps
`bucket`. Sources (`calcofi-files-public`) go to their private twin
`calcofi-files-private`, so a source is still kept; staged parquet
(`calcofi-db`) goes to the preview bucket, or nowhere until one is set.

## Usage

``` r
gcs_bucket_for_dataset(
  bucket,
  dataset_key,
  excluded = release_excluded_datasets(getOption("calcofi.workflow_dir", getwd())),
  preview_bucket = Sys.getenv("CALCOFI_PREVIEW_BUCKET")
)
```

## Arguments

- bucket:

  the bucket the caller asked for

- dataset_key:

  `{provider}_{dataset}`

- excluded:

  the unreleased dataset keys (default:
  [`release_excluded_datasets()`](https://calcofi.io/calcofi4db/reference/release_excluded_datasets.md)
  of the workflows directory, `getOption("calcofi.workflow_dir")`, else
  the working directory, where a notebook renders)

- preview_bucket:

  the private bucket for unreleased datasets (`CALCOFI_PREVIEW_BUCKET`;
  empty means none is configured)

## Value

`bucket`, the preview bucket, or `NA` when the upload must be skipped
