# Read every registry the dataset catalog joins

One call for `category.csv`, `provider.csv`, `license.csv`,
`dataset_status.csv`, `distribution.csv`, `portal.csv`, the descriptive
sidecars and a reader for each dataset's `questions.csv` — validated as
they are read, so an unregistered value fails here rather than in a
page.

## Usage

``` r
read_catalog_registries(metadata_dir)
```

## Arguments

- metadata_dir:

  the `workflows/metadata` directory

## Value

A list: `category`, `provider`, `license`, `dataset_status`,
`distribution`, `portal` (tibbles), `sidecars` (named list),
`questions(dataset_key)` (a function returning the open/proposed rows on
`related_table = dataset`, or NULL) and `metadata_dir`.
