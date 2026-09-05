# Check every record of the dataset catalog

One row per (dataset, finding); a clean dataset has a single `ok` row.
The structural half always runs
([`catalog_findings()`](https://calcofi.io/calcofi4db/reference/catalog_findings.md));
the network half (`network = TRUE`, i.e. not `CALCOFI_SKIP_LINK_CHECK`)
probes every distribution URL once with a one-byte ranged GET —
404/410/451 is `url_dead` (error), 5xx / no answer is `url_unreachable`
(warn); `retired` and `superseded` rows are not probed (they are
expected to be gone). Holdings are checked for name, category, provider
and their links.

## Usage

``` r
check_dataset_catalog(
  record,
  registries = NULL,
  network = TRUE,
  probe = NULL,
  timeout = 30
)
```

## Arguments

- record:

  from
  [`build_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/build_dataset_catalog.md)
  (or a `datasets.json` path)

- registries:

  from
  [`read_catalog_registries()`](https://calcofi.io/calcofi4db/reference/read_catalog_registries.md);
  NULL trusts the record's own `registered` flags

- network:

  probe the URLs (default TRUE)

- probe:

  the probe function `function(url) status`; the tests inject one

- timeout:

  seconds per request

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html):
`dataset_key`, `finding`, `level`, `detail`, `url`, `exempt`,
`question`.

## See also

[`assert_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/assert_dataset_catalog.md)
