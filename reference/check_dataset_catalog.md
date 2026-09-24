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
  timeout = 30,
  pending_base = NULL
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

- pending_base:

  a URL root this release writes after the check (its STAC root,
  [`release_stac_base()`](https://calcofi.io/calcofi4db/reference/release_stac_base.md));
  a dead URL under it is `url_pending`. NULL: none.

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html):
`dataset_key`, `finding`, `level`, `detail`, `url`, `exempt`,
`question`.

## Details

**A URL this release has not written yet is `url_pending`, not
`url_dead`.** The record lists each public dataset's STAC collection
under the release's own STAC root, and `release_database.qmd` uploads
that tree AFTER this check. A dataset in its first release has no
collection there yet, so the check failed every new dataset (first seen
with `calcofi_ctd-derived`, 2026-09-24, the first new dataset since STAC
landed on 2026-09-05), while the existing ones passed only on the
previous run's objects. With `pending_base`, a 404/410/451 under it is
`url_pending` (level `pending`, never blocking here), and
[`recheck_pending_urls()`](https://calcofi.io/calcofi4db/reference/recheck_pending_urls.md)
re-probes exactly those rows once the tree is uploaded, turning any that
still fails into `url_dead`.

## See also

[`assert_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/assert_dataset_catalog.md),
[`recheck_pending_urls()`](https://calcofi.io/calcofi4db/reference/recheck_pending_urls.md)
