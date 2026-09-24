# Re-probe the URLs [`check_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/check_dataset_catalog.md) left pending, after their upload

Takes the check's table, probes exactly its `url_pending` rows again and
returns them re-classified: an answer is `ok`, a 404/410/451 is now
`url_dead` (error), anything else `url_unreachable` (warn). Pass the
result to
[`assert_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/assert_dataset_catalog.md),
which stops on a dead one — so a new dataset's STAC collection that the
upload did NOT write fails the release before anything is promoted,
exactly as a dead URL did before 4.17.1.

## Usage

``` r
recheck_pending_urls(d, probe = NULL, timeout = 30)
```

## Arguments

- d:

  the table from
  [`check_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/check_dataset_catalog.md)

- probe:

  the probe function `function(url) status`; the tests inject one

- timeout:

  seconds per request

## Value

the `url_pending` rows of `d`, re-classified (0 rows when none were
pending).

## See also

[`check_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/check_dataset_catalog.md),
[`assert_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/assert_dataset_catalog.md)
