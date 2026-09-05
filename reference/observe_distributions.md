# Ask every portal what it says about our external copies

One observer per `portal.csv` `observe_method` — `edi-pasta` (the newest
revision through EDI's cite service, since PASTA's
`/package/eml/{scope}/{id}` answers 403 anonymously), `doi` (does
doi.org resolve), `obis-api` (the dataset's `updated`), `ncbi-esummary`,
`zenodo-api`, `erddap-das` (`date_modified` / `time_coverage_end`, and a
404 means the id is gone), `caloos` / `http` (liveness). Nothing is
written into the registry and no row is ever dropped: the result is a
parallel observation, and `retired` is a *status*, not a deletion.

## Usage

``` r
observe_distributions(registry, portals = NULL, fetch = NULL, quiet = FALSE)
```

## Arguments

- registry:

  the tibble from
  [`read_distribution_registry()`](https://calcofi.io/calcofi4db/reference/read_distribution_registry.md),
  or the union from
  [`distribution_targets()`](https://calcofi.io/calcofi4db/reference/distribution_targets.md)

- portals:

  the tibble from
  [`read_portal_registry()`](https://calcofi.io/calcofi4db/reference/read_portal_registry.md),
  or NULL (every row is then observed by `http`)

- fetch:

  the HTTP function (see
  [`check_dataset_citation()`](https://calcofi.io/calcofi4db/reference/check_dataset_citation.md));
  the tests inject one over saved responses, so the suite never touches
  the network

- quiet:

  suppress the per-row progress line

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html): the
registry's key columns plus `method`, `registry_status`, `status` (one
of
[`observation_statuses()`](https://calcofi.io/calcofi4db/reference/observation_statuses.md)),
`observed_utc`, `http_status`, `revision`, `updated`, `note`.
