# Check every dataset's citation, license and DOI, structurally and against its authority

One row per (dataset, finding); a clean dataset has a single `ok` row.
The structural half always runs: `citation_main` must be non-empty
(`missing_citation`), carry a 4-digit year (`no_year`) and a locator — a
DOI, a URL in the string, or a `link_data_source` (`no_locator`);
`license` must be an active id in `metadata/license.csv`
(`missing_license` when empty or `unknown`, `license_unregistered`
otherwise), and `custom` needs a `license_url`
(`license_custom_no_url`); `doi` must be bare (`10.…/…`).

## Usage

``` r
check_dataset_citation(
  ingest_yaml,
  network = TRUE,
  cache_dir = NULL,
  license_csv = NULL,
  refresh = FALSE,
  fetch = NULL,
  timeout = 30
)
```

## Arguments

- ingest_yaml:

  named list from
  [`read_ingest_yaml()`](https://calcofi.io/calcofi4db/reference/read_ingest_yaml.md)

- network:

  fetch from the authorities (default TRUE); FALSE runs the structural
  half and compares against whatever is already cached

- cache_dir:

  the `metadata/` root: holds `license.csv`, each dataset's
  `questions.csv` and its `citation_authority.json` cache. Defaults to
  `metadata/` beside the first ingest's `.qmd`.

- license_csv:

  path to the license registry (default `{cache_dir}/license.csv`)

- refresh:

  ignore the cache and fetch again (default FALSE)

- fetch:

  the HTTP function, `function(url, accept = NULL, method = "GET", …)`
  returning `list(status, content, url)`; the default uses curl. The
  tests inject one that serves saved responses.

- timeout:

  seconds per request

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html):
`dataset_key`, `finding`, `detail`, `authority`, `authority_citation`,
`checked`, plus `level` (`ok`/`error`/`warn`, see
[`citation_findings()`](https://calcofi.io/calcofi4db/reference/citation_findings.md)),
`exempt` and `question`.

## Details

The network half (`network = TRUE`, i.e. not `CALCOFI_SKIP_LINK_CHECK`)
asks the source's own authority, chosen by `link_data_source`: EDI's
cite service (`packageid=<scope>.<id>.<rev>`, or the newest revision
when only `scope`/`identifier` are given), an NCEI landing page ("Cite
as"), an ERDDAP `.das` (globals), and DataCite for any DOI (its
`rightsList` SPDX id, and doi.org content negotiation for the formatted
citation). A declared `doi` must answer 200/30x at doi.org
(`doi_unresolved`). Every fetch is cached in
`{cache_dir}/{provider}/{dataset}/citation_authority.json` (`authority`,
`url`, `citation`, `license`, `creator`, `title`, `checked`, …) so a
re-run costs nothing; pass `refresh = TRUE` to fetch again. A cached
authority is compared even when `network = FALSE`. A fetched citation
(or SPDX license) that differs from the declared one after
[`normalize_citation()`](https://calcofi.io/calcofi4db/reference/normalize_citation.md)
is `authority_drift`, with both strings in `detail`; a resolver that
cannot be reached is `authority_unavailable`. **Nothing is ever written
into a notebook's YAML** — the author's string is the record, the
authority is a proposal.

A finding is `exempt` when the dataset's `questions.csv`
(`{cache_dir}/{provider}/{dataset}/questions.csv`) holds an `open` or
`proposed` row with `related_table = dataset` whose `related_field` is
empty or names the field the finding is about (`citation_main`,
`license`, `doi`); `question` carries the row's label.
[`assert_dataset_citation()`](https://calcofi.io/calcofi4db/reference/assert_dataset_citation.md)
stops on any non-exempt `error`-level row.

## See also

[`read_license_registry()`](https://calcofi.io/calcofi4db/reference/read_license_registry.md),
[`assert_dataset_citation()`](https://calcofi.io/calcofi4db/reference/assert_dataset_citation.md),
[`release_citation()`](https://calcofi.io/calcofi4db/reference/release_citation.md)
