# The consent gate: no dataset reaches the public database before its provider accepts

Reads each released dataset's standing consent question — the row of
`metadata/{provider}/{dataset}/questions.csv` whose `related_field` is
`consent` — and classifies it:

- `none`:

  no `questions.csv`, or no consent row (ok: the dataset predates the
  consent process)

- `pending`:

  the row is `open` or `proposed`: the ask is out

- `declined`:

  the row is `wontfix`, or `answered` with an answer that is not a yes

- `given`:

  the row is `answered` and the answer begins with `yes`, or with
  `no ask needed` (the 2026-10-22 convention for a dataset openly
  licensed at its public source,
  `who = "Ben Best (licence at source)"`); it is `ok` only when `who`
  names who gave it

## Usage

``` r
check_dataset_consent(
  workflow_dir,
  datasets = NULL,
  exempt = character(),
  legacy = NULL,
  halt = TRUE
)
```

## Arguments

- workflow_dir:

  the workflows checkout: holds the `ingest_*.qmd` notebooks and
  `metadata/{provider}/{dataset}/questions.csv`

- datasets:

  dataset keys (`{provider}_{dataset}`) to check; default: the datasets
  the release will include (`in_release` not `false`) plus `exempt`

- exempt:

  dataset keys reported but never failed (a preview run's
  `CALCOFI_RELEASE_INCLUDE`)

- legacy:

  dataset keys released before the gate existed, for which a pending ask
  is reported but does not fail; default: every notebook with no
  `in_release` key. Pass
  [`character()`](https://rdrr.io/r/base/character.html) to require a
  given consent everywhere.

- halt:

  stop when any dataset is not ok (default TRUE), naming each failing
  dataset and its state

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html), one row
per dataset: `dataset_key`, `consent` (`none` \| `pending` \| `declined`
\| `given`), `label`, `status`, `answer`, `who`, `answered_date`, `ok`,
plus `exempt`, `legacy` and `why` (the basis of `ok`).

## Details

A dataset is `ok` when its consent is `none`, or `given` with a `who`. A
`pending` ask is ok for a dataset in `exempt` (a private preview run
passes `CALCOFI_RELEASE_INCLUDE`: the preview is where the decision is
made) and for a dataset in `legacy` (one that entered the public
database before the gate existed; by default a notebook with no
`in_release` key at all — un-holding a dataset leaves `in_release: true`
in its notebook, so a dataset that came in through the preview pathway
is never legacy). `declined` is ok only for an exempt dataset: a
provider's no withdraws the data. A public run has an empty `exempt`, so
every dataset un-held through the preview must carry a given consent
before `release_database.qmd` proceeds.

## See also

[`read_questions()`](https://calcofi.io/calcofi4db/reference/read_questions.md),
[`release_excluded_datasets()`](https://calcofi.io/calcofi4db/reference/release_excluded_datasets.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# release_database.qmd, after ds_excluded is settled
check_dataset_consent(here::here(), exempt = release_include, halt = TRUE)
} # }
```
