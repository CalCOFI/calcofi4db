# Read a dataset's `questions.csv`, validated and ranked

The single reader for the provider-question registry. Reads strictly
(`na = ""`, everything character, so a date or an id like `01` is never
silently retyped), checks the controlled vocabulary, and returns the
questions ranked `blocker` → `low` then by `label`.

## Usage

``` r
read_questions(path, validate = TRUE)
```

## Arguments

- path:

  path to a `questions.csv`

- validate:

  error on an unknown `status`/`priority`, a duplicate `label`, or a
  `label` that disagrees with `id` (default TRUE)

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html), all
columns character, ranked.

## Details

Two identifiers, deliberately:

- **`id`** — `{provider}_{dataset}_{nn}`, globally unique and durable.
  This is what a cross-dataset reference or an issue tracker cites.

- **`label`** — the short form (`Q15`), unique *within* the dataset.
  This is what prose in a notebook says, and what the rendered table
  shows first, so "see Q15" resolves for a reader.

## Examples

``` r
if (FALSE) { # \dontrun{
read_questions("metadata/calcofi/ctd-cast/questions.csv")
} # }
```
