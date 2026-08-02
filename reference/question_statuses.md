# The controlled vocabulary of the question registry

`status`:

- `open`:

  asked, no answer and no proposal

- `proposed`:

  **we have an answer to approve, not a problem to hand over** —
  `proposed_answer` holds what we did or suggest, and the provider is
  confirming it

- `answered`:

  settled; `answer` holds the resolution

- `wontfix`:

  closed without an answer, deliberately

## Usage

``` r
question_statuses()

question_priorities()
```

## Value

Character vector of the allowed values.

## Details

`priority`: `blocker` (the ingest cannot be released as-is), `high`,
`normal`, `low`.

## Examples

``` r
question_statuses()
#> [1] "open"     "proposed" "answered" "wontfix" 
question_priorities()
#> [1] "blocker" "high"    "normal"  "low"    
```
