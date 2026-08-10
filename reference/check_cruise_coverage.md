# Cruises that carry samples but no observations — the silent-loss guard

A cruise can leave `obs` without leaving `sample`, and nothing about
that violates a foreign key: FK validation runs child -\> parent, so
every surviving `obs` row still has a parent, and a parent with **no
children** breaks no constraint. Release `v2026.08.08` shipped in
exactly that state — 10 `calcofi_ctd-cast` cruises kept all 1,186 of
their casts and lost all 874,000 of their observations, because a Google
Drive placeholder read as zero rows and the direction letter the
thinning step needs came from the filename of a conflict copy. No check
anywhere looked at the parent side.

## Usage

``` r
check_cruise_coverage(
  con,
  obs_tbl = "obs",
  max_orphan_cruises = 0L,
  halt = TRUE,
  verbose = TRUE
)
```

## Arguments

- con:

  a DBI connection holding `sample` and `obs`

- obs_tbl:

  name of the observation table (default `"obs"`)

- max_orphan_cruises:

  integer allowance, or a named integer vector keyed by `dataset_key`
  for a per-dataset ratchet. Use `0` where the correct answer is known
  to be zero (an ingest asserting its own output); use the current
  counts as a ratchet at release time so a *new* orphan fails while a
  documented backlog does not. May only ever be lowered.

- halt:

  logical; [`stop()`](https://rdrr.io/r/base/stop.html) when the
  allowance is exceeded (default `TRUE`)

- verbose:

  logical; message the summary

## Value

a data.frame, one row per `dataset_key`, with `cruises`,
`cruises_no_obs`, `orphan_samples` and `emits_obs` (invisibly when
`verbose = FALSE`)

## Details

The grain is the **cruise**, deliberately, and it is not the sample. A
CTD `sample` row is one physical cast *per direction* while `obs` keeps
a single direction, so about half of `calcofi_ctd-cast`'s cast rows
legitimately carry no observations and a per-sample assertion would be
wrong on arrival. A whole cruise with none is never legitimate.

A dataset that emits **no** observations at all is exempt rather than
failing 587 times: `sio_pic-zooplankton` is a net-tow registry whose
biovolumes are still pending from the provider, so contributing `sample`
alone is its designed state. The rule is therefore relative — *if a
dataset contributes observations, every one of its cruises must* — which
needs no allowlist to say so.
