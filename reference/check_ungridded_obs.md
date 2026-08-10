# Observations that resolve no CalCOFI grid cell — reported, never dropped

Until v2026.08.11 every ingest's core projection filtered
`WHERE grid_key IS NOT NULL`, so an observation whose event did not land
on a station grid cell never reached `obs` at all — while the `sample`
arm kept the event. That asymmetry is what let four `calcofi_mets`
cruises reach a release as 11,762 underway samples with zero
observations, their 1.7M measurements reachable only through the
supplemental table.

## Usage

``` r
check_ungridded_obs(con, obs_tbl = "obs", verbose = TRUE)
```

## Arguments

- con:

  a DBI connection holding `obs`

- obs_tbl:

  name of the observation table (default `"obs"`)

- verbose:

  logical; message the headline

## Value

a data.frame, one row per `dataset_key`: `n_obs`, `n_ungridded`,
`pct_ungridded`, `n_no_position` (ungridded AND no lat/lon at all), and
`finding`, a sentence ready to paste into a `questions.csv` `context`
cell

## Details

Excluding them was also inconsistent with the pipeline's own reasoning:
`obs_mets_full` had already been deliberately gated on *a position*
rather than on `grid_key`, because "a ship on transit is legitimately
outside the CalCOFI station grid" — and `calcofi_phytoplankton` is
region-pooled and has emitted ungridded `obs` from the start. The
headline table now agrees with both: no grid cell is not a reason to
delete an observation.

It IS a reason to ask. An ungridded observation is one of three things
and the pipeline cannot tell them apart: a genuinely off-grid position
(transit, an historical station outside the modern pattern), a coarser
spatial notion (a region-pooled sample with no point at all), or **a
coordinate error** — the sign-flipped `Longitude_W` that put five
CalCOFI cruises in the Taiwan Strait was invisible precisely because
being off-grid silently removed the rows. So this reports every
dataset's share and is meant to drive a `questions.csv` entry per
dataset, not to be quietly tolerated.
