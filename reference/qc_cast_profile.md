# Fetch one physical cast's profile, both directions

Returns the full-resolution scans for the cast a `sample_key` belongs to
— **both** the down- and upcast, since the point of plotting a profile
during review is to see them overlaid. `obs` carries only one direction
per physical cast (that is what thinning does), so the default source is
the supplemental `obs_ctd_full`.

## Usage

``` r
qc_cast_profile(
  con,
  sample_key,
  measurement_types = NULL,
  obs_tbl = "obs_ctd_full",
  cruise_key = NULL
)
```

## Arguments

- con:

  a DBI connection carrying `obs_ctd_full` (or `obs_tbl`) and `sample`

- sample_key:

  any one direction's key; both are returned

- measurement_types:

  restrict to these types; `NULL` for all

- obs_tbl:

  source table (`"obs_ctd_full"`, or `"obs"` for the thinned set)

- cruise_key:

  partition to prune to; `NULL` looks it up from `sample`

## Value

a data frame of `sample_key`, `cast_dir` (`down`/`up`), `depth_m`,
`measurement_type`, `measurement_value`, `measurement_qual`, `datetime`,
ordered by type, direction and depth

## Details

`cruise_key` is not a filter for the caller's convenience, it is a
performance precondition: `obs_ctd_full` is hive-partitioned by
`cruise_key`, so supplying it prunes ~212M rows to one cruise. When it
is not supplied this looks it up from `sample` — one cheap query, rather
than letting a profile fetch scan the whole archive.
