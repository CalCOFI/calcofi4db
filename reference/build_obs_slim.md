# The bio or env realm of `obs`, browser-shaped — and, since 3.31.0, its physical store

Slims `obs` to the columns a lens needs, joins the gear and effort of
the observation's own sample (`sample.tow_type`; `std_haul_factor`,
`prop_sorted`, `volume_sampled` from `sample_measurement`), stamps
`root_id`, `year`, `quarter`, `depth_bin` (10 m), `hex7`, `qual_ok`
(from `qual_ok_sql`) and the D8 densities + `effort_class` (from
`density_sql`). Depth is the observation's, falling back to its sample's
and then its root's, so a net tow carries its integrated span. Both
realms get the same schema (effort and taxon columns are NULL for env —
a NULL column costs nothing in parquet), so one set of SQL templates
serves both.

## Usage

``` r
build_obs_slim(
  con,
  realm = c("bio", "env"),
  qual_ok_sql,
  density_sql,
  tbl = NULL
)
```

## Arguments

- con:

  DuckDB connection holding `obs`, `sample`, `sample_measurement`,
  `measurement_type` and the `sample_root` built by
  [`build_sample_root()`](https://calcofi.io/calcofi4db/reference/build_sample_root.md).

- realm:

  `"bio"` or `"env"`.

- qual_ok_sql:

  the quality predicate over alias `o` —
  `calcofi4r::cc_qual_ok_sql("o")`.

- density_sql:

  the density select-list over the unaliased effort columns —
  `calcofi4r::cc_density_sql()`.

- tbl:

  output table (default `obs_{realm}`).

## Value

Invisibly, the row count.

## Details

Since 3.31.0 (pre-release plan D-S1) the pair is a **strict superset of
`obs` under a name mapping**: each row also carries `sample_key` (the
observation's own sampling event — without it a consumer reaches only
the root and loses the net / bottle grain), `measurement_prec` and
`hex_id` (the res-10 H3 cell `hex7` is the parent of); `realm` is
implied by the table and `measurement_value` is `value`.
[`obs_view_sql()`](https://calcofi.io/calcofi4db/reference/obs_view_sql.md)
is the UNION ALL that reconstructs `obs` from the pair under its
original 18 column names, and
[`check_obs_pair_parity()`](https://calcofi.io/calcofi4db/reference/check_obs_pair_parity.md)
asserts the pair holds exactly `obs`'s rows. The one deliberate
difference is the depth fallback above: where `obs` has no depth for a
bio row (a net tow whose span lives on `sample`), the pair — and
therefore the view — carries the sample's span; a non-NULL `obs` depth
is never changed.
