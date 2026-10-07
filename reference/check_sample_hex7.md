# Check `hex7` on `sample` and `sample_root` against the positions and against the observations

The release gate for
[`add_sample_hex7()`](https://calcofi.io/calcofi4db/reference/add_sample_hex7.md).
One row per rule and table:

## Usage

``` r
check_sample_hex7(
  con,
  sample_tbl = "sample",
  root_tbl = "sample_root",
  obs_tbls = c("obs_bio", "obs_env")
)
```

## Arguments

- con:

  DuckDB connection.

- sample_tbl, root_tbl:

  the stamped `sample` and the
  [`build_sample_root()`](https://calcofi.io/calcofi4db/reference/build_sample_root.md)
  table.

- obs_tbls:

  observation tables carrying `sample_key`, `latitude`, `longitude`,
  `hex7`; those absent from `con` are skipped.

## Value

A tibble `check`, `table`, `n`, `n_bad`, `status` (`ok` \| `fail` \|
`report`). Assert `all(status != "fail")`.

## Details

- `position_has_hex7` — `n` rows with a finite position, `n_bad` of them
  without a `hex7`;

- `hex7_has_position` — `n` rows with a `hex7`, `n_bad` of them without
  a finite position. Together these two say `count(hex7)` equals the
  count of finite positions, row for row;

- `hex7_is_res7` — `n` rows with a `hex7`, `n_bad` whose H3 resolution
  field is not 7;

- `root_equals_sample` — `n` root samples (no parent), `n_bad` roots
  whose `hex7` differs between the two tables, plus roots missing from
  `root_tbl`, plus `root_tbl` rows that are not a root sample;

- `obs_same_position` (per table of `obs_tbls` present) — `n`
  observations sitting at exactly their own sample's position, `n_bad`
  of them in another cell than their sample. This is the proof, on the
  data, that the sample side and the observation side share one
  definition;

- `obs_other_cell` — `n` observations where both the row and its sample
  carry a cell, `n_bad` in another cell than their sample. **Reported,
  never a failure**: an observation carries its own position (each scan
  of a CTD cast, a DIC draw), the sample carries the event's, and a cast
  that drifts across a cell edge puts some of its scans in the
  neighbouring hexagon.

A finite position is both coordinates present and neither `NaN` nor
infinite. Needs no extension: the resolution is read from the cell's own
bits.
