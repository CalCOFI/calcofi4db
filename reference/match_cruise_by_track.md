# Match Records to a Cruise by Space-Time Proximity to an Occupied-Station Track

Assigns `cruise_key` to rows that carry a date and a position but no
cruise FK, by finding the nearest station occupation in a reference
*track* (any table of `cruise_key` + datetime + lon/lat, e.g. the
`sample` shard of an already-ingested dataset) on the same day, within
`max_km`.

## Usage

``` r
match_cruise_by_track(
  con,
  data_tbl,
  ref_tbl,
  cruise_key_col = "cruise_key",
  datetime_col = "datetime_start_utc",
  lon_col = "longitude",
  lat_col = "latitude",
  ref_datetime_col = NULL,
  ref_lon_col = NULL,
  ref_lat_col = NULL,
  group_col = NULL,
  window_days = 0,
  max_km = 25,
  min_share = 0.5,
  return_stats = TRUE
)
```

## Arguments

- con:

  DBI connection to DuckDB.

- data_tbl:

  Character. Table to populate `cruise_key` on.

- ref_tbl:

  Character. Reference track table carrying `cruise_key_col`,
  `ref_datetime_col` and `ref_lon_col`/`ref_lat_col`.

- cruise_key_col:

  Character. Cruise key column, on both tables (default "cruise_key").

- datetime_col, lon_col, lat_col:

  Character. Timestamp (compared by date) and position columns on
  `data_tbl`.

- ref_datetime_col, ref_lon_col, ref_lat_col:

  Character. The same on `ref_tbl`; each defaults to its `data_tbl`
  counterpart.

- group_col:

  Character or NULL. Column naming the survey/cruise grouping (e.g.
  "cruise_label"). When NULL (default) each row is assigned its own
  nearest match; when set, the group consensus described above is used.

- window_days:

  Numeric. Maximum absolute date difference in days (default 0, i.e.
  same day — the observer is aboard on the day).

- max_km:

  Numeric. Maximum distance for a row to match, in km (default 25).

- min_share:

  Numeric. Minimum share of a group's votes the winning `cruise_key`
  must hold, in 0-1 (default 0.5). Ignored when `group_col` is NULL.

- return_stats:

  Logical. If TRUE (default) return a stats list.

## Value

If `return_stats`, a list with `matched`, `total`, `pct` and `groups` —
a data frame of one row per group with the winning `cruise_key`,
`votes`, `votes_total`, `share` and `n_rows` (NULL `cruise_key` for an
unresolved group; absent when `group_col` is NULL). Side effect: adds
and populates `cruise_key_col` on `data_tbl`.

## Details

Datasets whose observers ride a CalCOFI ship — the bird/mammal census
(issue \#74) is the motivating case — record a survey label rather than
a cruise, so the cruise can only be recovered from where and when the
platform actually was. Year-month parsed from a survey label is *not*
sufficient: it is ambiguous whenever several ships sailed in one month,
and it is wrong outright for a cruise that straddles a month boundary.

With `group_col` set, the match becomes a **consensus** rather than a
per-row assignment: every row of a group (one survey = one cruise) votes
with its own nearest-station match, the modal `cruise_key` wins if it
holds at least `min_share` of the votes, and that winner is written to
*all* rows of the group — including rows too far from any station to
have voted. This is both more robust (a single transect that strays near
another ship's station cannot mis-assign itself) and higher-yield (a
group resolves as a whole). A group whose votes are too split, or that
has no vote at all, is left NULL rather than guessed.

Only cruises present in the reference track can be assigned, so pointing
`ref_tbl` at a track whose `cruise_key`s are all present in the `cruise`
reference table guarantees the emitted FK resolves. Rows that cannot be
matched keep `NULL`, which is the honest answer for a survey that rode a
non-CalCOFI cruise.

Distance uses the cosine-corrected equirectangular approximation, which
is well under 1% error at the sub-degree separations that matter here
and is far cheaper than `ST_Distance_Sphere()` over the candidate join.
Candidates are pre-filtered to a bounding box of `max_km`, so an absurd
antipodal "nearest" row can never be considered.

## Examples

``` r
if (FALSE) { # \dontrun{
# reference track = the ichthyo sample shard (its cruise_keys are exactly
# the `cruise` reference table's, so the emitted FK always resolves)
s <- match_cruise_by_track(
  con, "bird_mammal_transect", "cruise_track",
  datetime_col = "date", lon_col = "longitude", lat_col = "latitude",
  group_col    = "cruise_label")
cat(glue::glue("{s$matched}/{s$total} ({s$pct}%)"), "\n")
s$groups[is.na(s$groups$cruise_key), ]  # surveys with no CalCOFI cruise
} # }
```
