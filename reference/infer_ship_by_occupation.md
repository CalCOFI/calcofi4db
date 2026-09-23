# Infer a ship-less event's ship from the station occupation it matches

[`resolve_cruise_key()`](https://calcofi.io/calcofi4db/reference/resolve_cruise_key.md)
needs a `ship_key`: span containment is only unambiguous within one ship
(no two cruises of one ship overlap), and a month with two or three
ships at sea is exactly where a bare `YYMM` designation cannot choose
between them (April 2004–2006: 31JD + 32NM + 33LB/33OA). A source that
records no ship but does record a CalCOFI station and a time — the CDFW
Dungeness crab sorting log, whose archived jars ARE CalCOFI tows — still
names its cruise, through the station occupation: at most one ship
occupies a given `site_key` within a day.

## Usage

``` r
infer_ship_by_occupation(
  con,
  table_name,
  datetime_col,
  site_key_col = "site_key",
  occupation_sql = paste("SELECT cruise_key, site_key, datetime FROM sample",
    "WHERE dataset_key = 'swfsc_ichthyo' AND sample_type = 'site'"),
  cruise_tbl = "cruise",
  tolerance_hours = 24,
  ship_key_col = "ship_key",
  method_col = "ship_key_method",
  candidates_col = "cruise_key_candidates"
)
```

## Arguments

- con:

  DBI connection to DuckDB holding `table_name` and `cruise_tbl`.

- table_name:

  Event table to annotate (updated in place, so it must not carry a
  CRS-tagged `GEOMETRY` column yet).

- datetime_col:

  Timestamp column on the event table.

- site_key_col:

  Column holding the CalCOFI `site_key` (`"LLL.L SSS.S"`) on the event
  table (default `"site_key"`).

- occupation_sql:

  A `SELECT` returning `cruise_key`, `site_key` and `datetime`, one row
  per reference station occupation. Default: the `swfsc_ichthyo` site
  rows of `sample`.

- cruise_tbl:

  Cruise reference table, with `cruise_key` + `ship_key`.

- tolerance_hours:

  Hours an occupation's `datetime` may differ from the event's and still
  count (default 24 — a sorting log's local clock can sit 8 h off the
  reference's UTC).

- ship_key_col:

  Column to fill (created if absent; default `"ship_key"`).

- method_col:

  Column recording `"occupation"` for inferred rows (created if absent;
  default `"ship_key_method"`).

- candidates_col:

  Column listing the candidate cruises of an ambiguous row, or `NULL` to
  not write it (default `"cruise_key_candidates"`).

## Value

A tibble with one row per outcome: `outcome` (`"source"` = already had a
ship, `"occupation"`, `"ambiguous"`, `"none"`) and `n`.

## Details

For every row whose `ship_key_col` is NULL, the candidate cruises are
the distinct `cruise_key`s of the reference occupations
(`occupation_sql`) at the same `site_key` whose `datetime` is within
`tolerance_hours` of the event's.

- exactly one candidate, whose ship `cruise_tbl` knows: `ship_key_col`
  is set to that cruise's ship and `method_col` to `"occupation"`;

- two or more: nothing is set, and the candidates are written to
  `candidates_col` (sorted, comma-separated) so the ambiguity can be
  reported or asked about — never broken by a guess;

- one candidate the reference cannot place on a ship: treated as
  ambiguous (candidates written, no ship);

- none: nothing is set.

Only the ship is inferred. Run
[`resolve_cruise_key()`](https://calcofi.io/calcofi4db/reference/resolve_cruise_key.md)
next: its span step then keys the row to the matched cruise (the
occupation lies inside that cruise's span by construction), and its
designation/month steps still apply where the span does not. A row that
already carries a ship is never touched, and a re-run first clears what
an earlier run inferred.
