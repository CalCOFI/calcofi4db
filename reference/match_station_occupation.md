# Stamp `sample.station_uuid`: the SWFSC station occupation an event belongs to

Every `sample` root (an event with no parent within its own dataset — a
bottle cast, a CTD cast, an ichthyo site, ...) is matched to the ichthyo
`site` row (its `source_uuid`) representing the same SWFSC station
occupation, in priority order:

1.  **self** — the root itself IS an ichthyo site
    (`dataset_key = 'swfsc_ichthyo'`): its own `source_uuid`.

2.  **order_occ** — exactly one ichthyo site shares
    `(cruise_key, site_key, order_occ)`.

3.  **datetime** — exactly one ichthyo site at `(cruise_key, site_key)`
    has a `datetime` within `tolerance_hours` of the root's (whether it
    is the only candidate at all, or the only one inside the window).

4.  otherwise `NULL` (`station_uuid_method` `NULL` too).

The match is computed once per ROOT and copied to every row sharing its
`root_sample_key`, which is what makes the crab's examined subsamples
(parented DIRECTLY to an ichthyo site via `parent_sample_key` /
`root_sample_key` — they never enter the match SQL, whose `roots` CTE
only sees rows where `sample_key = root_sample_key`) inherit that site's
`station_uuid` for free, with no separate matching logic needed. Their
`station_uuid_method` is relabeled **`"parent"`** rather than `"self"`
in that copy step, purely so a consumer can tell "this row IS the SWFSC
station occupation" (`"self"`, ichthyo's own site/tow/net) apart from
"this row is a foreign dataset's row directly under one" (`"parent"`).

## Usage

``` r
match_station_occupation(con, sample_tbl = "sample", tolerance_hours = 24)
```

## Arguments

- con:

  DBI connection holding `sample_tbl` (with `source_uuid`,
  `root_sample_key`, `cruise_key`, `site_key`, `order_occ`, `datetime`).

- sample_tbl:

  Table name (default `"sample"`).

- tolerance_hours:

  Hours a candidate's `datetime` may differ from the root's before it
  stops counting (default 24).

## Value

Invisibly, a tibble: `dataset_key`, `method` (`"self"` \| `"order_occ"`
\| `"datetime"` \| `"none"`), `n` — over ROOT samples only (`"parent"`
never appears here: by construction it only ever labels a NON-root row).

## Details

Rebuilds `sample_tbl` (DuckDB cannot `UPDATE` a table with a CRS-tagged
`geom` column), and asserts afterward that its row count and
`sample_key` uniqueness are unchanged — the v2026.08.25 lesson (a join
that fans out is a bug in the match, never data to accept).
