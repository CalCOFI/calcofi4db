# Add the observed date span of each cruise to the cruise reference

Computes `date_min` / `date_max` (DATE) per `cruise_key` from an event
query and writes them onto `cruise_tbl`, so downstream ingests can match
events to cruises by containment rather than by calendar month (see
[`resolve_cruise_key()`](https://calcofi.io/calcofi4db/reference/resolve_cruise_key.md)).
Run this in the ingest that owns the `cruise` reference
(`ingest_swfsc_ichthyo.qmd`), after `cruise_key` has been propagated to
the event tables; every other ingest loads that shard read-only.

## Usage

``` r
add_cruise_date_span(con, event_sql, cruise_tbl = "cruise")
```

## Arguments

- con:

  DBI connection to DuckDB.

- event_sql:

  A `SELECT` returning two columns named `cruise_key` and `datetime` —
  one row per event (site, tow, cast, ...). Rows with a NULL in either
  are ignored.

- cruise_tbl:

  Name of the cruise reference table (default `"cruise"`).

## Value

Invisibly, a tibble with one row per cruise: `cruise_key`, `date_ym` (if
present), `date_min`, `date_max`, `n_events`, `spills_month` (span
extends outside the designated month) and `overlaps` (the span
intersects another cruise of the same ship — a reference-data error that
would make span matching ambiguous; assert `sum(overlaps) == 0`).
