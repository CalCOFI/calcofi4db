# Resolve `cruise_key` on an event table by span, designation, then month

Writes `cruise_key` (and `cruise_key_method`) onto `table_name` for
every row with a matched `ship_key`, trying in order:

1.  **span** — the reference cruise of the same ship whose
    `date_min - tolerance_days .. date_max + tolerance_days` contains
    the event (nearest span centre on the rare tie);

2.  **source** — `cruise_ym_col`, the source's own YYYYMM designation,
    when supplied and well-formed;

3.  **month** — the event's own year-month (the legacy rule).

Every key is `YYYY-MM-` + the ship's NODC code from `ship_tbl`.

## Usage

``` r
resolve_cruise_key(
  con,
  table_name,
  datetime_col,
  ship_key_col = "ship_key",
  cruise_ym_col = NULL,
  cruise_tbl = "cruise",
  ship_tbl = "ship",
  tolerance_days = 3L,
  require_in_cruise = FALSE,
  method_col = "cruise_key_method"
)
```

## Arguments

- con:

  DBI connection to DuckDB holding `table_name`, `cruise_tbl` (with
  `date_min`/`date_max` from
  [`add_cruise_date_span()`](https://calcofi.io/calcofi4db/reference/add_cruise_date_span.md))
  and `ship_tbl`.

- table_name:

  Event table to annotate.

- datetime_col:

  Timestamp/date column on the event table.

- ship_key_col:

  Column holding the matched `ship_key` (default `"ship_key"`); rows
  with NULL get no key.

- cruise_ym_col:

  Optional column carrying the source's cruise designation as YYYYMM
  (e.g. the bottle database's `Cruise`). Values not matching
  `^\\d{4}(0[1-9]|1[0-2])$` are ignored for that row.

- cruise_tbl, ship_tbl:

  Reference table names.

- tolerance_days:

  Days added to each end of a cruise's observed span before testing
  containment (default 3 — a hydrocast can precede the first plankton
  tow by a day or two).

- require_in_cruise:

  If TRUE, keys from steps 2–3 that do not exist in `cruise_tbl` are
  left NULL (use for datasets that only join to known cruises); step-1
  keys always exist by construction.

- method_col:

  Name of the column recording which step resolved each row (`"span"`,
  `"source"`, `"month"`, or NULL). Set to `NULL` to not record it.

## Value

A tibble with one row per method: `method`, `n`, `n_in_cruise`.
