# Fail (or ratchet) the release on a `cruise_key` that does not hold up

Ten checks over `cruise_key` as it is actually used, not as it is
assumed to behave — see the WS-B design memo for the incidents each one
guards against. Checks 1-5 and 7 are hard failures from the first run
(`n` must be exactly 0); check 6 is hard with `known_outside_span` as
named exceptions (an UNLISTED violator still fails); checks 8-10 are
ratchets — the current count must not exceed the allowance, which may
only ever be lowered.

## Usage

``` r
check_cruise_key_integrity(
  con,
  tolerance_days = 31L,
  known_outside_span = character(),
  manifest_ichthyo = NULL,
  ratchets = list(span_overlaps_max = 2L, derived_max = 152L, key_null_max =
    c(calcofi_dic = 3255L, `sio_pic-zooplankton` = 5087L, `cdfw_dungeness-crab` = 1639L,
    calcofi_bottle = 49L)),
  halt = TRUE,
  sample_tbl = "sample",
  obs_tbl = "obs",
  cruise_tbl = "cruise",
  ship_tbl = "ship"
)
```

## Arguments

- con:

  DBI connection holding `sample_tbl`, `obs_tbl`, `cruise_tbl`,
  `ship_tbl`.

- tolerance_days:

  Days a `'swfsc'` cruise's span may be exceeded by one of its own
  events before check 6 flags it (default 31 — 99.97% of the 21,987
  outside-span events measured at v2026.08.25 are within this).

- known_outside_span:

  `sample_key`s exempt from check 6 (named or not; only the values are
  used) — an UNLISTED violator still fails.

- manifest_ichthyo:

  A single integer: the ichthyo ingest's own `mismatches$cruise_uuid`
  `n_mismatch` count (from `manifest.json`), or `NULL` if not supplied
  (check 7 then fails, naming the gap).

- ratchets:

  A list with `span_overlaps_max`, `derived_max` (both single integers)
  and `key_null_max` (a named integer vector by `dataset_key`; an
  un-named dataset's allowance is 0, so a first NULL there fails).

- halt:

  [`stop()`](https://rdrr.io/r/base/stop.html) on any failing hard check
  or exceeded ratchet (default `TRUE`).

- sample_tbl, obs_tbl, cruise_tbl, ship_tbl:

  Table names.

## Value

A tibble: `check`, `dataset_key` (or a table/scope label where the check
is not per-dataset), `n`, `mode` (`"fail"` \| `"ratchet"`), `finding`.

## Details

1.  `cruise_key` matches `^\\d{4}-(0[1-9]|1[0-2])-[A-Za-z0-9]{4}$` on
    `cruise_tbl`, `sample_tbl`, `obs_tbl`.

2.  `cruise.date_ym`'s `YYYY-MM` equals the key's own.

3.  the key's NODC segment equals `ship.ship_nodc` of `cruise.ship_key`.

4.  every non-NULL `sample.cruise_key` / `obs.cruise_key` names a real
    `cruise` row (run
    [`complete_cruise_reference()`](https://calcofi.io/calcofi4db/reference/complete_cruise_reference.md)
    first, or this is the 153,306-row finding it exists to close).

5.  `cruise_key_method = 'swfsc'` rows have a unique non-NULL
    `cruise_uuid`; `'derived'` rows have none.

6.  every event's date falls within
    `[date_min - tolerance_days, date_max + tolerance_days]` of its
    cruise — for `'swfsc'` rows only (a `'derived'` row's span is its
    own events' min/max by construction, so it cannot be violated);
    `known_outside_span` names exempt `sample_key`s.

7.  the ichthyo notebook's own `cruise_uuid` vs `cruise_key` check
    (`manifest_ichthyo`, its `mismatches$cruise_uuid` count) is 0, and
    every ichthyo `site`/`tow`/`net` row has a non-NULL `source_uuid`.

8.  (ratchet `span_overlaps_max`) event spans of two cruises of one ship
    overlap by more than 3 days.

9.  (ratchet `derived_max`) `cruise_key_method = 'derived'` row count.

10. (ratchet `key_null_max`, per `dataset_key`) root samples with a
    `NULL` `cruise_key`.
