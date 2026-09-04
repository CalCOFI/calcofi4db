# Complete the `cruise` reference with cruises no SWFSC site row names

The SWFSC ichthyo export's `cruise` table is a station-occupation cruise
list, not a designation registry: 152 `cruise_key`s that `sample`
carries (bottle, CTD, METS, picoplankton — 1949-1950 and post-export
years mostly) name no row in it. This adds one `cruise` row per such key
so
[`check_cruise_key_integrity()`](https://calcofi.io/calcofi4db/reference/check_cruise_key_integrity.md)'s
FK check (and every ordinary FK join) holds by construction, stamping
`cruise_key_method` (`"swfsc"` for the pre-existing rows, `"derived"`
for the added ones) and `cruise_key_datasets` (a sorted comma list of
every dataset that keys events to that cruise) on ALL rows — not just
the ones it adds — so a consumer can always tell which kind of row it is
looking at.

## Usage

``` r
complete_cruise_reference(
  con,
  sample_tbl = "sample",
  cruise_tbl = "cruise",
  ship_tbl = "ship"
)
```

## Arguments

- con:

  DBI connection holding `sample_tbl`, `cruise_tbl`, `ship_tbl`.

- sample_tbl, cruise_tbl, ship_tbl:

  Table names.

## Value

Invisibly, a tibble of the rows ADDED (empty if none were needed) —
`cruise_key`, `ship_key`, `date_ym`, `date_min`, `date_max`,
`cruise_key_datasets`.

## Details

A derived row's `ship_key` is resolved from the key's own NODC segment
(`split_part(cruise_key, '-', 3)`) against `ship_tbl`; a key naming an
NODC `ship_tbl` does not know is an error (a derivation the release
cannot stand behind), collected across every offending key into one
message rather than failing on the first. `date_ym` is the key's own
`YYYY-MM`; `date_min` / `date_max` are the min/max event date of the
sample rows carrying that key (a derived row's span is therefore always
contained in itself — see
[`check_cruise_key_integrity()`](https://calcofi.io/calcofi4db/reference/check_cruise_key_integrity.md)'s
check 6). `cruise_uuid` is left `NULL`: no derived cruise has one, by
definition.

`cruise_tbl` may arrive as a VIEW over static parquet (as it does in
`release_database.qmd`, before the per-cruise enrichment step rebuilds
it as a TABLE) — this materializes it into a TABLE either way, because
it must add both columns and rows.
