# Correct a source's ship code on one cruise only

A source can name the wrong ship for a single cruise while naming that
ship correctly on every other cruise. A global rename
(`ship_renames.csv`, by name and code) cannot fix that without breaking
the cruises it already gets right. The CalCOFI bottle database and the
picoplankton `studyName` call the July 2009 cruise `2009-07-14-C-31M4`
(MCARTHUR, which left service in 2003), while every other dataset and
the SWFSC reference call it `2009-07-33MH` (MCARTHUR II), so the bottle
casts were released on a `derived` cruise of their own and joined to
nothing. Cruise 198502 is a real 31M4 cruise.

## Usage

``` r
correct_cruise_ship(
  con,
  table_name,
  corrections_csv,
  dataset_key,
  cruise_ym_col = "cruise",
  ship_name_col = NULL
)
```

## Arguments

- con:

  DBI connection holding `table_name`.

- table_name:

  Table with a `ship_code` column and `cruise_ym_col`.

- corrections_csv:

  Path to the registry, with columns `ship_code`, `cruise_ym`,
  `ship_code_new`, `dataset_key` and `evidence`.

- dataset_key:

  This ingest's dataset key. Only rows listing it, or listing no
  dataset, apply.

- cruise_ym_col:

  Column holding the source's own cruise designation as YYYYMM
  (character or number; compared as an integer).

- ship_name_col:

  Optional column to set to `ship_name_new` where the registry gives one
  (default `NULL`, so no name is changed).

## Value

A tibble of the applied rows with `n_updated`.

## Details

Each row of `corrections_csv` names a source ship code and a source
cruise designation (`cruise_ym`, YYYYMM), plus the corrected code, and
the rows of `table_name` holding both get the corrected `ship_code`. Run
it before
[`derive_cruise_key_on_casts()`](https://calcofi.io/calcofi4db/reference/derive_cruise_key_on_casts.md).
A row that lists `dataset_key` (`;`-separated, or empty for every
dataset) and matches nothing in that table is an error. A correction
that no longer applies should be deleted from the registry, not left to
pass silently.
