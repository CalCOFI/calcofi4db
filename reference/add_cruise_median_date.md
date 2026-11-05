# Stamp each cruise's median cast date, month, season and season year

A cruise's month is the calendar month (UTC) of the **median datetime of
its root samples** in the release — the casts, station occupations and
bottle casts that carry its `cruise_key` and a datetime — not the
`YYYY-MM` its key designates. A cruise ID is set before the ship sails
and names sometimes the month the cruise started, sometimes the month it
mostly covered; across the 681 CalCOFI cruises of v2026.10.30, 642
centre on the month of their key and 39 on the month beside it (12
sampled mostly the month before, 27 the month after; 1981-02 JD centres
on 1 March) (Rasmus Swalethorp 2026-10-16, Ben Best 2026-10-20, agreed
2026-10-27). The release writes it once onto the `cruise` reference, and
every month-keyed lookup —
[`build_climatology()`](https://calcofi.io/calcofi4db/reference/build_climatology.md),
[`build_obs_slim()`](https://calcofi.io/calcofi4db/reference/build_obs_slim.md),
the Explorer, ctd-transects and calcofi4r's anomalies — reads it from
there.

## Usage

``` r
add_cruise_median_date(con, cruise_tbl = "cruise", sample_tbl = "sample")
```

## Arguments

- con:

  DBI connection to DuckDB holding `cruise_tbl` and `sample_tbl`.

- cruise_tbl:

  the cruise reference (default `"cruise"`), with `cruise_key`.

- sample_tbl:

  the consolidated sample table (default `"sample"`), with `sample_key`,
  `root_sample_key`, `cruise_key`, `datetime`.

## Value

Invisibly, a tibble with one row per cruise: `cruise_key`, `month_key`
(the key's MM), `date_median`, `month_median`, `season`, `season_year`,
`n_roots` (dated root samples) and `month_changed` (the median month
differs from the key's).

## Details

Writes four columns onto `cruise_tbl` (added if absent): `date_median`
(DATE; NULL for a cruise with no dated sample), `month_median` (TINYINT;
the median date's month, else the key's MM), `season` (`DJF`, `MAM`,
`JJA`, `SON`;
[`cruise_season_sql()`](https://calcofi.io/calcofi4db/reference/cruise_quarter_sql.md))
and `season_year` (SMALLINT; the calendar year of `date_median`, or of
the key, **plus one for December**, so Dec 2025 belongs to winter 2026
with the January and February after it;
[`cruise_season_year_sql()`](https://calcofi.io/calcofi4db/reference/cruise_quarter_sql.md)).

The median is taken over root sample rows
(`sample_key = COALESCE(root_sample_key, sample_key)`) so a cast with 24
bottles counts once; a cruise whose root rows carry no datetime falls
back to the median over all its dated sample rows, and one with none at
all to its key.
