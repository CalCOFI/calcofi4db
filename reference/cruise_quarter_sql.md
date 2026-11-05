# The seasons every CalCOFI anomaly and season filter use: DJF, MAM, JJA, SON

Since calcofi4db 4.34.0 (Rasmus Swalethorp, 2026-10-16: "keep the
quarters matching seasons, so quarter 1 would be Dec-Feb"; agreed
2026-10-27) a season is three calendar months starting in December —
winter `DJF`, spring `MAM`, summer `JJA`, fall `SON` — and **December
counts toward the following year's winter** (Dec 2025 + Jan-Feb 2026 =
winter 2026, the climatology convention, so a winter is never split
across two years). The `quarter` column the release carries on
`climatology`, `obs_env` and `obs_bio` keeps its 1-4 vocabulary with
this meaning: 1 = DJF, 2 = MAM, 3 = JJA, 4 = SON (through v2026.10.30 it
was Q1 Jan-Mar ... Q4 Oct-Dec).

## Usage

``` r
cruise_quarter_sql(month_expr)

cruise_season_sql(month_expr)

cruise_season_year_sql(month_expr, year_expr)

cruise_quarter(month)

CC_SEASONS
```

## Arguments

- month_expr:

  SQL expression giving the cruise's month (1-12).

- year_expr:

  SQL expression giving the calendar year of that month.

- month:

  integer month (1-12), vectorised.

## Value

a character SQL fragment.

## Details

`cruise_quarter_sql()` / `cruise_season_sql()` /
`cruise_season_year_sql()` are the one rule as DuckDB SQL over a month
(and year) expression, so the release, the Explorer's slices and a
notebook cannot disagree; `cruise_quarter()` is the vectorised R twin.

## Examples

``` r
cruise_quarter_sql("month")
#> (CASE WHEN (month) IN (12, 1, 2) THEN 1 WHEN (month) IN (3, 4, 5) THEN 2 WHEN (month) IN (6, 7, 8) THEN 3 WHEN (month) IN (9, 10, 11) THEN 4 END)
cruise_season_year_sql("month_median", "year(date_median)")
#> (CASE WHEN (month_median) = 12 THEN (year(date_median)) + 1 ELSE (year(date_median)) END)::SMALLINT
```
