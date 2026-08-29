# Declare `category` / `variable` on measurement types that already exist

The two descriptive columns the CalCOFI Explorer's *Browse* tab reads
(explorer UI plan D14): `category` — one of the registered categories
(`metadata/category.csv`: *Physical Oceanography*, *Nutrients &
Chemistry*, *Carbonate System*, *Productivity & Pigments*, *Meteorology
& Sea State*, …) — and `variable`, the crosswalk that says which types
measure the same thing comparably across datasets (`temperature` for the
bottle's `temperature` and the CTD's `temperature_ave`), which the
explorer carried in `src/variables.ts` as a stopgap. Like
[`declare_measurement_bounds()`](https://calcofi.io/calcofi4db/reference/declare_measurement_bounds.md)
it changes **only** these columns, only on rows that already exist,
refuses an unknown `measurement_type`, and writes with `na = ""`. A
registry predating the columns gains them.

## Usage

``` r
declare_measurement_fields(
  fields,
  path,
  categories = NULL,
  overwrite = FALSE,
  quiet = FALSE
)
```

## Arguments

- fields:

  data.frame with `measurement_type` and at least one of `category`,
  `variable`. `NA` leaves that field as it is.

- path:

  path to `metadata/measurement_type.csv`

- categories:

  the allowed `category` values — the `category` column of
  `metadata/category.csv`; `NULL` skips the check (not recommended)

- overwrite:

  allow replacing a value that is already declared (default FALSE)

- quiet:

  suppress the summary message

## Value

The full updated registry, invisibly if nothing changed.

## See also

[`build_coverage()`](https://calcofi.io/calcofi4db/reference/build_coverage.md),
which puts both onto `coverage.json`'s `variables[]`.
