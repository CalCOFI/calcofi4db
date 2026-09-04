# Declare `category` / `variable` / `derivation` / `is_canonical` / NERC ids on measurement types that already exist

Six descriptive columns, none of them an ingest's own definition:

- `category` — one of the registered categories
  (`metadata/category.csv`: *Physical Oceanography*, *Nutrients &
  Chemistry*, *Carbonate System*, *Productivity & Pigments*,
  *Meteorology & Sea State*, …), read by the CalCOFI Explorer's *Browse*
  tab (explorer UI plan D14).

- `variable` — the crosswalk that says which types measure the same
  thing comparably across datasets (`temperature` for the bottle's
  `temperature` and the CTD's `temperature_ave`), which the explorer
  carried in `src/variables.ts` as a stopgap.

- `derivation` — free text saying how a *derived* type was produced (the
  `_cruise_corr` vs `_sta_corr` distinction, or that a pre-QC `r_*` type
  is "interpolated to standard depth and carries no quality code by
  design").

- `is_canonical` — whether the type reaches the default `obs`/`ctd_thin`
  selection; a provider-confirmed fact like "the bottle's `r_*` series
  are interpolated, so they are not canonical" belongs here, not in an
  ingest's own literal.

- `nerc_p01` — the NERC BODC Parameter Usage Vocabulary (P01) concept
  URI that a DwC/OBIS eMoF export emits as `measurementTypeID`.

- `units_nerc_p06` — the NERC P06 unit concept URI, emitted as
  `measurementUnitID`.

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
  `variable`, `derivation`, `is_canonical`, `nerc_p01`, `units_nerc_p06`
  ([`declarable_measurement_fields()`](https://calcofi.io/calcofi4db/reference/declarable_measurement_fields.md)).
  `NA` leaves that field as it is.

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

## Details

The two vocabulary columns are validated against
[`nerc_uri_prefixes()`](https://calcofi.io/calcofi4db/reference/nerc_uri_prefixes.md):
a value must be a full concept URI in the right collection
(`.../collection/P01/current/<CODE>/`). They are filled **only on an
exact vocabulary match** — a concept every one of whose stated facets
(quantity, matrix, phase, method) the registry or the dataset's
documented protocol actually supplies. A generic concept is an exact
match at coarser specificity (`TEMPPR01`, *Temperature of the water
body*); a concept that adds a facet nobody recorded is not. So an empty
cell means "no concept says exactly this", never "not looked at", and
inventing one to fill the column is the same mistake as inventing a
bound to quiet
[`check_measurement_bounds()`](https://calcofi.io/calcofi4db/reference/check_measurement_bounds.md).

Like
[`declare_measurement_bounds()`](https://calcofi.io/calcofi4db/reference/declare_measurement_bounds.md)
this changes **only** these columns, only on rows that already exist,
refuses an unknown `measurement_type`, and writes with `na = ""`. A
registry predating a column gains it.

## See also

[`build_coverage()`](https://calcofi.io/calcofi4db/reference/build_coverage.md),
which puts `category`/`variable` onto `coverage.json`'s `variables[]`.
