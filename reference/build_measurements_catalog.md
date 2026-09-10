# Build the measurements catalog record (`measurements.json`)

One entry per measurement key the release's `obs_env` carries — the
registry's `variable` where set, else the `measurement_type` — with its
series (one per `measurement_type × dataset`), each series' counts by
year, calendar month, depth band and quality code, its observed
quantiles, the registry's declared bounds, the source and flag columns,
the NERC ids, and the other keys sharing its P01 concept that are
deliberately kept apart.

## Usage

``` r
build_measurements_catalog(
  con,
  record,
  measurement_type,
  variable = NULL,
  category,
  release_version = NULL,
  release_date = NULL,
  underway_datasets = "calcofi_mets",
  supplemental_tables = c(ctd_raw = "obs_ctd_full", mets_measurement = "obs_mets_full"),
  supplemental_rows = NULL
)
```

## Arguments

- con:

  a DBI connection holding the release table `obs_env` (and, optionally,
  `climatology` and the `obs_*_full` supplementals)

- record:

  the `datasets.json` record — a path or the list from
  [`build_dataset_catalog()`](https://calcofi.io/calcofi4db/reference/build_dataset_catalog.md)
  — read for `datasets[].dataset_name_short`, `color` and `category`,
  and for the catalog order of `datasets[]`

- measurement_type:

  the measurement vocabulary, from
  [`read_measurement_type()`](https://calcofi.io/calcofi4db/reference/read_measurement_type.md)
  (`metadata/measurement_type.csv`)

- variable:

  the label registry (`metadata/variable.csv`): a data frame with at
  least `variable` and `label`. `NULL` (the default) means no registry
  is available and every measurement falls back to its canonical series'
  description with the `no_label` flag.

- category:

  the category registry (`metadata/category.csv`): `category`, `order`,
  `realm`, `icon` (and optionally `key`)

- release_version:

  the release version (default: the record's)

- release_date:

  the release date, `YYYY-MM-DD` (default: the record's)

- underway_datasets:

  dataset keys whose series are underway intakes rather than casts,
  which is what makes a shared P01 `underway_vs_cast`. Named here rather
  than inferred, because nothing in the release states it.

- supplemental_tables:

  named character vector mapping a registry `_source_table` to the
  full-resolution release table its non-released series live in. Only a
  registry row from one of these source tables can be a
  `full_resolution_only[]` row: a row from anywhere else that never
  reaches `obs_env` is simply not released, not "full resolution only".

- supplemental_rows:

  named numeric vector, release table -\> row count, used for any
  `supplemental_tables` entry the connection does not carry (a promoted
  release read through `cc_get_db()` does not attach them). Read it from
  that release's own `catalog.json`; never type it. `full_rows` is
  `counts$obs_env_rows` plus these, and is `NA` when a supplemental is
  neither on the connection nor supplied.

## Value

A list ready for
[`write_measurements_catalog()`](https://calcofi.io/calcofi4db/reference/write_measurements_catalog.md)
/ `jsonlite::write_json(auto_unbox = TRUE)`, validating against
`inst/schema/measurements.schema.json`.

## Details

Everything is read: `obs_env` (and, when present, `climatology` and the
`obs_*_full` supplementals) on `con`, the dataset names, colours,
categories and order from `record` (the `datasets.json` the release has
just written, so a dataset dot cannot disagree between `/datasets/` and
`/measurements/`), and the vocabulary from the registries. Nothing is
authored and nothing is fetched.

Six grouped queries do the counting — never one per key — and every
per-series lookup is a split index, so the builder is linear in the
number of `obs_env` groups.

**The label.** `label` comes from `metadata/variable.csv` when that
registry carries a row for the key. Absent a row it falls back to the
canonical series' registry `description` and the measurement carries the
`no_label` flag: a column note is not a title, and the builder does not
invent one.

## See also

[`write_measurements_catalog()`](https://calcofi.io/calcofi4db/reference/write_measurements_catalog.md),
[`validate_measurements_catalog()`](https://calcofi.io/calcofi4db/reference/validate_measurements_catalog.md),
[`check_measurements_catalog()`](https://calcofi.io/calcofi4db/reference/check_measurements_catalog.md),
[`measurement_series_flags()`](https://calcofi.io/calcofi4db/reference/measurement_series_flags.md)
