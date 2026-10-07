# Diff an ingest's staged output against a published release

The dry run behind the rule "a data fix is diffed against the release
before it is re-staged" (workflows `measurement-bounds` skill): for
every measurement-bearing table an ingest stages (`obs`, `obs_ctd_full`,
`sample_measurement`), compares each value with the same value in a
release and counts, **per table x `measurement_type`**, the rows added,
removed, filled, blanked, changed and unchanged, the rows whose
`measurement_qual` changed, the largest absolute change and the
duplicate keys on each side. Every measurement type present on either
side gets a row, and so does every type named in `measurement_type` that
is present on neither (all zeros), so a series cannot drop out of the
breakdown unseen.

## Usage

``` r
diff_stage_vs_release(
  dataset_key,
  release = "latest",
  stage_dir = cc_stage_path("parquet", dataset_key),
  tables = names(DIFF_STAGE_KEYS),
  cruise_key = NULL,
  measurement_type = NULL,
  tolerance = 1e-09,
  max_rows = 10000,
  chunk_rows = 2e+07,
  con = NULL,
  verbose = FALSE
)

diff_stage_vs_release_rows(d)
```

## Arguments

- dataset_key:

  the dataset, e.g. `"calcofi_ctd-cast"`

- release:

  a local directory of release parquet, a catalog list
  ([`calcofi4r::cc_catalog()`](https://calcofi.io/calcofi4r/reference/cc_catalog.html))
  or a version string resolved through `calcofi4r` (default `"latest"`)

- stage_dir:

  the ingest's stage directory (default
  `cc_stage_path("parquet", dataset_key)`)

- tables:

  measurement-bearing tables to diff (default all of
  `names(DIFF_STAGE_KEYS)`); a table on neither side is skipped, one on
  one side only is diffed against nothing

- cruise_key:

  optional character: restrict both sides to these cruises
  (`sample_measurement`, which has no `cruise_key`, through its samples'
  cruise in the stage `sample` / `obs` and the release `sample`)

- measurement_type:

  optional character: restrict both sides to these types

- tolerance:

  absolute tolerance below which two numbers count as `same` (default
  `1e-9`)

- max_rows:

  how many differing rows to keep in `attr(, "rows")`, largest change
  first (default 10000; 0 keeps none)

- chunk_rows:

  a table larger than this is diffed in batches of cruises of about this
  many rows (default 2e7)

- con:

  optional DuckDB connection; by default one from
  [`get_duckdb_con()`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md)
  with `memory_limit = "3GB"`, `threads = 2`, closed on exit

- verbose:

  print one line per table / batch (default `FALSE`)

- d:

  the result of `diff_stage_vs_release()`

## Value

a tibble, one row per table x `measurement_type`, ordered by both:
`table`, `measurement_type`, `n_release`, `n_stage`, `n_unchanged`,
`n_added`, `n_removed`, `n_filled`, `n_blanked`, `n_nan_null`,
`n_changed`, `n_qual_changed`, `max_abs_change` (largest
`|stage - release|` among `changed`, `NA` when none),
`n_dup_keys_release`, `n_dup_keys_stage`. Attributes: `rows`, a tibble
of up to `max_rows` differing rows (`table`, the key columns,
`cruise_key`, `status`, `value_release`, `value_stage`, `abs_change`,
`qual_release`, `qual_stage`, `qual_changed`), and `elapsed` (seconds).
`diff_stage_vs_release_rows()` returns the `rows` attribute.

## Details

**Key.** A value is identified by the columns in `DIFF_STAGE_KEYS`: for
`obs` and `obs_ctd_full` `dataset_key`, `sample_key`, `depth_min_m`,
`depth_max_m`, `taxon_key`, `life_stage` and `measurement_type`
(compared with `IS NOT DISTINCT FROM`, so a NULL `taxon_key` matches a
NULL); for `sample_measurement` `dataset_key`, `sample_key` and
`measurement_type`. `obs_id` / `sample_measurement_id` are reassigned at
every staging run and are never compared.

**Duplicates.** A key may hold more than one row
(CalCOFI/workflows#131). The rows of one key are numbered on each side
in order of value then qual and paired by that number, so a key with two
rows in the release and one in the stage counts one pair plus one
`removed`. `n_dup_keys_release` / `n_dup_keys_stage` count the keys
holding more than one row.

**Value status** of a paired row (exactly one): `same` (both NULL, both
NaN, equal, or within `tolerance`), `changed` (both numbers,
`|stage - release| > tolerance`), `filled` (release NULL or NaN, stage a
number), `blanked` (release a number, stage NULL or NaN) or `nan_null`
(NaN on one side, NULL on the other: `NaN` is not `NULL`). An unpaired
row is `added` (stage only) or `removed` (release only).
`n_qual_changed` counts paired rows whose `measurement_qual` differs
(NULL-safe), independently of the value; `n_unchanged` is a paired row
with value `same` **and** the same qual. So
`n_release = n_removed + paired` and `n_stage = n_added + paired`, where
`paired = n_same + n_changed + n_filled + n_blanked + n_nan_null`.

**Sources.** The stage side is `{stage_dir}/{table}.parquet` or the
hive-partitioned `{stage_dir}/{table}/` (default
`cc_stage_path("parquet", dataset_key)`). The release side is a local
directory holding a release's parquet (same layout), a catalog list from
[`calcofi4r::cc_catalog()`](https://calcofi.io/calcofi4r/reference/cc_catalog.html),
or a version string (`"v2026.10.01"`, `"latest"`) resolved through
[`calcofi4r::cc_catalog()`](https://calcofi.io/calcofi4r/reference/cc_catalog.html) +
[`calcofi4r::cc_release_sources()`](https://calcofi.io/calcofi4r/reference/cc_release_sources.html),
never a `releases/{v}/parquet/` path built by hand. The release's `obs`
is its `obs_bio` + `obs_env` pair (`value` read as `measurement_value`),
falling back to a table named `obs` only for a release without the pair;
the release side is always filtered to `dataset_key`.

**Scale.** A table whose larger side exceeds `chunk_rows` is diffed in
batches of whole `cruise_key`s (partition files outside a batch are
never opened), so a 284-million-row `obs_ctd_full` runs inside a 3 GB
DuckDB. A sample whose `cruise_key` itself changed then shows as
`removed` in one batch and `added` in another.

## Examples

``` r
if (FALSE) { # \dontrun{
d <- diff_stage_vs_release(
  "calcofi_ctd-cast",
  release    = "~/_big/calcofi/releases/v2026.10.01/parquet",
  cruise_key = "2026-07-3322")
d
diff_stage_vs_release_rows(d)
} # }
```
