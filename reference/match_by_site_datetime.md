# Match Records to a Reference Table by Key + Datetime Window

Adds a foreign-key column to `data_tbl` and populates it with the
primary key of the nearest-in-time row of `ref_tbl` that shares the same
key column (e.g. `site_key`) and falls within `window_days` of the data
row's datetime. This is the standard cross-dataset bridge for datasets
that lack an explicit cast/cruise FK (issue \#47). Extracted from
`ingest_calcofi_dic.qmd` so every ingest reuses the same logic rather
than re-writing inline SQL.

## Usage

``` r
match_by_site_datetime(
  con,
  data_tbl,
  ref_tbl,
  fk_col = "cast_id",
  ref_pk = "cast_id",
  key_col = "site_key",
  datetime_col = "datetime_start_utc",
  ref_key_col = NULL,
  ref_datetime_col = NULL,
  window_days = 3,
  return_stats = TRUE
)
```

## Arguments

- con:

  DBI connection to DuckDB.

- data_tbl:

  Character. Table to add the FK column to (e.g. "dic_sample").

- ref_tbl:

  Character. Reference table to match against (e.g. "casts").

- fk_col:

  Character. FK column to create on `data_tbl` (default "cast_id").

- ref_pk:

  Character. Primary-key column on `ref_tbl` (default "cast_id").

- key_col:

  Character. Shared key matched exactly on both tables (default
  "site_key").

- datetime_col:

  Character. Timestamp column on `data_tbl`, compared by date (default
  "datetime_start_utc").

- ref_key_col:

  Character. Key column on `ref_tbl` (default: same as `key_col`). Set
  when the reference table names the key differently.

- ref_datetime_col:

  Character. Timestamp column on `ref_tbl` (default: same as
  `datetime_col`). Set when the two tables name their timestamp
  differently — e.g. matching a dataset on `datetime_start_utc` to a
  not-yet- normalized reference still on `datetime_utc`.

- window_days:

  Numeric. Maximum absolute date difference, in days (default 3).

- return_stats:

  Logical. If TRUE (default), return a stats list; otherwise return
  invisible NULL.

## Value

If `return_stats`, a list with `matched`, `total`, `pct`, and an
`unmatched` data frame of distinct unmatched key/date rows. Side effect:
adds and populates `fk_col` on `data_tbl`.

## Examples

``` r
if (FALSE) { # \dontrun{
s <- match_by_site_datetime(con, "dic_sample", "casts")
cat(glue::glue("matched {s$matched}/{s$total} ({s$pct}%)"), "\n")
} # }
```
