# Match Records to the Nearest Reference Row Along a Continuous Axis

Adds a foreign-key column to `data_tbl` and populates it with the
primary key of the `ref_tbl` row that is nearest along a continuous axis
(e.g. `depth_m`) within `tolerance`, restricted to rows sharing an
already-matched parent FK (e.g. `cast_id`). Use after
[`match_by_site_datetime()`](https://calcofi.io/calcofi4db/reference/match_by_site_datetime.md)
to descend from a parent match (cast) to a child match (Niskin bottle).
Extracted from `ingest_calcofi_dic.qmd`.

## Usage

``` r
match_nearest_by_depth(
  con,
  data_tbl,
  ref_tbl,
  fk_col = "bottle_id",
  ref_pk = "bottle_id",
  parent_fk = "cast_id",
  axis_col = "depth_m",
  tolerance = 1,
  return_stats = TRUE
)
```

## Arguments

- con:

  DBI connection to DuckDB.

- data_tbl:

  Character. Table to add the FK column to (e.g. "dic_sample").

- ref_tbl:

  Character. Reference table to match against (e.g. "bottle").

- fk_col:

  Character. FK column to create on `data_tbl` (default "bottle_id").

- ref_pk:

  Character. Primary-key column on `ref_tbl` (default "bottle_id").

- parent_fk:

  Character. Parent FK present on both tables that scopes the match
  (default "cast_id"); only rows with a non-NULL parent are matched.

- axis_col:

  Character. Continuous column minimized in absolute difference (default
  "depth_m").

- tolerance:

  Numeric. Maximum absolute difference on `axis_col` (default 1.0).

- return_stats:

  Logical. If TRUE (default), return a stats list; otherwise return
  invisible NULL.

## Value

If `return_stats`, a list with `matched`, `eligible` (rows with a
non-NULL parent), and `pct`. Side effect: adds and populates `fk_col`.

## Examples

``` r
if (FALSE) { # \dontrun{
match_by_site_datetime(con, "dic_sample", "casts")
match_nearest_by_depth(con, "dic_sample", "bottle")
} # }
```
