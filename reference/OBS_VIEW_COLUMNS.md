# The 18 columns of `obs`, in order

The public shape of `obs` (v2026.02 → v2026.09) that
[`obs_view_sql()`](https://calcofi.io/calcofi4db/reference/obs_view_sql.md)
reconstructs from `obs_bio` + `obs_env`. Order matters: a consumer that
`UNION`s or reads positionally sees the view exactly as it saw the
table.

## Usage

``` r
OBS_VIEW_COLUMNS
```
