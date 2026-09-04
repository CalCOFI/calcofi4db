# `obs` as a view over `obs_bio` + `obs_env`

The UNION ALL that reconstructs `obs` — its 18 columns, in
[OBS_VIEW_COLUMNS](https://calcofi.io/calcofi4db/reference/OBS_VIEW_COLUMNS.md)
order, under their original names — from the bifurcated pair
(pre-release plan D-S1): `realm` is the constant each branch
contributes, `value` becomes `measurement_value`. The default sources
are the **tokens** `{{obs_bio}}` / `{{obs_env}}`, which is how the SQL
is stored in a release's `catalog.json` (`views.obs`): every resolver —
`calcofi4r::cc_get_db()`, `calcofi4py.cc_get_db()`, db-query's
`__TBL:obs__` — substitutes its own way of reading each table
([`substitute_view_tables()`](https://calcofi.io/calcofi4db/reference/release_view_tables.md)),
a quoted table name inside a connection or a `read_parquet(...)` over
the catalog's objects.

## Usage

``` r
obs_view_sql(bio = "{{obs_bio}}", env = "{{obs_env}}")
```

## Arguments

- bio, env:

  what to put after `FROM` for each realm: a token, a quoted table name,
  or a `read_parquet(...)` expression.

## Value

A length-one SQL string (no trailing semicolon; wrap in parentheses to
use it in a `FROM`).

## Examples

``` r
cat(obs_view_sql())
#> SELECT obs_id, 'bio' AS realm, dataset_key, sample_key, grid_key, cruise_key,
#>        latitude, longitude, datetime, depth_min_m, depth_max_m,
#>        taxon_key, life_stage, measurement_type, value AS measurement_value,
#>        measurement_qual, measurement_prec, hex_id
#> FROM {{obs_bio}}
#> UNION ALL
#> SELECT obs_id, 'env' AS realm, dataset_key, sample_key, grid_key, cruise_key,
#>        latitude, longitude, datetime, depth_min_m, depth_max_m,
#>        taxon_key, life_stage, measurement_type, value AS measurement_value,
#>        measurement_qual, measurement_prec, hex_id
#> FROM {{obs_env}}
cat(obs_view_sql('"obs_bio"', '"obs_env"'))
#> SELECT obs_id, 'bio' AS realm, dataset_key, sample_key, grid_key, cruise_key,
#>        latitude, longitude, datetime, depth_min_m, depth_max_m,
#>        taxon_key, life_stage, measurement_type, value AS measurement_value,
#>        measurement_qual, measurement_prec, hex_id
#> FROM "obs_bio"
#> UNION ALL
#> SELECT obs_id, 'env' AS realm, dataset_key, sample_key, grid_key, cruise_key,
#>        latitude, longitude, datetime, depth_min_m, depth_max_m,
#>        taxon_key, life_stage, measurement_type, value AS measurement_value,
#>        measurement_qual, measurement_prec, hex_id
#> FROM "obs_env"
```
