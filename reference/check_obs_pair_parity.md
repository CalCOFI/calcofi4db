# Assert that `obs_bio` + `obs_env` hold exactly the rows of `obs`

The gate behind D-S1: before `obs` can be served as
[`obs_view_sql()`](https://calcofi.io/calcofi4db/reference/obs_view_sql.md)
over the pair, the pair must reproduce it. Per `(realm, dataset_key)`
this compares the row count, the number of distinct `obs_id`s and an
order-independent signature (`bit_xor(hash(...))`) of every column
except depth between `obs` and the view run over the pair, and — joining
the two on `obs_id` — counts the rows whose depth the pair **filled**
(NULL in `obs`, the sample's span in the pair: the documented fallback
of
[`build_obs_slim()`](https://calcofi.io/calcofi4db/reference/build_obs_slim.md))
and the rows whose non-NULL depth it **changed** (never allowed). Any
group on one side only, any count / signature mismatch, or any changed
depth is an error naming the group.

## Usage

``` r
check_obs_pair_parity(con, obs = "obs", bio = "obs_bio", env = "obs_env")
```

## Arguments

- con:

  DuckDB connection holding `obs`, `bio` and `env`.

- obs, bio, env:

  table names.

## Value

Invisibly, a tibble with one row per `(realm, dataset_key)`: `n_obs`,
`n_pair`, `n_id_pair`, `sig_ok`, `n_depth_filled`, `n_depth_changed`,
`ok`.
