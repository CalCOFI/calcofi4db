# SQL that widens long `obs` rows into one column per measurement type

The database keeps every quantity in a single `measurement_value`
column, which CF forbids — a variable has one unit and one
`standard_name`. This builds the pivot, at the **occurrence grain**
rather than the event grain.

## Usage

``` r
obs_wide_sql(
  dataset_key,
  measurement_types,
  obs_tbl = "obs",
  grain = c("sample_key", "depth_min_m", "taxon_key", "life_stage"),
  carry = character(),
  order_by = grain,
  count_col = NULL,
  value_col = "measurement_value"
)
```

## Arguments

- dataset_key:

  Dataset provenance stamp.

- measurement_types:

  Character vector of types to widen into columns, normally
  `plan_dataset_netcdf()$measurement_types` (the union across all
  partitions).

- obs_tbl:

  Table or view to read.

- grain:

  Columns defining one output row. **The default includes `taxon_key`
  and `life_stage` deliberately** — see Details.

- carry:

  Columns functionally determined by `sample_key` (position, time,
  `cruise_key`, `grid_key`) to carry through with `any_value()`.

- order_by:

  Columns for the `ORDER BY`; defaults to `grain`. A contiguous ragged
  array requires the instance column to sort first.

- count_col:

  Optional name for a `COUNT(*)` column, so the caller can see how many
  long rows collapsed into each wide row and assert that none were
  silently dropped.

- value_col:

  Column holding the measured value.

## Value

A single SQL string.

## Details

**Why the grain includes `taxon_key`.** For a biological dataset `obs`
is one row per (event, taxon, life stage, measurement). Grouping by
`sample_key` alone therefore collapses every taxon in a sample into one
row: on `cce-lter_zooscan` that turns 34,109 occurrences over 23 taxa
into 1,483 rows and loses 96% of the data, with `MAX()` silently
choosing one taxon's value. The loss is invisible in the output — the
file is well-formed and the variables have plausible values.

A `measurement_type` outside `[A-Za-z0-9_]`, or colliding with a
coordinate name the writers create, is rejected rather than quoted: both
are also invalid or ambiguous as netCDF variable names.

## Examples

``` r
obs_wide_sql("cce-lter_zooscan", c("abundance", "biomass"),
             carry = c("latitude", "longitude"))
#> SELECT sample_key, depth_min_m, taxon_key, life_stage,
#>     any_value(latitude) AS latitude,
#>     any_value(longitude) AS longitude,
#>     MAX(measurement_value) FILTER (WHERE measurement_type = 'abundance')::DOUBLE AS "abundance",
#>     MAX(measurement_value) FILTER (WHERE measurement_type = 'biomass')::DOUBLE AS "biomass"
#> FROM obs
#> WHERE dataset_key = 'cce-lter_zooscan'
#> GROUP BY sample_key, depth_min_m, taxon_key, life_stage
#> ORDER BY sample_key, depth_min_m, taxon_key, life_stage
```
