# Combine a CTD sensor pair by the provider's quality flags

The rule, in precedence order:

1.  a sensor flagged **8** (questionable) or **9** (bad) is dropped;

2.  a **1** (use primary) on either sensor selects sensor 1 alone, a
    **2** (use secondary) selects sensor 2 alone — when both say the
    same thing; if they disagree (one says 1, the other 2) neither is
    trusted over the other and the mean is used;

3.  otherwise the **mean** of the two; one alone when the other is
    `NULL`; `NULL` when neither survives.

## Usage

``` r
combine_sensor_pair(v1, v2, q1 = NA, q2 = NA)

combine_sensor_pair_sql(v1, v2, q1, q2)
```

## Arguments

- v1, v2:

  numeric: the two sensors' values (sensor 1 = primary).

- q1, q2:

  their quality codes (character or numeric; `NA` = good).

## Value

`combine_sensor_pair()`: a numeric vector; `combine_sensor_pair_sql()`:
a length-one character SQL expression.

## Details

Flags are compared as the source writes them — `"8"`, `"8.0"`, `8`, `NA`
and `""` all mean what the vocabulary says — so a consumer can pass
`measurement_qual` straight from `obs`. `combine_sensor_pair_sql()`
returns the same rule as one SQL expression over four column names or
expressions, for use in a DuckDB query (the transect builder, the
Explorer's SQL).

## Examples

``` r
combine_sensor_pair(c(10, 10, 10, 10, NA, 10), c(12, 12, 12, 12, 12, 12),
                    q1 = c(NA, "8", "1", "2", NA, "9"), q2 = c(NA, NA, NA, NA, NA, "9.0"))
#> [1] 11 12 10 12 12 NA
# 11 10 10 12 12 NA
combine_sensor_pair_sql("salinity_1_corr", "salinity_2_corr", "q1", "q2")
#> [1] "CASE WHEN COALESCE((regexp_replace(CAST(q1 AS VARCHAR), '\\\\.0+$', '') = '1' OR regexp_replace(CAST(q2 AS VARCHAR), '\\\\.0+$', '') = '1'), FALSE) AND NOT COALESCE((regexp_replace(CAST(q1 AS VARCHAR), '\\\\.0+$', '') = '2' OR regexp_replace(CAST(q2 AS VARCHAR), '\\\\.0+$', '') = '2'), FALSE) AND (CASE WHEN regexp_replace(CAST(q1 AS VARCHAR), '\\\\.0+$', '') IN ('8', '9') THEN NULL ELSE salinity_1_corr END) IS NOT NULL THEN (CASE WHEN regexp_replace(CAST(q1 AS VARCHAR), '\\\\.0+$', '') IN ('8', '9') THEN NULL ELSE salinity_1_corr END)\nWHEN COALESCE((regexp_replace(CAST(q1 AS VARCHAR), '\\\\.0+$', '') = '2' OR regexp_replace(CAST(q2 AS VARCHAR), '\\\\.0+$', '') = '2'), FALSE) AND NOT COALESCE((regexp_replace(CAST(q1 AS VARCHAR), '\\\\.0+$', '') = '1' OR regexp_replace(CAST(q2 AS VARCHAR), '\\\\.0+$', '') = '1'), FALSE) AND (CASE WHEN regexp_replace(CAST(q2 AS VARCHAR), '\\\\.0+$', '') IN ('8', '9') THEN NULL ELSE salinity_2_corr END) IS NOT NULL THEN (CASE WHEN regexp_replace(CAST(q2 AS VARCHAR), '\\\\.0+$', '') IN ('8', '9') THEN NULL ELSE salinity_2_corr END)\nWHEN (CASE WHEN regexp_replace(CAST(q1 AS VARCHAR), '\\\\.0+$', '') IN ('8', '9') THEN NULL ELSE salinity_1_corr END) IS NOT NULL AND (CASE WHEN regexp_replace(CAST(q2 AS VARCHAR), '\\\\.0+$', '') IN ('8', '9') THEN NULL ELSE salinity_2_corr END) IS NOT NULL THEN ((CASE WHEN regexp_replace(CAST(q1 AS VARCHAR), '\\\\.0+$', '') IN ('8', '9') THEN NULL ELSE salinity_1_corr END) + (CASE WHEN regexp_replace(CAST(q2 AS VARCHAR), '\\\\.0+$', '') IN ('8', '9') THEN NULL ELSE salinity_2_corr END)) / 2\nELSE COALESCE((CASE WHEN regexp_replace(CAST(q1 AS VARCHAR), '\\\\.0+$', '') IN ('8', '9') THEN NULL ELSE salinity_1_corr END), (CASE WHEN regexp_replace(CAST(q2 AS VARCHAR), '\\\\.0+$', '') IN ('8', '9') THEN NULL ELSE salinity_2_corr END)) END"
```
