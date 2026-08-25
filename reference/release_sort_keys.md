# Sort keys (and partition column) for every released table

The ORDER BY that makes an export deterministic must be a unique total
order: partition column first, then the clustering sort, then the
primary key as the tiebreak. A released table missing from this registry
— and without a primary key in
[`core_relationships()`](https://calcofi.io/calcofi4db/reference/core_relationships.md)
— makes
[`export_release_parquet()`](https://calcofi.io/calcofi4db/reference/export_release_parquet.md)
refuse to write, rather than write non-deterministically.

## Usage

``` r
release_sort_keys(
  core_sort = c("grid_key NULLS LAST", "depth_min_m NULLS LAST", "measurement_type",
    "datetime")
)
```

## Arguments

- core_sort:

  clustering sort for the long observation tables.

## Value

Named list:
`table -> list(partition_by = <col or NULL>, order_by = <chr>)`.
