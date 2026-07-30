# Discover a dataset's sampling hierarchy from the core `sample` table

Walks the `parent_sample_key` adjacency list to recover the sampling
levels and their nesting order, without any per-dataset configuration.
This is the generic replacement for the hand-written level lists in the
per-dataset `publish_*_to-netcdf.qmd` notebooks.

## Usage

``` r
discover_sample_levels(con, dataset_key)
```

## Arguments

- con:

  DuckDB connection carrying a core `sample` table

- dataset_key:

  Dataset provenance stamp, e.g. `"swfsc_ichthyo"`

## Value

A tibble, one row per `sample_type`, ordered root-first:

- sample_type:

  the level name

- n:

  rows at this level

- parent_sample_type:

  the parent level, or `NA` for a root

- depth:

  0 for a root, 1 for its children, …

- n_orphan:

  rows whose `parent_sample_key` does not resolve

Returns a zero-row tibble when the dataset has no `sample` rows.

## Details

Levels are returned in **topological order** (roots first), which is the
order a netCDF-4 file must define them in: a child's `parent_index`
points into its parent's dimension, so the parent must already exist.

A level's parent is determined by majority vote over the resolved
parents of its rows: `sample_type` is a categorical label, and a single
mislabelled row should not invent a whole extra level. Rows whose parent
does not resolve are counted in `n_orphan` rather than dropped, because
an orphan is a data problem the caller must see — silently discarding it
is how a level's row count stops matching the table it came from.

## Examples

``` r
if (FALSE) { # \dontrun{
con <- cc_get_db()
discover_sample_levels(con, "swfsc_ichthyo")
#> sample_type n      parent_sample_type depth n_orphan
#> site        13108  NA                     0        0
#> tow         26216  site                   1        0
#> net         52432  tow                    2        0
} # }
```
