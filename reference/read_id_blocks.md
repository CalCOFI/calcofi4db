# Read the id-block registry

`metadata/id_block.csv` gives every dataset a number that is never
changed and never reused: `dataset_key, id_block, added`. It is what
lets a surrogate id say which dataset it belongs to, so one dataset
growing cannot renumber another (through v2026.11.25 the ids were one
row number over the whole table, and 17 new CTD casts re-identified
every row of fifteen other datasets).

## Usage

``` r
read_id_blocks(root = ".", path = file.path(root, "metadata", "id_block.csv"))
```

## Arguments

- root:

  workflows repo root (holds `metadata/id_block.csv`)

- path:

  the registry file

## Value

A data frame `dataset_key` (chr), `id_block` (dbl, whole, unique,
1..900000), sorted by `id_block`.
