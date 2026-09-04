# Tag the hand-picked rows of the registry as `source = "manual"`

The registry never carried a literal `"manual"`: every fetched row was
stamped `source = "worms"` whether the value was auto-filled or
hand-picked. The two are told apart by construction —
[`ensure_taxon_common()`](https://calcofi.io/calcofi4db/reference/ensure_taxon_common.md)
only ever auto-fills the one candidate WoRMS offered — so a filled row
whose value is not that single candidate was necessarily a human edit.
This writes that reconstruction into the registry once (it is
idempotent), and
[`ensure_taxon_common()`](https://calcofi.io/calcofi4db/reference/ensure_taxon_common.md)
keeps the tag from then on.
[`apply_taxon_common()`](https://calcofi.io/calcofi4db/reference/apply_taxon_common.md)
reads it as rank 1 of the precedence.

## Usage

``` r
mark_taxon_common_manual(cache_csv, verbose = TRUE)
```

## Arguments

- cache_csv:

  path to the registry

- verbose:

  report how many rows were tagged

## Value

the number of rows newly tagged `manual`, invisibly
