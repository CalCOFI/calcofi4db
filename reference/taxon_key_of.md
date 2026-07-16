# Encode an authority-prefixed `taxon_key`

The single rule for minting the global taxon key: **`worms:<worms_id>`
for all taxa, except birds (`is_bird = TRUE`, i.e. class Aves) which key
on `itis:<itis_id>`**. Falls back to `itis:` when no `worms_id` is
present, and to `NA` when neither id resolves (callers then apply a
dataset-local key). All prefixes are lowercase. Vectorized.

## Usage

``` r
taxon_key_of(worms_id, itis_id = NA_integer_, is_bird = FALSE)
```

## Arguments

- worms_id:

  integer WoRMS AphiaID(s) (NA where unknown)

- itis_id:

  integer ITIS TSN(s) (NA where unknown)

- is_bird:

  logical; TRUE for class Aves, forcing the `itis:` prefix

## Value

character vector of `taxon_key`s (NA where neither id resolves)

## Examples

``` r
taxon_key_of(217452L, 161729L)              # "worms:217452"  (Pacific sardine)
#> [1] "worms:217452"
taxon_key_of(137179L, 174715L, is_bird = TRUE)  # "itis:174715"  (Great Cormorant)
#> [1] "itis:174715"
```
