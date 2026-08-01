# Strip the direction suffix from a CTD cast `sample_key`

CTD `sample_key`s end in the cast direction — `…:cast:9802_008d` /
`…:cast:9802_008u` — so a station occupation that logged both directions
is two `sample` rows sharing a base.

## Usage

``` r
qc_cast_base(sample_key)
```

## Arguments

- sample_key:

  character vector of cast keys

## Value

the keys with a single trailing `d`/`u` removed; keys without one are
returned unchanged

## Details

THE OBVIOUS IMPLEMENTATION IS WRONG. `sub("d$", "", x)` is fine, but
`gsub("d", "", x)` or `replace(x, 'd', '')` also eats the `d` in the
`calcofi_ctd-cast` prefix and silently returns a key that matches
nothing. The same trap is called out in `ctd_updown_disagreement.sql`,
which is where it was first hit.

## Examples

``` r
qc_cast_base("calcofi_ctd-cast:cast:9802_008d")
#> [1] "calcofi_ctd-cast:cast:9802_008"
```
