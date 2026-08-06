# Normalize a source taxon name for an authority lookup

Strips the open-nomenclature and qualifier noise that source
spreadsheets carry in their column headers and species lists, so the
name reaches WoRMS in a form it can match. `"Bathophilus sp."` becomes
`"Bathophilus"` (WoRMS holds the genus, never the `sp.` form),
`"Phaeocystis cf pouchetti"` becomes `"Phaeocystis pouchetti"`,
`"indistinguished Pterosperma spp."` becomes `"Pterosperma"`.

## Usage

``` r
clean_taxon_name(x)
```

## Arguments

- x:

  character vector of source names

## Value

character vector of cleaned names (NA in, NA out)

## Details

**Use the result as the lookup query only — never as `ds_taxa_code`.**
For `sio_mesopelagic-fish` the local code *is* the verbatim column
header and is the join key from `obs`; rewriting it would silently
orphan every observation.

This generalizes the hand-maintained `name_query` column that
`metadata/calcofi/phytoplankton/taxon_worms.csv` already carries for one
dataset.

## Examples

``` r
clean_taxon_name(c("Bathophilus sp.", "Phaeocystis cf pouchetti",
                   "indistinguished Pterosperma spp.", "Uria aalge"))
#> [1] "Bathophilus"           "Phaeocystis pouchetti" "Pterosperma"          
#> [4] "Uria aalge"           
```
