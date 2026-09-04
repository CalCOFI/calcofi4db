# Encode an authority-prefixed `taxon_key`

The single rule for minting the global taxon key, stated once:

## Usage

``` r
taxon_key_of(worms_id, itis_id = NA_integer_, class = NA_character_)
```

## Arguments

- worms_id:

  integer WoRMS AphiaID(s) (NA where unknown)

- itis_id:

  integer ITIS TSN(s) (NA where unknown)

- class:

  character; the taxon's class from the lineage (`"Aves"` selects the
  `itis:` authority). NA means "not known to be Aves".

## Value

character vector of `taxon_key`s (NA where no authority id resolves)

## Details

A taxon keys itis:\<tsn\> exactly when its class is Aves and an
acceptedTSN resolves; otherwise worms:\<aphia\>; otherwise NA, which
callersturn into the dataset-local fallback \<dataset_key\>:\<code\>
thatcheck_taxon_ids() refuses unless allow-listed.

The class is a fact from the authority's classification (staged by
[`ensure_taxon_lineage()`](https://calcofi.io/calcofi4db/reference/ensure_taxon_lineage.md)),
not a flag a source declares: only one dataset ever carried an `is_bird`
column, so Aves reaching the release through any other dataset would
have keyed `worms:` and one species could have carried two keys. Birds
key on ITIS because WoRMS bird taxonomy lags (it still says
*Oceanodroma*, *Puffinus*, *Phalacrocorax*). A bird with no accepted TSN
keys `worms:` and gets a note in `taxon.notes` — visible, not silent.
All prefixes are lowercase. Vectorized over the ids; `class` recycles.

## Examples

``` r
taxon_key_of(217452L, 161729L)                       # "worms:217452"  (Pacific sardine)
#> [1] "worms:217452"
taxon_key_of(137179L, 174715L, class = "Aves")       # "itis:174715"   (Great Cormorant)
#> [1] "itis:174715"
taxon_key_of(137179L, NA_integer_, class = "Aves")   # "worms:137179"  (a bird with no TSN)
#> [1] "worms:137179"
```
