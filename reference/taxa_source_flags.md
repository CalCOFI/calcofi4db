# The `flags[]` a `taxa.json` source row can carry

A `sources[]` entry is one `dataset_taxon` row — what a dataset calls a
taxon. A flag says how that name or id differs from the accepted one, so
a species page can show a quiet pill beside it rather than two names
with no explanation:

## Usage

``` r
taxa_source_flags()
```

## Value

A character vector, the enum of `taxa.schema.json`.

## Details

- `synonym` — the dataset's `ds_scientific_name` is not the taxon's
  accepted `scientific_name` (*Antennarius avalonis* → *Fowlerichthys
  avalonis*).

- `sp_to_genus` — a `… sp.` / `… spp.` / `… sp A` name that resolved to
  a Genus-rank taxon (mesopelagic "Cyclothone sp." → *Cyclothone*). It
  replaces `synonym`: the name is not wrong, it is less precise.

- `rekeyed` — the id the source supplied for the taxon's **own key
  authority** has since been deprecated there, and the row is keyed to
  the successor. An `itis:` key compares `itis_id`, a `worms:` key
  compares `worms_id`; a dataset-local key is keyed by no authority and
  can never be re-keyed. 27 rows on v2026.09.06, all Farallon
  (`taxon.notes`: "itis:174550 deprecated in ITIS -\> itis:1255048").

- `id_conflict` — the source's id for a **secondary** authority
  disagrees with the key authority's cross-reference: nothing was
  re-keyed, the two authorities' own crosswalks disagree. 27 rows on
  v2026.09.06, all ichthyoplankton — `worms:`-keyed taxa whose source
  ITIS hint differs from the `itis_id` WoRMS publishes as its external
  link (`taxon.notes`: "2026-08-05: itis_id 622362 via WoRMS external
  link"). Until calcofi4db 4.9.0 these were reported as `rekeyed`, which
  said on a page that an id had moved when none had.

- `no_name` — the dataset carries only a code (`ds_scientific_name` is
  NULL).

`rekeyed` and `id_conflict` are computed over the two authorities that
can key a taxon (WoRMS, ITIS). A `gbif_id` is carried in `sources[].ids`
for the reader but never flagged: it keys nothing, and one ichthyo row
disagrees on it alone.

## Examples

``` r
taxa_source_flags()
#> [1] "synonym"     "sp_to_genus" "rekeyed"     "id_conflict" "no_name"    
```
