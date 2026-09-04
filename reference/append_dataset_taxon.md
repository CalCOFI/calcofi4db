# Stage a dataset's taxon vocabulary in `dataset_taxon` (taxon plan D1)

The ingest declares its vocabulary; the package resolves it. This writes
one row per local taxon into `dataset_taxon` with `taxon_key` **empty**
— filled later, in place, by
[`resolve_dataset_taxon()`](https://calcofi.io/calcofi4db/reference/resolve_dataset_taxon.md)
from the authorities — and replaces any rows the table already holds for
`dataset_key`.

## Usage

``` r
append_dataset_taxon(con, dataset_key, df, ds_prefix = dataset_key)
```

## Arguments

- con:

  a DuckDB connection

- dataset_key:

  `provider_dataset` of the observing dataset (what `obs` joins on)

- df:

  the vocabulary, one row per local taxon (columns above)

- ds_prefix:

  prefix of `ds_taxon_key` (`"<ds_prefix>:<ds_taxa_code>"`); defaults to
  `dataset_key`. `swfsc_ichthyo` uses `"calcofi"`, the shared CalCOFI
  species list.

## Value

(invisibly) the number of rows staged

## Details

The column contract is explicit, and a deviation is a hard stop at
ingest rather than an `NA` at release (which is how dropping `itis_id`
from a species table would have un-keyed every seabird without an error
anywhere):

|  |  |  |
|----|----|----|
| column | required | meaning |
| `ds_taxa_code` | yes; unique; non-NA | the code `obs` stores — verbatim, never cleaned |
| `ds_scientific_name` | yes (NA allowed for an operational class) | the source's name; the lookup query after [`clean_taxon_name()`](https://calcofi.io/calcofi4db/reference/clean_taxon_name.md) |
| `ds_common_name` | no |  |
| `worms_id`, `itis_id`, `gbif_id`, `rank` | no; ids integer | what **the source supplied** — hints to resolution, stored together as `ds_source_json` |

Errors on a missing required column, an unknown column, a duplicate or
NA code, an id that does not coerce to an integer, or an empty frame.

`ds_source_json` is one JSON object of whatever ids / rank the source
supplied (e.g. `{"itis_id":174715}`), `NULL` when it supplied nothing.
It sits beside `taxon.worms_id` / `itis_id` so "what did the source
claim?" can be audited against "what does the authority say?" with
`json_extract(ds_source_json, '$.itis_id')`. The notebook never writes
JSON by hand.

## See also

[`resolve_dataset_taxon()`](https://calcofi.io/calcofi4db/reference/resolve_dataset_taxon.md),
[`check_dataset_taxon()`](https://calcofi.io/calcofi4db/reference/check_dataset_taxon.md)

## Examples

``` r
if (FALSE) { # \dontrun{
append_dataset_taxon(con, "farallon_bird-mammal", d_species |>
  transmute(ds_taxa_code = species, ds_scientific_name = scientific_name,
            ds_common_name = common_name, itis_id))
ensure_taxon_xref(con, mt_taxon, tx_over, cache_csv = here("metadata/taxon_xref.csv"))
ensure_taxon_lineage(con, mt_taxon, tx_over, cache_csv = here("metadata/taxon_lineage.csv"))
resolve_dataset_taxon(con, mt_taxon, tx_over)
} # }
```
