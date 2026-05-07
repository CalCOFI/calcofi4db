# Build Taxonomic Hierarchy Table from WoRMS

For each unique `worms_id` in the species table, retrieves the full
classification from WoRMS (kingdom, phylum, class, order, family, genus,
species) and stores it in a taxon table. Optionally also fetches ITIS
classification.

## Usage

``` r
build_taxon_table(
  con,
  species_tbl = "species",
  taxon_tbl = "taxon",
  include_itis = TRUE,
  batch_size = 50
)
```

## Arguments

- con:

  DBI connection to DuckDB

- species_tbl:

  character; name of species table with worms_id column

- taxon_tbl:

  character; name for output taxon table (default "taxon")

- include_itis:

  logical; also fetch ITIS classification (default TRUE)

- batch_size:

  integer; number of species per API batch (default 50)

## Value

tibble of taxon hierarchy rows written to con

## Details

Also creates a `taxa_rank` lookup table with rank ordering consistent
with the WoRMS taxonomy hierarchy.

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_duckdb_con("calcofi.duckdb")
taxon_rows <- build_taxon_table(con)
} # }
```
