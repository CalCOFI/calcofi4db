# Standardize Species Identifiers Using WoRMS/ITIS/GBIF APIs

For each species in the species table, queries WoRMS (via `worrms`
package) to get the current accepted AphiaID, then queries ITIS (via
`taxize`) for the ITIS TSN, and optionally GBIF for the GBIF backbone
key. Updates the species table with canonical identifiers.

## Usage

``` r
standardize_species(
  con,
  species_tbl = "species",
  id_col = "species_id",
  sci_name_col = "scientific_name",
  update_in_place = TRUE,
  include_gbif = TRUE,
  batch_size = 50
)
```

## Arguments

- con:

  DBI connection to DuckDB with species table

- species_tbl:

  character; name of species table (default "species")

- id_col:

  character; name of species ID column (default "species_id")

- sci_name_col:

  character; column with scientific names (default "scientific_name")

- update_in_place:

  logical; if TRUE, UPDATE the table directly (default TRUE)

- include_gbif:

  logical; if TRUE, also query GBIF backbone (default TRUE)

- batch_size:

  integer; number of species per API batch (default 50)

## Value

tibble with species_id, scientific_name, worms_id (AphiaID), itis_id
(TSN), gbif_id, taxonomic_status, accepted_name

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_duckdb_con("calcofi.duckdb")
sp_results <- standardize_species(con)
} # }
```
