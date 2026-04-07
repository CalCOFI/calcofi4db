# Standardize Species Using Local spp.duckdb Lookups

Updates the species table with WoRMS AphiaID, ITIS TSN, and GBIF
backbone key using fast SQL joins against a local MarineSensitivity
species database (`spp.duckdb`). Falls back to the WoRMS API only for
species not found locally.

## Usage

``` r
standardize_species_local(
  con,
  spp_db_path,
  species_tbl = "species",
  overwrite = FALSE,
  api_fallback = TRUE
)
```

## Arguments

- con:

  DBI connection to DuckDB with a species table

- spp_db_path:

  Path to the MarineSensitivity spp.duckdb file

- species_tbl:

  Character; name of species table (default "species")

- overwrite:

  Logical; if TRUE, re-standardize even if columns already populated
  (default FALSE)

- api_fallback:

  Logical; if TRUE, query WoRMS API for species not found locally
  (default TRUE)

## Value

Tibble with species_id, scientific_name, worms_id, itis_id, gbif_id

## Details

This is much faster than
[`standardize_species()`](https://calcofi.io/calcofi4db/reference/standardize_species.md)
which queries external APIs for every species. Intended for use in
ingest workflows where the local spp.duckdb is available.

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_duckdb_con("calcofi.duckdb")
sp <- standardize_species_local(
  con         = con,
  spp_db_path = "/path/to/spp.duckdb")
} # }
```
