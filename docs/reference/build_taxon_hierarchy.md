# Build Taxon Hierarchy from Local spp.duckdb via Recursive CTEs

Builds the `taxon` and `taxa_rank` tables using recursive CTEs against
the local MarineSensitivity species database (`spp.duckdb`). This is
much faster than
[`build_taxon_table()`](https://calcofi.io/calcofi4db/reference/build_taxon_table.md)
which queries WoRMS/ITIS APIs for each species.

## Usage

``` r
build_taxon_hierarchy(
  con,
  spp_db_path,
  species_tbl = "species",
  overwrite = FALSE
)
```

## Arguments

- con:

  DBI connection to DuckDB with a species table containing worms_id and
  itis_id columns

- spp_db_path:

  Path to the MarineSensitivity spp.duckdb file

- species_tbl:

  Character; name of species table (default "species")

- overwrite:

  Logical; if TRUE, rebuild even if taxon table exists (default FALSE)

## Value

Tibble of taxon hierarchy rows written to con

## Details

Creates both WoRMS and ITIS hierarchies by walking up the
`parentNameUsageID` chain from each species' WoRMS/ITIS ID.

## Examples

``` r
if (FALSE) { # \dontrun{
con <- get_duckdb_con("calcofi.duckdb")
taxon <- build_taxon_hierarchy(
  con         = con,
  spp_db_path = "/path/to/spp.duckdb")
} # }
```
