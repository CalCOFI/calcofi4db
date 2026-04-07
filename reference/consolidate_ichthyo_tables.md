# Consolidate Ichthyoplankton Tables into Tidy Format

Transforms 5 separate ichthyoplankton tables (egg, eggstage, larva,
larvastage, larvasize) into a single tidy table with columns: net_uuid,
species_id, life_stage, measurement_type, measurement_value, tally.

## Usage

``` r
consolidate_ichthyo_tables(
  con,
  output_tbl = "ichthyo",
  egg_tbl = "egg",
  eggstage_tbl = "egg_stage",
  larva_tbl = "larva",
  larvastage_tbl = "larva_stage",
  larvasize_tbl = "larva_size",
  larva_stage_vocab = NULL,
  net_id_col = "net_uuid"
)
```

## Arguments

- con:

  DuckDB connection

- output_tbl:

  Name of output table (default: "ichthyo")

- egg_tbl:

  Name of egg totals table (default: "egg")

- eggstage_tbl:

  Name of egg stage table (default: "egg_stage")

- larva_tbl:

  Name of larva totals table (default: "larva")

- larvastage_tbl:

  Name of larva stage table (default: "larva_stage")

- larvasize_tbl:

  Name of larva size table (default: "larva_size")

- larva_stage_vocab:

  Tibble mapping stage text (YOLK, PREF, etc.) to integers

- net_id_col:

  Name of net identifier column in source tables (default: "net_uuid")

## Value

Invisibly returns the connection after creating ichthyo table

## Details

The output retains `net_uuid` as the foreign key to the net table
(source UUIDs are kept as primary identifiers). Use
[`assign_deterministic_uuids()`](https://calcofi.io/calcofi4db/reference/assign_deterministic_uuids.md)
to add an `ichthyo_uuid` primary key column derived from the composite
natural key.

## Examples

``` r
if (FALSE) { # \dontrun{
larva_vocab <- tibble::tibble(
  stage_txt = c("YOLK", "PREF", "FLEX", "POST", "TRNS"),
  stage_int = 1:5)

consolidate_ichthyo_tables(
  con               = con,
  larva_stage_vocab = larva_vocab)

# assign ichthyo_uuid — deterministic UUID v5 from composite natural key
assign_deterministic_uuids(
  con        = con,
  table_name = "ichthyo",
  id_col     = "ichthyo_uuid",
  key_cols   = c("net_uuid", "species_id", "life_stage",
                 "measurement_type", "measurement_value"))
} # }
```
