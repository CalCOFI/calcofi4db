# Create Lookup Table from Vocabulary Definitions

Creates a unified lookup table from vocabulary definitions for egg
stages, larva stages, and tow types.

## Usage

``` r
create_lookup_table(
  con,
  lookup_tbl = "lookup",
  egg_stage_vocab = NULL,
  larva_stage_vocab = NULL,
  tow_type_vocab = NULL
)
```

## Arguments

- con:

  DuckDB connection

- lookup_tbl:

  Name of lookup table to create (default: "lookup")

- egg_stage_vocab:

  Tibble with egg stage vocabulary (columns: stage_int,
  stage_description)

- larva_stage_vocab:

  Tibble with larva stage vocabulary (columns: stage_int, stage_txt,
  stage_description)

- tow_type_vocab:

  Optional tibble with tow type vocabulary (columns: lookup_num,
  lookup_chr, description)

## Value

Invisibly returns the connection after creating lookup table

## Examples

``` r
if (FALSE) { # \dontrun{
egg_vocab <- tibble::tibble(
  stage_int = 1:11,
  stage_description = paste0("egg, stage ", 1:11, " of 11 (Moser & Ahlstrom, 1985)"))

larva_vocab <- tibble::tibble(
  stage_int = 1:5,
  stage_txt = c("YOLK", "PREF", "FLEX", "POST", "TRNS"),
  stage_description = c(
    "larva, yolk sac",
    "larva, preflexion",
    "larva, flexion",
    "larva, postflexion",
    "larva, transformation"))

create_lookup_table(con, egg_stage_vocab = egg_vocab, larva_stage_vocab = larva_vocab)
} # }
```
