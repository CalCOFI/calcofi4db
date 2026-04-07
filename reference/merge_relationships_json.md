# Merge Multiple Relationships JSON Files

Combines primary keys and foreign keys from multiple per-dataset
`relationships.json` files into a single merged file. PKs use
last-writer-wins for shared tables; FKs are concatenated and
deduplicated.

## Usage

``` r
merge_relationships_json(paths, output_path)
```

## Arguments

- paths:

  Character vector of paths to `relationships.json` files

- output_path:

  Path for the merged output file

## Value

Path to the merged `relationships.json` file

## Examples

``` r
if (FALSE) { # \dontrun{
merge_relationships_json(
  paths = c(
    "data/parquet/swfsc_ichthyo/relationships.json",
    "data/parquet/calcofi_bottle/relationships.json"),
  output_path = "data/releases/v2026.03/relationships.json")
} # }
```
