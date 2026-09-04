# Read the `taxon_group` rule registry (`metadata/taxon_group.csv`)

Strict read — every column character, an empty cell is `NA` (never the
string `"NA"`), and the shape is validated: `rule` is `class` (every
vocabulary taxon whose `class` equals `rule_value`, cross-dataset by
construction) or `dataset_taxon` (rows of one dataset's vocabulary
matched on `match_column` ∈ `ds_taxa_code` / `ds_scientific_name` /
`ds_common_name` = `match_value`).

## Usage

``` r
read_taxon_group_rules(path)
```

## Arguments

- path:

  path to the registry CSV

## Value

a data.frame of rules (columns `taxon_group_key`, `description`, `rule`,
`rule_value`, `dataset_key`, `match_column`, `match_value`)
