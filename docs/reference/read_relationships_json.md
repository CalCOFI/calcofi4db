# Read Relationships JSON and Optionally Apply to dm

Reads a `relationships.json` file. If a `dm` object is provided, applies
the PKs and FKs from the JSON to it. Otherwise returns the parsed list.

## Usage

``` r
read_relationships_json(path, dm = NULL)
```

## Arguments

- path:

  Path to `relationships.json` file

- dm:

  Optional `dm` object to apply relationships to

## Value

If `dm` is provided, returns the dm with PKs/FKs applied. Otherwise
returns the parsed JSON as a list.

## Examples

``` r
if (FALSE) { # \dontrun{
# read and apply to dm
d <- dm::dm_from_con(con, learn_keys = FALSE)
d <- read_relationships_json("relationships.json", dm = d)
dm::dm_draw(d)

# read as raw list
rels <- read_relationships_json("relationships.json")
} # }
```
