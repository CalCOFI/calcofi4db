# Read `metadata/life_stage.csv`, the life-stage registry

One row per distinct `obs.life_stage` value, with the Darwin Core label
(`dwc_lifeStage`) and the NERC S11 concept URI where one is exact, plus
`life_stage_parent` for a substage S11 does not carve (`furcilia F1` -\>
`furcilia`). Two values are recorded as **not life stages** — euphausiid
`damaged` and ichthyo `invert` — and carry neither a label nor a parent;
[`dwc_occurrence()`](https://calcofi.io/calcofi4db/reference/dwc_occurrence.md)
sends those to `occurrenceRemarks`, never to `lifeStage`.

## Usage

``` r
read_life_stage_registry(path)
```

## Arguments

- path:

  path to `metadata/life_stage.csv`

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html), all
columns character.

## See also

[`read_gear_registry()`](https://calcofi.io/calcofi4db/reference/read_gear_registry.md),
[`dwc_occurrence()`](https://calcofi.io/calcofi4db/reference/dwc_occurrence.md)
