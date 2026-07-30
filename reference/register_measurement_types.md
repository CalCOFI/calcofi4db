# Append new measurement types to the shared registry, safely

Replaces the read / `bind_rows` / `write_csv` cycle that each ingest
used to hand-roll. Reads and validates the registry, appends only types
not already present, and writes with `na = ""` so empty cells stay
empty. Writes nothing when there is nothing new to add.

## Usage

``` r
register_measurement_types(new_types, path, quiet = FALSE)
```

## Arguments

- new_types:

  data.frame of candidate rows; must have a `measurement_type` column.
  Columns absent from the registry are dropped with a warning, so a
  stray column cannot silently widen the registry.

- path:

  path to `metadata/measurement_type.csv`

- quiet:

  suppress the "added N type(s)" message

## Value

The full updated registry (invisibly if nothing changed), suitable for
`dbWriteTable(con, "measurement_type", ...)`.

## Examples

``` r
if (FALSE) { # \dontrun{
d_meas_type <- register_measurement_types(
  my_new_types, here::here("metadata/measurement_type.csv"))
dbWriteTable(con, "measurement_type", as.data.frame(d_meas_type), overwrite = TRUE)
} # }
```
