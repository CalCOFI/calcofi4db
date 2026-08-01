# Read a Sea-Bird `.btl` bottle summary

Each bottle contributes several rows tagged `(avg)` / `(sdev)` / `(min)`
/ `(max)`. Only `(avg)` is read, for two reasons: the others describe
the scatter of the scans the bottle was fired over rather than separate
observations, and structurally they are not the same table — the
`(sdev)` row omits the bottle number and carries a time where the
`(avg)` row carries a date, so it does not share the column layout.

## Usage

``` r
read_sbe_btl(path, statistic = "avg")
```

## Arguments

- path:

  file path

- statistic:

  tag to keep; `"avg"` is the only complete layout

## Value

a tibble with a `sbe_header` attribute
