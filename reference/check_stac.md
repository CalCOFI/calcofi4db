# Validate a written STAC catalog

Runs `stac-validator` (pip, `stac_validator`) over every written
document when it is on `PATH`, and always runs a structural check: that
every document parses, carries `type`/`stac_version`/`id`, that every
`child`/`item` link resolves to a file that was written, and that every
asset has an `href` and a `type`. The structural half always runs, so a
machine without the validator still fails on a broken catalog.

## Usage

``` r
check_stac(dir, network = !nzchar(Sys.getenv("CALCOFI_SKIP_LINK_CHECK")))
```

## Arguments

- dir:

  the directory
  [`build_stac()`](https://calcofi.io/calcofi4db/reference/build_stac.md)
  wrote

- network:

  run the external validator (it fetches the STAC JSON schemas);
  defaults to off when `CALCOFI_SKIP_LINK_CHECK` is set

## Value

A [tibble](https://tibble.tidyverse.org/reference/tibble.html)
`document`, `finding`, `level`, `detail` — one `ok` row per document
that passed.
