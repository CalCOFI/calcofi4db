# Assert a frozen release is structurally complete

Passing the consumer-contract suite says the **data** is right. It says
nothing about whether the release is **readable**, and those are
different questions — `test_release.qmd` asked only the first until
2026-08-14, when it passed 28/28 against genuinely-good parquet and
promoted `latest.txt` to a release with no `catalog.json`. That is the
file `cc_get_db()` opens, so every consumer resolving through `latest`
got a 404 while the tests were green.

## Usage

``` r
check_release_complete(
  version,
  bucket = "calcofi-db",
  required = RELEASE_REQUIRED_OBJECTS,
  halt = TRUE
)
```

## Arguments

- version:

  release version (e.g. `"v2026.08.14"`)

- bucket:

  GCS bucket holding `ducklake/releases/`

- required:

  object names that must exist directly under the release

- halt:

  logical; [`stop()`](https://rdrr.io/r/base/stop.html) when something
  is missing (default `TRUE`)

## Value

invisibly, a data.frame with `object` and `exists`

## Details

The cause was ordering: `upload_frozen` pushes parquet first and the
JSON sidecars after, and the render died in between. The parquet was
complete and valid, which is precisely why the tests passed.
