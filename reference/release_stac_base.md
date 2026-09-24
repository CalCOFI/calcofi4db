# The STAC root a release writes

One rule, so the catalog builder, the catalog check and
`release_database.qmd`'s STAC upload cannot disagree: a staging release
prefix writes `stac-staging/`, every other one `stac/`.

## Usage

``` r
release_stac_base(release_prefix = "ducklake/releases", bucket = "calcofi-db")
```

## Arguments

- release_prefix:

  the bucket-relative releases prefix the run writes to

- bucket:

  the GCS bucket

## Value

the https root, e.g. `https://storage.googleapis.com/calcofi-db/stac`

## Examples

``` r
release_stac_base("ducklake-staging/releases")
#> [1] "https://storage.googleapis.com/calcofi-db/stac-staging"
```
