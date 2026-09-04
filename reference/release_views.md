# Views a release carries beside its tables

The registry behind `catalog.json`'s top-level `views` map (3.31.0,
pre-release plan D-S1): one entry per view, with the SQL over
`{{table}}` tokens
([`obs_view_sql()`](https://calcofi.io/calcofi4db/reference/obs_view_sql.md)),
the source `tables` it needs, the physical table it `replaces` (marked
`deprecated` in the catalog while it still ships) and `removed_in`, the
release its objects disappear in.
[`build_release_catalog()`](https://calcofi.io/calcofi4db/reference/build_release_catalog.md)
consults it by default and includes a view only when every source table
is in the release.

## Usage

``` r
release_views(removed_in = "next")
```

## Arguments

- removed_in:

  the release `obs`'s objects are dropped in (default `"next"`).

## Value

Named list of `list(sql, tables, replaces, removed_in)`.

## Details

`"next"` is deliberate for `removed_in`: release versions are dates and
the next one is not known at freeze time. A consumer treats any non-NULL
`removed_in` as "migrate now".
