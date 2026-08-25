# Decide, per object, whether to upload or reuse

Decide, per object, whether to upload or reuse

## Usage

``` r
freeze_plan(
  objects,
  prev_catalog,
  version,
  layout = c("compat", "canonical"),
  release_prefix = CC_RELEASE_PREFIX
)
```

## Arguments

- objects:

  tibble from
  [`release_objects()`](https://calcofi.io/calcofi4db/reference/release_objects.md)
  (all tables, row-bound).

- prev_catalog:

  previous release's catalog (list) or NULL.

- version, release_prefix:

  the release and its prefix.

- layout:

  `"compat"` (objects live under the release prefix; unchanged ones are
  server-side copied from the previous release) or `"canonical"`
  (objects live under
  [CC_TABLES_PREFIX](https://calcofi.io/calcofi4db/reference/CC_RELEASE_PREFIX.md);
  unchanged ones already exist there).

## Value

`objects` plus `path` (bucket-relative destination), `action` (`upload`
\| `copy` \| `exists`) and `source` (bucket-relative path copied from,
or NA).
