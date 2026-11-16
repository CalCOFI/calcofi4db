# Decide, per published object, whether to upload it or point at the one already there

The publishers'
[`freeze_plan()`](https://calcofi.io/calcofi4db/reference/freeze_plan.md):
an object whose `content_hash` the previous manifest already lists for
the same dataset and format **exists** — nothing is written or uploaded,
it keeps the previous object's `since`, `path`, `bytes` and `sha256`,
and only the manifest's release moves on; any other is an **upload**
under its own content-addressed path, `since` this release. Nothing is
ever deleted: an object a newer content replaces stays addressable at
its path (thinning is a separate, explicit act, as for release tables).

## Usage

``` r
publish_objects_plan(objects, prev = NULL, version)
```

## Arguments

- objects:

  data frame, one row per object this release would publish:
  `dataset_key`, `format`, `content_hash`, and optionally `path`,
  `bytes`, `sha256`

- prev:

  data frame of the previous manifest's objects (same columns plus
  `since`), or NULL

- version:

  the release being published from

## Value

`objects` with `since`, `action` (`upload` \| `exists`), and `path`,
`bytes`, `sha256` taken from `prev` for an existing object.
