# Fingerprint what one dataset's published output is a function of

Two digests, the release's two identities carried over to published
objects (`R/release.R`): `content_hash` is the object's identity and
names its path
([`publish_object_path()`](https://calcofi.io/calcofi4db/reference/publish_object_path.md)),
exactly as a release table's `content_hash` names its canonical object —
the `.short_sig()` of the dataset's row signatures in every table the
output holds (`data:*`, the release's own order-independent signatures),
the metadata it carries (`meta:*`) and the publisher's pinned output
format
([CC_PUBLISH_FORMAT](https://calcofi.io/calcofi4db/reference/CC_PUBLISH_FORMAT.md)),
and **not** the code: as a parquet writer change does not re-upload a
release table, a code edit does not re-publish an object until the
publisher's format version is bumped. `hash` additionally covers the
code, so
[`changed_inputs()`](https://calcofi.io/calcofi4db/reference/changed_inputs.md)
can still say a code file moved.

## Usage

``` r
publish_fingerprint(
  data = character(),
  metadata = list(),
  code = character(),
  format = NULL
)
```

## Arguments

- data:

  named character from
  [`publish_data_parts()`](https://calcofi.io/calcofi4db/reference/publish_data_parts.md)

- metadata:

  named list of anything else the output reads — the record digest from
  [`publish_record_digest()`](https://calcofi.io/calcofi4db/reference/publish_record_digest.md),
  a registry table, a sidecar list; each element is digested and named
  `meta:{name}`

- code:

  named character from
  [`publish_code_parts()`](https://calcofi.io/calcofi4db/reference/publish_code_parts.md)

- format:

  the publisher's format version, e.g. `CC_PUBLISH_FORMAT[["zip"]]`;
  NULL leaves it out (`content_hash` is then over data and metadata
  only)

## Value

A list shaped like
[`input_fingerprint()`](https://calcofi.io/calcofi4db/reference/input_fingerprint.md)'s
— `hash` and named `parts` — plus `content_hash` (32 hex characters, the
release objects' form) and `data_hash` (the same over the `data:*` parts
alone, so a rebuild for a metadata change reads as "same data, new
object").
