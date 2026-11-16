# Content-addressed path of a published object

The publishers' form of
[`canonical_path()`](https://calcofi.io/calcofi4db/reference/canonical_path.md):
the identity is a directory and the file keeps its readable name, so one
object per distinct content is stored once and every release that
carries the same content points at it.

## Usage

``` r
publish_object_path(
  publisher,
  dataset_key,
  content_hash,
  file = NULL,
  prefix = "publish"
)
```

## Arguments

- publisher:

  `zip`, `netcdf`, `edi`, `dwca`, … (the folder under `prefix`)

- dataset_key:

  the dataset

- content_hash:

  the object's identity
  ([`publish_fingerprint()`](https://calcofi.io/calcofi4db/reference/publish_fingerprint.md));
  its first 24 characters name the directory, as in
  [`canonical_path()`](https://calcofi.io/calcofi4db/reference/canonical_path.md)

- file:

  the file name inside it; NULL gives the directory

- prefix:

  bucket-relative prefix

## Value

A bucket-relative path:
`{prefix}/{publisher}/{dataset_key}/{hash24}/{file}`.
