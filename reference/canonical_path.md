# Canonical (content-addressed) object path for a table or partition

Canonical (content-addressed) object path for a table or partition

## Usage

``` r
canonical_path(
  table,
  content_hash,
  partition_by = NULL,
  partition_value = NULL,
  prefix = CC_TABLES_PREFIX
)
```

## Arguments

- table:

  table name.

- content_hash:

  the object's content signature (see
  [`release_objects()`](https://calcofi.io/calcofi4db/reference/release_objects.md)).

- partition_by, partition_value:

  partition column and value, or NULL.

- prefix:

  bucket-relative prefix.

## Value

A bucket-relative path.
