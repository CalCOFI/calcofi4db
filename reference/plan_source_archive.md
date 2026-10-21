# Decide what archiving each source file needs

Pure: compares the local files with what the archive (and optionally a
mirror such as `_sync/`) already holds. A file whose archive path exists
is `present`; one the mirror holds with the same name and MD5 is `copy`
(server side, no bytes leave the machine); anything else is `upload`.

## Usage

``` r
plan_source_archive(local, archived = character(), mirror = NULL)
```

## Arguments

- local:

  tibble with `file` (base name), `md5` (hex) and `archive` (object path
  from
  [`source_archive_path()`](https://calcofi.io/calcofi4db/reference/source_archive_path.md))

- archived:

  object paths already under the archive prefix

- mirror:

  optional tibble of mirror objects: `name` (object path) and `md5`
  (hex)

## Value

`local` with `action` (`present`, `copy`, `upload`) and `copy_from` (the
mirror object path for a `copy`, else `NA`)
