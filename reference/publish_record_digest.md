# Digest a dataset's catalog record, ignoring what changes every release

Digest a dataset's catalog record, ignoring what changes every release

## Usage

``` r
publish_record_digest(record, drop = PUBLISH_RECORD_VOLATILE)
```

## Arguments

- record:

  one dataset record from `datasets.json` (a list), or any list

- drop:

  top-level keys to leave out; defaults to the internal
  `PUBLISH_RECORD_VOLATILE` (`objects`, `since_version`,
  `distributions`, `registrations`, `status`)

## Value

An md5 over the record's canonical JSON (names sorted at every level),
so key order never matters and a changed value always does.
