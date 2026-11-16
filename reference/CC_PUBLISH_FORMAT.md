# Pinned output format of each publisher

The publishers' counterpart of
[CC_PARQUET_WRITER](https://calcofi.io/calcofi4db/reference/CC_PARQUET_WRITER.md):
a published object's identity
([`publish_fingerprint()`](https://calcofi.io/calcofi4db/reference/publish_fingerprint.md)'s
`content_hash`) covers its data, its metadata and this version, not the
code that wrote it, so a fix to how a publisher writes its files reaches
users only when its entry here is bumped — which gives every object of
that publisher a new `content_hash`, a new path and a new upload. Record
a bump in workflows' RELEASES.md.

## Usage

``` r
CC_PUBLISH_FORMAT
```
