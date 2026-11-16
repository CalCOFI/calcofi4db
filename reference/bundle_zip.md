# Zip files so the same content always gives the same bytes

A zip entry records each file's modification time and permissions, and
the time in local time: two builds of identical files a minute apart, or
on machines in different time zones, gave different bytes, and every
hash-compared upload saw a change. Every member gets one fixed instant
and mode `644`, the archive is written with `TZ=UTC`, and the members go
in the order given.

## Usage

``` r
bundle_zip(root, files, zip_path, stamp, compression_level = 6L)
```

## Arguments

- root:

  the directory the member paths are relative to

- files:

  member paths relative to `root`, in archive order

- zip_path:

  the archive to write (replaced)

- stamp:

  a POSIXct every member is dated to

- compression_level:

  0–9

## Value

`zip_path`, invisibly.
