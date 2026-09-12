# Does a portal need a fresh upload?

The built bytes against the bytes last deposited: a package or archive
is current only while its `content_hash` is the one uploaded.
Vectorised.

## Usage

``` r
publish_upload_status(
  content_hash,
  uploaded_hash = NA_character_,
  uploaded_version = NA_character_
)
```

## Arguments

- content_hash:

  the built output's hash (NA: nothing built)

- uploaded_hash:

  the hash recorded when it was last deposited (NA: never)

- uploaded_version:

  the release the deposited copy was built from, for the label

## Value

A data frame: `upload_status` (`not built` \| `never uploaded` \|
`current` \| `changed since {version}`) and `needs_upload` (logical).
