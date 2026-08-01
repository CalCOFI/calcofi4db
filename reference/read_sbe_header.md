# Parse a Sea-Bird `*` / `**` header block

Parse a Sea-Bird `*` / `**` header block

## Usage

``` r
read_sbe_header(lines)
```

## Arguments

- lines:

  lines of the file (or of a `.hdr`)

## Value

named list with any of `ship`, `cruise`, `station`, `cast`, `latitude`,
`longitude`, `datetime`, `file_name`; absent keys are simply not present
