# Decode `\\uXXXX` escapes in a character vector (what ERDDAP's CSV emits for non-ASCII)

Decode `\\uXXXX` escapes in a character vector (what ERDDAP's CSV emits
for non-ASCII)

## Usage

``` r
unescape_unicode(x)
```

## Arguments

- x:

  character

## Value

character of the same length, UTF-8
