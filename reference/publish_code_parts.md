# Digest a publisher's own code

A `.qmd` contributes only its code chunks, with comment lines dropped —
prose and comments are the document, not the output, and a narrative
edit must not rebuild every package. Chunk options (`#|`) are kept:
`eval:` changes what runs. Any other file contributes its whole text. A
missing file is `"<missing>"`, never skipped.

## Usage

``` r
publish_code_parts(files)
```

## Arguments

- files:

  paths of the notebook, its `libs/` helpers and the package sources it
  calls

## Value

A named character vector, `code:{basename}` -\> md5.
