# One dataset's data inputs, table by table

One dataset's data inputs, table by table

## Usage

``` r
publish_data_parts(signatures, dataset_key, tables)
```

## Arguments

- signatures:

  the data frame from
  [`publish_object_signatures()`](https://calcofi.io/calcofi4db/reference/publish_object_signatures.md)

- dataset_key:

  the dataset

- tables:

  the tables this publisher reads for it

## Value

A named character vector, `data:{table}` -\> a digest of the dataset's
signatures in that table (`"<absent>"` when it has no rows there,
`"<missing>"` when the release has no such table). Order-independent
over objects.
