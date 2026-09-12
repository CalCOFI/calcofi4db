# Append-or-update `metadata/measurement_face.csv` by key

`face_kind` in
`structure | composition | scale | organism | standsin | none` (D2-D3).
A `standsin` row's `face_of` names the key whose face it borrows and
`stands_in_note` is the chip's short text; a row that is not `standsin`
never carries `face_of` (no borrowed ids on a key that has its own
concept, D3).

## Usage

``` r
register_measurement_face(new_rows, path, quiet = FALSE)
```

## Arguments

- new_rows:

  data.frame with `key` and any other column of
  `.measurement_face_cols()`

- path:

  path to `metadata/measurement_face.csv`

- quiet:

  suppress the added/updated message

## Value

the full updated registry
