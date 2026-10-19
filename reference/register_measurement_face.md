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

## Details

`short_label` is the key's authored short title, the name a page head
shows beside its face (issue \#30 D13); the full `label` from
`variable.csv` keeps the method notes. It may be empty (none authored
yet). When set it is non-blank, at most 48 characters, does not end in
punctuation and is not used by another key (ignoring case), counting the
keys already in the file. A row breaking any of these is refused.
