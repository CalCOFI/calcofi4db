# Recover the column names from a fixed-width Sea-Bird ASCII header

The `.asc` header is fixed-width, and in most CalCOFI files adjacent
names touch: `Sbeox0ML/L` and `Sbeox0Mm/Kg` arrive as
`Sbeox0ML/LSbeox0Mm/Kg`. Splitting on whitespace therefore produces the
wrong number of columns and silently mis-assigns every column after the
collision — which in a QC tool is worse than not reading the file at
all.

## Usage

``` r
sbe_split_header(header, data_rows)
```

## Arguments

- header:

  the header line

- data_rows:

  a few data lines to take column edges from

## Value

character vector of column names

## Details

Both the numbers and the header names are RIGHT-ALIGNED in their column,
so the aligned edge is each field's stop position. Those are taken from
the data rows (where fields are always separated) and the header is
sliced at them.

It ERRORS rather than guessing when the result is not self-consistent —
an empty name, a name containing a space, or a count that does not match
the data. Measured on the CalCOFI archive this reads ~86% of `.asc`
files cleanly; the rest report the problem and ask for the `.cnv`, whose
header is unambiguous.
