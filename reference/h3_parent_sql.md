# H3 parent of a cell as plain SQL (no extension)

An H3 index stores its resolution in bits 52–55 and one 3-bit digit per
resolution, unused digits set to 7. The parent at resolution `res` is
therefore the same index with the resolution field rewritten and every
finer digit set to 7 — pure bit arithmetic, which a browser without the
`h3` extension can run. Verified against `h3_cell_to_parent()` in the
tests.

## Usage

``` r
h3_parent_sql(hex, res)
```

## Arguments

- hex:

  SQL expression for a `UBIGINT` H3 cell.

- res:

  target resolution (coarser than the cell's).

## Value

A SQL expression string; `NULL` cells stay `NULL`.

## Examples

``` r
h3_parent_sql("hex7", 5)
#> [1] "(((hex7 & ~(15::UBIGINT << 52)) | (5::UBIGINT << 52)) | ((1::UBIGINT << 30) - 1))"
```
