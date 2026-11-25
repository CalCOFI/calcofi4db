# Rows of id space each dataset owns in `root_id`

`root_id` stays a 32-bit `INTEGER` (the browser tables carry it on every
row), so its blocks are smaller than the other ids':
`id_block * CC_ROOT_ID_BLOCK_SIZE + n`. Five million root samples per
dataset (the largest held 82,343 at v2026.11.25) and blocks up to 429.
[`build_sample_root()`](https://calcofi.io/calcofi4db/reference/build_sample_root.md)
stops if either is exceeded; `root_id` would then be widened to
`BIGINT`.

## Usage

``` r
CC_ROOT_ID_BLOCK_SIZE
```
