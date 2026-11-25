# Rows of id space each dataset owns in a surrogate id column

A surrogate id (`obs_id`, `obs_attribute_id`, `sample_measurement_id`)
is `id_block * CC_ID_BLOCK_SIZE + n`, where `id_block` is the dataset's
number in `metadata/id_block.csv` and `n` counts the dataset's own rows
from 1. Ten billion rows per dataset per table leaves every id below
2^53 (a JavaScript safe integer) for any block up to 900,000.

## Usage

``` r
CC_ID_BLOCK_SIZE
```
