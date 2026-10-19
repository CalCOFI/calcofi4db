# The CSV text of a registry without its leading `#` header comment

Only the block of `#`-prefixed lines at the top of the file is a
comment. `readr::read_csv(comment = "#")` instead cuts every line at its
first `#`, which silently truncates a row whose URL carries a fragment
(`…/bottle-sampling-methods/#:~:text=…`) and drops the fields after it.

## Usage

``` r
.registry_csv_text(path)
```

## Arguments

- path:

  path to a registry CSV

## Value

the remaining text, wrapped in [`I()`](https://rdrr.io/r/base/AsIs.html)
for
[`readr::read_csv()`](https://readr.tidyverse.org/reference/read_delim.html)
