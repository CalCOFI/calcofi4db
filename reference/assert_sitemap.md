# Stop when [`check_sitemap()`](https://calcofi.io/calcofi4db/reference/check_sitemap.md) found an error

Stop when
[`check_sitemap()`](https://calcofi.io/calcofi4db/reference/check_sitemap.md)
found an error

## Usage

``` r
assert_sitemap(d, quiet = FALSE, allow_dead = character())
```

## Arguments

- d:

  the tibble from
  [`check_sitemap()`](https://calcofi.io/calcofi4db/reference/check_sitemap.md)

- quiet:

  suppress the summary line

- allow_dead:

  a regex of `loc`s whose `url_dead` is known and accepted — one use
  only, and it is temporary: the calcofi.io dataset pages 404 until the
  landing repo generates them (plan Phase 1). Never widen it to hide a
  dead external record; that is the finding the sitemap exists to catch.

## Value

`d`, invisibly.
