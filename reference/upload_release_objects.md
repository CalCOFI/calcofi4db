# Execute a freeze plan against GCS

Uploads `upload` objects from `dir_out`, server-side copies `copy`
objects from their `source`, and — for the canonical layout — makes a
compat copy of every object at `compat_path` when `compat = TRUE`.

## Usage

``` r
upload_release_objects(plan, dir_out, bucket, compat = TRUE, dry_run = FALSE)
```

## Arguments

- plan:

  tibble from
  [`freeze_plan()`](https://calcofi.io/calcofi4db/reference/freeze_plan.md).

- dir_out:

  unused (kept for symmetry); uploads use `plan$local_path`.

- bucket:

  GCS bucket.

- compat:

  also write compat copies (canonical layout only).

- dry_run:

  print the plan, touch nothing.

## Value

Invisibly, a summary tibble of bytes by action.
