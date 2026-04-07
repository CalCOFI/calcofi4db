# Suggest Next Version

Suggests the next version based on current version and type of change.

## Usage

``` r
suggest_next_version(change_type = "patch")
```

## Arguments

- change_type:

  Type of change: "major", "minor", or "patch" (default: "patch")

## Value

Character string with suggested next version

## Examples

``` r
if (FALSE) { # \dontrun{
suggest_next_version("minor")  # If current is 1.0.0, suggests 1.1.0
suggest_next_version("major")  # If current is 1.0.0, suggests 2.0.0
} # }
```
