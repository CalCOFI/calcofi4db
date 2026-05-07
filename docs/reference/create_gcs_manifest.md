# Create a manifest of current GCS files

Generates a JSON manifest documenting the current state of files in GCS.

## Usage

``` r
create_gcs_manifest(
  bucket = "calcofi-files",
  prefix = "current/",
  output_path = NULL
)
```

## Arguments

- bucket:

  GCS bucket name (default: "calcofi-files")

- prefix:

  Path prefix to include (default: "current/")

- output_path:

  Path to save manifest JSON (default: NULL returns data)

## Value

Data frame of file metadata, or path to saved JSON

## Examples

``` r
if (FALSE) { # \dontrun{
manifest <- create_gcs_manifest()
create_gcs_manifest(output_path = "manifest_2026-01-31.json")
} # }
```
