# A connection an uploaded cast can be QC'd on

The upload becomes `obs` and `sample` — the names every rule uses — so
the registry runs against it verbatim. `obs_ctd_full` is the same data:
an uploaded cast IS full resolution, so the profile rules apply to it
directly rather than skipping.

## Usage

``` r
qc_upload_con(core, dir_workflows, gebco_tif = NULL)
```

## Arguments

- core:

  output of
  [`ctd_upload_to_core()`](https://calcofi.io/calcofi4db/reference/ctd_upload_to_core.md)

- dir_workflows:

  root of the workflows checkout, for
  [`qc_stage_reference()`](https://calcofi.io/calcofi4db/reference/qc_stage_reference.md)

- gebco_tif:

  optional bathymetry raster

## Value

a DBI connection; the caller closes it

## Details

Nothing here touches a release. The connection is in-memory and dies
with the session.
