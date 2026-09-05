# The CalCOFI role address used as the EML contact of last resort

Decision 23 of the 2026-09-05 dataset-catalog plan: `data@calcofi.io` is
the public contact for CalCOFI data, forwarded today, with
`calcofi-data@ucsd.edu` planned as a UCSD Google Group beside it. It is
used only when neither the dataset's own `contact` nor a creator email
is on record;
[`check_eml()`](https://calcofi.io/calcofi4db/reference/check_eml.md)
reports `contact_role_address` (warn) whenever it is, so a provider
address arriving later is an improvement anyone can see is due.

## Usage

``` r
eml_contact_address()
```

## Value

A length-1 character vector.
