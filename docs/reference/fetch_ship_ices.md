# Fetch Ship Codes from ICES Reference Codes API

Retrieves the full list of ship/platform codes from the ICES
Vocabularies API. The default code type GUID corresponds to "SHIPC"
(ship codes).

## Usage

``` r
fetch_ship_ices(
  ices_api = "https://vocab.ices.dk/services/api",
  ices_ship_code_type = "7f9a91e1-fb57-464a-8eb0-697e4b0235b5"
)
```

## Arguments

- ices_api:

  character; ICES API base URL

- ices_ship_code_type:

  character; GUID for ship code type

## Value

tibble with columns: ship_nodc, ship_name, remarks, src

## Examples

``` r
if (FALSE) { # \dontrun{
d_ices <- fetch_ship_ices()
} # }
```
