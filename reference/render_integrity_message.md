# Render Data Integrity Check Message

Renders the markdown message from check_data_integrity() output. Use
this in output: asis chunks to display formatted messages.

## Usage

``` r
render_integrity_message(integrity_check)
```

## Arguments

- integrity_check:

  List output from check_data_integrity()

## Value

Invisible NULL (message is rendered via cat())

## Examples

``` r
if (FALSE) { # \dontrun{
# In a Quarto chunk with output: asis
integrity_check <- check_data_integrity(d, "NOAA CalCOFI Database")
render_integrity_message(integrity_check)
} # }
```
