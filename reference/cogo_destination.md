# Destination point from a position, a true bearing and a distance

The coordinate-geometry (COGO) step that turns an observer's position, a
bearing and a range into the position of what was seen. Spherical (mean
radius 6,371,008.8 m, the sphere of
[`calcofi4r::cc_distance_km_sql()`](https://calcofi.io/calcofi4r/reference/cc_distance_km_sql.html));
at the ranges of a visual survey (\< 20 km) the error against the
ellipsoid is under 0.5%.

## Usage

``` r
cogo_destination(lon, lat, bearing, distance_m)
```

## Arguments

- lon, lat:

  origin, decimal degrees

- bearing:

  true bearing, degrees clockwise from north

- distance_m:

  distance along the surface, metres

## Value

a data frame with `longitude`, `latitude` (decimal degrees, longitude in
`[-180, 180)`); `NA` wherever an input is `NA`

## Examples

``` r
cogo_destination(-120, 34, 90, 1000)
#>   longitude latitude
#> 1 -119.9892       34
```
