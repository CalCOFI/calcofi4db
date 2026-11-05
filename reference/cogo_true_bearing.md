# True bearing from the ship's heading and a sighting's angle off the bow

Line-transect surveys record the angle between the ship's heading and
the line of sight (0 = dead ahead, 90 = abeam, 180 = astern) and the
side it was on. The true bearing is the heading plus the angle to
starboard, minus it to port.

## Usage

``` r
cogo_true_bearing(heading, angle, side)
```

## Arguments

- heading:

  ship's true heading, degrees (0-360)

- angle:

  angle off the bow, degrees (0-180)

- side:

  `"port"` or `"starboard"` (case-insensitive; `"P"`, `"S"`, `"Star"`
  accepted); anything else gives `NA`

## Value

true bearing in degrees, `[0, 360)`

## Examples

``` r
cogo_true_bearing(272, 25, "Port")  # 247
#> [1] 247
cogo_true_bearing(350, 30, "Star")  # 20
#> [1] 20
```
