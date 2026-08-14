# cc_station_regions() exists because the obvious answer — a convex hull per
# region — is wrong in three ways that a hull produces SILENTLY: a collinear
# region has no hull, interleaved regions overlap, and the hulls do not tile.
# Each of those is pinned below against the real Venrick membership, which is
# where all three actually showed up.

skip_if_not_installed("sf")

# equal-area CRS to measure in; degrees are not an area
EQ <- "+proj=aea +lat_1=32 +lat_2=35 +lat_0=33 +lon_0=-120 +datum=WGS84 +units=m"

# the four Venrick pooling regions, verbatim from definitions.xlsx sheet
# `Regions` (EDI knb-lter-cce.254.4). The sheet writes each station as a
# ROUNDED-line shorthand + station ("87.40" = line 86.7, station 40).
venrick <- function() {
  codes <- list(
    NE       = c("83.41","83.51","87.40","90.30","90.37"),
    SE       = c("93.30","93.40","93.50","93.60"),
    Alley    = c("77.51","77.60","77.70","80.51","80.60","80.70","83.60",
                 "87.50","87.60","90.53"),
    Offshore = c("77.80","80.80","83.70","83.80","83.90","87.70","87.80",
                 "87.90","90.60","90.70","90.80","90.90","93.70","93.80","93.90"))
  line_map <- c(`77` = 76.7, `80` = 80.0, `83` = 83.3,
                `87` = 86.7, `90` = 90.0, `93` = 93.3)
  d <- utils::stack(codes)
  data.frame(
    region  = as.character(d$ind),
    line    = unname(line_map[sub("[.].*$", "", d$values)]),
    station = as.numeric(sub("^.*[.]", "", d$values)))
}

test_that("every declared station places, including the six with no grid cell", {
  v <- venrick()
  expect_equal(nrow(v), 34)
  ll <- cc_calcofi_to_lonlat(v$line, v$station)
  expect_false(anyNA(ll$longitude))
  # 83.41, 83.51, 90.37, 77.51, 80.51 and 90.53 are intermediate inshore
  # stations outside the modern pattern: a `grid` lookup drops them, the
  # projection does not
  off_grid <- v$station %in% c(41, 51, 37, 53)
  expect_gte(sum(off_grid), 6)
  expect_false(anyNA(ll$longitude[off_grid]))
})

test_that("a collinear region still gets a real polygon", {
  # SE's four stations are all on line 93.3, so their convex hull is a slab of
  # essentially zero width — the failure that motivated the partition
  r <- cc_station_regions(venrick())
  se <- r[r$region == "SE", ]
  expect_equal(as.character(sf::st_geometry_type(se)), "POLYGON")
  expect_gt(as.numeric(sf::st_area(sf::st_transform(se, EQ))),
            1e9)   # > 1000 km2, i.e. not a sliver
})

test_that("regions tile the pooled domain: no overlap and no gaps", {
  r <- cc_station_regions(venrick())
  expect_equal(nrow(r), 4)

  g <- sf::st_geometry(sf::st_transform(r, EQ))

  # every DISTINCT pair, not st_intersection(g, g) — that includes each region
  # with itself and would report the whole domain as overlap
  pairs <- utils::combn(length(g), 2)
  ov <- apply(pairs, 2, function(ij) {
    x <- sf::st_intersection(g[ij[1]], g[ij[2]])
    if (!length(x)) 0 else sum(as.numeric(sf::st_area(x)))
  })
  expect_lt(sum(ov), 1)                              # < 1 m2, i.e. nothing

  # the union is the convex hull of all 34 stations: no gaps, and no extent
  # invented beyond the outermost station occupied
  pts  <- sf::st_as_sf(cc_calcofi_to_lonlat(venrick()$line, venrick()$station),
                       coords = c("longitude", "latitude"), crs = 4326)
  hull <- sf::st_convex_hull(sf::st_union(sf::st_transform(pts, EQ)))
  # 0.1%, not exact: the function hulls in an equal-area CRS fitted to the data
  # and returns 4326, so re-projecting here replaces each straight edge with a
  # chord. That residual is a projection artifact, not a gap.
  expect_equal(as.numeric(sf::st_area(sf::st_union(g))),
               as.numeric(sf::st_area(hull)), tolerance = 1e-3)
})

test_that("a region is one connected piece even when its own members are not adjacent", {
  # NE is the case the issue turns on: only 2 of its 5 stations resolve to a
  # grid cell and those two are not adjacent, so a union of member cells would
  # be multipart. Every point still goes to SOME member station, so it isn't.
  r  <- cc_station_regions(venrick())
  ne <- r[r$region == "NE", ]
  expect_equal(as.character(sf::st_geometry_type(ne)), "POLYGON")
  expect_silent(cc_station_regions(venrick()))       # no non-contiguity warning
})

test_that("the representative point falls inside its own region", {
  # regions are concave — Alley wraps around NE — so a centroid can land in the
  # neighbour, which would map the region onto the wrong water
  r   <- cc_station_regions(venrick())
  pts <- sf::st_as_sf(sf::st_drop_geometry(r)[c("longitude", "latitude")],
                      coords = c("longitude", "latitude"), crs = 4326)
  inside <- as.logical(diag(sf::st_within(pts, r, sparse = FALSE)))
  expect_true(all(inside))
})

test_that("station membership is per region, and a station in two fails loudly", {
  v <- venrick()
  r <- cc_station_regions(v)
  expect_equal(r$n_stations[r$region == "NE"],       5L)
  expect_equal(r$n_stations[r$region == "SE"],       4L)
  expect_equal(r$n_stations[r$region == "Alley"],    10L)
  expect_equal(r$n_stations[r$region == "Offshore"], 15L)

  bad <- rbind(v, data.frame(region = "SE", line = 90.0, station = 30))  # NE's
  expect_error(cc_station_regions(bad), "more than one region")
})

test_that("column names are configurable and the output keeps the caller's", {
  v <- venrick()
  names(v) <- c("pool", "lin", "sta")
  r <- cc_station_regions(v, group = "pool", line = "lin", station = "sta")
  expect_true("pool" %in% names(r))
  expect_false("region" %in% names(r))
})
