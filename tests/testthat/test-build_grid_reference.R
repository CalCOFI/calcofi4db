test_that("build_grid_reference builds a deterministic grid with native geometry", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("calcofi4r")
  skip_if_not_installed("sf")
  skip_if_not_installed("units")

  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  n <- build_grid_reference(con)
  expect_gt(n, 0)

  flds <- DBI::dbListFields(con, "grid")
  expect_true(all(c("grid_key", "geom", "geom_ctr", "area_km2", "zone") %in% flds))

  # grid_key follows the st{station}-ln{line}[_hist] convention
  gk <- DBI::dbGetQuery(con, "SELECT grid_key FROM grid LIMIT 50")$grid_key
  expect_true(all(grepl("^st[-0-9.]+-ln[-0-9.]+(_hist)?$", gk)))

  # geometry round-trips as a real GEOMETRY (not WKB blob)
  gt <- DBI::dbGetQuery(con,
    "SELECT data_type FROM information_schema.columns
      WHERE table_name='grid' AND column_name='geom'")$data_type
  expect_match(gt, "GEOMETRY", ignore.case = TRUE)

  # deterministic: a second build yields an identical grid_key set
  gk_all <- DBI::dbGetQuery(con, "SELECT grid_key FROM grid ORDER BY grid_key")$grid_key
  build_grid_reference(con)
  gk_all2 <- DBI::dbGetQuery(con, "SELECT grid_key FROM grid ORDER BY grid_key")$grid_key
  expect_identical(gk_all, gk_all2)
})
