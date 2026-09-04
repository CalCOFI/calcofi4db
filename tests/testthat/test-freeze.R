
test_that("validate_for_release() does not report the by-contract NULL provider-id columns", {
  con <- DBI::dbConnect(duckdb::duckdb()); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbExecute(con, "CREATE TABLE sample (sample_key VARCHAR, dataset_key VARCHAR, source_uuid UUID, station_uuid UUID)")
  DBI::dbExecute(con, "INSERT INTO sample VALUES ('d:site:1', 'd', NULL, NULL), ('d:site:2', 'd', NULL, NULL)")
  res <- validate_for_release(con, checks = "nulls", strict = FALSE)
  nulls <- res$checks[res$checks$check == "nulls", ]
  expect_false(any(grepl("source_uuid|station_uuid", nulls$message)))
  # a genuinely required key still reports
  DBI::dbExecute(con, "INSERT INTO sample VALUES (NULL, 'd', NULL, NULL)")
  res2 <- validate_for_release(con, checks = "nulls", strict = FALSE)
  expect_true(any(grepl("sample_key", res2$checks$message[res2$checks$check == "nulls"])))
})
