# tests for the QC rule engine (R/qc.R)
#
# The engine's whole value is that an unmet precondition reports `skip` and never
# `pass` — a rule that returns zero rows because its input is absent looks exactly
# like clean data. Those two cases get named assertions here so they cannot
# silently swap.

# a throwaway registry on disk: rules.csv + sql/, the same shape as
# workflows/metadata/qc_rules/
write_rule_dir <- function(rules, sql = list()) {
  dir <- withr::local_tempdir(.local_envir = parent.frame())
  dir.create(file.path(dir, "sql"))
  readr::write_csv(rules, file.path(dir, "rules.csv"), na = "")
  for (nm in names(sql)) writeLines(sql[[nm]], file.path(dir, "sql", nm))
  dir
}

rules_tbl <- function(...) {
  base <- tibble::tibble(
    rule_key = character(), rule_type = character(), severity = character(),
    target = character(), description = character(), sql_file = character(),
    params = character(), scope = character(), requires_types = character(),
    active = character())
  dplyr::bind_rows(base, tibble::tibble(...))
}

# a two-row obs table, one value inside a plausible range and one wildly outside
obs_con <- function(env = parent.frame()) {
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  DBI::dbWriteTable(con, "obs", data.frame(
    sample_key       = c("ds:cast:1", "ds:cast:2", "ds:cast:3"),
    cruise_key       = c("2020-01-AA", "2020-01-AA", "2020-01-AA"),
    dataset_key      = rep("calcofi_ctd-cast", 3),
    measurement_type = c("temperature_ave", "temperature_ave", "salinity_ave_corr"),
    depth_min_m      = c(10, 20, 10),
    measurement_value = c(12.5, 60.4, 33.5)))
  con
}

test_that("qc_parse_params reads k=v;k=v and tolerates blanks", {
  expect_equal(qc_parse_params("threshold=0.5;units=degC"),
               list(threshold = "0.5", units = "degC"))
  expect_equal(qc_parse_params(NA_character_), list())
  expect_equal(qc_parse_params(""), list())
  expect_equal(qc_parse_params(" a = 1 ; "), list(a = "1"))
  # a value may itself contain '=' — split on the FIRST separator only
  expect_equal(qc_parse_params("expr=a=b"), list(expr = "a=b"))
  expect_error(qc_parse_params("nope"), "malformed param")
})

test_that("qc_render_sql substitutes every placeholder", {
  expect_equal(
    qc_render_sql("SELECT * FROM obs WHERE v > {{threshold}} -- {{threshold}}",
                  list(threshold = "3")),
    "SELECT * FROM obs WHERE v > 3 -- 3")
})

test_that("qc_render_sql errors rather than leaving a placeholder unfilled", {
  # the failure this prevents: an unresolved {{threshold}} either parse-errors far
  # from its cause or, worse, renders empty and changes the rule's meaning
  expect_error(
    qc_render_sql("... {{threshold}} ...", list(units = "degC")),
    "needs param\\(s\\) not supplied: threshold")
})

test_that("qc_read_rules attaches SQL and parsed params, and honours active_only", {
  dir <- write_rule_dir(
    rules_tbl(
      rule_key = c("r_on", "r_off"),
      rule_type = "range", severity = "warning", target = "obs",
      description = c("on", "off"),
      sql_file = c("r_on.sql", "r_off.sql"),
      params = c("threshold=1", NA), scope = "all",
      requires_types = NA_character_, active = c("TRUE", "FALSE")),
    sql = list(r_on.sql = "SELECT 1", r_off.sql = "SELECT 2"))

  d <- qc_read_rules(dir)
  expect_equal(nrow(d), 1L)
  expect_equal(d$rule_key, "r_on")
  expect_equal(d$sql, "SELECT 1")
  expect_equal(d$params[[1]], list(threshold = "1"))

  expect_equal(nrow(qc_read_rules(dir, active_only = FALSE)), 2L)
})

test_that("qc_read_rules refuses a registry it cannot execute", {
  # a missing SQL file, or an active rule with no sql_file at all, must fail at
  # read time — both otherwise surface as a rule that quietly checks nothing
  dir_missing <- write_rule_dir(
    rules_tbl(rule_key = "r", rule_type = "range", severity = "warning",
              target = "obs", description = "d", sql_file = "absent.sql",
              params = NA_character_, scope = "all", requires_types = NA_character_,
              active = "TRUE"))
  expect_error(qc_read_rules(dir_missing), "missing SQL")

  dir_nosql <- write_rule_dir(
    rules_tbl(rule_key = "r", rule_type = "range", severity = "warning",
              target = "obs", description = "d", sql_file = NA_character_,
              params = NA_character_, scope = "all", requires_types = NA_character_,
              active = "TRUE"))
  expect_error(qc_read_rules(dir_nosql), "no sql_file")
})

test_that("qc_run_rule counts over the full result but returns at most `limit`", {
  con <- obs_con()
  rule <- qc_read_rules(write_rule_dir(
    rules_tbl(rule_key = "r_all", rule_type = "range", severity = "warning",
              target = "obs", description = "everything", sql_file = "r_all.sql",
              params = NA_character_, scope = "all", requires_types = NA_character_,
              active = "TRUE"),
    sql = list(r_all.sql = "SELECT sample_key AS subject_key, 'x' AS detail FROM obs")))

  res <- qc_run_rule(con, rule, limit = 1L)
  # n is the TRUE count; a truncated display must never understate the problem
  expect_equal(res$n, 3L)
  expect_equal(nrow(res$findings), 1L)
  expect_false(res$skipped)
  expect_true(is.na(res$error))
})

test_that("qc_run_rule renders params from the registry into the SQL", {
  con <- obs_con()
  rule <- qc_read_rules(write_rule_dir(
    rules_tbl(rule_key = "r_thr", rule_type = "range", severity = "warning",
              target = "obs", description = "over threshold",
              sql_file = "r_thr.sql", params = "threshold=40", scope = "all",
              requires_types = NA_character_, active = "TRUE"),
    sql = list(r_thr.sql = paste(
      "SELECT sample_key AS subject_key, 'over' AS detail FROM obs",
      "WHERE measurement_value > {{threshold}}"))))

  res <- qc_run_rule(con, rule)
  expect_equal(res$n, 1L)
  expect_equal(res$findings$subject_key, "ds:cast:2")
})

test_that("a rule whose input type is absent SKIPS — it never reports pass", {
  # the regression this locks down: the bottle-vs-sensor rules returned 0 rows
  # against a release that carried no bottle types, which read as "calibration is
  # fine" when nothing had been compared
  con  <- obs_con()
  rule <- qc_read_rules(write_rule_dir(
    rules_tbl(rule_key = "r_needs", rule_type = "crosscheck", severity = "warning",
              target = "obs", description = "needs a type that is not there",
              sql_file = "r_needs.sql", params = NA_character_, scope = "all",
              requires_types = "btl_temperature,temperature_ave", active = "TRUE"),
    sql = list(r_needs.sql = "SELECT sample_key AS subject_key, 'x' AS detail FROM obs")))

  res <- qc_run_rule(con, rule, present_types = qc_present_types(con))
  expect_true(res$skipped)
  expect_true(is.na(res$n))
  expect_match(res$skip_reason, "btl_temperature")

  # and it runs normally once the type IS present
  res_ok <- qc_run_rule(con, rule, present_types = c(
    "btl_temperature", "temperature_ave"))
  expect_false(res_ok$skipped)
  expect_equal(res_ok$n, 3L)
})

test_that("a cruise-scoped rule skips when no cruise is supplied", {
  con  <- obs_con()
  rule <- qc_read_rules(write_rule_dir(
    rules_tbl(rule_key = "r_cruise", rule_type = "profile", severity = "warning",
              target = "obs_ctd_full", description = "per cruise",
              sql_file = "r_cruise.sql", params = NA_character_, scope = "cruise",
              requires_types = NA_character_, active = "TRUE"),
    sql = list(r_cruise.sql = paste(
      "SELECT sample_key AS subject_key, 'x' AS detail FROM obs",
      "WHERE cruise_key = '{{cruise_key}}'"))))

  res <- qc_run_rule(con, rule)
  expect_true(res$skipped)
  expect_match(res$skip_reason, "needs a cruise")

  res_scoped <- qc_run_rule(con, rule, scope_values = list(cruise_key = "2020-01-AA"))
  expect_false(res_scoped$skipped)
  expect_equal(res_scoped$n, 3L)
})

test_that("qc_run_rule captures a SQL error instead of aborting the run", {
  con  <- obs_con()
  rule <- qc_read_rules(write_rule_dir(
    rules_tbl(rule_key = "r_bad", rule_type = "range", severity = "warning",
              target = "obs", description = "broken", sql_file = "r_bad.sql",
              params = NA_character_, scope = "all", requires_types = NA_character_,
              active = "TRUE"),
    sql = list(r_bad.sql = "SELECT * FROM table_that_does_not_exist")))

  res <- qc_run_rule(con, rule)
  expect_false(is.na(res$error))
  expect_true(is.na(res$n))
})

test_that("qc_summarize distinguishes pass, flag, FAIL, skip and ERROR", {
  rules <- rules_tbl(
    rule_key = c("r_pass", "r_flag", "r_fail", "r_skip", "r_err"),
    rule_type = "range",
    severity = c("warning", "warning", "error", "warning", "warning"),
    target = "obs", description = "d", sql_file = NA_character_,
    params = NA_character_, scope = "all", requires_types = NA_character_,
    active = "TRUE")

  results <- list(
    list(rule_key = "r_pass", n = 0L,  elapsed_s = 0.1, error = NA_character_,
         skipped = FALSE, skip_reason = NA_character_),
    list(rule_key = "r_flag", n = 12L, elapsed_s = 0.1, error = NA_character_,
         skipped = FALSE, skip_reason = NA_character_),
    list(rule_key = "r_fail", n = 3L,  elapsed_s = 0.1, error = NA_character_,
         skipped = FALSE, skip_reason = NA_character_),
    list(rule_key = "r_skip", n = NA_integer_, elapsed_s = 0, error = NA_character_,
         skipped = TRUE, skip_reason = "input absent from obs: btl_temperature"),
    list(rule_key = "r_err",  n = NA_integer_, elapsed_s = 0.1,
         error = "Catalog Error", skipped = FALSE, skip_reason = NA_character_))

  s <- qc_summarize(results, rules)
  expect_equal(s$status,
               c("pass", "flag", "FAIL", "skip", "ERROR"))
  # skip must never be folded into pass: they say opposite things about coverage
  expect_equal(s$note[s$rule_key == "r_skip"],
               "input absent from obs: btl_temperature")
  expect_equal(s$note[s$rule_key == "r_err"], "Catalog Error")
})

# -- cast profiles -------------------------------------------------------------

test_that("qc_cast_base strips only the direction suffix", {
  # the regression this locks down: gsub("d", "") also eats the `d` in
  # `calcofi_ctd-cast`, returning a key that matches nothing at all
  expect_equal(qc_cast_base("calcofi_ctd-cast:cast:9802_008d"),
               "calcofi_ctd-cast:cast:9802_008")
  expect_equal(qc_cast_base("calcofi_ctd-cast:cast:9802_008u"),
               "calcofi_ctd-cast:cast:9802_008")
  # no suffix: unchanged, not truncated
  expect_equal(qc_cast_base("calcofi_ctd-cast:cast:9802_008"),
               "calcofi_ctd-cast:cast:9802_008")
  expect_equal(qc_cast_base(c("a:1d", "a:1u")), c("a:1", "a:1"))
})

test_that("qc_cast_direction reads the suffix", {
  expect_equal(
    qc_cast_direction(c("x:1d", "x:1u", "x:1", NA_character_)),
    c("down", "up", NA, NA))
})

test_that("qc_cast_profile returns both directions of one physical cast", {
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))

  keys <- c("calcofi_ctd-cast:cast:9802_008d", "calcofi_ctd-cast:cast:9802_008u")
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = c(keys, "calcofi_ctd-cast:cast:9802_009d"),
    cruise_key = c("1998-02-31JD", "1998-02-31JD", "1998-02-31JD")))
  DBI::dbWriteTable(con, "obs_ctd_full", data.frame(
    sample_key = c(rep(keys, each = 2), "calcofi_ctd-cast:cast:9802_009d"),
    cruise_key = "1998-02-31JD",
    depth_min_m = c(10, 20, 10, 20, 10),
    measurement_type = c("temperature_ave", "temperature_ave",
                         "temperature_ave", "salinity_ave_corr",
                         "temperature_ave"),
    measurement_value = c(15.1, 13.2, 15.0, 33.4, 99),
    measurement_qual = NA_character_,
    datetime = as.POSIXct("1998-02-01 12:00:00", tz = "UTC")))

  p <- qc_cast_profile(con, keys[1])
  expect_setequal(unique(p$sample_key), keys)
  expect_setequal(unique(p$cast_dir), c("down", "up"))
  # the OTHER cast (…_009d) shares a cruise but not the base — it must not leak in
  expect_false(any(p$measurement_value == 99))
  expect_equal(nrow(p), 4L)

  # asking from the upcast key returns the same physical cast
  expect_equal(nrow(qc_cast_profile(con, keys[2])), 4L)

  # type filter
  pt <- qc_cast_profile(con, keys[1], measurement_types = "temperature_ave")
  expect_equal(unique(pt$measurement_type), "temperature_ave")
  expect_equal(nrow(pt), 3L)
})

test_that("qc_cast_profile can read the thinned obs too", {
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbWriteTable(con, "sample", data.frame(
    sample_key = "ds:cast:1d", cruise_key = "C1"))
  DBI::dbWriteTable(con, "obs", data.frame(
    sample_key = "ds:cast:1d", cruise_key = "C1", depth_min_m = 5,
    measurement_type = "temperature_ave", measurement_value = 12,
    measurement_qual = NA_character_,
    datetime = as.POSIXct("2020-01-01", tz = "UTC")))

  p <- qc_cast_profile(con, "ds:cast:1d", obs_tbl = "obs")
  expect_equal(nrow(p), 1L)
  expect_equal(p$cast_dir, "down")
})
