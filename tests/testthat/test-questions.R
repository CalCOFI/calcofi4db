# The provider-question registry. 17 CSVs accumulated four spellings of "done"
# (open / answered / resolved / wontfix) and two of "normal", because every ingest
# notebook read the file with a bare read_csv() and sorted by its own hand-written
# factor level vector — a status nobody's vector listed sorted to the bottom and
# was never seen again. These pin that the vocabulary is now enforced.

q_fixture <- function(env = parent.frame(), ...) {
  d <- tibble::tibble(
    label            = c("Q01", "Q02", "Q03"),
    id               = c("calcofi_ctd-cast_01", "calcofi_ctd-cast_02",
                         "calcofi_ctd-cast_03"),
    question         = c("a?", "b?", "c?"),
    context          = c("ctx a", "ctx b", ""),
    status           = c("open", "proposed", "answered"),
    priority         = c("normal", "blocker", "low"),
    proposed_answer  = c(NA, "we did X, please confirm", NA),
    answer           = c(NA, NA, "yes"),
    asked_date       = c("2026-08-01", "2026-08-01", "2026-07-01"),
    answered_date    = c(NA, NA, "2026-07-15"),
    who              = "CalCOFI data team (SIO)",
    related_table    = c("obs", "sample", NA),
    related_field    = c("measurement_value", NA, NA))
  d <- dplyr::mutate(d, ...)
  path <- withr::local_tempfile(fileext = ".csv", .local_envir = env)
  readr::write_csv(d, path, na = "")
  path
}

test_that("read_questions() ranks blocker first, then by label", {
  q <- read_questions(q_fixture())
  expect_equal(q$label, c("Q02", "Q01", "Q03"))   # blocker, normal, low
  expect_equal(q$priority, c("blocker", "normal", "low"))
})

test_that("everything reads back as character", {
  # an id suffix of "01" retyped to the number 1, or an empty date retyped to
  # logical NA, both corrupt the file on the next write
  q <- read_questions(q_fixture())
  expect_true(all(vapply(q, is.character, logical(1))))
  expect_equal(q$asked_date[q$label == "Q03"], "2026-07-01")
})

test_that("an unknown status or priority is an error naming the value", {
  expect_error(read_questions(q_fixture(status = c("open", "resolved", "open"))),
               "unknown question status")
  expect_error(read_questions(q_fixture(status = c("open", "resolved", "open"))),
               "resolved")
  expect_error(read_questions(q_fixture(priority = c("medium", "high", "low"))),
               "unknown question priority")
  expect_error(read_questions(q_fixture(priority = c("medium", "high", "low"))),
               "medium")
  # and the error must say what IS allowed, so it is actionable
  expect_error(read_questions(q_fixture(status = c("asked", "open", "open"))),
               "open \\| proposed \\| answered \\| wontfix")
})

test_that("labels must be unique within a dataset and well formed", {
  expect_error(read_questions(q_fixture(label = c("Q01", "Q01", "Q03"))),
               "duplicate question label")
  expect_error(read_questions(q_fixture(label = c("Q01", "2", "Q03"))),
               "malformed question label")
  # hydro-master's second namespace is legal: label is authored, not derived
  expect_no_error(read_questions(q_fixture(label = c("Q01", "QR01", "Q02b"))))
})

test_that("a missing column is named rather than surfacing later as NULL", {
  path <- q_fixture()
  d <- readr::read_csv(path, na = "", show_col_types = FALSE)
  readr::write_csv(d[, setdiff(names(d), "proposed_answer")], path, na = "")
  expect_error(read_questions(path), "missing column\\(s\\): proposed_answer")
})

test_that("the write_csv(na='NA') round trip is refused", {
  path <- q_fixture()
  readr::write_csv(readr::read_csv(path, show_col_types = FALSE), path)  # default na
  expect_error(read_questions(path), "sentinel strings")
})

test_that("questions_datatable() drops all-empty columns and keeps the rest", {
  d <- read_questions(q_fixture())
  w <- questions_datatable(d)
  cols <- names(w$x$data)

  expect_true(all(c("label", "priority", "status", "question") %in% cols))
  expect_true("proposed_answer" %in% cols)   # one row has one
  expect_true("related_field"   %in% cols)   # one row has one
  expect_equal(cols[1], "label")             # what prose cites, read first

  # a registry with no answers at all renders no `answer` column
  d2 <- read_questions(q_fixture(answer = NA_character_,
                                 proposed_answer = NA_character_))
  cols2 <- names(questions_datatable(d2)$x$data)
  expect_false("answer" %in% cols2)
  expect_false("proposed_answer" %in% cols2)
})
