# RELEASES.md is the database NEWS file; these pin the three things the release
# depends on: a version's section is found (exact or by range), `# Unreleased`
# is promoted exactly once and refused when empty, and the rendered notes carry
# the narrative plus a generated appendix, byte-stably.

rn_md <- c(
  "# CalCOFI integrated database — release notes", "", "intro", "",
  "# Unreleased", "",
  "## Something is now true", "", "because.", "",
  "# v2026.08.14 (2026-08-14)", "", "## Dungeness enters", "", "text a", "",
  "# v2026.08.04 – v2026.08.06 (2026-08-04 … 2026-08-06)", "", "three releases", "",
  "# v2026.03 (2026-03-06)", "", "old")

test_that("release_notes_section() finds exact and range headings, not others", {
  expect_equal(release_notes_section(rn_md, "v2026.08.14")$body, "## Dungeness enters\n\ntext a")
  expect_equal(release_notes_section(rn_md, "v2026.08.05")$body, "three releases")
  expect_equal(release_notes_section(rn_md, "v2026.08.04")$body, "three releases")
  expect_equal(release_notes_section(rn_md, "v2026.03")$body, "old")
  expect_null(release_notes_section(rn_md, "v2026.08.25"))
  expect_null(release_notes_section(rn_md, "v2026.08.07"))    # outside the range
  expect_equal(release_notes_section(rn_md, "v2026.08.14")$date, "2026-08-14")
})

test_that("promote_unreleased() renames a non-empty Unreleased once and refuses an empty one", {
  out <- promote_unreleased(rn_md, "v2026.08.25", as.Date("2026-08-25"))
  lines <- strsplit(out, "\n")[[1]]
  expect_equal(sum(grepl("^# Unreleased", lines)), 1)
  expect_true("# v2026.08.25 (2026-08-25)" %in% lines)
  expect_equal(release_notes_section(lines, "v2026.08.25")$body,
               "## Something is now true\n\nbecause.")
  expect_equal(release_notes_section(lines, "Unreleased"), NULL)   # versions only
  # idempotent: the new Unreleased is empty and the section exists -> unchanged
  expect_equal(promote_unreleased(lines, "v2026.08.25"), out)
  # empty Unreleased and no section -> hard stop
  expect_error(promote_unreleased(lines, "v2026.09.01"), "Unreleased")
  # both present -> refuse to guess
  both <- c(lines[1:5], "", "## new", "", "x", lines[7:length(lines)])
  expect_error(promote_unreleased(both, "v2026.08.25"), "BOTH")
})

test_that("render_release_notes() carries the narrative and a generated appendix", {
  cat_ <- list(version = "v2026.08.14", release_date = "2026-08-14", total_size = 2023247413,
               tables = data.frame(name = c("obs", "sample", "obs_ctd_full"),
                                   rows = c(25624046, 1466254, 259309891),
                                   partitioned = c(TRUE, FALSE, TRUE),
                                   supplemental = c(FALSE, FALSE, TRUE)))
  meta <- list(datasets = data.frame(dataset_key = c("calcofi_bottle", "swfsc_ichthyo")))
  tr <- list(n_pass = 28, n_fail = 0, n_skip = 4, tested_at = "2026-08-14T00:00:00Z")
  md <- render_release_notes("v2026.08.14", rn_md, cat_, meta, tr,
                             c(calcofi4db = "3.19.0"), promoted = TRUE)
  expect_match(md, "^# CalCOFI integrated database release v2026\\.08\\.14")
  expect_match(md, "\\*\\*Release date:\\*\\* 2026-08-14 · \\*\\*promoted\\*\\*")
  expect_match(md, "## Dungeness enters")
  expect_match(md, "\\| `obs` \\| 25,624,046 \\| partitioned \\|")
  expect_match(md, "\\| `obs_ctd_full` \\| 259,309,891 \\| supplemental \\|")
  expect_match(md, "\\*\\*3 tables, 286,400,191 rows, 2\\.02 GB\\.\\*\\*")
  expect_match(md, "\\*\\*Datasets \\(2\\):\\*\\* `calcofi_bottle`, `swfsc_ichthyo`")
  expect_match(md, "28 pass / 0 fail / 4 skip")
  expect_match(md, "calcofi4db 3\\.19\\.0")
  expect_match(md, 'cc_get_db\\(version = "v2026\\.08\\.14"\\)')
  # byte-stable
  expect_identical(md, render_release_notes("v2026.08.14", rn_md, cat_, meta, tr,
                                            c(calcofi4db = "3.19.0"), promoted = TRUE))
  # a range-documented version says so, and a missing section fails
  r5 <- render_release_notes("v2026.08.05", rn_md, catalog = list(release_date = "2026-08-05"))
  expect_match(r5, "Documented with v2026.08.04")
  expect_match(r5, "\\*\\*Release date:\\*\\* 2026-08-05")   # its own date, not the range's
  expect_error(render_release_notes("v2026.08.25", rn_md), "no section")
})

test_that("publish_release_notes() writes the local file from the sidecars (no upload)", {
  d <- withr::local_tempdir()
  dir.create(file.path(d, "v2026.08.14"))
  jsonlite::write_json(list(version = "v2026.08.14", release_date = "2026-08-14",
                            total_size = 1e9, tables = data.frame(name = "obs", rows = 5)),
                       file.path(d, "v2026.08.14", "catalog.json"), auto_unbox = TRUE)
  f <- file.path(d, "RELEASES.md"); writeLines(rn_md, f)
  out <- publish_release_notes("v2026.08.14", f, d, bucket = NULL)
  expect_true(file.exists(out))
  expect_match(paste(readLines(out), collapse = "\n"), "Dungeness enters")
  expect_error(publish_release_notes("v2026.08.25", f, d, bucket = NULL), "no section")
})
