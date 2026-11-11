# RELEASES.md is the database NEWS file; these pin the three things the release
# depends on: a version's section is found (exact or by range), `# Unreleased`
# is promoted exactly once and refused when empty, and the rendered notes carry
# the narrative plus a generated appendix, byte-stably.

rn_md <- c(
  "# CalCOFI integrated database — release notes", "", "intro", "",
  "# Unreleased", "",
  "## Something is now true", "", "because.", "",
  "# v2026.09.10", "", "## Undated heading", "", "text u", "",
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
  expect_true("# v2026.08.25" %in% lines)
  expect_false(any(grepl("^# v2026.08.25 \\(", lines)))   # no date in the heading (2026-09-11)
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

test_that("render_release_notes() marks a deprecated table, in both catalog forms (D-S1)", {
  # list form (jsonlite simplifyVector = FALSE), as cc_catalog() reads it
  cat_l <- list(version = "v2026.08.14", release_date = "2026-08-14", total_size = 1e9,
                tables = list(
                  list(name = "obs", rows = 26261931, partitioned = TRUE, supplemental = FALSE,
                       deprecated = TRUE, replaced_by = list("obs_bio", "obs_env"), removed_in = "next"),
                  list(name = "obs_bio", rows = 1255348, partitioned = FALSE, supplemental = FALSE)),
                views = list(obs = "SELECT 1"))
  md <- render_release_notes("v2026.08.14", rn_md, cat_l)
  expect_match(md, "\\| `obs` \\| 26,261,931 \\| deprecated → `obs_bio`, `obs_env` \\(objects removed in next\\) \\|")
  expect_match(md, "\\| `obs_bio` \\| 1,255,348 \\|  \\|")
  # data-frame form (simplifyVector = TRUE), as publish_release_notes() reads the sidecar
  cat_d <- jsonlite::fromJSON(jsonlite::toJSON(cat_l, auto_unbox = TRUE), simplifyVector = TRUE)
  expect_true(is.data.frame(cat_d$tables))
  expect_identical(render_release_notes("v2026.08.14", rn_md, cat_d), md)
  # no deprecation fields at all: unchanged rendering
  expect_match(render_release_notes("v2026.08.14", rn_md, list(release_date = "2026-08-14",
    tables = data.frame(name = "obs", rows = 5, partitioned = TRUE, supplemental = FALSE))),
    "\\| `obs` \\| 5 \\| partitioned \\|")
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

# the public page (RELEASES.html) is rendered from release_notes_page(): the
# source keeps `# Unreleased` for the release gate, the page never leads with an
# empty one, and each release shows the DOI versions.json records for it.
rn_page_md <- c(
  "# CalCOFI integrated database — release notes", "", "intro", "",
  "# Unreleased", "",
  "# v2026.11.09", "", "## More casts", "", "text a", "",
  "# v2026.08.25", "", "## No DOI yet", "", "text b", "",
  "# v2026.08.04 – v2026.08.06", "", "three releases")
rn_page_versions <- data.frame(
  version = c("v2026.11.09", "v2026.08.25", "v2026.08.06", "v2026.08.05", "v2026.08.04"),
  doi     = c("10.5281/zenodo.24083894", NA, NA, NA, NA))

test_that("release_notes_page() drops an empty Unreleased and prints each release's own DOI", {
  expect_identical(
    strsplit(release_notes_page(rn_page_md, rn_page_versions), "\n")[[1]],
    c("# CalCOFI integrated database — release notes", "", "intro", "",
      "# v2026.11.09", "",
      "**DOI:** [10.5281/zenodo.24083894](https://doi.org/10.5281/zenodo.24083894)", "",
      "## More casts", "", "text a", "",
      "# v2026.08.25", "", "## No DOI yet", "", "text b", "",        # a release with no DOI shows none
      "# v2026.08.04 – v2026.08.06", "", "three releases"))
  # the same text as one string, and versions.json read either way jsonlite reads it
  one  <- paste(rn_page_md, collapse = "\n")
  json <- jsonlite::toJSON(list(versions = rn_page_versions), na = "null")
  expect_identical(release_notes_page(one, rn_page_versions),
                   release_notes_page(rn_page_md, rn_page_versions))
  expect_identical(release_notes_page(rn_page_md, jsonlite::fromJSON(json)),
                   release_notes_page(rn_page_md, rn_page_versions))
  expect_identical(release_notes_page(rn_page_md, jsonlite::fromJSON(json, simplifyVector = FALSE)),
                   release_notes_page(rn_page_md, rn_page_versions))
  # the source is the caller's: the heading the release gate needs is still in it
  expect_true("# Unreleased" %in% rn_page_md)
})

test_that("release_notes_page() keeps an Unreleased section that has entries", {
  md  <- append(rn_page_md, c("## Something is now true", "", "because.", ""), after = 6)
  out <- strsplit(release_notes_page(md, rn_page_versions), "\n")[[1]]
  expect_identical(out[5:10], c("# Unreleased", "", "## Something is now true", "", "because.", ""))
  expect_identical(out[11:13], c(
    "# v2026.11.09", "", "**DOI:** [10.5281/zenodo.24083894](https://doi.org/10.5281/zenodo.24083894)"))
  # whitespace alone is not an entry
  expect_false(any(grepl("^# Unreleased", strsplit(
    release_notes_page(append(rn_page_md, c("  ", ""), after = 6)), "\n")[[1]])))
})

test_that("release_notes_page() prints no DOI it was not given, and never one twice", {
  # no versions.json: only the empty Unreleased goes
  expect_identical(strsplit(release_notes_page(rn_page_md), "\n")[[1]], rn_page_md[-(5:6)])
  # no `doi` column, an empty one, and a value that is not a DOI: no line
  for (v in list(data.frame(version = "v2026.11.09"),
                 data.frame(version = "v2026.11.09", doi = ""),
                 data.frame(version = "v2026.11.09", doi = "pending"),
                 list(list(version = "v2026.11.09", doi = NULL))))
    expect_false(grepl("**DOI:**", release_notes_page(rn_page_md, v), fixed = TRUE))
  # a resolver URL in `doi` is read as its DOI
  expect_match(release_notes_page(rn_page_md, data.frame(
    version = "v2026.11.09", doi = "https://doi.org/10.5281/zenodo.24083894")),
    "**DOI:** [10.5281/zenodo.24083894](https://doi.org/10.5281/zenodo.24083894)", fixed = TRUE)
  # a range heading names each version in its span that has one, the middle one included
  rng <- rn_page_versions
  rng$doi[rng$version %in% c("v2026.08.04", "v2026.08.05")] <- c("10.5281/zenodo.5", "10.5281/zenodo.4")
  out <- strsplit(release_notes_page(rn_page_md, rng), "\n")[[1]]
  expect_identical(out[which(out == "# v2026.08.04 – v2026.08.06") + 0:3], c(
    "# v2026.08.04 – v2026.08.06", "",
    paste0("**DOI:** v2026.08.04 [10.5281/zenodo.4](https://doi.org/10.5281/zenodo.4) · ",
           "v2026.08.05 [10.5281/zenodo.5](https://doi.org/10.5281/zenodo.5)"), ""))
  # a version's DOI sits under its own heading only, and a heading that merely
  # mentions a version (not its entry) gets none
  md2 <- c("# Unreleased (after v2026.11.09)", "", "## pending", "", "# v2026.11.09", "", "x",
           "", "# v2026.11.09", "", "again")
  expect_identical(sum(grepl("^\\*\\*DOI:\\*\\*", strsplit(
    release_notes_page(md2, rn_page_versions), "\n")[[1]])), 1L)
})
