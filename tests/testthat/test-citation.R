# The attribution contract (WS-A0, 2026-09-03): every dataset in a release carries a
# citation that was CHECKED, a license from a REGISTRY, a MEASURED source_accessed,
# and the release cites itself. Nothing here touches the network: the resolver
# parsers run on saved responses under fixtures/citation/, and the end-to-end
# check is driven with an injected `fetch` that serves those same files.

fx <- function(f) testthat::test_path("fixtures", "citation", f)
fx_read <- function(f) paste(readLines(fx(f), warn = FALSE, encoding = "UTF-8"), collapse = "\n")

# a fetcher that answers from the fixtures and records every URL it was asked for
fake_fetch <- function(map, status_default = 404L) {
  asked <- character()
  f <- function(url, accept = NULL, method = "GET", ...) {
    asked <<- c(asked, url)
    for (pat in names(map)) if (grepl(pat, url, fixed = TRUE)) {
      v <- map[[pat]]
      if (is.list(v)) return(v)
      return(list(status = 200L, content = v, url = url))
    }
    list(status = status_default, content = "", url = url)
  }
  attr(f, "asked") <- function() asked
  f
}

# a metadata root with license.csv (+ optional questions.csv per dataset)
make_meta_dir <- function(env = parent.frame(), questions = list()) {
  dir <- withr::local_tempdir(.local_envir = env)
  writeLines(c(
    "license,name,url,status,notes",
    "CC-BY-4.0,Creative Commons Attribution 4.0,https://creativecommons.org/licenses/by/4.0/,active,",
    "CC0-1.0,CC0 1.0 Universal,https://creativecommons.org/publicdomain/zero/1.0/,active,",
    "US-PD,US Government work,https://www.usa.gov/government-works,active,\"17 U.S.C. § 105\"",
    "custom,Custom terms,,active,requires license_url",
    "unknown,Not yet established,,active,fails the index unless a question is open",
    "CC-BY-3.0,Creative Commons Attribution 3.0,https://creativecommons.org/licenses/by/3.0/,deprecated,superseded"),
    file.path(dir, "license.csv"))
  for (k in names(questions)) {
    pd <- strsplit(k, "_", fixed = TRUE)[[1]]
    qd <- file.path(dir, pd[1], paste(pd[-1], collapse = "_"))
    dir.create(qd, recursive = TRUE)
    writeLines(c(
      "label,id,question,context,status,priority,proposed_answer,answer,asked_date,answered_date,who,related_table,related_field",
      questions[[k]]), file.path(qd, "questions.csv"))
  }
  dir
}

# one ingest block, as read_ingest_yaml() would return it
yaml_ds <- function(provider, dataset, ...) {
  m <- list(...)
  list(provider = provider, dataset = dataset,
       provider_dataset = paste0(provider, "_", dataset),
       dataset_meta = c(list(dataset_name = paste(provider, dataset)), m))
}
as_yaml <- function(...) {
  l <- list(...)
  stats::setNames(l, vapply(l, function(x) x$provider_dataset, ""))
}

# license registry ---------------------------------------------------------------

test_that("read_license_registry() validates shape, vocabulary and sentinel strings", {
  dir <- make_meta_dir()
  reg <- read_license_registry(file.path(dir, "license.csv"))
  expect_setequal(names(reg), c("license", "name", "url", "status", "notes"))
  expect_true(all(reg$status %in% license_statuses()))
  expect_equal(nrow(reg), 6)

  bad <- file.path(dir, "bad.csv")
  writeLines(c("license,name,url,status,notes", "X,x,,retired,"), bad)
  expect_error(read_license_registry(bad), "status")
  writeLines(c("license,name,url,status,notes", "X,x,,active,", "X,y,,active,"), bad)
  expect_error(read_license_registry(bad), "duplicate")
  writeLines(c("license,name,url,status,notes", "X,x,NA,active,"), bad)
  expect_error(read_license_registry(bad), "sentinel")
  writeLines(c("license,name,status", "X,x,active"), bad)
  expect_error(read_license_registry(bad), "missing column")
})

# structural findings: one fixture per finding ------------------------------------

test_that("check_dataset_citation() reports each structural finding on its own row", {
  dir <- make_meta_dir()
  y <- as_yaml(
    yaml_ds("aa", "clean", citation_main = "Doe, J. (2020). A dataset. https://doi.org/10.1234/abc",
            license = "CC-BY-4.0", doi = "10.1234/abc"),
    yaml_ds("aa", "nocite", citation_main = "", license = "CC-BY-4.0"),
    yaml_ds("aa", "noyear", citation_main = "Doe, J. A dataset. https://example.org/d",
            license = "CC-BY-4.0"),
    yaml_ds("aa", "nolocator", citation_main = "Doe, J. (2020). A dataset.", license = "CC-BY-4.0"),
    yaml_ds("aa", "linked", citation_main = "Doe, J. (2020). A dataset.", license = "CC-BY-4.0",
            link_data_source = "https://example.org/portal"),
    yaml_ds("aa", "nolicense", citation_main = "Doe, J. (2020). A dataset. https://x.org", license = ""),
    yaml_ds("aa", "unknownlic", citation_main = "Doe, J. (2020). A dataset. https://x.org",
            license = "unknown"),
    yaml_ds("aa", "freetext", citation_main = "Doe, J. (2020). A dataset. https://x.org",
            license = "CC BY 4.0"),
    yaml_ds("aa", "deprecated", citation_main = "Doe, J. (2020). A dataset. https://x.org",
            license = "CC-BY-3.0"),
    yaml_ds("aa", "customnourl", citation_main = "Doe, J. (2020). A dataset. https://x.org",
            license = "custom"),
    yaml_ds("aa", "customok", citation_main = "Doe, J. (2020). A dataset. https://x.org",
            license = "custom", license_url = "https://x.org/terms"),
    yaml_ds("aa", "baddoi", citation_main = "Doe, J. (2020). A dataset. https://x.org",
            license = "CC-BY-4.0", doi = "https://doi.org/10.1234/abc"),
    yaml_ds("aa", "both", citation_main = "", license = ""))
  d <- check_dataset_citation(y, network = FALSE, cache_dir = dir)

  expect_equal(names(d)[1:6], c("dataset_key", "finding", "detail", "authority",
                                "authority_citation", "checked"))
  expect_true(all(c("level", "exempt", "question") %in% names(d)))
  f <- function(k) sort(d$finding[d$dataset_key == k])
  expect_equal(f("aa_clean"),       "ok")
  expect_equal(f("aa_nocite"),      "missing_citation")
  expect_equal(f("aa_noyear"),      "no_year")
  expect_equal(f("aa_nolocator"),   "no_locator")
  expect_equal(f("aa_linked"),      "ok")                       # link_data_source is a locator
  expect_equal(f("aa_nolicense"),   "missing_license")
  expect_equal(f("aa_unknownlic"),  "missing_license")
  expect_equal(f("aa_freetext"),    "license_unregistered")
  expect_equal(f("aa_deprecated"),  "license_unregistered")
  expect_equal(f("aa_customnourl"), "license_custom_no_url")
  expect_equal(f("aa_customok"),    "ok")
  expect_equal(f("aa_baddoi"),      "doi_unresolved")           # not a bare DOI, no network needed
  # two problems -> two rows; a dataset is never summarised into one
  expect_equal(f("aa_both"), c("missing_citation", "missing_license"))
  expect_equal(unique(d$level[d$finding == "ok"]), "ok")
  expect_true(all(d$level[d$finding != "ok"] == "error"))
  expect_false(any(d$exempt))
  expect_true(all(is.na(d$authority)))
  # the empty-vs-unknown distinction survives in detail
  expect_match(d$detail[d$dataset_key == "aa_nolicense"], "empty")
  expect_match(d$detail[d$dataset_key == "aa_unknownlic"], "unknown")
})

test_that("an open or proposed question on the dataset exempts the finding it names", {
  dir <- make_meta_dir(questions = list(
    aa_one = c(
      "Q01,aa_one_01,license?,ctx,proposed,normal,CC-BY-4.0,,,,PI,dataset,license"),
    aa_two = c(
      "Q01,aa_two_01,cite?,ctx,open,normal,,,,,PI,dataset,"),
    aa_three = c(
      "Q01,aa_three_01,cite?,ctx,answered,normal,,done,,2026-01-01,PI,dataset,citation_main"),
    aa_four = c(
      "Q01,aa_four_01,units?,ctx,open,normal,,,,,PI,obs,measurement_value")))
  y <- as_yaml(
    yaml_ds("aa", "one",   citation_main = "", license = ""),
    yaml_ds("aa", "two",   citation_main = "", license = ""),
    yaml_ds("aa", "three", citation_main = "", license = "CC-BY-4.0"),
    yaml_ds("aa", "four",  citation_main = "", license = "CC-BY-4.0"))
  d <- check_dataset_citation(y, network = FALSE, cache_dir = dir)
  g <- function(k, f) d[d$dataset_key == k & d$finding == f, ]
  # a row naming the field exempts that field only
  expect_true(g("aa_one", "missing_license")$exempt)
  expect_equal(g("aa_one", "missing_license")$question, "Q01")
  expect_false(g("aa_one", "missing_citation")$exempt)
  # a row with no field exempts every finding on the dataset
  expect_true(g("aa_two", "missing_citation")$exempt)
  expect_true(g("aa_two", "missing_license")$exempt)
  # answered rows and rows on another table exempt nothing
  expect_false(g("aa_three", "missing_citation")$exempt)
  expect_false(g("aa_four", "missing_citation")$exempt)

  expect_error(assert_dataset_citation(d), "aa_one.*missing_citation")
  expect_silent(assert_dataset_citation(d[d$dataset_key == "aa_two", ], quiet = TRUE))
})

# resolver parsers on saved responses -----------------------------------------------

test_that("parse_edi_cite() keeps the ESIP string verbatim and pulls the DOI", {
  p <- parse_edi_cite(fx_read("edi_cite_knb-lter-cce.254.4.txt"))
  expect_match(p$citation, "^CalCOFI - Scripps Institution of Oceanography, California Current Ecosystem LTER, and E\\. Venrick\\. 2023\\.")
  expect_equal(p$doi, "10.6073/pasta/60edabfbfd85c623fce05822befaa071")
  expect_equal(p$year, "2023")
})

test_that("parse_erddap_das() reads the NC_GLOBAL strings, including multi-line ones", {
  p <- parse_erddap_das(fx_read("erddap_CAC_FI_SBAS_obs.das"))
  expect_equal(p$title, "CalCOFI Farallon Institute Seabirds: Observations")
  expect_equal(p$creator_name, "Sarah Ann Thompson")
  expect_equal(p$institution, "Farallon Institute, Scripps Institution of Oceanography")
  expect_match(p$license, "^please see: https://oceanview\\.pfeg\\.noaa\\.gov/")
  expect_null(p$citation)
  # a multi-line value (cufes' license) is joined, not truncated at the first line
  das <- 'Attributes {\n NC_GLOBAL {\n    String license "The data may be used\nand redistributed for free.";\n    String title "T";\n }\n}\n'
  expect_equal(parse_erddap_das(das)$license, "The data may be used and redistributed for free.")
})

test_that("parse_ncei_landing() takes the 'Cite as' block minus its placeholders", {
  p <- parse_ncei_landing(fx_read("ncei_landing_0301029.html"))
  expect_match(p$citation, "^Keeling, Charles D\\.; Lueker, Timothy J\\.")
  expect_match(p$citation, "https://doi\\.org/10\\.25921/3w9f-jd72\\.?$")
  expect_false(grepl("indicate subset used|Accessed \\[date\\]", p$citation))
  expect_equal(p$doi, "10.25921/3w9f-jd72")
})

test_that("parse_datacite() lifts the SPDX license, publisher, year and creators", {
  p <- parse_datacite(fx_read("datacite_10.25921_3w9f-jd72.json"))
  expect_equal(p$doi, "10.25921/3w9f-jd72")
  expect_equal(p$license, "CC-BY-4.0")
  expect_equal(p$year, "2025")
  expect_equal(p$publisher, "NOAA National Centers for Environmental Information")
  expect_equal(p$creators[1], "Keeling, Charles D.")
  expect_length(p$creators, 7)
  expect_match(p$title, "^Discrete profile dissolved inorganic carbon")
  expect_equal(p$url, "https://www.ncei.noaa.gov/archive/accession/0301029")
})

test_that("parse_doi_bibliography() strips the markup doi.org content negotiation returns", {
  p <- parse_doi_bibliography(fx_read("doi_apa_10.25921_3w9f-jd72.txt"))
  expect_match(p, "^Keeling, Charles D\\., Lueker, Timothy J\\.")
  expect_match(p, "& Mau, Aaron\\. \\(2025\\)\\. Discrete profile")
  expect_false(grepl("<i>|&amp;", p))
})

test_that("normalized citations ignore case, punctuation, whitespace and a trailing period", {
  a <- "CalCOFI - Scripps Institution of Oceanography, and E. Venrick. 2023. Title. https://doi.org/10.6073/pasta/ABC."
  b <- "CalCOFI - Scripps Institution of Oceanography, and E. Venrick. 2023. Title. https://doi.org/10.6073/pasta/abc"
  expect_equal(normalize_citation(a), normalize_citation(b))
  expect_false(normalize_citation(a) == normalize_citation(sub("2023", "2024", b)))
})

# the end-to-end network path, served from fixtures --------------------------------

test_that("check_dataset_citation() resolves EDI, NCEI, ERDDAP and DataCite, caches, and reports drift", {
  dir <- make_meta_dir()
  edi_url  <- "https://cite.edirepository.org/cite/knb-lter-cce.254.4?style=ESIP"
  ncei_url <- "https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.nodc:0301029"
  das_url  <- "https://oceanview.pfeg.noaa.gov/erddap/tabledap/CAC_FI_SBAS_obs.das"
  fetch <- fake_fetch(list(
    "cite.edirepository.org/cite/knb-lter-cce.254.4"  = fx_read("edi_cite_knb-lter-cce.254.4.txt"),
    "cite.edirepository.org/cite/knb-lter-cce.313.1"  = "Ohman, M.D. 2022. BTEDB ver 1. Environmental Data Initiative. https://doi.org/10.6073/pasta/4a92.",
    "cite.edirepository.org/cite/knb-lter-cce.313.2"  = list(status = 400L, content = "", url = ""),
    "ncei.noaa.gov/access/metadata/landing-page"       = fx_read("ncei_landing_0301029.html"),
    "CAC_FI_SBAS_obs.das"                              = fx_read("erddap_CAC_FI_SBAS_obs.das"),
    "api.datacite.org/dois/10.25921/3w9f-jd72"         = fx_read("datacite_10.25921_3w9f-jd72.json"),
    "doi.org/10.25921/3w9f-jd72"                       = list(status = 302L, content = "", url = ""),
    "doi.org/10.6073/pasta/60edabfbfd85c623fce05822befaa071" = list(status = 302L, content = "", url = ""),
    "doi.org/10.6073/pasta/4a92"                       = list(status = 302L, content = "", url = ""),
    "doi.org/10.9999/nope"                             = list(status = 404L, content = "", url = "")))
  esip <- sub("\\.$", "", fx_read("edi_cite_knb-lter-cce.254.4.txt"))
  y <- as_yaml(
    # EDI with an explicit revision: the author copied the ESIP string -> no drift
    yaml_ds("calcofi", "phyto", citation_main = esip, license = "CC0-1.0",
            doi = "10.6073/pasta/60edabfbfd85c623fce05822befaa071",
            link_data_source = "https://portal.edirepository.org/nis/mapbrowse?packageid=knb-lter-cce.254.4"),
    # EDI without a revision: newest found by probing the cite service (rev 2 answers 400)
    yaml_ds("cce-lter", "euph", citation_main = "Ohman, M.D. 2022. BTEDB ver 1. Environmental Data Initiative. https://doi.org/10.6073/pasta/4a92",
            license = "custom", license_url = "https://portal.edirepository.org/x",
            link_data_source = "https://portal.edirepository.org/nis/mapbrowse?scope=knb-lter-cce&identifier=313"),
    # NCEI: abbreviated author names in the YAML vs the landing page -> drift (warn), DOI resolves,
    # and DataCite's SPDX rights agree with the declared license
    yaml_ds("calcofi", "dic", citation_main = "Keeling, C.D.; Lueker, T.J. (2025). Discrete profile DIC (NCEI Accession 0301029). NOAA NCEI. https://doi.org/10.25921/3w9f-jd72",
            license = "CC-BY-4.0", doi = "10.25921/3w9f-jd72", link_data_source = ncei_url),
    # ERDDAP: no citation string in the .das -> nothing to compare; title/creator cached
    yaml_ds("farallon", "birds", citation_main = "", license = "custom",
            license_url = "https://oceanview.pfeg.noaa.gov/x.pdf",
            link_data_source = "https://oceanview.pfeg.noaa.gov/erddap/tabledap/CAC_FI_SBAS_obs.html"),
    # a DOI that does not resolve
    yaml_ds("aa", "deadoi", citation_main = "Doe (2020). X. https://x.org", license = "CC-BY-4.0",
            doi = "10.9999/nope"))
  d <- check_dataset_citation(y, network = TRUE, cache_dir = dir, fetch = fetch)
  f <- function(k) sort(d$finding[d$dataset_key == k])

  expect_equal(f("calcofi_phyto"), "ok")
  expect_equal(d$authority[d$dataset_key == "calcofi_phyto"], "edi")
  expect_equal(d$authority_citation[d$dataset_key == "calcofi_phyto"], fx_read("edi_cite_knb-lter-cce.254.4.txt"))
  expect_equal(d$checked[d$dataset_key == "calcofi_phyto"], format(Sys.Date()))

  expect_equal(f("cce-lter_euph"), "ok")
  expect_true(any(grepl("knb-lter-cce.313.2", attr(fetch, "asked")())))   # probed past rev 1

  expect_equal(f("calcofi_dic"), "authority_drift")
  expect_equal(d$level[d$dataset_key == "calcofi_dic"], "warn")
  expect_match(d$detail[d$dataset_key == "calcofi_dic"], "^declared: Keeling, C\\.D\\.")
  expect_match(d$detail[d$dataset_key == "calcofi_dic"], "\nauthority \\(ncei, [0-9-]{10}\\): Keeling, Charles D\\.")
  expect_equal(d$authority[d$dataset_key == "calcofi_dic"], "ncei")

  expect_equal(f("farallon_birds"), "missing_citation")
  expect_equal(d$authority[d$dataset_key == "farallon_birds"], "erddap")
  expect_true(is.na(d$authority_citation[d$dataset_key == "farallon_birds"]))

  expect_equal(f("aa_deadoi"), "doi_unresolved")
  expect_match(d$detail[d$dataset_key == "aa_deadoi"], "404")

  # every fetch was cached where the brief says, with the documented keys
  cache <- file.path(dir, "calcofi", "dic", "citation_authority.json")
  expect_true(file.exists(cache))
  j <- jsonlite::fromJSON(cache)
  expect_true(all(c("authority", "url", "citation", "license", "creator", "title", "checked") %in% names(j)))
  expect_equal(j$authority, "ncei")
  expect_equal(j$license, "CC-BY-4.0")
  expect_match(j$citation, "^Keeling, Charles D\\.")
  j2 <- jsonlite::fromJSON(file.path(dir, "farallon", "birds", "citation_authority.json"))
  expect_equal(j2$creator, "Sarah Ann Thompson")
  expect_equal(j2$title, "CalCOFI Farallon Institute Seabirds: Observations")
  # the authority never reaches the YAML: the block passed in is untouched
  expect_equal(y$calcofi_dic$dataset_meta$citation_main,
               "Keeling, C.D.; Lueker, T.J. (2025). Discrete profile DIC (NCEI Accession 0301029). NOAA NCEI. https://doi.org/10.25921/3w9f-jd72")

  # a second run costs nothing: the cache answers, the fetcher is never called
  n_before <- length(attr(fetch, "asked")())
  d2 <- check_dataset_citation(y, network = TRUE, cache_dir = dir, fetch = fetch)
  expect_equal(length(attr(fetch, "asked")()), n_before)
  expect_equal(d2$finding, d$finding)
  # and network = FALSE still reads the cache for drift, without fetching
  d3 <- check_dataset_citation(y, network = FALSE, cache_dir = dir, fetch = fetch)
  expect_equal(length(attr(fetch, "asked")()), n_before)
  expect_equal(sort(d3$finding[d3$dataset_key == "calcofi_dic"]), "authority_drift")
})

test_that("a resolver that cannot be reached is authority_unavailable, and nothing is cached", {
  dir <- make_meta_dir()
  fetch <- fake_fetch(list("cite.edirepository.org" = list(status = 503L, content = "", url = "")))
  y <- as_yaml(yaml_ds("calcofi", "phyto", citation_main = "X. 2023. Y. https://doi.org/10.1/z",
                       license = "CC0-1.0",
                       link_data_source = "https://portal.edirepository.org/nis/mapbrowse?packageid=knb-lter-cce.254.4"))
  d <- check_dataset_citation(y, network = TRUE, cache_dir = dir, fetch = fetch)
  expect_equal(d$finding, "authority_unavailable")
  expect_equal(d$level, "warn")
  expect_match(d$detail, "503")
  expect_false(file.exists(file.path(dir, "calcofi", "phyto", "citation_authority.json")))
  # a license the authority disagrees with is drift too
  fetch2 <- fake_fetch(list(
    "api.datacite.org/dois/10.25921/3w9f-jd72" = fx_read("datacite_10.25921_3w9f-jd72.json"),
    "doi.org/10.25921/3w9f-jd72"               = list(status = 302L, content = "", url = "")))
  y2 <- as_yaml(yaml_ds("aa", "dc", citation_main = "Keeling (2025). X. https://doi.org/10.25921/3w9f-jd72",
                        license = "CC0-1.0", doi = "10.25921/3w9f-jd72"))
  d2 <- check_dataset_citation(y2, network = TRUE, cache_dir = dir, fetch = fetch2)
  expect_true("authority_drift" %in% d2$finding)
  expect_match(d2$detail[d2$finding == "authority_drift"], "license: declared CC0-1.0.*CC-BY-4.0")
  expect_equal(unique(d2$authority), "datacite")
})

# the new dataset_meta keys reach both the table and the sidecar --------------------

test_that("ingest_yaml_to_dataset_df() and .dataset_entry() carry the additive attribution keys", {
  y <- as_yaml(yaml_ds("aa", "one", citation_main = "C", license = "CC-BY-4.0", doi = "10.1/x",
                       license_url = "https://l", acknowledgement = "thanks", contact = "mailto:a@b",
                       citation_others = list("paper A", "paper B"), pi_names = "P"))
  df <- ingest_yaml_to_dataset_df(y)
  expect_true(all(c("doi", "license_url", "acknowledgement", "contact") %in% names(df)))
  expect_equal(df$doi, "10.1/x"); expect_equal(df$contact, "mailto:a@b")
  expect_equal(df$citation_others, "paper A;paper B")
  # existing columns keep their names and order (nothing renamed or dropped)
  expect_equal(names(df)[1:17], c("provider", "dataset", "dataset_name", "dataset_name_short",
    "category", "color", "description", "citation_main", "citation_others", "link_calcofi_org",
    "link_data_source", "link_others", "tables", "coverage_temporal", "coverage_spatial",
    "license", "pi_names"))
  e <- .dataset_entry("aa", "one", y$aa_one$dataset_meta)
  expect_equal(e$doi, "10.1/x"); expect_equal(e$acknowledgement, "thanks")
  expect_equal(e$citation_others, list("paper A", "paper B"))
  # a scalar citation_others still serializes as an array; an empty one is absent
  expect_equal(.dataset_entry("aa", "b", list(citation_others = "only"))$citation_others, list("only"))
  expect_null(.dataset_entry("aa", "b", list(citation_others = ""))$citation_others)
  expect_null(.dataset_entry("aa", "b", list())$doi)
})

# source_accessed is measured -----------------------------------------------------------

test_that("source_accessed_from_git() reads the sidecar's last commit date, NA when untracked", {
  skip_if(Sys.which("git") == "", "git not on PATH")
  root <- withr::local_tempdir()
  git <- function(...) system2("git", c("-C", root, ...), stdout = TRUE, stderr = TRUE)
  git("init", "-q")
  git("config", "user.email", "t@t"); git("config", "user.name", "t")
  d1 <- file.path(root, "data", "parquet", "aa_one"); dir.create(d1, recursive = TRUE)
  d2 <- file.path(root, "data", "parquet", "bb_two"); dir.create(d2, recursive = TRUE)
  writeLines("{}", file.path(d1, "manifest.json"))
  writeLines("{}", file.path(d2, "manifest.json"))     # never committed
  git("add", "data/parquet/aa_one/manifest.json")
  withr::with_envvar(c(GIT_AUTHOR_DATE = "2026-08-25T02:52:25+02:00",
                       GIT_COMMITTER_DATE = "2026-08-25T02:52:25+02:00"),
                     git("commit", "-q", "-m", "run"))
  out <- source_accessed_from_git(c(d1, d2))
  expect_equal(out$dataset_key, c("aa_one", "bb_two"))
  expect_equal(out$source_accessed, as.Date(c("2026-08-25", NA)))
  expect_equal(out$source_accessed_method, c("sidecar_commit", NA))
  expect_match(out$source_accessed_ref[1], "^[0-9a-f]{40}$")
  expect_true(is.na(out$source_accessed_ref[2]))
  # not a repository at all
  out2 <- source_accessed_from_git(withr::local_tempdir())
  expect_true(is.na(out2$source_accessed))
})

test_that("stamp_source_access() records download / file_mtime, and the release prefers sources[]", {
  f <- withr::local_tempfile(lines = "x")
  Sys.setFileTime(f, as.POSIXct("2026-07-01 12:00:00", tz = "UTC"))
  s <- stamp_source_access(files = f, urls = "https://example.org/a.zip")
  expect_equal(names(s), c("source", "method", "accessed", "bytes"))
  expect_equal(s$method, c("file_mtime", "download"))
  expect_equal(format(s$accessed[1], "%Y-%m-%d", tz = "UTC"), "2026-07-01")
  expect_true(abs(as.numeric(difftime(s$accessed[2], Sys.time(), units = "mins"))) < 5)
  expect_equal(s$bytes[1], file.size(f)); expect_true(is.na(s$bytes[2]))

  # metadata.json sources[] wins over git; without it git is the fallback
  dir <- withr::local_tempdir()
  d1 <- file.path(dir, "aa_one"); dir.create(d1)
  jsonlite::write_json(list(sources = sources_block(s)), file.path(d1, "metadata.json"),
                       auto_unbox = TRUE, null = "null")
  d2 <- file.path(dir, "bb_two"); dir.create(d2)
  r <- resolve_source_accessed(c(d1, d2))
  expect_equal(r$source_accessed[1], as.Date(format(max(s$accessed), "%Y-%m-%d", tz = "UTC")))
  expect_equal(r$source_accessed_method[1], "download")     # the newest stamp's method
  expect_true(is.na(r$source_accessed[2]))                  # no sources[], not a repo
  expect_equal(names(r), c("dataset_key", "source_accessed", "source_accessed_method", "source_accessed_ref"))
})

# the release cites itself ---------------------------------------------------------------

test_that("release_citation() is byte-pinned, with and without a DOI, and for all versions", {
  expect_equal(
    release_citation("v2026.09.03", "2026-09-03"),
    "CalCOFI (2026). CalCOFI Integrated Database, release v2026.09.03 [Data set]. Scripps Institution of Oceanography, NOAA Fisheries, and California Department of Fish and Wildlife. https://calcofi.io/db-schema/?v=v2026.09.03")
  expect_equal(
    release_citation("v2026.09.03", "2026-09-03", doi = "10.5281/zenodo.22281995"),
    "CalCOFI (2026). CalCOFI Integrated Database, release v2026.09.03 [Data set]. Scripps Institution of Oceanography, NOAA Fisheries, and California Department of Fish and Wildlife. https://doi.org/10.5281/zenodo.22281995")
  expect_equal(
    release_citation("v2026.09.03", all_versions = TRUE),
    "CalCOFI (2026). CalCOFI Integrated Database [Data set]. Scripps Institution of Oceanography, NOAA Fisheries, and California Department of Fish and Wildlife. https://doi.org/10.5281/zenodo.22281994")
  # the year comes from the version when no date is given
  expect_match(release_citation("v2025.12.31"), "^CalCOFI \\(2025\\)")
  expect_error(release_citation("2026.09.03"), "vYYYY")
})

test_that("add_release_citation() writes citation + concept_doi into a catalog, doi only when known", {
  cat_ <- list(version = "v2026.09.03", release_date = "2026-09-03", tables = list())
  c1 <- add_release_citation(cat_)
  expect_equal(c1$concept_doi, "10.5281/zenodo.22281994")
  expect_match(c1$citation, "db-schema/\\?v=v2026\\.09\\.03$")
  expect_null(c1$doi)
  c2 <- add_release_citation(c1, doi = "10.5281/zenodo.22281995")
  expect_equal(c2$doi, "10.5281/zenodo.22281995")
  expect_match(c2$citation, "doi\\.org/10\\.5281/zenodo\\.22281995$")
  # an existing doi is kept when none is passed
  expect_equal(add_release_citation(c2)$doi, "10.5281/zenodo.22281995")
})

test_that("zenodo_record_for_tag() finds the record by its GitHub tree identifier, else by version", {
  j <- fx_read("zenodo_related_v2026.09.03-alpha.json")
  r <- zenodo_record_for_tag(j, "v2026.09.03-alpha")
  expect_equal(r$doi, "10.5281/zenodo.22281995")
  expect_equal(r$concept_doi, "10.5281/zenodo.22281994")
  expect_equal(r$record_id, 22281995)
  expect_equal(r$version, "v2026.09.03-alpha")
  expect_null(zenodo_record_for_tag(j, "v2026.09.04"))
  # the concept listing carries the same record; a tag it lacks is NULL
  j2 <- fx_read("zenodo_concept_22281994.json")
  expect_equal(zenodo_record_for_tag(j2, "v2026.09.03-alpha")$doi, "10.5281/zenodo.22281995")
  expect_null(zenodo_record_for_tag(j2, "v2026.01.01"))
  # end to end with an injected fetcher: the tree query answers first
  fetch <- fake_fetch(list("related.identifier" = j))
  z <- zenodo_doi_for_tag("v2026.09.03-alpha", fetch = fetch)
  expect_equal(z$doi, "10.5281/zenodo.22281995")
  expect_equal(length(attr(fetch, "asked")()), 1)
  # the fallback: tree query empty, concept listing has it
  fetch2 <- fake_fetch(list("related.identifier" = '{"hits":{"hits":[]}}', "conceptdoi" = j2))
  expect_equal(zenodo_doi_for_tag("v2026.09.03-alpha", fetch = fetch2)$doi, "10.5281/zenodo.22281995")
  expect_null(zenodo_doi_for_tag("v2099.01.01", fetch = fetch2))
})

test_that("zenodo_metadata() and citation_cff() name the three partners and the PIs, deterministically", {
  df <- data.frame(provider = c("aa", "bb", "cc"), dataset = c("x", "y", "z"),
                   pi_names = c("Todd Martz; Aaron Mau", "J. Anthony Koslow", "Todd Martz; "),
                   stringsAsFactors = FALSE)
  z <- zenodo_metadata(df)
  expect_equal(z$upload_type, "dataset")
  expect_equal(z$title, "CalCOFI Integrated Database")
  expect_equal(vapply(z$creators, `[[`, "", "name"),
               c("Scripps Institution of Oceanography, UC San Diego",
                 "NOAA Fisheries, Southwest Fisheries Science Center",
                 "California Department of Fish and Wildlife"))
  expect_equal(z$license, "cc-by-4.0")
  expect_null(z$version)                                  # the tag fills it (measured)
  ctr <- z$contributors
  pis <- Filter(function(x) x$type == "DataCollector", ctr)
  expect_equal(vapply(pis, `[[`, "", "name"), c("Koslow, J. Anthony", "Martz, Todd", "Mau, Aaron"))
  cur <- Filter(function(x) x$type == "DataCurator", ctr)
  expect_equal(vapply(cur, `[[`, "", "name"), c("Best, Ben", "Huang, Betty"))
  rel <- vapply(z$related_identifiers, `[[`, "", "identifier")
  expect_true(any(grepl("storage.googleapis.com/calcofi-db/ducklake/releases", rel)))
  expect_true(any(grepl("calcofi.io/db-schema", rel)))
  zv <- zenodo_metadata(df, version = "v2026.09.03")
  expect_equal(zv$version, "v2026.09.03")
  expect_true(any(grepl("releases/v2026.09.03/catalog.json", vapply(zv$related_identifiers, `[[`, "", "identifier"))))

  cff <- citation_cff("v2026.09.03-alpha", "2026-09-03")
  expect_equal(cff$`cff-version`, "1.2.0")
  expect_equal(cff$type, "dataset")
  expect_equal(cff$doi, "10.5281/zenodo.22281994")
  expect_equal(cff$version, "v2026.09.03-alpha")
  expect_equal(cff$`date-released`, "2026-09-03")
  expect_equal(cff$license, "CC-BY-4.0")
  expect_equal(length(cff$authors), 3)
  # written files round-trip and the date stays a quoted string
  dir <- withr::local_tempdir()
  p <- write_citation_files(dir, df, version = "v2026.09.03-alpha", date_released = "2026-09-03")
  expect_setequal(basename(p), c(".zenodo.json", "CITATION.cff"))
  expect_equal(jsonlite::fromJSON(p[[".zenodo.json"]], simplifyVector = FALSE)$license, "cc-by-4.0")
  expect_true(any(grepl("^date-released: '2026-09-03'$", readLines(p[["CITATION.cff"]]))))
  expect_equal(yaml::read_yaml(p[["CITATION.cff"]])$version, "v2026.09.03-alpha")
})

test_that("render_release_notes() gains a How to cite section: the release, then each dataset", {
  md <- c("# notes", "", "# v2026.08.14 (2026-08-14)", "", "## body", "", "text")
  cat_ <- list(version = "v2026.08.14", release_date = "2026-08-14",
               citation = "CalCOFI (2026). CalCOFI Integrated Database, release v2026.08.14 [Data set]. X. https://doi.org/10.5281/zenodo.1",
               doi = "10.5281/zenodo.1")
  meta <- list(datasets = list(
    calcofi_dic = list(citation_main = "Keeling (2025). DIC. https://doi.org/10.25921/3w9f-jd72",
                       license = "CC-BY-4.0", doi = "10.25921/3w9f-jd72"),
    swfsc_cufes = list(citation_main = NULL, license = "custom",
                       license_url = "https://coastwatch.pfeg.noaa.gov/erddap/tabledap/erdCalCOFIcufes.das")))
  out <- render_release_notes("v2026.08.14", md, cat_, meta)
  expect_match(out, "## How to cite")
  expect_match(out, "> CalCOFI \\(2026\\)\\. CalCOFI Integrated Database, release v2026\\.08\\.14")
  expect_match(out, "- `calcofi_dic` — Keeling \\(2025\\)\\. DIC\\. https://doi\\.org/10\\.25921/3w9f-jd72 · CC-BY-4\\.0")
  expect_match(out, "- `swfsc_cufes` — \\*citation pending\\* · custom \\(https://coastwatch")
  # the section comes before Access, and the release line is computed when the catalog lacks one
  expect_true(regexpr("## How to cite", out) < regexpr("## Access", out))
  out2 <- render_release_notes("v2026.08.14", md, list(version = "v2026.08.14", release_date = "2026-08-14"))
  expect_match(out2, "db-schema/\\?v=v2026\\.08\\.14")
  # a data.frame datasets block (the pre-freeze notes chunk) renders the same way
  meta_df <- list(datasets = data.frame(dataset_key = "calcofi_dic", citation_main = "K (2025). D.",
                                        license = "CC-BY-4.0", stringsAsFactors = FALSE))
  expect_match(render_release_notes("v2026.08.14", md, cat_, meta_df), "- `calcofi_dic` — K \\(2025\\)\\. D\\. · CC-BY-4\\.0")
})
