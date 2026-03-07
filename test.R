# =============================================================================
# test_vcpi_client.R
# Tests for the vcpi SDK package (data.py) via reticulate
#
# Setup:
#   1. Install / build the vcpi Python package so it is importable
#   2. Set credentials in .Renviron (recommended) or inline:
#        Sys.setenv(TVC_TOKEN    = "your-token-here")
#        Sys.setenv(SUPABASE_KEY = "your-key-here")
#   3. Restart the R session so reticulate picks up the installed package,
#      then source this file.
# =============================================================================

library(reticulate)
library(testthat)

# -----------------------------------------------------------------------------
# 0. Environment & Python setup
# -----------------------------------------------------------------------------

# Point reticulate at a specific virtualenv or conda env if needed:
# use_virtualenv("~/.venvs/vcpi", required = TRUE)
# use_condaenv("vcpi", required = TRUE)

# Credentials — only TVC_TOKEN is required from the user.
# SUPABASE_KEY is a publishable key baked into the package itself.
if (nchar(Sys.getenv("TVC_TOKEN")) == 0)
  stop("TVC_TOKEN is not set. Run: Sys.setenv(TVC_TOKEN = 'your-token')")

# Import the SDK's data module directly — no path hacks needed after install
vcpi <- import("vcpi.data")

# Helper: convert a Polars DataFrame to an R data.frame.
# Polars -> pandas -> R data.frame via reticulate's built-in pandas conversion.
# This works correctly for both empty and non-empty frames, avoiding the
# fragile Arrow path that breaks on empty schema conversion through reticulate.
polars_to_r <- function(pdf) {
  reticulate::py_to_r(pdf$to_pandas())
}

# Simple pass/fail reporter used outside of testthat blocks
pass <- function(msg) cat(sprintf("  \u2713 PASS  %s\n", msg))
fail <- function(msg) cat(sprintf("  \u2717 FAIL  %s\n", msg))

cat("\n====================================================\n")
cat(" vcpi_client integration test suite\n")
cat("====================================================\n\n")


# =============================================================================
# 1. Module-level sanity checks
# =============================================================================
cat("--- 1. Module sanity checks ---\n")

test_that("module imports cleanly", {
  expect_false(is.null(vcpi))
})

test_that("SUPABASE_KEY is the bundled publishable constant (non-empty)", {
  key <- vcpi$SUPABASE_KEY
  expect_true(nchar(key) > 0)
  expect_match(key, "^sb_publishable_")   # sanity-check the key format
})

test_that("timeout constants are positive numbers", {
  expect_gt(vcpi$TIMEOUT_METADATA, 0)
  expect_gt(vcpi$TIMEOUT_DATASET,  0)
  expect_gt(vcpi$TIMEOUT_STREAM,   0)
})

test_that("EMPTY_CHEM_DF has the correct schema", {
  df <- polars_to_r(vcpi$EMPTY_CHEM_DF)
  expect_true(all(c("compound", "user_compound_id", "smiles", "purity_pct", "molecular_weight", "log_p", "tpsa") %in% names(df)))
  expect_equal(nrow(df), 0L)
})


# =============================================================================
# 2. Authentication helpers
# =============================================================================
cat("\n--- 2. Authentication ---\n")

test_that("_get_token() returns a non-empty string", {
  tok <- vcpi$`_get_token`()
  expect_type(tok, "character")
  expect_gt(nchar(tok), 0)
})

test_that("_get_token() is cached on second call (same object)", {
  t1 <- vcpi$`_get_token`()
  t2 <- vcpi$`_get_token`()
  expect_identical(t1, t2)
})

test_that("_headers() contains required keys", {
  h <- vcpi$`_headers`()
  expect_true("apikey"        %in% names(h))
  expect_true("Authorization" %in% names(h))
  expect_true("Content-Type"  %in% names(h))
  expect_match(h[["Authorization"]], "^Bearer ")
})


# =============================================================================
# 3. list_datasets()
# =============================================================================
cat("\n--- 3. list_datasets() ---\n")

datasets_r <- NULL   # populated here, reused in later sections

test_that("list_datasets() returns a data.frame with rows", {
  raw       <- vcpi$list_datasets()
  datasets_r <<- polars_to_r(raw)
  expect_s3_class(datasets_r, "data.frame")
  expect_gt(nrow(datasets_r), 0L)
})

test_that("list_datasets() result contains a job_id column", {
  skip_if(is.null(datasets_r), "list_datasets() did not return data")
  expect_true("job_id" %in% names(datasets_r))
})

# Grab the first available job_id for downstream tests
FIRST_JOB_ID <- if (!is.null(datasets_r) && "job_id" %in% names(datasets_r))
  datasets_r$job_id[[1]] else NULL

if (!is.null(FIRST_JOB_ID))
  cat(sprintf("  Using job_id = '%s' for downstream tests\n", FIRST_JOB_ID))


# =============================================================================
# 4. query() — metadata & chemistry (no parquet touch)
# =============================================================================
cat("\n--- 4. query() metadata & chemistry ---\n")

preview_r <- NULL

test_that("query() metadata returns a data.frame with <= 5 rows", {
  skip_if(is.null(FIRST_JOB_ID), "No job_id available")
  raw       <- vcpi$query(job_id = FIRST_JOB_ID, sql = "SELECT * FROM metadata LIMIT 5")
  preview_r <<- polars_to_r(raw)
  expect_s3_class(preview_r, "data.frame")
  expect_lte(nrow(preview_r), 5L)
})

test_that("query() metadata has expected QC columns", {
  skip_if(is.null(preview_r), "metadata query not loaded")
  expect_true(all(c("sequenced_id", "job_id", "compound",
                    "percent_mitochondrial", "cell_line") %in% names(preview_r)))
})

test_that("query() chemistry returns correct columns", {
  skip_if(is.null(FIRST_JOB_ID), "No job_id available")
  raw <- vcpi$query(job_id = FIRST_JOB_ID, sql = "SELECT * FROM chemistry LIMIT 5")
  df  <- polars_to_r(raw)
  expect_s3_class(df, "data.frame")
  expect_true(all(c("compound", "smiles", "log_p") %in% names(df)))
})

test_that("query() metadata+chemistry join works without touching parquet", {
  skip_if(is.null(FIRST_JOB_ID), "No job_id available")
  raw <- vcpi$query(
    job_id = FIRST_JOB_ID,
    sql    = "SELECT m.sequenced_id, m.user_compound_id, c.log_p
              FROM metadata m
              JOIN chemistry c ON c.compound = m.compound
              LIMIT 5"
  )
  df <- polars_to_r(raw)
  expect_s3_class(df, "data.frame")
  expect_true(all(c("sequenced_id", "user_compound_id", "log_p") %in% names(df)))
})


# =============================================================================
# 5. load_metadata()
# =============================================================================
cat("\n--- 5. load_metadata() ---\n")

test_that("load_metadata() returns a non-empty data.frame or skips gracefully", {
  skip_if(is.null(FIRST_JOB_ID), "No job_id available")
  
  result <- tryCatch(
    polars_to_r(vcpi$load_metadata(FIRST_JOB_ID)),
    error = function(e) {
      msg <- conditionMessage(e)
      if (grepl("401|404|Unauthorized|Not Found", msg, ignore.case = TRUE)) {
        skip(paste("Metadata not available for this job_id:", msg))
      }
      stop(e)
    }
  )
  
  expect_s3_class(result, "data.frame")
  expect_gt(nrow(result), 0L)
})


# =============================================================================
# 6. load_chem()
# =============================================================================
cat("\n--- 6. load_chem() ---\n")

test_that("load_chem() returns a data.frame with correct columns", {
  skip_if(is.null(FIRST_JOB_ID), "No job_id available")
  raw <- vcpi$load_chem(FIRST_JOB_ID)
  df  <- polars_to_r(raw)
  expect_s3_class(df, "data.frame")
  expect_true(all(c("compound", "user_compound_id", "smiles", "purity_pct", "molecular_weight", "log_p", "tpsa") %in% names(df)))
})

test_that("_safe_load_chem() returns EMPTY_CHEM_DF for a bogus job_id", {
  raw <- vcpi$`_safe_load_chem`("__nonexistent_job__")
  expect_equal(raw$height, 0L)
  expect_true(all(c("compound", "user_compound_id", "smiles", "purity_pct", "molecular_weight", "log_p", "tpsa") %in% raw$columns))
})


# =============================================================================
# 7. query() — advanced
# =============================================================================
cat("\n--- 7. query() advanced ---\n")

test_that("query() collective mode returns a data.frame", {
  raw <- vcpi$query()
  df  <- polars_to_r(raw)
  expect_s3_class(df, "data.frame")
})

test_that("query() metadata COUNT returns a single-row result", {
  skip_if(is.null(FIRST_JOB_ID), "No job_id available")
  raw <- vcpi$query(
    job_id = FIRST_JOB_ID,
    sql    = "SELECT COUNT(*) AS n FROM metadata"
  )
  df <- polars_to_r(raw)
  expect_equal(nrow(df), 1L)
  expect_true("n" %in% names(df))
  expect_gt(df$n[[1]], 0L)
})

test_that("query() mito filter returns only passing samples", {
  skip_if(is.null(FIRST_JOB_ID), "No job_id available")
  raw <- vcpi$query(
    job_id = FIRST_JOB_ID,
    sql    = "SELECT sequenced_id, percent_mitochondrial
              FROM metadata
              WHERE percent_mitochondrial < 0.2"
  )
  df <- polars_to_r(raw)
  expect_s3_class(df, "data.frame")
  expect_true(all(df$percent_mitochondrial < 0.2))
})

test_that("query() sequencing table is not available (use load_experiment instead)", {
  skip_if(is.null(FIRST_JOB_ID), "No job_id available")
  expect_error(
    vcpi$query(job_id = FIRST_JOB_ID,
               sql    = "SELECT * FROM sequencing LIMIT 1"),
    regexp = "sequencing|Table.*not found|Catalog Error",
    ignore.case = TRUE
  )
})


# =============================================================================
# 8. describe()
# =============================================================================
cat("\n--- 8. describe() ---\n")

test_that("describe() returns a named list with metadata and chemistry", {
  skip_if(is.null(FIRST_JOB_ID), "No job_id available")
  schemas <- vcpi$describe(FIRST_JOB_ID)
  expect_true(all(c("metadata", "chemistry") %in% names(schemas)))
})

test_that("describe() metadata schema has column_name and column_type", {
  skip_if(is.null(FIRST_JOB_ID), "No job_id available")
  schemas <- vcpi$describe(FIRST_JOB_ID)
  df <- polars_to_r(schemas[["metadata"]])
  expect_true("column_name" %in% names(df))
  expect_true("column_type" %in% names(df))
  expect_gt(nrow(df), 0L)
})


# =============================================================================
# 9. load_experiment()
# =============================================================================
cat("\n--- 9. load_experiment() ---\n")

experiment <- NULL

test_that("load_experiment() returns a named list with four keys", {
  skip_if(is.null(FIRST_JOB_ID), "No job_id available")
  experiment  <<- vcpi$load_experiment(FIRST_JOB_ID)
  expected_keys <- c("data", "metadata", "chemistry", "job_id")
  expect_true(all(expected_keys %in% names(experiment)))
})

test_that("load_experiment() $data is a non-empty data.frame", {
  skip_if(is.null(experiment), "load_experiment() not run")
  df <- polars_to_r(experiment$data)
  expect_s3_class(df, "data.frame")
  expect_gt(nrow(df), 0L)
})

test_that("load_experiment() $job_id matches the requested job", {
  skip_if(is.null(experiment), "load_experiment() not run")
  expect_equal(experiment$job_id, FIRST_JOB_ID)
})

test_that("load_experiment() $chemistry has correct schema", {
  skip_if(is.null(experiment), "load_experiment() not run")
  df <- polars_to_r(experiment$chemistry)
  expect_true(all(c("compound", "user_compound_id", "smiles", "purity_pct", "molecular_weight", "log_p", "tpsa") %in% names(df)))
})


# =============================================================================
# Summary
# =============================================================================
cat("\n====================================================\n")
cat(" All tests complete.\n")
cat("====================================================\n\n")