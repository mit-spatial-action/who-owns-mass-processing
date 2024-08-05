# See README.md for definitions.


REFRESH <- TRUE 
REFRESH <- TRUE

PUSH_REMOTE <- list(
  load = FALSE,
  proc = FALSE,
  dedupe = FALSE
)

ROUTINES <- list(
  load = FALSE,
  load = TRUE,
  proc = TRUE,
  dedupe = FALSE
  dedupe = TRUE
)


COMPANY_TEST_COUNT <- 50000
COMPANY_TEST <- TRUE

RETURN_INTERMEDIATE <- TRUE
COMPANY_TEST <- FALSE

COSINE_THRESH <- 0.85
INDS_THRESH <- 0.95

ZIP_INT_THRESH <- 1

QUIET <- FALSE

CRS <- 2249

OC_PATH <- '2024-04-12'
GDB_PATH <- "L3_AGGREGATE_FGDB_20240703"

DATA_PATH <- "data"
RESULTS_PATH <- "results"

# PROBABLY DON'T EVER WANT TO CHANGE.

# Use cached tigris geographies.
options(tigris_use_cache = TRUE)

# Prevent annoying "`summarise()` has grouped output by..." error.
options(dplyr.summarise.inform = FALSE)

GDB_PATH <- "L3_AGGREGATE_FGDB_20240703"