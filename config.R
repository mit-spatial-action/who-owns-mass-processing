# This overrides values of ROUTINES, COMPANY_TEST, and MUNI_IDS
COMPLETE_RUN <- FALSE

# If true, load.R will re-ingest data from sources.
REFRESH <- TRUE

# If not NULL, will run script on a subset of municipalities.
# Can be provided as numeric or character vector...
MUNI_IDS <- c(274, 49, 35)
# ...or "HNS" selects Healthy Neighborhoods Municipalities (+ Cambridge and Somerville)
MUNI_IDS <- "hns"

# To run on all MA municipalities, uncomment this line.
# MUNI_IDS <- NULL

COMPANY_TEST_COUNT <- 50000
COMPANY_TEST <- TRUE

# Should intermediate tables be returned from run()
RETURN_INTERMEDIATE <- TRUE

# These are thresholds for cosine similarity-based de-duplication.
COSINE_THRESH <- 0.85
INDS_THRESH <- 0.95

# Sets the threshold for "complete" intersection between ZIPS and states/municipalities.
# Note that a value of 1 is substantially faster as it does not require a geometric intersection
# and can simply use a sf::st_contains_properly.
ZIP_INT_THRESH <- 1

QUIET <- FALSE

# Coordinate Reference System into which spatial layers will be projected.
CRS <- 2249

# Folder containing OpenCorporates data products.
OC_PATH <- '2024-04-12'

# Filename of MassGIS Parcels geodatabase.
GDB_PATH <- "L3_AGGREGATE_FGDB_20240703"