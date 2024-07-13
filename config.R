# If true, load.R will re-ingest data from sources.
REFRESH <- TRUE

# If not NULL, will run script on a subset of municipalities.
# Can be provided as numeric or character vector.
MUNI_IDS <- c(274, 49, 176, 35)

# To run on all MA municipalities, uncomment this line.
# MUNI_IDS <- NULL

# Coordinate Reference System into which spatial layers will be projected.
CRS <- 2249

# Folder containing OpenCorporates data products.
OC_PATH <- '2024-04-12'

# Filename of MassGIS Parcels geodatabase.
GDB_PATH <- "L3_AGGREGATE_FGDB_20240703"

# Name of directory where (some) source data is located.
DATA_PATH <- "data"

# Name of directory in which results are written.
RESULTS_PATH <- "results"

# Use cached tigris geographies.
options(tigris_use_cache = TRUE)