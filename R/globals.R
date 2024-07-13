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

log_message <- function(status) {
  #' Print message to `logr` logs.
  #'
  #' @param status Status to print.
  #' @returns Nothing.
  #' @export
  time <- format(Sys.time(), "%a %b %d %X %Y")
  message <- stringr::str_c(time, status, sep = ": ")
  logr::log_print(message)
}

write_multi <- function(df, name, formats = c("csv", "rds", "pg")){
  if ("csv" %in% formats) {
    # Write pipe-delimited text file of edges.
    readr::write_delim(
      df,
      file.path(RESULTS_DIR, stringr::str_c(name, "csv", sep = ".")),
      delim = "|", 
      quote = "needed"
    )
  }
  if ("rds" %in% formats) {
    saveRDS(
      df, 
      file = file.path(RESULTS_DIR, stringr::str_c(name, "rds", sep = "."))
    )
  }
  df
}