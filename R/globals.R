# If true, load.R will re-ingest data from sources.
REFRESH_DATA <- TRUE

# If not NULL, will run script on a subset of municipalities.
# Can be provided as numeric or character vector.
# TEST_MUNIS <- c(274, 49, 176)

# To run on all MA municipalities, uncomment this line.
TEST_MUNIS <- NULL

# Coordinate Reference System into which spatial layers will be projected.
CRS <- 2249

# Folder containing OpenCorporates data products.
OC_FOLDER <- '2024-04-12'

# Name of file containing OpenCorporates companies.
OC_COMPANIES <- "companies.csv"

# Name of file containing OpenCorporates companies.
OC_OFFICERS <- "officers.csv"

# Filename of MassGIS Parcels geodatabase.
ASSESS_GDB_FOLDER <- "L3_AGGREGATE_FGDB_20240703"

# Name of directory where (some) source data is located.
DATA_DIR <- "data"

# Name of directory in which results are written.
RESULTS_DIR <- "results"

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