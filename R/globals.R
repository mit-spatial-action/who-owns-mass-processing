# Use cached tigris geographies
options(tigris_use_cache = TRUE)

REFRESH_DATA <- FALSE

CRS <- 2249

TEST <- TRUE
TEST_MUNIS <- c(274, 49, 176)

DATA_DIR <- "data"
# Name of directory in which results are written.
RESULTS_DIR <- "results"
# Filename of delimited text file containing individuals.
INDS <- "CSC_CorporationsIndividualExport_VB.txt"
# Filename of delimited text file containing corporate entities.
CORPS <- "CSC_CorpDataExports_VB.txt"
# Filename of MassGIS Parcels geodatabase.
ASSESS_GDB_FOLDER <- "L3_AGGREGATE_FGDB_20240703/"

# Name of delimited text output files.
ASSESS_OUT_NAME <- "assess"
OWNERS_OUT_NAME <- "owners"
CORPS_OUT_NAME <- "corps"
INDS_OUT_NAME <- "inds"
NODES_OUT_NAME <- "nodes"
EDGES_OUT_NAME <- "edges"
FILINGS_OUT_NAME <- "filings"
# Name of RData image.
RDATA_OUT_NAME <- "results"

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