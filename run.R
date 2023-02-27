source("deduplicate-owners.R")
source("addresses-to-parcels.R")

# Name of directory in which input data is stored.
DATA_DIR <- "data"
# Name of directory in which results are written.
RESULTS_DIR <- "results"
# Filename of delimited text file containing individuals.
INDS <- "CSC_CorporationsIndividualExport_VB.txt"
# Filename of delimited text file containing corporate entities.
CORPS <- "CSC_CorpDataExports_VB.txt"
# Filename of MassGIS Parcels geodatabase.
ASSESS_GDB <- "MassGIS_L3_Parcels.gdb"
# Name of CSV containing limited collection of HNS municipalities
MUNI_CSV <- "hns_munis"
# Name of delimited text output files.
ASSESS_OUT_NAME <- "assess"
OWNERS_OUT_NAME <- "owners"
CORPS_OUT_NAME <- "corps"
INDS_OUT_NAME <- "inds"
NODES_OUT_NAME <- "nodes"
EDGES_OUT_NAME <- "edges"
# Name of RData image.
RDATA_OUT_NAME <- "results"

run <- function(subset = "test", deduplicate_owners=TRUE, connect_evictors=TRUE, return_results = TRUE) {
  # Create and open log file with timestamp name.
  lf <- log_open(
    file.path(
      "logs",
      format(Sys.time(), "%Y-%m-%d_%H%M%S")
    )
  )
  if (subset == "hns") {
    town_ids <- read_csv(
      file.path(DATA_DIR, paste(MUNI_CSV, "csv", sep = "."))
    ) %>%
      pull(town_id) %>%
      paste(collapse = ", ")
  } else if (subset == "test") {
    town_ids <- c(274, 49)
  } else if (subset == "all") {
    town_ids <- FALSE
  } else {
    stop("Invalid subset.")
  }
  if (deduplicate_owners) {
    run_deduplicate(town_ids = town_ids, return_results = TRUE)
  }
  if (connect_evictors) {
    connect_evictors(town_ids = town_ids)
  }
  # Close logs.
  log_close()
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  run(subset = "all", deduplicate_owners=TRUE, connect_evictors=TRUE, return_results = FALSE)
}
