source("R/loaders.R")
source("config.R")

run <- function() {
  # Open log ====
  lf <- logr::log_open(
    format(Sys.time(), "%Y-%m-%d_%H%M%S"),
    logdir = TRUE
  )
  
  # Ingest or Load Data ====
  load_ingest_read_all(
    data_path = DATA_PATH,
    muni_ids = MUNI_IDS,
    refresh = REFRESH,
    crs = CRS,
    gdb_path = GDB_PATH,
    oc_path = OC_PATH,
    tables = c("assess")
  )
  
  # Close log ====
  logr::log_close()
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  run()
}