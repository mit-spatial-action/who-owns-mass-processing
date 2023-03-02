source("R/globals.R")
source("R/deduplicaters.R")
source("R/filing_linkers.R")
source("R/run_utils.R")

run_all <- function(subset = "test", return_results = FALSE) {
  # Create and open log file with timestamp name.
  lf <- logr::log_open(
      format(Sys.time(), "%Y-%m-%d_%H%M%S"),
      logdir = TRUE
    )
  process_deduplication(
    town_ids = subset_town_ids(subset),
    return_results = return_results
    )
  process_link_filings(town_ids = subset_town_ids(subset))
  # Close logs.
  logr::log_close()
}


# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  run_all(subset = "all", return_results = FALSE)
}
