source("R/globals.R")
source("R/deduplicaters.R")
source("R/filing_linkers.R")

subset_town_ids <- function(subset) {
  if (subset == "hns") {
    readr::read_csv(
        file.path(
          DATA_DIR, 
          stringr::str_c(MUNI_CSV, "csv", sep = ".")
          )
      ) %>%
      dplyr::pull(town_id) %>%
      stringr::str_c(collapse = ", ")
  } else if (subset == "test") {
    c(274, 49)
  } else if (subset == "all") {
    FALSE
  } else {
    stop("Invalid subset.")
  }
}

run_all <- function(
      subset = "test", 
                return_results = TRUE) {
  # Create and open log file with timestamp name.
  lf <- logr::log_open(
      format(Sys.time(), "%Y-%m-%d_%H%M%S"),
      logdir = TRUE
    )
  process_deduplication(town_ids = subset_town_ids(subset), return_results = TRUE)
  # Close logs.
  logr::log_close()
}


# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  run_all(subset = "all", return_results = FALSE)
}
