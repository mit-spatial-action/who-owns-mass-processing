source("globals.R")
source("deduplicate-owners.R")
source("addresses-to-parcels.R")

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
