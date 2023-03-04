source("R/globals.R")

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

subset_town_ids <- function(subset) {
  if (subset == "hns") {
    MA_MUNIS %>%
      dplyr::filter(HNS == 1) %>%
      dplyr::pull(town_id) 
    # %>%
    #   stringr::str_c(collapse = ", ")
  } else if (subset == "test") {
    TEST_MUNIS
  } else if (subset == "all") {
    FALSE
  } else {
    stop("Invalid subset.")
  }
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