util_log_message <- function(status) {
  #' Print message to `logr` logs, or to message if log is not opened.
  #'
  #' @param status Status to print.
  #' @returns Nothing.
  #' @export
  if(logr::log_status() == "open") {
    logr::log_print(status)
  } else {
    message(status)
  }
}

# write_multi <- function(df, name, formats = c("csv", "rds", "pg")){
#   if ("csv" %in% formats) {
#     # Write pipe-delimited text file of edges.
#     readr::write_delim(
#       df,
#       file.path(RESULTS_DIR, stringr::str_c(name, "csv", sep = ".")),
#       delim = "|", 
#       quote = "needed"
#     )
#   }
#   if ("rds" %in% formats) {
#     saveRDS(
#       df, 
#       file = file.path(RESULTS_DIR, stringr::str_c(name, "rds", sep = "."))
#     )
#   }
#   df
# }