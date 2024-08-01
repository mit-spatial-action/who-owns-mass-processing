util_log_message <- function(status, header=FALSE, timestamp=TRUE) {
  #' Print message to `logr` logs, or to message if log is not opened.
  #'
  #' @param status Status to print.
  #' @returns Nothing.
  #' @export
  if (timestamp) {
    status <- stringr::str_c(format(Sys.time()), ": ", status)
  }
  if (header) {
    status <- stringr::str_c(
      "\n",
      status,
      "\n",
      strrep("=", nchar(status)),
      "\n",
      sep=""
    )
  }
  if(logr::log_status() == "open") {
    logr::log_print(status, hide_notes = TRUE)
  } else {
    message(status)
  }
}
