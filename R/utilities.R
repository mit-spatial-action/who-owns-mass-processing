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