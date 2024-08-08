source("R/loaders.R")
source("R/utilities.R")
source("config.R")

load_results <- function(prefix, load_boundaries=TRUE) {
  
  util_test_conn(prefix)
  conn <- util_conn(prefix)
  on.exit(DBI::dbDisconnect(conn))
  
  tables <- c(
    "owners", "companies", "officers", "metacorps_cosine", "parcels_point",
    "metacorps_network", "sites", "sites_to_owners")
  if (load_boundaries) {
    tables <- c(
      tables,
      c("zips", "munis", "tracts", "block_groups")
    )
  }
  tables_exist <- util_check_for_tables(
    conn,
    tables
  )
  if (!tables_exist) {
    stop(glue::glue("VALIDATION: Tables don't seem to exist on '{prefix}' database."))
  } else {
    util_log_message(glue::glue("VALIDATION: Loading tables from '{prefix}' database."), header=TRUE)
    for(t in tables) {
      util_log_message(glue::glue("INPUT/OUTPUT: Loading {t} from '{prefix}' database."))
      assign(t, load_postgis_read(conn, t), envir = .GlobalEnv)
    }
  }
  invisible(NULL)
}
