source("R/loaders.R")
source('R/standardizers.R')
source('R/deduplicaters.R')
source("R/processors.R")
source("R/utilities.R")
source("R/runner.R")
source("config.R")

manage_run <- function() {
  
  # Change Defaults if COMPLETE_RUN is set.
  # ===
  
  if (COMPLETE_RUN) {
    REFRESH <- TRUE
    COMPANY_TEST <- FALSE
    MUNI_IDS <- NULL
    ROUTINES <- list(
      load = TRUE,
      proc = TRUE,
      dedupe = TRUE
    )
  }
  
  # Open Log
  # ===
  
  lf <- logr::log_open(
    format(Sys.time(), "%Y-%m-%d_%H%M%S"),
    logdir = TRUE
  )
  
  on.exit(logr::log_close())
  
  # Print splash screen to log.
  # ===
  
  util_print_splash()
  
  # Test Validity of (and Zero-Pad) Municipality IDs
  # ===
  
  MUNI_IDS <- util_test_muni_ids(
    muni_ids=MUNI_IDS,
    path=DATA_PATH,
    quiet=QUIET
  )
  
  # Test DB Values and Connections
  # ===
  db_vals <- unique(unlist(unname(PUSH_DBS)))
  if(!all(stringr::str_detect(db_vals, "^[a-zA-Z\\_]*$"))) {
    stop("VALIDATION: You provided invalid database prefixes---must be made up of characters and underscores.")
  }
  
  for (db in db_vals) {
    util_test_conn(db)
  }
  rm(db_vals)
  
  # Test Thresholds
  threshes <- c(COSINE_THRESH, INDS_THRESH, ZIP_INT_THRESH)
  if (any(threshes > 1)) {
    stop("VALIDATION: COSINE_THRESH, INDS_THRESH, and ZIP_INT_THRESH must be less than 1.")
  } else {
    util_log_message("VALIDATION: COSINE_THRESH, INDS_THRESH, and ZIP_INT_THRESH are valid!")
  }
  rm(threshes)
  
  # Test CRS
  # ===
  
  if(suppressWarnings(is.na(sf::st_crs(CRS)$input))) {
    stop("VALIDATION: You provided an invalid CRS. Check config.R.")
  } else {
    util_log_message("VALIDATION: CRS is valid!")
  }
  
  # Test OC Path
  
  if (!is.null(OC_PATH)) {
    oc_exists <- dir.exists(file.path(DATA_PATH, OC_PATH))
    if (!oc_exists) {
      stop("VALIDATION: You provided an invalid OC_PATH. Check config.R.")
    } else {
      companies_exist <- any(stringr::str_detect(
        list.files(file.path(DATA_PATH, OC_PATH)), 
        "companies.csv"
        ))
      officers_exist <- any(stringr::str_detect(
        list.files(file.path(DATA_PATH, OC_PATH)), 
        "officers.csv"
      ))
      if(!(companies_exist & officers_exist)) {
        stop("VALIDATION: OC_PATH is a folder but doesn't contain 'companies.csv' and 'officers.csv'. Check config.R.")
      } else {
        util_log_message("VALIDATION: OC_PATH is valid! It contains 'companies.csv' and 'officers.csv'")
      }
      rm(companies_exist, officers_exist)
    }
    rm(oc_exists)
  } else {
    util_log_message("VALIDATION: Passed NULL to OC_PATH. Will run without OC.")
  }
  
  # Test GDB Path
  as_file <- file.exists(file.path(DATA_PATH, GDB_PATH))
  as_folder <- dir.exists(file.path(DATA_PATH, GDB_PATH))
  
  if(!as_file & !as_folder) {
    stop("VALIDATION: You provided an invalid GDB_PATH. Check config.R.")
  }
  
  rm(as_file)
  
  if(as_folder) {
    if(
      !any(
        stringr::str_detect(
          list.files(file.path(DATA_PATH, GDB_PATH)), 
          ".gdb")
        )
    ) {
      stop("VALIDATION: GDB_PATH is a folder but contains no GDBs. Check config.R.")
    }
  }
  
  util_log_message("VALIDATION: GDB_PATH is valid!")
  
  rm(as_folder)
  
  # Quick confirmation.
  # ===
  
  if(!util_prompt_check("VALIDATION: Are you ready to begin the process? (Y/N)")) {
    return(invisible(NULL))
  }
  
  # Confirm With User if More Intensive Config Options are Set
  # ===
  
  if(!util_prompts(REFRESH, MUNI_IDS, COMPANY_TEST)) {
    return(invisible(NULL))
  }
  
  run(
    data_path=DATA_PATH,
    muni_ids=MUNI_IDS,
    refresh=REFRESH,
    crs=CRS,
    gdb_path=GDB_PATH,
    oc_path=OC_PATH,
    thresh=COSINE_THRESH,
    inds_thresh=INDS_THRESH,
    zip_int_thresh=ZIP_INT_THRESH,
    routines=ROUTINES,
    company_test=COMPANY_TEST,
    company_test_count=COMPANY_TEST_COUNT,
    push_dbs=PUSH_DBS,
    return_intermediate=RETURN_INTERMEDIATE,
    quiet=QUIET
  )
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  invisible(manage_run())
  util_log_message("CLOSING R SESSION.", header=TRUE)
} else {
  results <- manage_run()
  if (!is.null(results)) {
    util_log_message("ASSIGNING R OBJECTS.", header=TRUE)
    for(i in 1:length(results)) assign(names(results)[i], results[[i]])
  }
  rm(results)
}