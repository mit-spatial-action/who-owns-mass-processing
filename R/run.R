source("R/loaders.R")
source('R/standardizers.R')
source('R/deduplicaters.R')
source("R/processors.R")
source("R/utilities.R")
source("R/runner.R")
source("config.R")

manage_run <- function() {
  
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
  
  config$muni_ids <- util_test_muni_ids(
    muni_ids=config$muni_ids,
    path=config$data_path,
    quiet=config$quiet
  )
  
  # Test DB Values and Connections
  # ===
  db_vals <- unique(unlist(unname(config$push_dbs)))
  if(!all(stringr::str_detect(db_vals, "^[a-zA-Z\\_]*$"))) {
    stop("VALIDATION: You provided invalid database prefixes---must be made up of characters and underscores.")
  }
  
  for (db in db_vals) {
    util_test_conn(db)
  }
  rm(db_vals)
  
  # Test Thresholds
  threshes <- c(config$thresh$cosine, config$thresh$inds, cosine$thresh$zip_int)
  if (any(threshes > 1)) {
    stop("VALIDATION: config$thresh$cosine, config$thresh$inds, and cosine$thresh$zip_int must be less than 1.")
  } else {
    util_log_message("VALIDATION: config$thresh$inds, config$thresh$inds, and cosine$thresh$zip_int are valid!")
  }
  rm(threshes)
  
  # Test CRS
  # ===
  
  if(suppressWarnings(is.na(sf::st_crs(config$crs)$input))) {
    stop("VALIDATION: You provided an invalid CRS. Check config.R.")
  } else {
    util_log_message("VALIDATION: CRS is valid!")
  }
  
  # Test OC Path
  
  if (!is.null(config$oc$path)) {
    path <- file.path(config$data_path, config$oc$path)
    oc_exists <- dir.exists(path)
    if (!oc_exists) {
      stop("VALIDATION: You provided an invalid config$oc$path. Check config.R.")
    } else {
      companies_exist <- file.exists(file.path(path, config$oc$companies))
      officers_exist <- file.exists(file.path(path, config$oc$officers))
      if(!(companies_exist & officers_exist)) {
        stop(
          glue::glue("VALIDATION: config$oc$path is a folder but doesn't contain '{config$oc$companies}' and '{config$oc$officers}'. Check config.R.")
          )
      } else {
        util_log_message(
          glue::glue("VALIDATION: config$oc$path is valid! It contains '{config$oc$companies}' and '{config$oc$officers}'")
          )
      }
      rm(companies_exist, officers_exist)
    }
    rm(oc_exists)
  } else {
    util_log_message("VALIDATION: Passed NULL to config$oc$path. Will run without OC.")
  }
  
  # Test GDB Path
  as_file <- file.exists(file.path(config$data_path, config$gdb$path))
  as_folder <- dir.exists(file.path(config$data_path, config$gdb$path))
  
  if(!as_file & !as_folder) {
    stop("VALIDATION: You provided an invalid config$data_path. Check config.yml.")
  }
  
  rm(as_file)
  
  if(as_folder) {
    if(
      !any(
        stringr::str_detect(
          list.files(file.path(config$data_path, config$gdb$path)), 
          ".gdb")
        )
    ) {
      stop("VALIDATION: config$gdb_path is a folder but contains no GDBs. Check config.yml.")
    }
  }
  
  util_log_message("VALIDATION: config$gdb_path is valid!")
  
  rm(as_folder)
  
  # Confirm With User if More Intensive Config Options are Set
  # ===
  
  if(!util_prompts(config$refresh, config$muni_ids, config$company_count > 0)) {
    return(invisible(NULL))
  }
  
  # Quick confirmation.
  # ===
  
  if(!util_prompt_check("VALIDATION: Preflight complete! Are you ready to begin the process? (Y/N)")) {
    return(invisible(NULL))
  }
  
  run(
    data_path=config$data_path,
    muni_ids=config$muni_ids,
    refresh=config$refresh,
    crs=config$crs,
    gdb_path=config$gdb$path,
    assess_layer=config$gdb$assess,
    parcel_layer=config$gdb$parcel,
    oc_path=config$oc$path,
    most_recent=config$most_recent,
    thresh=config$thresh$cosine,
    inds_thresh=config$thresh$inds,
    zip_int_thresh=config$thresh$zip_int,
    routines=config$routines,
    company_count=config$company_count,
    push_dbs=config$push_dbs,
    return_intermediate=config$return_intermediate,
    quiet=config$quiet
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