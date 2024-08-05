source("R/loaders.R")
source('R/standardizers.R')
source('R/deduplicaters.R')
source("R/processors.R")
source("R/utilities.R")
source("config.R")

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

run <- function(data_path=DATA_PATH,
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
                push_remote=PUSH_REMOTE,
                quiet=QUIET) {
  
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
  
  if(!util_prompt_check("VALIDATION: Are you ready to begin the process? (Y/N)")) {
    return(invisible(NULL))
  }
  
  # Confirm With User if More Intensive Config Options are Set
  # ===
  
  if(!util_prompts(refresh, muni_ids, company_test)) {
    return(invisible(NULL))
  }
  
  # Test Validity of (and Zero-Pad) Municipality IDs
  # ===
  
  muni_ids <- util_test_muni_ids(
    muni_ids=muni_ids,
    path=data_path,
    quiet=quiet
  )
  
  # Check for Existence of Tables Needed for Each Subroutine
  # ===
  
  tables <- util_run_which_tables(routines, push_remote)
  tables_exist <- util_run_tables_exist(tables, push_remote)
  
  # Check for Existence of Tables Needed for Each Subroutine
  # ===
  
  if (!company_test) {
    company_test_count <- NULL
  }
  
  # Build list of output tables
  # ===
  
  out <- util_table_list()
  for(i in 1:length(out)) assign(names(out)[i], out[[i]])
  
  util_what_should_run(
    routines, 
    tables_exist, 
    refresh
  ) |>
    wrapr::unpack(
      routines,
      load_init,
      proc_init
    )

  # Ingest or Load Data
  # ===
  
  if (routines$load) {
    load_read_write_all(
      data_path=data_path,
      muni_ids=muni_ids,
      crs=crs,
      gdb_path=gdb_path,
      oc_path=oc_path,
      zip_int_thresh=zip_int_thresh,
      tables=tables$load,
      quiet=quiet,
      company_test_count=company_test_count,
      # Don't refresh if load tables exist and user has specified a subroutine.
      refresh=refresh & load_init,
      remote_db=push_remote$load
    ) |>
      wrapr::unpack(
        munis,
        zips,
        block_groups,
        tracts,
        places,
        parcels,
        init_assess = assess,
        init_addresses = addresses,
        init_companies = companies,
        init_officers = officers
      )
  }
  
  # Process All Input Tables
  # ===
  
  if (routines$proc) {
    proc_all(
      assess=init_assess,
      companies=init_companies,
      officers=init_officers,
      addresses=init_addresses,
      zips=zips,
      parcels=parcels,
      places=places,
      crs=crs,
      tables=tables$proc,
      quiet=quiet,
      # Don't refresh if tables exist and user has specified a subroutine.
      refresh=refresh & proc_init,
      remote_db=push_remote$proc
    ) |>
      wrapr::unpack(
        parcels_point,
        proc_assess = assess,
        proc_sites = sites,
        proc_owners = owners,
        proc_companies = companies,
        proc_officers = officers
      )
  }

  # De-duplicate!
  # ===
  
  if (routines$dedupe) {
    dedupe_all(
      owners=proc_owners,
      companies=proc_companies,
      officers=proc_officers,
      sites=proc_sites,
      addresses=init_addresses,
      thresh=thresh,
      inds_thresh=inds_thresh,
      quiet=quiet,
      refresh=refresh,
      remote_db=push_remote$dedupe
    ) |>
      wrapr::unpack(
        dedupe_sites_to_owners = sites_to_owners,
        dedupe_owners = owners,
        dedupe_companies = companies,
        dedupe_officers = officers,
        dedupe_sites = sites,
        dedupe_metacorps_network = metacorps_network,
        dedupe_metacorps_cosine = metacorps_cosine,
        dedupe_addresses = addresses
      )
  }
  
  for (output in names(out)) {
    out[[output]] <- get(output)
  }
  
  util_log_message(
    "PROCESS COMPLETE!",
    header = TRUE
  )
  
  return(out)
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  run()
} else {
  out <- run()
  if(!is.null(out)) {
    for(i in 1:length(out)) assign(names(out)[i], out[[i]])
  }
}