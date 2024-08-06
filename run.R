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
                push_dbs=PUSH_DBS,
                return_intermediate=RETURN_INTERMEDIATE,
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
  tables <- util_run_which_tables(routines)
  tables_exist <- util_run_tables_exist(tables, push_dbs)
  
  if (!company_test) {
    company_test_count <- NULL
  }
  
  if (interactive()) {
    out <- list()
  } else {
    out <- NULL
  }
  
  # Ingest or Load Data
  # ===
  if (routines$load | 
      ((routines$proc & refresh) | (routines$proc & !tables_exist$proc)) | 
      ((routines$dedupe & refresh) | (routines$dedupe & !tables_exist$dedupe))) {
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
      refresh=refresh & routines$load,
      push_db=push_dbs$load
    ) |>
      wrapr::unpack(
        munis,
        zips,
        block_groups,
        tracts,
        places,
        parcels,
        assess,
        addresses,
        companies,
        officers
      )
    
    if ((return_intermediate | (routines$load & (!routines$dedupe & !routines$proc))) & interactive()) {
      out[['init_assess']] <- assess
      out[['init_addresses']] <- addresses
      out[['init_companies']] <- companies
      out[['init_officers']] <- officers
      out[['places']] <- places
    }
    
    if (interactive()) {
      out[['parcels']] <- parcels
      out[['munis']] <- munis
      out[['zips']] <- zips
      out[['block_groups']] <- block_groups
      out[['tracts']] <- tracts
    }
    
    rm(block_groups, tracts, munis) |> suppressWarnings()
    invisible(gc())
  }
  
  # Process All Input Tables
  # ===
  if (routines$proc | 
      ((routines$dedupe & refresh) | (routines$dedupe & !tables_exist$dedupe))) {
    proc_all(
      assess=assess,
      companies=companies,
      officers=officers,
      addresses=addresses,
      zips=zips,
      parcels=parcels,
      places=places,
      crs=crs,
      tables=tables$proc,
      quiet=quiet,
      # Don't refresh if tables exist and user has specified a subroutine.
      refresh=refresh & routines$proc,
      push_db=push_dbs$proc
    ) |>
      wrapr::unpack(
        parcels_point,
        assess,
        sites,
        owners,
        companies,
        officers
      )
    
    rm(parcels, zips, places) |> suppressWarnings()
    
    if (interactive()) {
      out[['parcels_point']] <- parcels_point
    }
    
    rm(parcels_point) |> suppressWarnings()
    
    if ((return_intermediate | (routines$proc & !routines$dedupe)) & interactive()) {
      out[['proc_assess']] <- assess
      out[['proc_sites']] <- sites
      out[['proc_owners']] <- owners
      out[['proc_companies']] <- companies
      out[['proc_officers']] <- officers
    }
    
    rm(assess) |> suppressWarnings()
    invisible(gc())
  }

  # De-duplicate!
  # ===
  if (routines$dedupe) {
    dedupe_all(
      owners=owners,
      companies=companies,
      officers=officers,
      sites=sites,
      addresses=addresses,
      thresh=thresh,
      inds_thresh=inds_thresh,
      tables=tables$dedupe,
      quiet=quiet,
      refresh=refresh & routines$dedupe,
      push_db=push_dbs$dedupe
    ) |>
      wrapr::unpack(
        sites_to_owners,
        owners,
        companies,
        officers,
        sites,
        metacorps_network,
        metacorps_cosine,
        addresses
      )
    
    if (interactive()) {
      out[['sites_to_owners']] <- sites_to_owners
      out[['owners']] <- owners
      out[['companies']] <- companies
      out[['officers']] <- officers
      out[['sites']] <- sites
      out[['metacorps_network']] <- metacorps_network
      out[['metacorps_cosine']] <- metacorps_cosine
      out[['addresses']] <- addresses
    }
  }
  
  util_log_message(
    "PROCESS COMPLETE!",
    header = TRUE
  )
  
  invisible(gc())
  return(out)
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  invisible(run())
} else {
  out <- run()
  for(i in 1:length(out)) assign(names(out)[i], out[[i]])
}