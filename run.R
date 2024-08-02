source("R/loaders.R")
source('R/standardizers.R')
source('R/deduplicaters.R')
source("R/processing.R")
source("R/utilities.R")
source("config.R")
# Important that this sits below config.
source("R/globals.R")

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
                return_intermediate=RETURN_INTERMEDIATE,
                quiet=QUIET,
                return=TRUE) {
  
  # Open Log
  # ===
  
  lf <- logr::log_open(
    format(Sys.time(), "%Y-%m-%d_%H%M%S"),
    logdir = TRUE
  )
  
  on.exit(logr::log_close())

  if(!util_prompts(refresh, muni_ids, company_test)) {
    return(NULL)
  }
  
  # Test Validity of Municipality IDs
  muni_ids <- util_test_muni_ids(
    muni_ids=muni_ids,
    path=data_path,
    quiet=quiet
  )
  
  tables <- util_run_which_tables(routines, push_remote)
  tables_exist <- util_run_tables_exist(tables, push_remote)
  
  if (!company_test) {
    company_test_count <- NULL
  }
  
  out <- list(
    owners = NULL,
    companies = NULL,
    officers = NULL,
    sites = NULL,
    addresses = NULL,
    metacorps = NULL,
    parcels = NULL,
    munis = NULL,
    zips = NULL,
    places = NULL,
    init_assess = NULL,
    init_parcels = NULL,
    init_addresses = NULL,
    init_companies = NULL,
    init_officers = NULL,
    proc_assess = NULL,
    proc_sites = NULL,
    proc_owners = NULL,
    proc_companies = NULL,
    proc_officers = NULL
  )

  # Ingest or Load Data
  # ===
  init_state <- routines$load
  if (((routines$proc & tables_exist$proc) | (routines$dedupe & tables_exist$dedupe)) & !routines$load & !refresh) {
    routines$load <- FALSE
  } else if (routines$proc | routines$dedupe) {
    routines$load <- TRUE
  }
  if (routines$load) {
    load_read_write_all(
      data_path=data_path,
      muni_ids=muni_ids,
      crs=crs,
      gdb_path=gdb_path,
      oc_path=oc_path,
      zip_int_thresh=zip_int_thresh,
      tables=tables$load,
      tables_exist=tables_exist$load,
      quiet=quiet,
      company_test_count=company_test_count,
      # Don't refresh if load tables exist and user has specified a subroutine.
      refresh=refresh & init_state,
      remote_db=push_remote$load
    ) |>
      wrapr::unpack(
        munis,
        zips,
        block_groups,
        tracts,
        places,
        assess,
        parcels,
        addresses,
        companies,
        officers
      )
    if (return_intermediate & return) {
      out[['munis']] <- munis
      out[['zips']] <- zips
      out[['places']] <- places
      out[['tracts']] <- tracts
      out[['block_groups']] <- block_groups
      out[['init_assess']] <- assess
      out[['init_addresses']] <- addresses
      out[['init_companies']] <- companies
      out[['init_officers']] <- officers
    }
    
    if (return) {
      out[['parcels']] <- parcels
    }
  }
  
  # Process All Input Tables
  # ===
  
  init_state <- routines$proc
  if (routines$dedupe & tables_exist$dedupe & !routines$proc & !refresh) {
    routines$proc <- FALSE
  } else if (routines$dedupe) {
    routines$proc <- TRUE
  }
  
  if(!routines$load & routines$proc) {
    assess = NULL
    companies = NULL
    officers = NULL
    addresses = NULL
    zips = NULL
    parcels=NULL
  }
  
  if (routines$proc) {
    proc_all(
      assess=assess,
      companies=companies,
      officers=officers,
      addresses=addresses,
      zips=zips,
      parcels=parcels,
      places=places,
      tables=tables$proc,
      tables_exist=tables_exist$proc,
      quiet=quiet,
      # Don't refresh if tables exist and user has specified a subroutine.
      refresh=refresh & init_state,
      remote_db=push_remote$proc
    ) |>
      wrapr::unpack(
        assess,
        sites,
        owners,
        companies,
        officers
      )
    
    if (return_intermediate & return) {
      out[['proc_assess']] <- assess
      out[['proc_sites']] <- sites
      out[['proc_owners']] <- owners
      out[['proc_companies']] <- companies
      out[['proc_officers']] <- officers
    }
  }
  
  
  

  # De-duplicate!
  # ===
  
  if(!routines$proc & routines$dedupe) {
    owners = NULL
    companies = NULL
    officers = NULL
    sites = NULL
    addresses = NULL
  }
  
  if (routines$dedupe) {
    dedupe_all(
      owners=owners,
      companies=companies,
      officers=officers,
      sites=sites,
      addresses=addresses,
      thresh=thresh,
      inds_thresh=inds_thresh,
      quiet=quiet,
      refresh=refresh,
      remote_db=push_remote$dedupe
    ) |>
      wrapr::unpack(
        sites_to_owners,
        owners,
        companies,
        officers,
        sites,
        metacorps,
        addresses
      )
    if (return) {
      out[['owners']] <- owners
      out[['companies']] <- companies
      out[['officers']] <- officers
      out[['sites']] <- sites
      out[['sites_to_owners']] <- sites_to_owners
      out[['metacorps']] <- metacorps
      out[['addresses']] <- addresses
    }
  }
    
  if (return) {
    return(out)
  } else {
    return(NULL)
  }
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  run(return=FALSE)
} else {
  run() |>
    wrapr::unpack(
      parcels,
      owners,
      companies,
      officers,
      sites,
      sites_to_owners,
      metacorps,
      addresses,
      munis,
      zips,
      places,
      init_assess,
      init_addresses,
      init_companies,
      init_officers,
      proc_assess,
      proc_sites,
      proc_owners,
      proc_companies,
      proc_officers
    )
}