source("R/loaders.R")
source('R/standardizers.R')
source('R/deduplicaters.R')
source("R/flows.R")
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
                company_test=COMPANY_TEST,
                company_test_count=COMPANY_TEST_COUNT,
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
  
  # Test Validity of Municipality IDs
  muni_ids <- load_test_muni_ids(
    muni_ids=muni_ids,
    path=data_path,
    quiet=quiet
  )
  
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
  load_read_write_all(
    data_path=data_path,
    muni_ids=muni_ids,
    crs=crs,
    gdb_path=gdb_path,
    oc_path=oc_path,
    zip_int_thresh=zip_int_thresh,
    quiet=quiet,
    company_test_count=company_test_count,
    refresh=refresh
  ) |>
    wrapr::unpack(
      munis,
      zips,
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
    out[['init_assess']] <- assess
    out[['init_parcels']] <- parcels
    out[['init_addresses']] <- addresses
    out[['init_companies']] <- companies
    out[['init_officers']] <- officers
  }

  if (return) {
    out[['parcels']] <- parcels
  }

  rm(munis)
  
  # Process All Input Tables
  # ===
  flow_process_all(
    assess=assess,
    companies=companies,
    officers=officers,
    addresses=addresses,
    zips=zips,
    parcels=parcels,
    places=places,
    quiet=quiet,
    refresh=refresh
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

  rm(assess, zips, places, parcels)



  # De-duplicate!
  # ===
  dedupe_all(
    owners=owners,
    companies=companies,
    officers=officers,
    sites=sites,
    addresses=addresses,
    thresh=thresh,
    inds_thresh=inds_thresh,
    quiet=quiet,
    refresh=refresh
  ) |>
    wrapr::unpack(
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
    out[['metacorps']] <- metacorps
    out[['addresses']] <- addresses
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
      metacorps,
      addresses,
      munis,
      zips,
      places,
      init_assess,
      init_parcels,
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