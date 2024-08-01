source("R/loaders.R")
source('R/standardizers.R')
source('R/deduplicaters.R')
source("R/flows.R")
source("R/utilities.R")
source("config.R")

run <- function(data_path=DATA_PATH,
                muni_ids=MUNI_IDS,
                refresh=REFRESH,
                crs=CRS,
                gdb_path=GDB_PATH,
                oc_path=OC_PATH,
                quiet=QUIET,
                thresh=THRESH,
                inds_thresh=INDS_THRESH) {
  # Open Log
  # ===
  
  lf <- logr::log_open(
    format(Sys.time(), "%Y-%m-%d_%H%M%S"),
    logdir = TRUE
  )
  
  # Ingest or Load Data
  # ===
  
  load_read_write_all(
    data_path=data_path,
    muni_ids=muni_ids,
    crs=crs,
    gdb_path=gdb_path,
    oc_path=oc_path,
    quiet=quiet,
    refresh=refresh
  ) |>
    wrapr::unpack(
      munis <- munis,
      zips <- zips,
      places <- places,
      assess <- assess,
      parcels <- parcels,
      addresses <- addresses,
      companies <- companies,
      officers <- officers
    )
  
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
      sites <- sites,
      owners <- owners,
      companies <- companies,
      officers <- officers
    )
  
  rm(assess, zips, places)
  
  # De-duplicate!
  # ===
  
  out <- dedupe_all(
    owners=owners,
    companies=companies,
    officers=officers,
    sites=sites,
    addresses=addresses,
    thresh=thresh,
    inds_thresh=inds_thresh,
    quiet=quiet,
    refresh=refresh
    )
  
  # Close Log
  logr::log_close()
  out
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  run()
} else {
  run() |>
    wrapr::unpack(
      owners <- owners,
      companies <- companies,
      officers <- officers,
      sites <- sites,
      metacorps <- metacorps,
      addresses <- addresses
    )
}