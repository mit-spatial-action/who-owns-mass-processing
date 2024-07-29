source("R/loaders.R")
source('R/standardizers.R')
source('R/deduplicaters.R')
source("R/flows.R")
source("config.R")

run <- function(data_path=DATA_PATH,
                muni_ids=MUNI_IDS,
                refresh=REFRESH,
                crs=CRS,
                gdb_path=GDB_PATH,
                oc_path=OC_PATH) {
  # Open Log
  # ===
  
  lf <- logr::log_open(
    format(Sys.time(), "%Y-%m-%d_%H%M%S"),
    logdir = TRUE
  )
  
  # Ingest or Load Data
  # ===
  
  load_ingest_read_all(
    data_path=data_path,
    muni_ids=muni_ids,
    refresh=refresh,
    crs=crs,
    gdb_path=gdb_path,
    oc_path=oc_path
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
    assess=assess |> dplyr::slice_head(n=50000),
    companies=companies |> dplyr::slice_head(n=50000),
    officers=officers |> dplyr::slice_head(n=50000),
    addresses=addresses,
    zips=zips,
    parcels=parcels,
    places=places
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
  
  dedupe_all(
    owners=owners,
    companies=companies,
    officers=officers,
    sites=sites,
    addresses=addresses
    ) |>
    wrapr::unpack(
      owners <- owners,
      officers <- officers,
      sites <- sites,
      metacorps <- metacorps,
      unique_addresses <- unique_addresses
    )
  
  # Close Log
  logr::log_close()
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  run()
}