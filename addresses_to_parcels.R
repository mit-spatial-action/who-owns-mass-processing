source("deduplicate-owners.R")
install.packages("nngeo")
library(nngeo)
library(RPostgres)
library(dplyr)
library(tidyr)
# Spatial support.
library(sf)

DATA_DIR <- "data"
BOSTON_NEIGHBORHOODS <- "bos_neigh.csv"
local_db_name <- "evictions_local"

# ---------- Connecting to local DB ---------- #
connect <- dbConnect(RPostgres::Postgres(), 
                     dbname=local_db_name,
                     host="localhost",
                     port=5432)
query <- "select * from filings as f left join plaintiffs as p on p.docket_id = f.docket_id"

filings <- st_read(connect, query=query) %>% st_set_geometry("geometry") %>%
  select(-contains('..')) # remove all duplicate columns    
# filings_backup <- filings
assess <- load_assess(file.path(DATA_DIR, ASSESS_GDB), town_ids = c(274, 49))
# process assess records (see run.R)
  
# ---------- Cleaning addresses ---------- #
filings <- process_records(filings, cols=c("street", "city", "zip", "name", "case_type", "docket_id", "def_attorney_id", "ptf_attorney_id"), addr_cols=c("street"), name_cols=c("name"))
filings <- std_cities(filings, cols=c("city"))
 
# filings <- std_split_addresses(filings, "street")
# filings <- std_street_types(filings, "street")
joined <- left_join(filings, assess, by=c("street"="site_addr", "city"="city"))


join_assess_to_parcels <- function(crs=2249, census=2249, gdb_path=file.path(DATA_DIR, ASSESS_GDB), town_ids = c(274, 49)) {
  df <- st_read(
    gdb_path,
    query = parcel_query
  ) %>%
    # Rename all columns to lowercase.
    rename_with(str_to_lower) %>%
    # Correct weird naming conventions of GDB.
    st_set_geometry("shape") %>%
    st_set_geometry("geometry") %>%
    # Select only unique id.
    select(c(loc_id)) %>%
    # Reproject to specified CRS.
    st_transform(crs) %>%
    # Cast from MULTISURFACE to MULTIPOLYGON.
    mutate(
      geometry = st_cast(geometry, "MULTIPOLYGON")
    ) 
  df <- st_get_censusgeo(df)
  assess <- std_char(assess)
  
  left_join(df, assess, by = c("loc_id" = "loc_id"))
}

assess_with_geometry <- join_assess_to_parcels()
assess_with_geometry <- process_records(assess_with_geometry, cols=c(colnames(assess_with_geometry)))

filings <- filings %>% st_set_geometry("geometry") %>% st_transform(2249)
found_addresses <- st_join(assess_with_geometry, filings, join=st_contains, 
                           k = 3,
                           maxdist = knn_bw * 5280,
                           progress = FALSE
)

found_adds_not_null <- found_addresses %>% filter(!is.na(name))

joined_by_name <- st_join(filings, assess_with_geometry, by=c("name"="owner1"))
joined_by_name %>% filter(!is.na(town_id))

filings_with_geometry <- filings %>% filter(!st_is_empty(geometry))
assess_with_geometry_not_null <- assess_with_geometry %>% filter(!st_is_empty(geometry))
assess_and_filings <- st_join(assess_with_geometry_not_null, 
                              filings_with_geometry, 
                              join=st_nn, 
                              maxdist=5280, 
                              k = 2,
                              progress = FALSE)

