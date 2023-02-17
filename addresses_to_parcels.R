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
  modify_at("docket_id.1", ~NULL) %>% modify_at("id", ~NULL) %>% modify_at("id.1", ~NULL)# remove duplicate docket_id, remove one of the `id` columns too
assess <- load_assess(file.path(DATA_DIR, ASSESS_GDB), town_ids = c(274, 49))
# process assess records (see run.R)
assess <- std_char(assess)
  
# ---------- Cleaning addresses ---------- #
filings <- std_char(filings)
filings <- std_remove_special(filings)
filings <- std_split_addresses(filings, "street")
filings <- std_street_types(filings, "street")
filings <- std_cities(filings, cols=c("city"))
joined <- left_join(assess, filings, by=c("site_addr"="street", "city"="city"))


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
  st_get_censusgeo(df)
  left_join(df, assess, by = c("loc_id" = "loc_id"))
}

assess_with_geometry <- join_assess_to_parcels
filings <- filings %>% st_set_geometry("geometry") %>% st_transform(2249)
found_addresses <- st_join(assess_with_geometry, filings, join=st_contains, 
                           k = 3,
                           maxdist = knn_bw * 5280,
                           progress = FALSE
)


length(found_addresses$own_addr[length(found_addresses$own_addr)>0])



