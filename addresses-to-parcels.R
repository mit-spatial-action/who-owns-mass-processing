source("deduplicate-owners.R")
install.packages("nngeo")
library(nngeo)
library(RPostgres)
library(dplyr)
library(tidyr)
library(tidytext)
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
assess <- load_assess(file.path(DATA_DIR, ASSESS_GDB), town_ids = c(274, 49))
# process assess records (see run.R)
  
# ---------- Cleaning addresses ---------- #
filings <- process_records(filings, cols=c("street", "city", "zip", "name", "case_type"), 
                           addr_cols=c("street"), name_cols=c("name"), 
                           keep_cols=c("def_attorney_id", "ptf_attorney_id", "docket_id", "street", "city", "zip", "name", "case_type"))
filings <- std_cities(filings, cols=c("city"))
 
joined <- left_join(filings, assess, by=c("street"="site_addr", "city"="city"))

parcel_query <- "SELECT * FROM L3_TAXPAR_POLY"

join_assess_to_parcels <- function(gdb_path=file.path(DATA_DIR, ASSESS_GDB), town_ids = c(274, 49)) {
  parcels <- load_parcels(gdb_path, town_ids = town_ids)
  # TODO: figure out why this fails sometimes:
  # Error in `auto_copy()`:
  # ! `x` and `y` must share the same src.
  # ℹ set `copy` = TRUE (may be slow).
  parcels <- st_get_censusgeo(parcels) %>% filter(!st_is_empty(geometry))
  left_join(parcels, assess, by = c("loc_id" = "loc_id"))
}

assess <- join_assess_to_parcels(gdb_path=file.path(DATA_DIR, ASSESS_GDB),
                                 town_ids = c(274, 49))

assess <- process_records(assess,  cols=c("owner1"),
                            addr_cols=c("own_addr"),
                            keep_cols=c("prop_id", "ma_prop_id", "own_addr", 
                                        "own_city", "own_zip", "own_state", 
                                        "loc_id"))

# transform filings to correct geometry
filings <- filings %>% st_set_geometry("geometry") %>% st_transform(2249)

# found assess parcels that contain filings
found_addresses <- st_join(assess_geo, filings, join=st_contains) %>% filter(!is.na(name))

# simple join by evictor name to parcel owner
joined_by_name <- st_join(filings, assess_geo, by=c("name"="owner1")) %>% filter(!is.na(owner1))

filings_with_geometry <- filings %>% filter(!st_is_empty(geometry))
assess_with_geometry_not_null <- assess_with_geometry 
# reprojecting assess geometries to points
assess_points <- st_point_on_surface(assess_with_geometry_not_null)
# finding nearby filings
assess_and_filings <- st_join(assess_points, 
                              filings_with_geometry, 
                              join=st_nn, 
                              maxdist=5280 * 0.1, 
                              k = 2,
                              progress = FALSE) %>% 
  filter(!st_is_empty(geometry)) %>% 
  filter(!is.na(street))

filing_names <- filings %>% filter(!is.na(name)) %>% unique('docket_id') %>% 
  std_remove_special(c('name')) %>%
  std_remove_middle_initial(c('name')) %>%
  std_the(c('name')) %>%
  std_and(c('name')) %>%
  mutate(name_simple=str_remove(str_to_upper(name), " LLC| AS | LP"))
#         name_simple=str_extract_all(name_simple, '([\\w]+(?:[a-zA-Z]))')

assess_names <- assess_with_geometry %>% filter(!is.na(owner1)) %>%
  std_remove_special(c('owner1')) %>%
  std_remove_middle_initial(c('owner1')) %>%
  std_the(c('owner1')) %>%
  std_and(c('owner1')) %>%
  mutate(name_simple=str_remove(owner1, " LLC| AS | LP"),
  ) 

filing_assess_names <- st_join(filing_names, assess_names, by=c('name_simple'='name_simple')) %>% 
  filter(!is.na(owner1)) %>% distinct()

length(unique(filing_assess_names$docket_id)) # 314 records

# unnest_tokens(name, text, token = "regex", pattern='([\\w]+(?:[a-zA-Z]))')

# add back in joining on street and city 
# possible you're getting many to ones (many assessors to one eviction)
# pretty rare but edge case
# ascending order of costlinesse
# column join on address and town
# where that fails, spatial join
# where that fails, try k nearest
# in a lot of cases the geocoder doesn't nail right on parcel
# of those in proximity, are there any we can reasonably think are tied? 
# knn_bw is a fraction of a mile 
# shrinking bandwidth will speed things up
# casting geometry to point will be a lot faster
# st_point_on_surface (or something) adds a requirement that point inside of polygon
# re: names, currently all of our joins are simple joins, but 
# would make sense to implement token based system
# use levenstein distance 

# column names to keep: 
# anything that's a result of a data cleaning process (like vectorizing names)
# including docket_id, parcel_id
