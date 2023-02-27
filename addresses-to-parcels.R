source("log.R")
source("std_helpers.R")
source("assess_helpers.R")

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

get_filings <- function() {
  connect <- dbConnect(RPostgres::Postgres(), 
                       dbname=local_db_name,
                       host="localhost",
                       port=5432)
  query <- "select * from filings as f left join plaintiffs as p on p.docket_id = f.docket_id"
  filings <- st_read(connect, query=query) %>% st_set_geometry("geometry") %>%
    select(-contains('..'))  %>% 
    process_records(cols=c("street", "city", "zip", "name", "case_type"), 
                    addr_cols=c("street"), name_cols=c("name"), 
                    keep_cols=c("def_attorney_id", "ptf_attorney_id", 
                                "docket_id", "street", "city", 
                                "zip", "name", "case_type")) %>% 
    std_cities(c("city"))
  
  
  }

join_by_address_strings <- function() {
  left_join(filings, assess, by=c("street"="own_addr", "city"="own_city")) %>% filter(!is.na(owner1))
}

add_parcels_to_assessors <- function(town_ids = c(274)) {
  # set up parcels, join asses records to parcel geometry 
  parcel_query <- "SELECT * FROM L3_TAXPAR_POLY"
  parcels <- load_parcels(file.path(DATA_DIR, ASSESS_GDB), town_ids = town_ids) %>% 
    st_get_censusgeo() %>% 
    filter(!st_is_empty(geometry))
  left_join(parcels, assess, by = c("loc_id" = "loc_id"))
}

# finding nearby filings to assess points 
match_nearby_filings <- function(assess_points_df, mile_multiplier = 0.1) {
  assess_and_filings <- st_join(assess_points_df, 
                                filings_with_geometry, 
                                join=st_nn, 
                                maxdist=5280 * mile_multiplier, 
                                k = 2,
                                progress = FALSE) %>% 
    filter(!st_is_empty(geometry)) %>% 
    filter(!is.na(street))
}


connect_evictors <- function(town_ids=c(274), mile_multiplier = 0.1) {
  filings <- get_filings()
  
  log_message("-------- Reading assessors table from file")
  assess <- load_assess(path = file.path(DATA_DIR, ASSESS_GDB), write=FALSE) 
  assess <- assess %>% process_records(cols=c("owner1"),
                    addr_cols=c("own_addr"),
                    keep_cols=c("prop_id", "own_addr", 
                                "own_city", "own_zip", "own_state", 
                                "loc_id"))
  
  log_message("Step 1. Join by address strings")
  joined_by_address_strings <- join_by_address_strings() %>% 
    distinct(docket_id, .keep_all = TRUE) %>% st_drop_geometry('geometry')
  # simple join by evictor name to parcel owner
  joined_by_name <- left_join(filings, assess, by=c("name"="owner1")) %>% 
    filter(!is.na(name)) %>% filter(!is.na(own_addr)) %>% 
    distinct(docket_id, .keep_all = TRUE) %>% st_drop_geometry('geometry')
  joined_by_name <- cbind(owner1=joined_by_name$name, joined_by_name)
  
  # merge two dataframes
  df <- full_join(joined_by_address_strings, joined_by_name) %>% distinct(docket_id, .keep_all = TRUE) 
  # %>% distinct(docket_id, .keep_all = TRUE)
   # log_message("-------- Standardize filing name column")
  # filing_names <- filings %>% std_owner_name(c('name')) %>% 
  #   mutate(name_simple=str_remove(str_to_upper(name), " LLC| AS | LP")) %>% 
  #   filter(!is.na(owner1))
  # assess_names <- assess %>% std_owner_name(c('owner1')) %>%
  #   mutate(name_simple=str_remove(str_to_upper(owner1), " LLC| AS | LP")) %>% 
  #   filter(!is.na(owner1))
  # 
  # log_message("Step 2. Join clean names")
  # filing_assess_names <- left_join(filing_names, assess_names, by=c('name_simple'='name_simple')) %>% 
  #   filter(!is.na(owner1)) %>% distinct()

  log_message("-------- Add parcels to assessors")
  assess <- add_parcels_to_assessors(town_ids = town_ids) %>% 
    filter(!st_is_empty(geometry))
  
  log_message("-------- Transform filings to correct geometry")
  filings <- filings %>% st_set_geometry("geometry") %>% st_transform(2249) %>% 
    filter(!st_is_empty(geometry))
  
  # log_message("-------- Add parcels to joined by name from step 1")
  # joined_by_name <- st_join(joined_by_name, assess, join=st_contains) 
  
  log_message("Step 2. Find assess parcels that contain filings") 
  found_addresses <- st_join(assess, filings, join=st_contains) %>% 
    filter(!is.na(name)) %>% st_drop_geometry('geometry') 
  
  # remove extra geo columns
  drops <- c("geoid_bg","geoid_t")
  found_addresses <- found_addresses[ , !(names(found_addresses) %in% drops)]
  
  # merge found_addresses to df 
  df <- full_join(df, found_addresses)
  
  log_message("-------- Reproject assess geometries to points")
  assess_points <- st_point_on_surface(assess)
  
  log_message(glue::glue("Step 3. Match filings to {mile_multiplier}ft away from assessor parcel point")) 
  nearby_filings <- match_nearby_filings(assess_points, mile_multiplier = mile_multiplier) %>% 
    st_drop_geometry('geometry')
  # drop extra geo columns
  nearby_filings <- nearby_filings[ , !(names(nearby_filings) %in% drops)]
  # merge last dataframe to big one and write to file
  df <- full_join(df, nearby_filings) %>% 
  write_delim(
    file.path(RESULTS_DIR, paste(EVICTORS_OUT_NAME, "csv", sep = ".")),
    delim = "|", quote = "needed"
  )
}

