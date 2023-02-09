source("deduplicate-owners.R")
library(RPostgreSQL)
library(dplyr)
library(tidyr)
# Spatial support.
library(sf)

DATA_DIR <- "data"
BOSTON_NEIGHBORHOODS <- "bos_neigh.csv"
local_db_name <- "evictions_local"

# ---------- Connecting to local DB ---------- #
connect <- dbConnect(PostgreSQL(), 
                     dbname=local_db_name,
                     host="localhost",
                     port=5432)
query <- "select * from filings as f left join plaintiffs as p on p.docket_id = f.docket_id"

filings <- st_read(connect, query=query) %>% st_set_geometry("geometry") %>%
  modify_at("docket_id.1", ~NULL) %>% modify_at("id", ~NULL) %>% modify_at("id.1", ~NULL)# remove duplicate docket_id, remove one of the `id` columns too

# ---------- Cleaning addresses ---------- #
filings <- std_uppercase_all(filings)
filings <- std_char(filings)
filings <- std_remove_special(filings)
filings <- std_split_addresses(filings, "street")
filings <- std_street_types(filings, "street")
filings <- clean_cities(filings)
joined <- left_join(assess, filings, by=c("site_addr"="street", "city"="city_cleaned"))
