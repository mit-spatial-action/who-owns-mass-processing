source("deduplicate-owners.R")
library(RPostgreSQL)
library(dplyr)
library(tidyr)
DATA_DIR <- "data"
BOSTON_NEIGHBORHOODS <- "bos_neigh.csv"
local_db_name <- "eviction_local"

# ---------- Connecting to local DB ---------- #
connect <- dbConnect(PostgreSQL(), 
                     dbname=local_db_name,
                     host="localhost",
                     port=5432)

filings <- 
  dbGetQuery(connect, "select * from filings as f left join plaintiffs as p on p.docket_id = f.docket_id") %>%
  modify_at("docket_id", ~NULL) %>% modify_at("id", ~NULL) # remove duplicate docket_id, remove one of the `id` columns too
  
# ---------- Cleaning addresses ---------- #
filings <- std_uppercase_all(filings)
filings <- std_char(filings)
filings <- std_remove_special(filings)
filings <- std_split_addresses(filings, "street")
filings <- std_street_types(filings, "street")
filings <- clean_cities(filings)
joined <- left_join(assess, filings, by=c("site_addr"="street", "city"="city_cleaned"))
