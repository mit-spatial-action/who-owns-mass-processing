source("deduplicate-owners.R")
library(RPostgreSQL)

local_db_name <- "eviction_local"

# ---------- Connecting to local DB ---------- #
connect <- dbConnect(PostgreSQL(), 
                     dbname=local_db_name,
                     host="localhost",
                     port=5432)

filings <- dbGetQuery(connect, "select * from filings")
  
# ---------- Cleaning addresses ---------- #
filings <- std_uppercase_all(filings)
filings <- std_char(filings)
filings <- std_split_addresses(filings, "street")
filings <- std_street_types(filings, "street")