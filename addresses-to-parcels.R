source("globals.R")
source("log.R")
source("std_helpers.R")
source("assess_helpers.R")

library(nngeo)
# library(tidytext)

load_filings <- function(test = FALSE, crs = 2249) {
  #' Pulls eviction filings from database.
  #'
  #' @returns A dataframe.
  #' @export
  # Construct SQL query.
  docket_col <- "docket"
  filings_table <- "filings"
  plaintiffs_table <- "plaintiffs"
  cols <- paste(docket_col, "add1", "city", "zip", "state", "geometry", sep = ",")
  q <- paste(
    "SELECT", cols, 
    "FROM", filings_table, "AS f"
  )
  # Set limit if test = TRUE
  if (test) {
    q <- paste(q, "LIMIT 1000")
  }
  # Pull filings.
  dbConnect(
      Postgres(),
      dbname = Sys.getenv("DB_NAME"),
      host = Sys.getenv("DB_HOST"),
      port = Sys.getenv("DB_PORT"),
      user = Sys.getenv("DB_USER"),
      password = Sys.getenv("DB_PASS"),
      sslmode = "allow"
      ) %>%
    st_read(query=q) %>% 
    select(-contains('..')) %>%
    st_transform(crs) %>%
    rename_with(str_to_lower) %>%
    filter(!is.na(add1)) %>%
    std_flow_strings(c("add1", "city")) %>%
    std_zip(c("zip")) %>% 
    std_flow_addresses(c("add1")) %>%
    std_flow_cities(c("city")) 
}

add_parcels_to_assessors <- function(town_ids = c(274)) {
  #' Set up parcels, join asses records to parcel geometry.
  #'
  #' @param town_ids List of numerical town ids.
  #' @returns A dataframe.
  #' @export
  parcel_query <- "SELECT * FROM L3_TAXPAR_POLY"
  parcels <- load_parcels(file.path(DATA_DIR, ASSESS_GDB), town_ids = town_ids) %>% 
    st_get_censusgeo() %>% 
    filter(!st_is_empty(geometry))
  left_join(parcels, assess, by = c("loc_id" = "loc_id"))
}

match_nearby_filings <- function(parcel_points_df, mile_multiplier = 0.1) {
  #' Match filings to nearby parcels.
  #'
  #' @param parcel_points_df Dataframe containing parcel centroids.
  #' @param mile_multiplier Bandwidth distance, in miles.
  #' @returns A dataframe.
  #' @export
  assess_and_filings <- st_join(
      assess_points_df, 
      filings_with_geometry, 
      join = st_nn, 
      maxdist = 5280 * mile_multiplier, 
      k = 2,
      progress = FALSE) %>% 
    filter(!st_is_empty(geometry)) %>% 
    filter(!is.na(street))
}


link_filings_to_assess <- function(town_ids=c(274), mile_multiplier = 0.1) {
  #' Workflow to connect evictions to assessors records.
  #'
  #' @param town_ids List of numerical town ids.
  #' @param mile_multiplier Bandwidth distance, in miles.
  #' @returns A dataframe.
  #' @export
  filings <- load_filings()
  
  log_message("-------- Reading assessors table from file")
  assess <- load_assess(path = file.path(DATA_DIR, ASSESS_GDB)) %>%
    # Run string standardization procedures.
    std_flow_strings(c("owner1", "own_addr", "site_addr", "own_zip", "city", "own_city")) %>%
    std_zip(c("zip", "own_zip")) %>% 
    std_flow_addresses(c("own_addr", "site_addr")) %>%
    std_flow_cities(c("city", "own_city")) %>%
    std_flow_names(c("owner1", "own_addr")) %>%
    # Extract 'care of' entities to co and remove from own_addr.
    mutate(
      co = case_when(
        str_detect(
          own_addr,
          "C / O"
        )
        ~ str_extract(own_addr, "(?<=C / O ).*$")
      ),
      own_addr = case_when(
        str_detect(
          own_addr,
          "C / O"
        )
        ~ na_if(str_extract(own_addr, ".*(?= ?C / O )"), ""),
        TRUE ~ own_addr
      )
    ) %>%
    # Flag owner-occupied properties on the basis of standardized addresses.
    mutate(
      ooc = case_when(
        own_addr == site_addr ~ TRUE,
        TRUE ~ FALSE
      )
    )
  
  log_message("Step 1. Join by address strings")
  
  filings_joined <- filings %>%
    left_join(select(assess, -c(zip)), by = c("add1" = "site_addr", "city" = "city"))
  
  joined_by_address <- join_by_address() %>% 
    distinct(docket_id, .keep_all = TRUE) %>% 
    st_drop_geometry('geometry')

  log_message("-------- Add parcels to assessors")
  assess <- add_parcels_to_assessors(town_ids = town_ids) %>% 
    filter(!st_is_empty(geometry))
  
  log_message("Step 2. Find assess parcels that contain filings") 
  found_addresses <- assess %>%
    st_join(filings, join=st_contains)
  
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

