source("R/globals.R")
source("R/log.R")
source("R/standardizers.R")
source("R/loaders.R")

add_parcels_to_assess <- function(town_ids = c(274)) {
  #' Set up parcels, join asses records to parcel geometry.
  #'
  #' @param town_ids List of numerical town ids.
  #' @returns A dataframe.
  #' @export
  parcels <- load_parcels(file.path(DATA_DIR, ASSESS_GDB), town_ids = town_ids) %>% 
    dplyr::filter(!st_is_empty(geometry)) %>%
    dplyr::left_join(assess, by = c("loc_id" = "loc_id"))
}

match_nearby_filings <- function(parcel_points_df, mile_multiplier = 0.1) {
  #' Match filings to nearby parcels.
  #'
  #' @param parcel_points_df Dataframe containing parcel centroids.
  #' @param mile_multiplier Bandwidth distance, in miles.
  #' @returns A dataframe.
  #' @export
  assess_and_filings <- sf::st_join(
      assess_points_df, 
      filings_with_geometry, 
      join = nngeo::st_nn, 
      maxdist = 5280 * mile_multiplier, 
      k = 2,
      progress = FALSE) %>% 
    dplyr::filter(!sf::st_is_empty(geometry)) %>% 
    dplyr::filter(!base::is.na(street))
}


process_link_filings <- function(town_ids=c(274), mile_multiplier = 0.1) {
  #' Workflow to connect eviction filings to assessors records.
  #'
  #' @param town_ids List of numerical town ids.
  #' @param mile_multiplier Bandwidth distance, in miles.
  #' @returns A dataframe.
  #' @export
  
  log_message("Matching filings on text.")
  assess <- load_assess(path = file.path(DATA_DIR, ASSESS_GDB)) %>%
    # Run string standardization procedures.
    std_flow_strings(c("owner1", "own_addr", "site_addr", "own_zip", "city", "own_city")) %>%
    std_zip(c("zip", "own_zip")) %>% 
    std_flow_addresses(c("own_addr", "site_addr")) %>%
    std_flow_cities(c("city", "own_city")) %>%
    std_flow_names(c("owner1", "own_addr")) %>%
    # Extract 'care of' entities to co and remove from own_addr.
    dplyr::mutate(
      co = dplyr::case_when(
        stringr::str_detect(
          own_addr,
          "C / O"
        )
        ~ stringr::str_extract(own_addr, "(?<=C / O ).*$")
      ),
      own_addr = dplyr::case_when(
        stringr::str_detect(
          own_addr,
          "C / O"
        )
        ~ dplyr::na_if(stringr::str_extract(own_addr, ".*(?= ?C / O )"), ""),
        TRUE ~ own_addr
      )
    ) %>%
    # Flag owner-occupied properties on the basis of standardized addresses.
    dplyr::mutate(
      ooc = dplyr::case_when(
        own_addr == site_addr ~ TRUE,
        TRUE ~ FALSE
      )
    )
  
  log_message("Step 1. Join by address strings")
  
  filings <- load_filings() %>%
    std_flow_strings(c("add1", "city")) %>%
    std_zip(c("zip")) %>% 
    std_flow_addresses(c("add1")) %>%
    std_flow_cities(c("city")) %>%
    dplyr::left_join(
      dplyr::select(assess, c(loc_id, site_addr, city)), 
      by = c("add1" = "site_addr", "city" = "city")
      )
  
  parcels <- load_parcels(file.path(DATA_DIR, ASSESS_GDB)) %>%
    dplyr::filter(loc_id %in% pull(assess, loc_id))
  
  filings_match <- filings %>%
    dplyr::filter(!is.na(loc_id))
  
  filings_no_match <- filings %>%
    dplyr::filter(
      base::is.na(loc_id) & 
        match_type %in% c("building", "parcel", "rooftop")
    ) %>%
    dplyr::select(-c(loc_id))
  
  log_message("Step 2. Find assess parcels that contain filings") 
  filings_results <- filings_no_match %>%
    sf::st_join(parcels, join=sf::st_within) %>%
    dplyr::bind_rows(filings_match)
  
  log_message("-------- Cast parcel geometries to points")
  assess_points <- sf::st_point_on_surface(assess)
  
  log_message(
    paste("Step 3. Match filings to", mile_multiplier, "miles away from assessor parcel point")
    ) 
  nearby_filings <- match_nearby_filings(assess_points, mile_multiplier = mile_multiplier) %>% 
    sf::st_drop_geometry('geometry')
  # drop extra geo columns
  nearby_filings <- nearby_filings[ , !(names(nearby_filings) %in% drops)]
  # merge last dataframe to big one and write to file
  df <- dplyr::full_join(df, nearby_filings) %>% 
  write_delim(
    file.path(RESULTS_DIR, paste(EVICTORS_OUT_NAME, "csv", sep = ".")),
    delim = "|", quote = "needed"
  )
}

