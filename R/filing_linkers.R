source("R/globals.R")
source("R/standardizers.R")
source("R/loaders.R")
source("R/run_utils.R")

match_nearby_filings <- function(parcel_points_df, miles = 0.1) {
  #' Match filings to nearby parcels.
  #'
  #' @param parcel_points_df Dataframe containing parcel centroids.
  #' @param miles Bandwidth distance, in miles.
  #' @returns A dataframe.
  #' @export
  assess_and_filings <- sf::st_join(
      assess_points_df, 
      filings_with_geometry, 
      join = nngeo::st_nn, 
      maxdist = 5280 * mile_multiplier, 
      k = 2,
      progress = FALSE) |> 
    dplyr::filter(!sf::st_is_empty(geometry)) |> 
    dplyr::filter(!is.na(street))
}

process_filings <- function(df) {
  #' Process eviction filings.
  #'
  #' @param df A dataframe of eviction filings.
  #' @returns A dataframe.
  #' @export
  df |>
    std_flow_strings(c("add1", "city")) |>
    std_zip(c("zip")) |> 
    std_flow_addresses(c("add1")) |>
    std_flow_cities(c("city"))
}

process_link_filings <- function(town_ids = FALSE, crs = 2249) {
  #' Workflow to connect eviction filings to assessors records.
  #'
  #' @param town_ids List of numerical town ids.
  #' @param crs Bandwidth distance, in miles.
  #' @returns A dataframe.
  #' @export
  assess_file <- file.path(
    RESULTS_DIR, 
    stringr::str_c(ASSESS_OUT_NAME, "rds", sep = ".")
    )
  if (file.exists(assess_file)) {
    log_message("Found previously processed assessors table. Loading...")
    assess <- readRDS(assess_file)
  } else {
    log_message("Loading and processing assessors table...")
    assess <- load_assess(path = file.path(DATA_DIR, ASSESS_GDB)) |>
      process_assess()
  }
  
  log_message("Joining filings to parcels by address and city.")
  filings <- load_filings(town_ids, crs = crs) |>
    process_filings() |>
    dplyr::left_join(
      dplyr::select(assess, c(loc_id, site_addr, city)), 
      by = c("add1" = "site_addr", "city" = "city"),
      na_matches = "never"
    )
  
  parcels <- load_parcels(
      file.path(DATA_DIR, ASSESS_GDB), 
      town_ids = town_ids,
      crs = crs
    ) |>
    dplyr::filter(loc_id %in% dplyr::pull(assess, loc_id))
  
  filings_address_match <- filings |>
    dplyr::filter(!is.na(loc_id)) |>
    dplyr::mutate(
      link_type = "address_city"
    )
  
  filings_no_address <- filings |>
    dplyr::filter(is.na(loc_id)) |>
    dplyr::select(-c(loc_id)) |>
    dplyr::left_join(
      dplyr::select(assess, c(loc_id, site_addr, zip)), 
      by = c("add1" = "site_addr", "zip" = "zip"),
      na_matches = "never"
    )
  
  filings_zip_match <- filings_no_address |>
    dplyr::filter(!is.na(loc_id)) |>
    dplyr::mutate(
      link_type = "address_zip"
    )
  
  filings_unmatched <- filings_no_address |>
    dplyr::filter(is.na(loc_id)) |>
    dplyr::select(-c(loc_id))
  
  filings_unmatchable <- filings_unmatched |>
    dplyr::filter(
      !(match_type %in% c("building", "parcel", "rooftop"))
      ) |>
    dplyr::mutate(
      link_type = NA_character_
    )
  
  log_message("Find assess parcels that contain filings") 
  filings_no_match <- filings_unmatched |>
    dplyr::filter(
      match_type %in% c("building", "parcel", "rooftop")
    )
  
  filings <- filings_no_match |>
    sf::st_join(parcels, join=sf::st_within) |>
    dplyr::mutate(
      link_type = dplyr::case_when(
        !is.na(loc_id) ~ "spatial"
        )
    ) |>
    dplyr::bind_rows(filings_address_match, filings_zip_match, filings_unmatchable) |> 
    sf::st_drop_geometry() |>
    dplyr::select(docket_id, loc_id, city, zip, link_type) |>
    write_multi(FILINGS_OUT_NAME)
  
  filings_by_parcel <- filings |>
    dplyr::group_by(loc_id) |>
    dplyr::summarize(
      filing_count = dplyr::n()
    ) |>
    write_multi("filings_per_parcel")
  filings
}

