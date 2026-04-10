source("R/standardizers.R")
source("R/loaders.R")

# OVERVIEW 
# The goal of this script is to link eviction filings and their plaintiffs to property owners

# STEP 1: Match eviction filing addresses to assessor records addresses
# STEP 1A: Match on address, city, zip, or address, city
# STEP 1B: For evictions that don't match directly on address, do a 
#          spatial nearest neighbor join (defined in match_nearby_filings() helper function)
#          to parcel data and then try to match addresses. 
# STEP 2: Match eviction plantiffs to owners


# NEW WORKFLOW - 
# 1. tie address to address in addresses table (or append if not found).
# 2. if address not found (or if existing address has no location), associate parcel loc_id with address.
# 3. join to owners on name/address.
# 4. join to owners on name (cosine similarity)/address.
# 5. for those filings unmatched in 3-4, match by name within parcel.


# HELPER FUNCTIONS --------------------------------------------------------

match_nearby_filings <- function(parcel_points_df, filings_df, miles = 0.1) {
  #' Match filings to 4 nearest parcels.
  #'
  #' @param parcel_points_df Dataframe containing parcel centroids.
  #' @param miles Bandwidth distance, in miles.
  #' @returns A dataframe.
  #' @export
  assess_and_filings <- sf::st_join(
    filings_df |>
      sf::st_as_sf() |>
      sf::st_transform(2249) |>
      dplyr::filter(!sf::st_is_empty(geometry)),
    parcel_points_df,
    join = nngeo::st_nn,
    k = 4,
    maxdist = 5280 * miles,
    progress = FALSE) |>
    dplyr::filter(!sf::st_is_empty(geometry)) |>
    dplyr::filter(!is.na(street))
}

process_filings <- function(df) {
  #' Clean and standardize eviction filings.
  #'
  #' @param df A dataframe of eviction filings.
  #' @returns A dataframe.
  #' @export
  df |>
    proc_evic("street", postal_col = "zip", zips = zips, places= places, state_col = "state", muni_col = "city") |>
    dplyr::distinct()
}

# CLEAN PLAINTIFF NAMES  ---------------------------------------------------
proc_evic_plantiff <- function(df, type = "plantiff") {
  
  plaintiffs |>
    dplyr::mutate(type = "plantiff") |>
    proc_name_co_dba_attn(
      "name",
      "name",
      retain = TRUE
    ) |>
    proc_name(
      "name",
      multiname = FALSE,
      type="plantiff"
    ) |>
    std_uppercase("name") |>
    std_remove_special("name") |>
    dplyr::select(-id)
  
}

# Function to run everything 
process_link_filings <- function(assess_df, evic_df = filings, parcels_points, town_ids = FALSE, crs = 2249) {
  #' Workflow to connect eviction filings to assessors records.
  #'
  #' @param assess_df Assessors data (sites)
  #' @param evic_df Eviction filings 
  #' @param town_ids List of numerical town ids.
  #' @param crs Bandwidth distance, in miles.
  #' @returns A dataframe.
  #' @export
  
  # join assessors data (sites) to addresses
  assessor <- sites |>
    dplyr::left_join(
      addresses, 
      by = c("addr_id" = "id", 
             "muni_id" = "muni_id"),
      na_matches = "never")
  
  # PART 1A - MATCH EVICTION FILINGS TO ASSESSORS RECORDS ADDRESSES BASED ON A COMBINATION OF ADDRESS, CITY, ZIP 
  # Join eviction filings to assessors data by address, city, and zip code 
  filings_clean <- filings |>
    # clean and standardize eviction filings addresses 
    process_filings() |>
    tidylog::left_join(
      dplyr::select(assessor, c(loc_id, addr, muni, postal)) |> dplyr::distinct(),
      by = c("street" = "addr", "city" = "muni", "zip" = "postal"),
      na_matches = "never") # 21,195 matched rows
  
  # df of eviction filings with direct address matches to assessors data (from city/zip join above)
  filings_address_match <- filings_clean |>
    dplyr::filter(!is.na(loc_id)) |>
    dplyr::mutate(
      link_type = "address_city"
    )
  
  # if didn't match on addr, city, and zip - match just on addr and zip
  filings_no_address <- filings_clean |>
    dplyr::filter(is.na(loc_id)) |>
    dplyr::select(-c(loc_id)) |>
    tidylog::left_join(
      dplyr::select(assessor, c(loc_id, addr, postal)), 
      by = c("street" = "addr", "zip" = "postal"),
      na_matches = "never"
    ) # 1,097 matched rows
  
  # filings that match on addr + zip 
  filings_zip_match <- filings_no_address |>
    dplyr::filter(!is.na(loc_id)) |>
    dplyr::mutate(
      link_type = "address_zip"
    )
  
  # still unmatched filings 
  filings_unmatched <- filings_no_address |>
    dplyr::filter(is.na(loc_id)) |>
    dplyr::select(-c(loc_id))
  
  # unmatchable filings because no parcel match potential - e.g., match_type %in% c(approximate, point, road, street) 
  filings_unmatchable <- filings_unmatched |>
    dplyr::filter(
      !(match_type %in% c("building", "parcel", "rooftop"))
    ) |>
    dplyr::mutate(
      link_type = NA_character_
    ) # 3,527 unmatchable 
  
  
  # filter parcels to just points in assessor data 
  parcels <- parcels_point |>
    dplyr::filter(loc_id %in% dplyr::pull(assessor, loc_id)) 
  
  
  # PART 1B - FOR EVICTIONS WITHOUT A DIRECT ADDRESS MATCH, TRY TO SPATIALLY JOIN TO PARCEL DATA  
  # AND THEN FUZZY MATCH ADDRESSES  
  
  # Filter to eviction filings unmatched on address - 27,606 records
  filings_no_match <- filings_unmatched |>
    dplyr::filter(
      match_type %in% c("building", "parcel", "rooftop")
    ) 
  
  # spatial join with parcel data
  filings_spatial <- match_nearby_filings(parcel_points_df=parcels, filings_df =filings_no_match) |> # helper function defined above
    # filter out unmatched rows 
    dplyr::filter(!is.na(loc_id)) |>
    # create a flag for values spatially joined
    dplyr::mutate(
      link_type = dplyr::case_when(
        !is.na(loc_id) ~ "spatial"
      )
    )|> 
    # join address data to be able to fuzzy match in next step 
    tidylog::left_join(addresses, by = c("loc_id"),  suffix = c("_evic", "_assess"))
  
  
  # Now we have a df of eviction filings and the 4 nearest parcel addresses
  # We need to clean this dataframe to just keep relevant address matches
  
  # First, match eviction addresses and parcel addresses directly - 9,682 records
  filings_spatial_direct <- filings_spatial |>
    # keep rows with perfect string match either in range or distinct
    dplyr::filter(body_evic == body_assess & start_evic >= start_assess & start_evic <= end_assess | 
                    start_evic == start_assess & end_evic == end_assess & body_evic == body_assess) |> 
    # CHECK - remove duplicates - in this case not keeping distinct loc_id because there are cases where the same address is in twice 
    # CHECK - Case like 17 Court where there is the single address (17 court) and also a range (17-19 court)
    dplyr::distinct(docket_id, .keep_all = TRUE) |>
    dplyr::mutate(
      link_type = "spatial_direct")
  
  
  # list of unmatched filing ids
  unmatched_filings <- filings_spatial_direct |>
    dplyr::pull(docket_id) 
  
  # now do fuzzy match if address range is the same - 770 obs
  filings_spatial_fuzzy <- filings_spatial |>
    # joing w/ original unmatched filings to see which didn't work w/ the full filter 
    dplyr::filter(!docket_id %in% unmatched_filings, 
                  # make sure address numbers match 
                  start_evic >= start_assess & end_evic<=end_assess) |>
    # calculate stringdit using full Damerau-Levenshtein distance
    dplyr::mutate(
      dl_dist = stringdist::stringdist(body_evic, body_assess, method = "dl")) |>
    # looking at the results, dl <4 are valid matches, more than that is not
    dplyr::filter(dl_dist <4) |>
    dplyr::distinct(loc_id, docket_id, .keep_all = TRUE) |>
    dplyr::mutate(
      link_type = "spatial_fuzzy")
  
  # COMBINE INTO ONE DF - 35,756 records - all values from filings match, but not all plaintiffs are in eviction records
  filings_spatial_clean <- dplyr::bind_rows(filings_spatial_direct |> sf::st_as_sf() |> sf::st_transform(2249) , 
                                            filings_spatial_fuzzy |> sf::st_as_sf() |> sf::st_transform(2249),
                                            filings_address_match |> sf::st_as_sf() |> sf::st_transform(2249), 
                                            filings_zip_match |> sf::st_as_sf() |> sf::st_transform(2249)) |>
    # join plantiff name by docket_id - NOTE: there are ~21K filings that we have location data for
    # so also have limited plantiff docket id match
    # there are ~3K duplicates which represent when multiple plantiffs are attached to the same docket ID 
    tidylog::left_join(proc_evic_plantiff(), by = c("docket_id")) 
  
  
  # STEP 2 - MATCH PLANTIFFS TO OWNERS
  
  # join assessors records to owners 
  con <- duckdb::dbConnect(duckdb::duckdb())
  
  # Register your R dataframes as virtual tables in DuckDB
  duckdb::duckdb_register(con, "owners", owners)
  duckdb::duckdb_register(con, "assessor", assessor)
  
  # Join them using dplyr syntax (executed via DuckDB)
  owners_assess <- dplyr::tbl(con, "owners") |>
    dplyr::distinct(name, addr_id, cosine_group, network_group) |>
    tidylog::left_join(dplyr::tbl(con, "assessor")|> dplyr::select(c(loc_id, addr, addr_id, muni, postal)) |> dplyr::distinct(), by = "addr_id") |>
    dplyr::collect() |># Pulls the final result back into R
    dplyr::distinct(name, addr_id, cosine_group, network_group,.keep_all= TRUE) # get one record per name and address
  
  # STEP 2A - DIRECT STRING MATCH TO OWNERS -  3,136 plaintiffs match owners + loc_id, 33,341 don't 
  filings_spatial_owners <- filings_spatial_clean |>
    tidylog::left_join(owners_assess, by = c("name", "loc_id")) 
  # TRYING FUZZY MATCH 
  
  # data frame of matched 
  filings_direct_own_match <- filings_spatial_owners |>
    dplyr::filter(!is.na(addr_id)) |>
    dplyr::mutate(owner_link = "name_loc_id")
  
  # STEP 2b: FUZZY MATCH USING COSINE SIMILARITY 
  # Filter out plaintiffs that matched - 1,718 new matches 
  fuzzy_match_plantiff <- filings_spatial_clean |>
    # filter evictions already matched 
    dplyr::anti_join(owners_assess, by = c("name")) |>
    tidylog::left_join(owners_assess, by = c("loc_id")) |>
    # calculate stringdit using cosine similarity and character level overlap 
    dplyr::mutate(
      # 1. Cosine Similarity (q=2 captures "chunks" of names)
      # This prevents "Jon" and "Ian" from looking too similar 
      cosine_sim = stringdist::stringsim(name.x, name.y, method = "cosine", q = 2),
      
      # 2. Character-level overlap (how many characters they share)
      # Using q=1 makes it a "bag of characters" comparison
      char_overlap = stringdist::stringsim(name.x, name.y, method = "cosine", q = 1)
    ) |>
    dplyr::filter(cosine_sim > 0.5)
  
  # STEP 2C - FOR REMAINING UNMATCHED - MATCH BY NAME WITHIN PARCEL 
  filin
  
  # match cleaned plantiff names toMatc eviction filings by docket_id
  filings_spatial_names <- filings_spatial_clean |>
    tidylog::left_join(plantiff_clean, by = c("docket_id"))
  
  # filings_spatial <- filings_no_match |>
  #   sf::st_as_sf() |>
  #   sf::st_transform(2249) |>
  #   sf::st_join(parcels, join=sf::st_intersects) |>
  #   dplyr::mutate(
  #     link_type = dplyr::case_when(
  #       !is.na(loc_id) ~ "spatial"
  #       )
  #   ) |>
  #   dplyr::bind_rows(filings_address_match, filings_zip_match, filings_unmatchable) 
  # 
  # sf::st_drop_geometry() |>
  # dplyr::select(docket_id, loc_id, city, zip, link_type) |>
  # write_multi(FILINGS_OUT_NAME)
  
  filings_by_parcel <- filings |>
    dplyr::group_by(loc_id) |>
    dplyr::summarize(
      filing_count = dplyr::n()
    ) |>
    write_multi("filings_per_parcel")
  filings
}

