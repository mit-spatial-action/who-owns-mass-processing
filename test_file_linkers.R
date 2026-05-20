source('load_results.R')
source('R/filing_linkers.R')
source('R/processors.R')

# load owner/parcel data
load_results("", load_boundaries=TRUE, summarize=TRUE)

# plaintiffs - landlord bringing filing 
# docket id if unique identifier from filings to plantiff
load_evic_results("EVICTION")

# load places data
places <- load_places(munis = munis, zips, crs = 2249)

# TEST FILING LINKING FUNCTIONS  ------------------------------------------

# Get subset of each file - Just Somerville
filings_bos <- filings |>
  dplyr::filter(city == "Boston")

parcels_point_bos <- parcels_point |>
  dplyr::filter(muni_id == "035")

log_message("Linking assessors data to addresses")
assessor <- sites |>
  tidylog::left_join(
    addresses, 
    by = c("addr_id" = "id", 
           "muni_id" = "muni_id"),
    na_matches = "never")

log_message("Joining filings to parcels by address and city.")
# MATCHING BY BOTH CITY AND ZIP - DOUBLE CHECK THIS - DEFINITELY CAUSING TOO INCLUSIZE MATCH IF JUST CITY BUT MAYBE CAN HANDLE THAT OTHER WAY? 
filings_clean <- filings_bos |>
  process_filings() |>
  tidylog::left_join(
    dplyr::select(assessor, c(loc_id, addr, muni, postal)) |> dplyr::distinct(),
    by = c("street" = "addr", "city" = "muni", "zip" = "postal"),
    na_matches = "never"
  )

# filter parcels to just points in assessor data 
parcels <- parcels_point_bos |>
  dplyr::filter(loc_id %in% dplyr::pull(assessor, loc_id))

# df of filings with direct address matches to assessors data
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
  )

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

# unmatchable filings because no parcel match potential - CHECK THIS
filings_unmatchable <- filings_unmatched |>
  dplyr::filter(
    !(match_type %in% c("building", "parcel", "rooftop"))
  ) |>
  dplyr::mutate(
    link_type = NA_character_
  )

# For addresses not found - associate parcel loc_id to address 
# Join parcel data to filings with buffer 
log_message("Find assess parcels that contain filings") 
filings_no_match <- filings_unmatched |>
  dplyr::filter(
    match_type %in% c("building", "parcel", "rooftop")
  ) 


# spatial join with parcel data
filings_spatial <- match_nearby_filings(parcel_points_df=parcels, filings_df =filings_no_match) |> 
  # filter unmatched rows 
  dplyr::filter(!is.na(loc_id)) |>
  dplyr::mutate(
    link_type = dplyr::case_when(
      !is.na(loc_id) ~ "spatial"
    )
  )|> 
  # join address data to be able to fuzzy match in next step 
  tidylog::left_join(addresses, by = c("loc_id"),  suffix = c("_evic", "_assess"))

# NEXT STEP - STRING FUZZY MATCH - OF THESE MATCHES DO ANY HAVE A STRING DISTANCE OF >.9
filings_spatial_clean <- filings_spatial |>
  # keep rows with perfect string match 
  dplyr::filter(start_evic == start_assess & end_evic == end_assess & body_evic == body_assess) |> 
  # CHECK - remove duplicates - in this case not keeping distinct loc_id because there are cases where the same address is in twice 
  dplyr::distinct(start_assess, end_assess, body_assess, docket_id, .keep_all = TRUE) |>
  dplyr::mutate(
    link_type = "spatial_direct")

# list of unmatched filing ids
test_case <- filings_spatial_clean |>
  dplyr::pull(docket_id) 

# Test edge cases 
filings_spatial_edge <- filings_spatial |>
  # joing w/ original unmatched filings to see which didn't work w/ the full filter 
  dplyr::filter(!docket_id %in% test_case) |>
  # keep rows with perfect address name sting match and that fall in address range
  dplyr::filter(body_evic == body_assess & start_evic >= start_assess & start_evic <= end_assess) |>   # CHECK - remove duplicates - in this case not keeping distinct loc_id because there are cases where the same address is in twice 
  dplyr::distinct(start_assess, end_assess, body_assess, docket_id, .keep_all = TRUE) |>
  dplyr::mutate(
    link_type = "spatial_edge")

# list of unmatched filing ids
test_case_2 <- filings_spatial_edge|>
  dplyr::pull(docket_id) 

# now do fuzzy match if address range is the same 
filings_spatial_fuzzy <- filings_spatial |>
  # joing w/ original unmatched filings to see which didn't work w/ the full filter 
  dplyr::filter(!docket_id %in% test_case & !docket_id %in% test_case_2, 
                # make sure address numbers match 
                start_evic >= start_assess & end_evic<=end_assess) |>
  # calculate stringdit using full Damerau-Levenshtein distance
  dplyr::mutate(
    dl_dist = stringdist::stringdist(body_evic, body_assess, method = "dl")) |>
  # looking at the results, dl <= 5 are valid matches, more than that is not
  dplyr::filter(dl_dist <6) |>
  dplyr::distinct(loc_id, docket_id, .keep_all = TRUE) |>
  dplyr::mutate(
    link_type = "spatial_fuzzy")



# CLEAN PLAINTIFF NAMES  ---------------------------------------------------
proc_evic_plantiff <- function(df, type = "plantiff") {
  
  plaintiffs |>
    std_uppercase("name") |>
    std_remove_special("name") |>
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
    dplyr::select(-id)
  
}


