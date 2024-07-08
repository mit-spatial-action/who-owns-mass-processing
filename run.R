source("R/globals.R")
source("R/standardizers.R")
# source("R/deduplicaters.R")
# source("R/filing_linkers.R")
source("R/loaders.R")
source("R/run_utils.R")

# Establish connection to local PostGIS instance.
conn <- load_conn()

refresh <- FALSE

MA_MUNIS <- load_layer_flow(
  conn, 
  "ma_munis", 
  loader=load_ma_munis(CRS), 
  refresh=refresh
)

ZIPS <- load_layer_flow(
  conn,
  "zips",
  load_zips(CRS, ma_munis = MA_MUNIS),
  refresh=refresh
)

PLACES <- load_layer_flow(
  conn,
  "places",
  load_places(CRS, ma_munis = MA_MUNIS),
  refresh=refresh
)

# Read assessors table.
ASSESS <- load_layer_flow(
  conn,
  "assess_raw",
  load_assess(
    path = file.path(DATA_DIR, ASSESS_GDB), 
    town_ids = subset_town_ids("test")
  ),
  refresh=TRUE
)

# Read parcels.
if(!load_check_for_table(conn, "parcels")) {
  PARCELS <- load_layer_flow(
    conn,
    "parcels_raw",
    loader=load_parcels(
      path=file.path(DATA_DIR, ASSESS_GDB), 
      town_ids=subset_town_ids("test"),
      crs=CRS
    ),
    refresh=refresh
  ) |>
  dplyr::semi_join(ASSESS, by = dplyr::join_by(loc_id == loc_id))
  
  MA_ADDRESSES <- load_layer_flow(
    conn,
    "ma_addresses",
    load_address_points(MA_MUNIS, crs=CRS),
    refresh=refresh
  )
  
  parcel_to_address <- PARCELS |>
    sf::st_join(ADDRESS_POINTS, join = sf::st_contains_properly) |>
    sf::st_drop_geometry() |>
    dplyr::group_by(loc_id) |>
    dplyr::summarize(
      unit_count = sum(unit_count)
    ) |>
    dplyr::ungroup()
  
}

a <- ASSESS |>
  std_flow_assess_parcel()

ab <- a |>
  dplyr::group_by(loc_id) |>
  dplyr::summarize(
    count = dplyr::n()
  )

DBI::dbDisconnect(conn)

assess_parcel_addr <- ASSESS |>
  std_flow_assess_parcel()

a <- assess_parcel_addr |>
  dplyr::select(
    loc_id,
    fy,
    addr_start,
    addr_end,
    addr_body,
    even,
    muni,
    state,
    postal,
    units,
    use_code
  ) |>
  dplyr::group_by(
    loc_id, addr_body, fy, state, muni, even,
    # Here to suppress "summarize() has grouped output..."
    # warning message.
    .groups = "keep"
  ) |>
  dplyr::summarize(
    addr_start = min(addr_start, na.rm = TRUE),
    addr_end = max(addr_end, na.rm = TRUE),
    postal = collapse::fmode(postal, na.rm = TRUE),
    use_code = collapse::fmode(use_code, na.rm = TRUE),
    dplyr::across(
      dplyr::any_of(c("units", "total_val", "bldg_val", "land_val", "total_val", "area")),
      ~ sum(., na.rm = TRUE)
    )
  ) |>
  dplyr::ungroup() |>
  dplyr::left_join(
    parcels_output,
    by = "loc_id",
    na_matches = "never"
  ) 


assess_owner_addr <- ASSESS |>
  dplyr::mutate(
    type_label = "owner"
  ) |>
  std_flow_owner_address(zips = ZIPS, places = PLACES) |>
  std_flow_owner_name("name") |>
  std_flow_owner_name("addr") |>
  flag_inst("name") |>
  flag_estate("name") |>
  std_remove_estate("name") |>
  flag_estate("addr") |>
  std_remove_estate("addr") |> 
  # Deal with cases where e.g., "LIFE ESTATE" (and nothing else) is in address, 
  # which we assume describes the name field.
  dplyr::mutate(
    estate_name = dplyr::case_when(
      is.na(addr) & estate_addr ~ TRUE,
      .default = estate_name
    ),
    estate_addr = dplyr::case_when(
      is.na(addr) & estate_addr ~ FALSE,
      .default = estate_addr
    )
  ) |>
  flag_trust("name") |>
  flag_inst("addr") |>
  flag_trust("addr")

# Split "TRUSTEES OF" from address.
tmp <- assess_owner_addr |>
  dplyr::filter(!inst_addr & trustee_addr) |>
  dplyr::mutate(
    mult = stringr::str_detect(
      addr, 
      ".+ TRUSTEES OF ",
    )
  )

tmp <- tmp |>
  dplyr::filter(mult) |>
  dplyr::mutate(
    id = dplyr::row_number()
  ) |>
  tidyr::separate_longer_delim(
    addr,
    stringr::regex(" TRUSTEES OF ")
  ) |>
  dplyr::group_by(id) |>
  dplyr::mutate(
    trust = dplyr::case_when(
      dplyr::row_number() > 1 ~ TRUE,
      .default = FALSE
    ),
    trustee = dplyr::case_when(
      dplyr::row_number() == 1 ~ TRUE,
      .default = FALSE
    )
  ) |>
  dplyr::ungroup() |>
  dplyr::bind_rows(
    tmp |>
      dplyr::filter(!mult)
  ) |>
  dplyr::mutate(
    addr = stringr::str_remove_all(addr, SEARCH$trustees)
  ) |> 
  std_trailing_leading("addr")

t <- t |>
  dplyr::mutate(
    and = stringr::str_detect(name, " AND ")
  )

t <- t |>
  dplyr::filter(and & (is.na(trust) | !trust)) |>
  tidyr::separate_longer_delim(
    name,
    stringr::regex(" AND ")
  )
  
  





|>
  dplyr::mutate(
    id = dplyr::row_number(),
    of = stringr::str_detect(name, "[ ,]+\\b(TRUST|TRUSTEES) OF ")
  )

# 1. When it's an individual (not an institution) name and "TRUST/TRUSTEE", 
# call it a trustee and remove "TRUST/TRUSTEE."
# 2. Identify trust/trustees in address, distinguishing between "TRUSTEES OF"
# and other cases.
# 3. If name and address are both trust types

t <- test |>
  dplyr::filter(trust & of) |>
  tidyr::separate_longer_delim(
    name,
    stringr::regex("[ ,]+\\b(TRUST|TRUSTEES) OF (?=.+)")
  ) |>
  dplyr::group_by(id) |>
  dplyr::mutate(
    type = dplyr::case_when(
      dplyr::row_number() > 1 ~ "trust",
      .default = "trustee"
    )
  ) |>
  dplyr::ungroup() |>
  dplyr::mutate()

t2 <- t |>
  dplyr::mutate(
    and = stringr::str_detect(name, " AND ")
  ) |>
  dplyr::filter(!inst & and & type != "trust") |>
  dplyr::mutate(
    id = dplyr::row_number(),
    last_name = stringr::str_extract(name, "^[A-Z]+")
  ) |>
  tidyr::separate_longer_delim(
    name,
    " AND "
  ) |>
  dplyr::mutate(
    words = stringr::str_count(name, "([ ,-]+)") + 1
  ) |>
  dplyr::group_by(id) |>
  
  




# "
t2 <- t |>
  dplyr::filter(splittable) |>
  dplyr::mutate(
    last_name = stringr::str_extract(name, "^[A-Z]+")
  ) |>
  tidyr::separate_longer_delim(
    name,
    " AND "
  ) |>
  dplyr::mutate(
    name = dplyr::case_when(
      stringr::str_detect(name, "^[A-Z]+( [A-Z])?$") ~ 
        stringr::str_c(name, last_name, sep = " "),
      .default = name
    ),
    name = stringr::str_squish(stringr::str_remove(
      name,
      "(?<=^| )[A-Z](?= |$)"
    )),
    id = dplyr::row_number()
  ) |>
  tidyr::separate_longer_delim(
    cols = name,
    delim = stringr::regex("[, ]+")
  ) |>
  dplyr::group_by(dplyr::across(-c(name))) |>
  dplyr::arrange(name, .by_group = TRUE) |>
  dplyr::summarize(
    ordered_name = stringr::str_squish(stringr::str_c(name, collapse = " "))
  ) |>
  dplyr::ungroup() |>
  dplyr::group_by(ordered_name, box) |>
  tidyr::fill(addr, addr_num, addr_body, addr_start, addr_end, even, .direction = "updown") |>
  dplyr::ungroup()

|>
  dplyr::select(-last_name) |>
  dplyr::bind_rows(
    t |>
      dplyr::filter(!splittable)
  ) |>
  dplyr::mutate(
    name = stringr::str_remove(name, "[, ]+$"),
    name = dplyr::case_when(
      stringr::str_detect(name, "^[A-Z \\-]+ ?, ?[A-Z]+") ~ 
        stringr::str_squish(
          stringr::str_c(
            stringr::str_extract(name, "(?<= ?, ?).+$"), 
            stringr::str_extract(name, "^[A-Z \\-]+(?= ?,)"),
            sep = " "
          )
        ),
      .default = name
    ),
    name = dplyr::case_when(
      stringr::str_detect(name, "^[A-Z]+ [A-Z]+ [A-Z]$") ~ 
        stringr::str_c(
          stringr::str_extract(name, "[A-Z]+ [A-Z]$"), 
          stringr::str_extract(name, "^[A-Z]+"), 
          sep = " "),
      .default = name
    ),
    name = stringr::str_remove_all(name, "(?<= )[A-Z]( |$)"),
    name = stringr::str_remove_all(name, ",")
  )

t3 <- t2 |>
  dplyr::mutate(
    group = zoomerjoin::jaccard_string_group(name, n_gram_width = 4, threshold = 0.75)
  )

owner <- owner |>
  dplyr::left_join(
    parcel |>
      dplyr::select(
        address_id = loc_id, 
        addr_start_y = addr_start, 
        addr_end_y = addr_end, 
        addr_body, 
        even, 
        muni,
        state
      ), 
    by = dplyr::join_by(
      addr_body, 
      even, 
      muni, 
      state, 
      between(
        addr_start, 
        addr_start_y, 
        addr_end_y
      )
    ),
    na_matches = "never"
  ) |>
  dplyr::select(-c(addr_start_y, addr_end_y))

owner_parcel_link <- assess_owner_addr |>
  std_link_owner_parcel(assess_parcel_addr)


test <- owner_parcel_link |>
  dplyr::select(-c(addr_num, floor, addr2)) |>
  dplyr::group_by(addr, muni, postal, state, country, addr_start, addr_end, even) |>
  dplyr::mutate(
    address_id = dplyr::case_when(
      is.na(address_id) ~ stringr::str_c("address", dplyr::cur_group_id(), sep = "_"),
      .default = address_id
    )
  ) |>
  dplyr::ungroup()

owner_to_address <- test |> 
  dplyr::distinct(address_id, corp_id)

unique_owners <- test |>
  dplyr::distinct(corp_id, name, relation, corp, trust)


unique_addresses <- test  |>
  dplyr::distinct(
    address_id, addr, muni, postal, state, country, po_box, addr_start, 
    addr_end, even
    )

unique_addresses |>
  dplyr::filter(!po_box) |>
  nrow()

addr_owner3 |>
  dplyr::filter(!po_box) |>
  



test <- addr_owner3 |>
  dplyr::filter(!po_box) |>
  


addresses <- addr_parcel_reduced |>
  dplyr::left_join(
    match |>
      dplyr::select(loc_id, owner_id = parcel_id),
    by = c("loc_id" = "loc_id"),
    na_matches = "never"
  )
  



run_all <- function(subset = "test", return_results = FALSE) {
  # Create and open log file with timestamp name.
  lf <- logr::log_open(
      format(Sys.time(), "%Y-%m-%d_%H%M%S"),
      logdir = TRUE
    )
  process_deduplication(
    town_ids = subset_town_ids(subset),
    return_results = return_results
    )
  process_link_filings(town_ids = subset_town_ids(subset))
  # Close logs.
  logr::log_close()
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  run_all(subset = "all", return_results = FALSE)
}