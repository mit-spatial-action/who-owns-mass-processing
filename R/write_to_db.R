library(RPostgres)
library(dplyr)
library(DBI)
library(sf)
library(geojsonio)

# Connect to PostgreSQL database
con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "who_owns",
  host = "localhost",
  port = 5432,
  user = "",
  password = ""
)

addresses_for_db <- addresses |> dplyr::rename(
  muni_str = muni
) 

addresses_for_db$start <- as.integer(addresses_for_db$start)
addresses_for_db$end <- as.integer(addresses_for_db$end)

parcels_point_for_db <- parcels_point |> dplyr::rename(
  id = loc_id, 
) |> dplyr::select(-c(wet))

missing_loc_ids_in_pp <- addresses_for_db |> 
  dplyr::filter(!is.na(loc_id)) |>
  dplyr::filter(!(loc_id %in% parcels_point$loc_id)) |>
  dplyr::select(c(loc_id, muni_id)) |>
  dplyr::distinct()

# update parcels_point with missing loc_ids (otherwise address df errors on adding to db)
updated_parcels_df <- data.frame()
for (row in 1:nrow(missing_loc_ids_in_pp)) {
  new_df <- data.frame(
    id = missing_loc_ids_in_pp[row,]$loc_id,
    muni_id = missing_loc_ids_in_pp[row,]$muni_id
  )
  updated_parcels_df <- dplyr::bind_rows(updated_parcels_df, new_df)
}

parcels_point_for_db <- dplyr::bind_rows(parcels_point_for_db, updated_parcels_df)


unique_munis <- addresses_for_db |> dplyr::distinct(muni_id, muni_str) |> 
  dplyr::filter(!is.na(muni_id)) |> 
  dplyr::filter(!(muni_id %in% munis$muni_id))

unique_munis <- unique_munis |>
  dplyr::rename(muni = muni_str, 
                id = muni_id)

munis_for_db <- munis |> dplyr::rename(
  id = muni_id
) 

munis_for_db <- munis_for_db |> dplyr::add_row(unique_munis)

# TODO: decide if we're keeping owner occupied sites (ooc) or if there's a threshold
sites_for_db <- sites |>
  dplyr::rename(address_id = addr_id) |>
  dplyr::mutate(units = as.integer(units))


# TODO: having to deduplicate (`distinct` method) because there are two entries 
# with the same ID (48) and different `site_id`
owners_for_db <- dplyr::left_join(owners, sites_to_owners |> 
                                    dplyr::select(-c(id)), by=c('id'='owner_id')) |> 
  dplyr::rename(
    address_id = addr_id,
    metacorp_id = network_group
  ) |> dplyr::distinct(id, .keep_all = TRUE)


company_types <- data.frame(matrix(ncol = 2, nrow=length(unique(companies$company_type))))
colnames(company_types) <- c("id", "name")
company_types <- company_types |> dplyr::mutate(
  id = row_number(), 
  name = unique(companies$company_type))

companies_for_db <- companies |> 
  dplyr::left_join(company_types, by=c('company_type' = 'name'), suffix = c("companies", "company_type")) |>
  dplyr::rename(
    id = idcompanies,
    company_type_id = idcompany_type,
    address_id = addr_id, 
    metacorp_id = network_id
  ) |>
  dplyr::select(-c(company_type))

# for some reason we have companies in owners table that don't exist in compannies table
# TODO: do something better than just getting rid of them
nrow(owners_for_db |> dplyr::filter(!(company_id %in% companies$id) ))
owners_for_db <- owners_for_db |> dplyr::filter(company_id %in% companies$id)

# UPDATE mytable SET the_geom  = ST_SetSRID(the_geom, newSRID);
dbWriteTable(con, "metacorps_network", metacorps_network, overwrite = FALSE, append = TRUE)
dbWriteTable(con, "munis", munis_for_db, overwrite = FALSE, append = TRUE)
dbWriteTable(con, "zips", zips, overwrite = FALSE, append = TRUE)

# dbWriteTable(con, "parcels", parcels, overwrite = FALSE, append = TRUE)
dbWriteTable(con, "tracts", tracts, overwrite = FALSE, append = TRUE)
dbWriteTable(con, "block_groups", block_groups, overwrite = FALSE, append = TRUE)
dbWriteTable(con, "parcels_point", parcels_point_for_db, overwrite = FALSE, append = TRUE)
dbWriteTable(con, "addresses", addresses_for_db, overwrite = FALSE, append = TRUE)
dbWriteTable(con, "company_types", company_types, overwrite = FALSE, append = TRUE)
dbWriteTable(con, "sites", sites_for_db, overwrite = FALSE, append = TRUE)
dbWriteTable(con, "who_owns_institution", companies_for_db, overwrite = FALSE, append = TRUE)
dbWriteTable(con, "owners", owners_for_db, overwrite = FALSE, append = TRUE)

# Close the connection
dbDisconnect(con)

# currently saving owners with 3 or more properties (partly for mapbox size reasons, <= 5MB)
mapbox_gis_df <- sites_for_db |> dplyr::filter(ooc == FALSE) |> 
  dplyr::select(c(id, address_id)) |>
  dplyr::left_join(owners_for_db |> dplyr::filter(inst == TRUE) |> dplyr::select(c(site_id, metacorp_id)), by=c('id' = 'site_id')) |>
  dplyr::left_join(addresses_for_db |> dplyr::select(c(id, loc_id)), by=c('address_id' = 'id')) |>
  dplyr::left_join(parcels_point_for_db |> dplyr::select(c(id, geometry)), by=c('loc_id' = 'id')) |>
  dplyr::filter(!(is.na(metacorp_id))) |>
  dplyr::left_join(metacorps_network |> dplyr::select(c(id, company_count)), by=c('metacorp_id' = 'id')) |>
  dplyr::filter(company_count > 2) |>
  dplyr::rename(site_id = id) |>
  dplyr::mutate(geometry = st_transform(geometry, crs = 4326)) |>
  dplyr::mutate(coords = sf::st_coordinates(geometry)) |>
  dplyr::mutate(longitude = coords[,1],
                latitude = coords[,2]) |>
  dplyr::select(-c(coords, site_id, address_id)) 



# mapbox_gis_json <- toJSON(mapbox_gis_df)


geojson <- geojson_list(mapbox_gis_df)
sf_df <- st_as_sf(mapbox_gis_df |> dplyr::filter(!(is.na(longitude))), coords = c("longitude", "latitude"), crs = 2249, remove = FALSE)
st_write(sf_df, "output.geojson", driver = "GeoJSON")
