points <- sites_to_owners |>
  dplyr::left_join(
    sites |>
      dplyr::select(-muni_id),
    by=dplyr::join_by(site_id==id)
    ) |>
  dplyr::left_join(
    owners |>
      dplyr::select(-addr_id) |>
      dplyr::rename(own_name=name),
    by=dplyr::join_by(owner_id==id)
  ) |>
  dplyr::left_join(
    metacorps_network |>
      dplyr::rename(meta_name=name),
    by=dplyr::join_by(network_group==id)
  ) |>
  dplyr::filter(
    !ooc | (unit_count > 1 | prop_count > 1)
  ) |>
  dplyr::left_join(
    addresses,
    by=dplyr::join_by(addr_id==id)
  ) |>
  dplyr::left_join(
    parcels_point |>
      dplyr::select(loc_id),
    by=dplyr::join_by(loc_id)
  ) |>
  sf::st_set_geometry("geometry")

points |>
  dplyr::select(site_id, network_group, owner_id, loc_id, addr, muni) |>
  sf::st_transform(4326) |>
  sf::st_write('mapbox_points.geojson', delete_dsn=TRUE)


st_grid_sf <- function(df, cellsize) {
  sf::st_make_grid(
    points, 
    cellsize=cellsize,
    square=FALSE, 
    flat_topped=FALSE
    ) |>
    sf::st_as_sf() |>
    sf::st_set_geometry("geometry") |>
    tibble::rowid_to_column("grid_id")
}
  
st_join_to_grids <- function(df, cellsizes, centroids=FALSE) {
  all <- list()
  for (size in cellsizes) {
    grid <- st_grid_sf(points, size)
    agg <- df |>
      sf::st_join(grid) |>
      sf::st_drop_geometry() |>
      dplyr::group_by(grid_id) |>
      dplyr::summarize(
        props = dplyr::n(),
        units = sum(units, na.rm=TRUE),
        units_mean = mean(unit_count, na.rm=TRUE),
        prop_mean = mean(prop_count, na.rm=TRUE),
        units_med = median(unit_count, na.rm=TRUE),
        prop_med = median(prop_count, na.rm=TRUE)
      ) |>
      dplyr::ungroup() |>
      dplyr::mutate(
        size=size
      ) |>
      tidyr::drop_na(units_mean)
    
    all[[as.character(size)]] <- grid |>
      dplyr::filter(grid_id %in% agg$grid_id) |>
      dplyr::left_join(
        agg,
        by=dplyr::join_by(grid_id)
      )
  }
  
  all <- all |>
    dplyr::bind_rows()
  
  if(centroids) {
    all <- all |>
      sf::st_centroid()
  }
  
  all
}
  
t <- st_join_to_grids(
    points, 
    cellsizes=list(
      units::as_units(0.25, "miles"), 
      units::as_units(0.5, "miles")
    ),
    centroids=TRUE
    ) 

t |>
  sf::st_transform(4326) |>
    sf::st_write("test_out.geojson", delete_dsn=TRUE)
  