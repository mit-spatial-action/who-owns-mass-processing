source('load_results.R')

st_grid_sf <- function(df, cellsize) {
  sf::st_make_grid(
      df, 
      cellsize=cellsize,
      square=FALSE, 
      flat_topped=FALSE
    ) |>
    sf::st_as_sf() |>
    sf::st_set_geometry("geometry") |>
    tibble::rowid_to_column("grid_id")
}

st_agg_to_grids <- function(df, cellsizes, centroids=FALSE) {
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

mapbox_preprocess <- function(load_prefix) {
  if (!utils_check_for_results()) {
    util_log_message("VALIDATION: Results not present in environment. Pulling from database. 🚀🚀🚀")
    load_results(prefix=load_prefix, load_boundaries=TRUE, summarize=TRUE)
  } else {
    util_log_message("VALIDATION: Results already present in environment. 🚀🚀🚀")
  }
}

if (!interactive()) {
  opts <- list(
    optparse::make_option(
      c("-l", "--load_prefix"), type = "character", default = NULL,
      help = "Prefix of parameters for database containing deduplication 
      results in .Renviron.", metavar = "character")
  )
  parser <- optparse::OptionParser(
    option_list=opts
  )
  opt <- optparse::parse_args(parser)
  
  if (is.null(opt$load_prefix)) {
    optparse::print_help(parser)
    stop("Load database prefix must be specified.", call. = FALSE)
  }
  mapbox_preprocess(prefix=opt$load_prefix)
}




  
points <- sites_to_owners |>
  dplyr::left_join(
    owners |>
      dplyr::select(-addr_id) |>
      dplyr::rename(own_name=name) |>
      dplyr::select(id, own_name, network_group,  inst, trust, trustees),
    by=dplyr::join_by(owner_id==id)
  ) |>
  dplyr::left_join(
    metacorps_network |>
      dplyr::select(id, prop_count, unit_count),
    by=dplyr::join_by(network_group==id)
  ) |>
  dplyr::left_join(
    sites |>
      dplyr::select(id, ooc),
    by=dplyr::join_by(site_id==id)
  ) |>
  dplyr::filter(
    !ooc | (unit_count > 1 | prop_count > 1)
  ) |>
  dplyr::group_by(
    site_id
  ) |>
  dplyr::arrange(
    dplyr::desc(prop_count), .by_group=TRUE
  ) |>
  dplyr::summarize(
    owners = stringr::str_flatten_comma(owner_id, na.rm=TRUE),
    own_name = dplyr::first(own_name),
    inst = dplyr::first(inst),
    trust = dplyr::first(trust),
    trustees = dplyr::first(trustees),
    network_group = dplyr::first(network_group)
  ) |>
  dplyr::ungroup() |>
  dplyr::left_join(
    metacorps_network |>
      dplyr::rename(meta_name=name),
    by=dplyr::join_by(network_group==id)
  ) |>
  dplyr::left_join(
    sites |>
      dplyr::select(-muni_id),
    by=dplyr::join_by(site_id==id)
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

t <- points |>
  dplyr::mutate(
    quartile = dplyr::ntile(prop_count, 4),
    quintile = dplyr::ntile(prop_count, 5)
  ) |>
  dplyr::select(site_id, network_group, owners, loc_id, addr, muni, quartile, quintile) |>
  sf::st_transform(4326) |>
  sf::st_write('mapbox_points.shp', delete_dsn=TRUE)

  
t <- st_agg_to_grids(
    points, 
    cellsizes=list(
      units::as_units(0.25, "miles"), 
      units::as_units(0.5, "miles")
    ),
    centroids=FALSE
    ) 

t |>
  sf::st_transform(4326) |>
    sf::st_write("summary_hex.geojson", delete_dsn=TRUE)
  