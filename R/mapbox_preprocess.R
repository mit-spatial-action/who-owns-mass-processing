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

st_agg_to_grids <- function(df, 
                            cellsizes, 
                            count_col_name,
                            sum_cols,
                            mean_cols,
                            ntile_cols,
                            ntile_bins = 10,
                            median_cols) {
  all <- list()
  for (size in cellsizes) {
    grid <- st_grid_sf(df, size)
    agg <- df |>
      sf::st_join(grid) |>
      sf::st_drop_geometry() |>
      dplyr::group_by(grid_id) |>
      dplyr::summarize(
        !!count_col_name := dplyr::n(),
        dplyr::across(
          {{ sum_cols }},
          ~ sum(.x, na.rm=TRUE),
          .names = "{.col}_sum"
        ),
        dplyr::across(
          {{ mean_cols }},
          ~ mean(.x, na.rm=TRUE),
          .names = "{.col}_mean"
        ),
        dplyr::across(
          {{ median_cols }},
          ~ median(.x, na.rm=TRUE),
          .names = "{.col}_median"
        )
      ) |>
      dplyr::ungroup() |>
      dplyr::mutate(
        dplyr::across(
          dplyr::any_of(ntile_cols),
          ~ dplyr::ntile(.x, n=ntile_bins),
          .names = "{.col}_ntile"
        )
      ) |>
      dplyr::mutate(
        size=size
      )
    
    all[[as.character(size)]] <- grid |>
      dplyr::filter(grid_id %in% agg$grid_id) |>
      dplyr::left_join(
        agg,
        by=dplyr::join_by(grid_id)
      )
  }
  
  all |>
    dplyr::bind_rows()
}

mapbox_process_tables <- function() {
  sites_to_owners |>
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
}

mapbox_write_points <- function(points, out_file, dest_dir) {
  points |>
    dplyr::select(site_id, network_group, own_name, loc_id, addr, muni) |>
    sf::st_transform(4326) |>
    sf::st_write(
      file.path(dest_dir, out_file), 
      delete_dsn=TRUE
      )
}

mapbox_write_hexes <- function(points, 
                               cellsizes,
                               out_hex_file, 
                               out_centroid_file,
                               dest_dir,
                               ntile_bins=10
                               ) {
  hexes <- points |>
    st_agg_to_grids(
      cellsizes=cellsizes,
      count_col_name = "props",
      sum_cols=c(units),
      mean_cols=c(unit_count, prop_count),
      median_cols=c(unit_count, prop_count),
      ntile_cols=c("prop_count_mean", "unit_count_mean"),
      ntile_bins=ntile_bins
    ) |>
    sf::st_transform(4326) 
  
  hexes |>
    sf::st_write(
      file.path(dest_dir, out_hex_file), 
      delete_dsn=TRUE
      )
  
  hexes |>
    sf::st_centroid() |>
    sf::st_write(
      file.path(dest_dir, out_centroid_file), 
      delete_dsn=TRUE
      )
}

mapbox_publish <- function(file, username, tileset, token, minzoom, maxzoom) {
  base::system2("chmod", args = c("+x", "mapbox_publish.sh"))
  base::system2(
    "./mapbox_publish.sh",
    args = c(
      "-f", base::shQuote(file),
      "-u", base::shQuote(username),
      "-t", base::shQuote(tileset),
      "-k", base::shQuote(token),
      "-m", base::shQuote(minzoom),
      "-x", base::shQuote(maxzoom)
    )
  )
}

mapbox_preprocess <- function(
    load_prefix,
    sites_name="who-owns-mass-sites",
    hexes_name="who-owns-mass-hexes",
    hex_centroids_name="who-owns-mass-hex-centroids",
    mb_token=Sys.getenv("MB_TOKEN"),
    mb_user=Sys.getenv("MB_USER"),
    dest_dir=RESULTS_PATH
    ) {
  if (!utils_check_for_results()) {
    util_log_message("VALIDATION: Results not present in environment. Pulling from database. 🚀🚀🚀")
    load_results(prefix=load_prefix, load_boundaries=TRUE, summarize=TRUE)
  } else {
    util_log_message("VALIDATION: Results already present in environment. 🚀🚀🚀")
  }
  if (!nchar(mb_token) > 0) {
    stop("VALIDATION: Mapbox token not set. Check that MB_TOKEN is defined in .Renviron.")
  }
  if (!nchar(mb_user) > 0) {
    stop("VALIDATION: Mapbox user not set. Check that MB_USER is defined in .Renviron.")
  }
  mapbox_points <- mapbox_process_tables()

  util_log_message("PROCESSING: Processing sites for display on Mapbox.")
  mapbox_points |>
    mapbox_write_points(
      out_file=stringr::str_c(
        sites_name,
        "geojson",
        sep="."
      ),
      dest_dir=dest_dir
    )

  util_log_message("PROCESSING: Producing hexes for display on Mapbox.")
  mapbox_points |>
    mapbox_write_hexes(
      cellsizes=list(
        units::as_units(0.25, "miles"),
        units::as_units(0.5, "miles")
    ),
    out_hex_file=stringr::str_c(
      hexes_name,
      "geojson",
      sep="."
    ),
    out_centroid_file=stringr::str_c(
      hex_centroids_name,
      "geojson",
      sep="."
      ),
    dest_dir=dest_dir
    )
  
  util_log_message("UPLOADING: Uploading sites to Mapbox Tileset.")
  mapbox_publish(
    file=file.path(
      dest_dir,
      stringr::str_c(
        sites_name,
        "geojson",
        sep="."
      )
      ),
    username=mb_user,
    tileset=sites_name,
    token=mb_token,
    minzoom=10,
    maxzoom=16
  )
  
  util_log_message("UPLOADING: Uploading hexes to Mapbox Tileset.")
  mapbox_publish(
    file=file.path(
      dest_dir, 
      stringr::str_c(
        hexes_name, 
        "geojson", 
        sep="."
      )
    ),
    username=mb_user,
    tileset=hexes_name,
    token=mb_token,
    minzoom=5,
    maxzoom=16
  )
  
  util_log_message("UPLOADING: Uploading hex centroids to Mapbox Tileset.")
  mapbox_publish(
    file=file.path(
      dest_dir, 
      stringr::str_c(
        hex_centroids_name, 
        "geojson", 
        sep="."
      )
    ),
    username=mb_user,
    tileset=hex_centroids_name,
    token=mb_token,
    minzoom=5,
    maxzoom=16
  )
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
  