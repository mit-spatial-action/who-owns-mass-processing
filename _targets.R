targets::tar_option_set(
  format = "qs",
  packages = c("config", "tigris", "readr")
)

targets::tar_source()

list(
  targets::tar_target(
    name = config_file,
    command = "config.yaml",
    format = "file"
  ),
  targets::tar_target(
    name = config,
    command = config::get(file = config_file) |>
      utils_validate_config()
  ),
  targets::tar_target(
    name = parcels_meta,
    command = {
      response <- httr::HEAD(config$parcels_remote)
      httr::headers(response)[["last-modified"]]
    }
  ),
  targets::tar_target(
    name = parcels_zip,
    {
      dummy <- parcels_meta
      path <- base::file.path(config$data_dir, "parcels.zip")
      load_remote(
        url = config$parcels_remote, 
        path = path
      )
      path
    },
    format = "file"
  ),
  targets::tar_target(
    parcels_path,
    command = {
      dir <- base::tempfile()
      utils::unzip(parcels_zip, exdir = dir)
      fs::dir_ls(dir, recurse = TRUE, regexp = "\\.gdb$", type = "directory")
    },
    format = "file"
  ),
  targets::tar_target(
    assess_init,
    command = sf::st_read(
      parcels_path,
      layer = config$assess_layer
    )
  ),
  targets::tar_target(
    parcels_init,
    command = sf::st_read(
      parcels_path,
      layer = config$parcels_layer
    )
  ),
  targets::tar_target(
    name = address_bos,
    command = load_boston_addresses()
  ),
  targets::tar_target(
    name = address_massgis,
    command = load_massgis_addresses()
  ),
  targets::tar_target(
    name = geonames_init,
    command = load_geonames(config$state)
  ),
  targets::tar_target(
    name = cbgs, 
    command = tigris::block_groups(config$state) |>
      st_preprocess(config$crs) |>
      dplyr::select(id = geoid)
  ),
  targets::tar_target(
    name = zips_init, 
    command = tigris::zctas(cb = FALSE)
  ),
  targets::tar_target(
    name = companies_file,
    command = config$companies_path,
    format = "file"
  ),
  targets::tar_target(
    name = companies_init,
    command = readr::read_csv(companies_file)
  ),
  targets::tar_target(
    name = officers_file,
    command = config$officers_path,
    format = "file"
  ),
  targets::tar_target(
    name = officers_init,
    command = readr::read_csv(officers_file)
  ),
  targets::tar_target(
    name = alt_names_file,
    command = config$alt_names_path,
    format = "file"
  ),
  targets::tar_target(
    name = alt_names_init,
    command = readr::read_csv(alt_names_file)
  ),
  targets::tar_target(
    name = munis_init,
    command = load_munis(state = config$state)
  ),
  targets::tar_target(
    name = munis,
    command = proc_munis(munis_init, crs = config$crs)
  ),
  targets::tar_target(
    name = states,
    command = tigris::states() |>
      st_preprocess(config$crs) |>
      dplyr::select(id = geoid, name, abbrev = stusps)
  ),
  targets::tar_target(
    name = zips,
    command = proc_zips(
      zips_init, 
      state = config$state,
      munis = munis, 
      states = states, 
      crs = config$crs, 
      thresh = config$zip_int_thresh)
  ),
  targets::tar_target(
    name = parcels,
    command = proc_parcels(
      parcels_init |> dplyr::slice_head(n=50000),
      cbgs = cbgs,
      crs = config$crs
    )
  ),
  targets::tar_target(
    name = companies,
    command = proc_companies(
      companies_init |> dplyr::slice_head(n=50000)
    )
  )
)