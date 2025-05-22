targets::tar_option_set(
  format = "qs",
  packages = c("config", "readr")
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
    command = config::get(file = config_file)
  ),
  targets::tar_target(
    name = parcel_meta,
    command = {
      response <- httr::HEAD(config$parcel_gdb)
      httr::headers(response)[["last-modified"]]
    }
  ),
  targets::tar_target(
    name = gdb_zip,
    {
      dummy <- parcel_meta
      load_remote(
        url = config$parcel_gdb, 
        path = base::file.path(config$data_dir, "parcels.zip")
      )
    },
    format = "file"
  ),
  targets::tar_target(
    gdb_path,
    command = {
      dir <- base::tempfile()
      utils::unzip(gdb_zip, exdir = dir)
      fs::dir_ls(dir, recurse = TRUE, regexp = "\\.gdb$", type = "directory")
    },
    format = "file"
  ),
  targets::tar_target(
    assess_init,
    command = sf::st_read(
      gdb_path,
      layer = config$assess_layer
    )
  ),
  targets::tar_target(
    parcel_init,
    command = sf::st_read(
      gdb_path,
      layer = config$parcel_layer
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
    name = geonames,
    command = load_geonames()
  ),
  targets::tar_target(
    name = block_groups, 
    command = tigris::block_groups(config$state)
  ),
  targets::tar_target(
    name = postal, 
    command = tigris::zctas()
  ),
  targets::tar_target(
    name = companies_file,
    command = config$companies_path,
    format = "file"
  ),
  targets::tar_target(
    name = companies,
    command = readr::read_csv(companies_file)
  ),
  targets::tar_target(
    name = officers_file,
    command = config$officers_path,
    format = "file"
  ),
  targets::tar_target(
    name = officers,
    command = readr::read_csv(officers_file)
  ),
  targets::tar_target(
    name = altnames_file,
    command = config$altnames_path,
    format = "file"
  ),
  targets::tar_target(
    name = altnames,
    command = readr::read_csv(altnames_file)
  ),
  targets::tar_target(
    name = munis_init,
    command = load_munis()
  )
)