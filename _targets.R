targets::tar_option_set(
  format = "qs"
)

targets::tar_source()

config <- targets::tar_config_yaml()
config <- config$testing

list(
  targets::tar_target(
    name = munis,
    command = load_munis(
      crs = config$crs,
      path = config$data_path
    )
  ),
  targets::tar_target(
    name = zips,
    command = load_zips(
      munis = munis,
      crs = config$crs,
      thresh = config$thresh$zip_int
    )
  ),
  targets::tar_target(
    name = places,
    command = load_places(
      munis=munis,
      zips=zips,
      crs=config$crs
    )
  ),
  targets::tar_target(
    name = tracts,
    command = load_tracts(
      state = config$state,
      crs = config$crs
      )
  ),
  targets::tar_target(
    name = block_groups,
    command = load_block_groups(
      state = config$state,
      crs = config$crs
    )
  ),
  targets::tar_target(
    name = assess,
    command = load_assess(
      path = config$data_path,
      gdb_path = file.path(config$data_path, config$gdb_path),
      layer = config$assess_layer,
      muni_ids = muni_ids,
      most_recent = most_recent
    )
  )
)
