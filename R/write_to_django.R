source('load_results.R')

django_munis <- function(munis, crs=4326) {
  munis |>
    dplyr::mutate(
      state = "MA",
      mapc = FALSE
    ) |>
    dplyr::rename(
      id = muni_id
    ) |>
    sf::st_transform(crs) |>
    tidyr::drop_na(id)
}

django_zips <- function(zips, crs=4326) {
  zips |>
    sf::st_transform(crs) |>
    dplyr::select(
      -dplyr::any_of(c("state", "unambig_state", "muni_unambig_from", "muni_unambig_to"))
    )
}

django_parcels_point <- function(parcels_point, crs=4326) {
  parcels_point <- parcels_point |> 
    dplyr::rename(
      id = loc_id,
    ) |> 
    dplyr::select(
      -dplyr::any_of("wet")
    ) |>
    sf::st_transform(crs)
  
  coords <- sf::st_coordinates(parcels_point)
  
  parcels_point$lng <- coords[, "X"]
  parcels_point$lat <- coords[, "Y"]
  
  parcels_point
}

django_truncate_tables <- function(conn) {
  django_tables <- c("metacorps_network", "tract", "block_group", "zip", "role",
                     "address", "company", "muni", "officer", "owner", 
                     "parcels_point", "site", "site_to_owner", "officer_to_role")
  
  django_tables_sep <- stringr::str_c(django_tables, collapse=",")
  
  DBI::dbExecute(conn, 
                 glue::glue("TRUNCATE TABLE {django_tables_sep} CASCADE;"))
}

write_to_django <- function(load_prefix, django_prefix) {
  
  if (!util_check_for_results()) {
    util_log_message("VALIDATION: Results not present in environment. Pulling from database. 🚀🚀🚀")
    load_results(load_prefix, load_boundaries=TRUE, summarize=TRUE)
  } else {
    util_log_message("VALIDATION: Results already present in environment. 🚀🚀🚀")
  }
  
  util_test_conn(django_prefix)
  conn <- util_conn(django_prefix)
  on.exit(DBI::dbDisconnect(conn))
  
  django_truncate_tables(conn)
  
  # Write Metacorps
  
  load_write(metacorps_network, conn, "metacorps_network", overwrite=FALSE, append=TRUE)
  
  # Write Munis
  
  munis |>
    django_munis() |>
    load_write(conn, "muni", overwrite=FALSE, append=TRUE)
  
  # Write Tracts
  
  tracts |>
    sf::st_transform(4326) |>
    load_write(conn, "tract", overwrite=FALSE, append=TRUE)
  
  # Write Block Groups
  
  block_groups |>
    sf::st_transform(4326) |>
    load_write(conn, "block_group", overwrite=FALSE, append=TRUE)
  
  # Write ZIPs
  
  zips |>
    django_zips()  |>
    load_write(conn, "zip", overwrite=FALSE, append=TRUE)
  
  # Write Parcel Points
  
  parcels_point |> 
    django_parcels_point() |>
    load_write(conn, "parcels_point", overwrite=FALSE, append=TRUE)
  
  # Write Addresses
  
  addresses |>
    dplyr::select(-muni_id) |>
    dplyr::rename(parcel_id=loc_id) |>
    dplyr::mutate(
      parcel_id = dplyr::case_when(
        parcel_id %in% parcels_point$loc_id ~ parcel_id,
      .default = NA_character_
      )
    ) |>
    load_write(conn, "address", overwrite=FALSE, append=TRUE)
  
  # Write Companies
  
  companies |>
    dplyr::rename(
      address_id = addr_id,
      metacorp_id = network_id
    ) |>
    load_write(conn, "company", overwrite=FALSE, append=TRUE)

  officers_to_roles <- officers |>
    dplyr::select(officer=id, role=positions) |>
    tidyr::separate_longer_delim(role, delim=",")
  
  # Write Officers
  
  officers |>
    dplyr::select(-c(positions, innetwork_company_count)) |>
    dplyr::rename(
      address_id = addr_id,
      metacorp_id = network_id
    ) |>
    load_write(conn, "officer", overwrite=FALSE, append=TRUE)

  # Write Role
  
  officers_to_roles |>
    dplyr::select(name=role) |>
    dplyr::distinct() |>
    tidyr::drop_na() |>
    load_write(conn, "role", overwrite=FALSE, append=TRUE)
  
  # Write Officer-to-Role through table.
  
  officers_to_roles |>
    dplyr::rename(
      officer_id = officer,
      role_id = role
    ) |>
    tidyr::drop_na() |>
    load_write(conn, "officer_to_role", overwrite=FALSE, append=TRUE)
  
  # Write owners
  
  owners |>
    dplyr::select(-cosine_group) |>
    dplyr::rename(
      address_id = addr_id,
      metacorp_id = network_group
    ) |>
    dplyr::mutate(
      company_id = dplyr::case_when(
        company_id %in% companies$id ~ company_id,
        .default = NA_integer_
      )
    ) |>
    load_write(conn, "owner", overwrite=FALSE, append=TRUE)
  
  # Write owner
  
  sites |>
    dplyr::rename(
      address_id = addr_id
    ) |>
    dplyr::mutate(
      units = as.integer(units)
    ) |> 
    load_write(conn, "site", overwrite=FALSE, append=TRUE)
  
  # Write site-to-owner through table.
  
  sites_to_owners |>
    dplyr::filter(
      site_id %in% sites$id & owner_id %in% owners$id
    ) |>
    dplyr::select(-id) |>
    load_write(conn, "site_to_owner", overwrite=FALSE, append=TRUE)
  
  
  invisible(NULL)
}

if (!interactive()) {
  opts <- list(
    optparse::make_option(
      c("-l", "--load_prefix"), type = "character", default = NULL,
      help = "Prefix of parameters for database containing deduplication 
      results in .Renviron.", metavar = "character"),
    optparse::make_option(
      c("-d", "--django_prefix"), type = "character", default = NULL,
      help = "Prefix of parameters for database containing Django models in 
      .Renviron.", metavar = "character")
  )
  parser <- optparse::OptionParser(
    option_list=opts
  )
  opt <- optparse::parse_args(parser)
  
  if (is.null(opt$load_prefix)) {
    optparse::print_help(parser)
    stop("Load database prefix must be specified.", call. = FALSE)
  }
  if (is.null(opt$django_prefix)) {
    optparse::print_help(parser)
    stop("Django database prefix must be specified.", call. = FALSE)
  }
  
  write_to_django(load_prefix=opt$load_prefix, django_prefix=opt$django_prefix)
}