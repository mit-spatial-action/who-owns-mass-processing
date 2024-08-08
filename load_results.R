source("R/loaders.R")
source("R/utilities.R")
source("config.R")

summ_site_group <- function(df, group_col) {
  util_log_message(glue::glue("PROCESSING: Summarizing owners table by '{group_col}."))
  df |>
    dplyr::filter(!is.na(.data[[group_col]])) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_col))) |>
    dplyr::summarize(
      prop_count = dplyr::n(),
      unit_count = sum(units),
      area = sum(area, na.rm = TRUE),
      val = sum(val, na.rm = TRUE),
      units_per_prop = unit_count / prop_count,
      val_per_prop = val / prop_count,
      val_per_area = val / area
    ) |>
    dplyr::ungroup()
}

load_results <- function(prefix, load_boundaries=TRUE, summarize=TRUE) {
  
  util_test_conn(prefix)
  conn <- util_conn(prefix)
  on.exit(DBI::dbDisconnect(conn))
  
  tables <- c(
    "owners", "companies", "officers", "metacorps_cosine", "parcels_point",
    "metacorps_network", "sites", "sites_to_owners", "addresses")
  if (load_boundaries) {
    tables <- c(
      tables,
      c("zips", "munis", "tracts", "block_groups")
    )
  }
  tables_exist <- util_check_for_tables(
    conn,
    tables
  )
  if (!tables_exist) {
    stop(glue::glue("VALIDATION: Tables don't seem to exist on '{prefix}' database."))
  } else {
    util_log_message(glue::glue("VALIDATION: Loading tables from '{prefix}' database."), header=TRUE)
    for(t in tables) {
      util_log_message(glue::glue("INPUT/OUTPUT: Loading {t} from '{prefix}' database."))
      assign(t, load_postgis_read(conn, t), envir = .GlobalEnv)
    }
  }
  invisible(NULL)
  
  if (summarize) {
    util_log_message("PROCESSING: Summarizing metacorps and officers.")
    own_summary <- sites_to_owners |>
      dplyr::left_join(
        owners,
        dplyr::join_by(owner_id == id)
      ) |>
      dplyr::left_join(
        sites |> 
          dplyr::select(id, units, bld_val, lnd_val, res_area, bld_area),
        dplyr::join_by(site_id == id)
      ) |>
      dplyr::filter(!is.na(name)) |>
      dplyr::mutate(
        val = bld_val + lnd_val,
        area = dplyr::case_when(
          (bld_area >= res_area) | (!is.na(bld_area) & is.na(res_area)) ~ bld_area,
          res_area > bld_area | (!is.na(res_area) & is.na(bld_area)) ~ res_area
        ),
        val_per_area = dplyr::case_when(
          !is.na(area) & !is.na(val) ~ area / val,
          .default = NA
        )
      )
    
    cosine_own <- own_summary |>
      summ_site_group("cosine_group")
    
    network_own <- own_summary |>
      summ_site_group("network_group")
    
    metacorps_cosine <<- metacorps_cosine |> 
      dplyr::left_join(
        cosine_own,
        by=dplyr::join_by(id == cosine_group)
      ) |>
      dplyr::arrange(
        dplyr::desc(prop_count)
      )
    
    util_log_message("PROCESSING: Doing additional summarizing across network metacorps.")
    metacorps_network <<- metacorps_network |> 
      dplyr::left_join(
        network_own,
        by=dplyr::join_by(id == network_group)
      ) |>
      dplyr::left_join(
        companies |>
          dplyr::group_by(network_id) |>
          dplyr::summarize(
            company_count = dplyr::n_distinct(id)
          ) |>
          dplyr::ungroup() |>
          dplyr::select(network_id, company_count),
        by=dplyr::join_by(id == network_id)
      ) |>
      dplyr::arrange(
        dplyr::desc(prop_count)
      )
    
    util_log_message("PROCESSING: Doing additional summarizing across officers.")
    officers <<- officers |>
      dplyr::left_join(
        metacorps_network |>
          dplyr::select(id, prop_count),
        by=dplyr::join_by(network_id == id )
      ) |>
      dplyr::arrange(
        dplyr::desc(prop_count)
      ) |>
      # dplyr::select(-prop_count) |>
      dplyr::group_by(network_id, name) |>
      dplyr::mutate(
        innetwork_company_count = dplyr::n_distinct(company_id)
      ) |>
      dplyr::ungroup() |>
      dplyr::group_by(network_id) |>
      dplyr::arrange(dplyr::desc(prop_count), dplyr::desc(innetwork_company_count)) |>
      dplyr::ungroup() |>
      dplyr::select(-prop_count)
  }
}
