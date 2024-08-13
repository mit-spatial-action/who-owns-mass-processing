summ_site_group <- function(df, group_col, metacorps) {
  util_log_message(glue::glue("PROCESSING: Summarizing owners table by '{group_col}'."))
  has_group <- df |>
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
  
  metacorps |> 
    dplyr::left_join(
      has_group,
      by=dplyr::join_by(id == !!group_col)
    ) |>
    dplyr::arrange(
      dplyr::desc(prop_count)
    )
}

summ_sites_to_owners <- function(df, owners, sites) {
  df <- df |>
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
      ),
      prop_count = 1,
      unit_count = units,
      units_per_prop = unit_count / prop_count,
      val_per_prop = val / prop_count,
      val_per_area = val / area
    )
}

summ_officers_innetwork_companies <- function(df, metacorps_network) {
  util_log_message("PROCESSING: Counting in-network companies per officer.")
  df |>
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

summ_metacorps_network_companies <- function(df, companies) {
  util_log_message("PROCESSING: Counting companies per network metacorp.")
  df |>
    dplyr::left_join(
      companies |>
        dplyr::group_by(network_id) |>
        dplyr::summarize(
          company_count = dplyr::n_distinct(id)
        ) |>
        dplyr::ungroup() |>
        dplyr::select(network_id, company_count),
      by=dplyr::join_by(id == network_id)
    )
}