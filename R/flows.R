# Preprocessors ====

flow_assess_preprocess <- function(df, path) {
  simp_cols <- c(
    "own_name", "site_addr", "site_muni", "site_postal", "own_addr", "own_muni", 
    "own_postal", "own_state", "own_country")
  util_log_message(glue::glue("Preprocessing Assessors' Tables."))
  df |>
    dplyr::rename_with(stringr::str_to_lower) |>
    dplyr::filter(!is.na(site_loc_id)) |>
    dplyr::mutate(
      site_addr = dplyr::case_when(
        is.na(site_addr) & 
          !is.na(addr_num) & 
          !is.na(full_str) ~ stringr::str_c(addr_num, full_str, sep = " "),
        .default = site_addr
      )
    ) |>
    dplyr::select(-c(addr_num, full_str)) |>
    # All parcels are in MA, in the US...
    dplyr::mutate(
      site_muni_id = std_pad_muni_ids(site_muni_id),
      site_ls_date = lubridate::fast_strptime(site_ls_date, "%Y%m%d", lt=FALSE),
      site_state = "MA", 
      site_country = "US"
    ) |>
    dplyr::left_join(
      load_muni_table(path) |> dplyr::rename(site_muni = muni),
      by = dplyr::join_by(site_muni_id == muni_id)
    ) |>
    tidyr::replace_na(list(site_units = 0)) |>
    std_luc(path, "site_muni_id", "site_use_code", "site_luc") |>
    std_flag_residential("site_luc", "site_muni_id", "site_res") |>
    std_units_from_luc("site_luc", "site_muni_id", "site_units") |>
    std_uppercase(simp_cols) |>
    std_replace_blank(simp_cols) |>
    std_remove_special(simp_cols) |>
    std_spacing_characters(simp_cols) |>
    std_leading_zeros(simp_cols, rmsingle = FALSE) |>
    std_squish(simp_cols)
}

flow_officers_preprocess <- function(df) {
  df |>
}

# Addresses ====

flow_address_text <- function(df, cols) {
  df |>
    std_remove_commas(cols) |>
    std_street_types(cols) |>
    std_directions(cols) |>
    std_frac_to_dec(cols) |>
    std_small_ordinals(cols) |>
    std_leading_zeros(cols, rmsingle = TRUE) |>
    std_hyphenate_range(cols) |>
    std_massachusetts(cols) |>
    std_squish(cols)
}

flow_assess_address_text <- function(df, site_id_col, site_addr_col, own_addr_col) {
  own_id_col <- stringr::str_replace(site_id_col, "site", "own")
  df <- df |>
    dplyr::mutate(
      !!own_id_col := dplyr::case_when(
        .data[[site_addr_col]] == .data[[own_addr_col]] ~ .data[[site_id_col]],
        .default = NA_character_
      )
    )
  
  matched <- df |>
    dplyr::filter(!is.na(.data[[own_id_col]])) |>
    flow_address_text(site_addr_col)
  
  
  df |>
    dplyr::filter(is.na(.data[[own_id_col]])) |>
    flow_address_text(c(site_addr_col, own_addr_col)) |>
    dplyr::mutate(
      !!own_id_col := dplyr::case_when(
        .data[[site_addr_col]] == .data[[own_addr_col]] ~ .data[[site_id_col]],
        .default = .data[[own_id_col]]
      )
    ) |> 
    dplyr::bind_rows(matched)
}

flow_addr2 <- function(df, cols, po_pmb = FALSE) {
  if (po_pmb) {
    df <- df |> 
      std_addr2_po_pmb(cols)
  }
  df |>
    std_addr2_remove_keywords(cols) |>
    std_addr2_floor(cols) |>
    std_addr2_twr(cols) |>
    std_addr2_bldg(cols) |>
    std_addr2_range(cols) |>
    std_addr2_alpha_num(cols) |>
    std_addr2_num_alpha(cols) |> 
    std_addr2_num(cols) |> 
    std_addr2_alpha(cols) |>
    std_squish(cols)
}



flow_assess_addr2 <- function(df, site_id_col, site_addr_col, own_addr_col) {
  own_id_col <- stringr::str_replace(site_id_col, "site", "own")
  
  matched <- df |>
    dplyr::filter(!is.na(.data[[own_id_col]])) |>
    flow_addr2(site_addr_col, po_pmb = FALSE)
  
  df |>
    dplyr::filter(is.na(.data[[own_id_col]])) |>
    flow_addr2(c(site_addr_col, own_addr_col), po_pmb = TRUE) |>
    std_remove_street_types(c(site_addr_col, own_addr_col), fill=TRUE) |>
    dplyr::mutate(
      !!own_id_col := dplyr::case_when(
        .data[[site_addr_col]] == .data[[own_addr_col]] ~ .data[[site_id_col]],
        .default = .data[[own_id_col]]
      )
    )  |>
    dplyr::bind_rows(matched)
}

flow_address_to_range <- function(df, cols) {
  df |>
    dplyr::mutate(
      dplyr::across(
        cols,
        list(
          range_ = ~ stringr::str_detect(.x, "^[0-9]+[A-Z]{0,2} *((1 \\/ 2)|\\.[0-9]|[A-Z]{1,2})?([ -]+[0-9]+[A-Z]{0,1} *((1 \\/ 2)|\\.[0-9])?)+ +(?=[A-Z0-9])"),
          num_init_ = ~ stringr::str_extract(.x, "^[0-9]+[A-Z]{0,2} *((1 \\/ 2)|\\.[0-9]|[A-Z]{1,2})?([ -]+(([0-9]+[A-Z]{0,1} *((1 \\/ 2)|\\.[0-9])?)*|[A-Z]))? +(?=[A-Z0-9])")
        )
      ),
      dplyr::across(
        cols,
        list(
          body = ~ stringr::str_remove(.x, get(paste0(dplyr::cur_column(), "_num_init_")))
        )
      ),
      dplyr::across(
        cols,
        list(
          start = ~ as.numeric(
            stringr::str_extract(
              stringr::str_replace(
                .x, 
                " 1 \\/ 2", 
                "\\.5"
              ),
              "^[0-9\\.]+")
          )
        )
      ),
      dplyr::across(
        cols,
        list(
          end_init_ = ~ dplyr::case_when(
            get(paste0(dplyr::cur_column(), "_range_")) ~ as.numeric(
              stringr::str_extract(
                stringr::str_replace(
                  .x, 
                  " 1 \\/ 2", 
                  "\\.5"
                ),
                "(?<=[- ])[0-9\\.]+"
              )
            ),
            .default = get(paste0(dplyr::cur_column(), "_start"))
          )
        )
      ),
      dplyr::across(
        cols,
        list(
          end = ~ dplyr::case_when(
            (get(paste0(dplyr::cur_column(), "_end_init_")) < get(paste0(dplyr::cur_column(), "_start"))) | is.na(get(paste0(dplyr::cur_column(), "_end_init_")))
            ~ get(paste0(dplyr::cur_column(), "_start")),
            .default = get(paste0(dplyr::cur_column(), "_end_init_"))
          )
        )
      ),
      dplyr::across(
        cols,
        list(
          even = ~ dplyr::case_when(
            floor(get(paste0(dplyr::cur_column(), "_start"))) %% 2 == 0 ~ TRUE,
            floor(get(paste0(dplyr::cur_column(), "_end"))) %% 2 == 1 ~ FALSE,
            .default = FALSE
          )
        )
      ),
      dplyr::across(
        cols,
        list(
          num_ = ~ dplyr::case_when(
            (get(paste0(dplyr::cur_column(), "_start")) == get(paste0(dplyr::cur_column(), "_end")))
            & get(paste0(dplyr::cur_column(), "_range_"))
            ~ stringr::str_c(get(paste0(dplyr::cur_column(), "_start"))),
            (get(paste0(dplyr::cur_column(), "_start")) != get(paste0(dplyr::cur_column(), "_end")))
            & get(paste0(dplyr::cur_column(), "_range_"))
            ~ stringr::str_c(get(paste0(dplyr::cur_column(), "_start")), get(paste0(dplyr::cur_column(), "_end")), sep="-"),
            .default = get(paste0(dplyr::cur_column(), "_num_init_"))
          ),
          parsed_ = ~ !is.na(get(paste0(dplyr::cur_column(), "_start"))) & !is.na(get(paste0(dplyr::cur_column(), "_end"))) & !is.na(get(paste0(dplyr::cur_column(), "_body")))
        )
      ),
      dplyr::across(
        cols,
        ~ dplyr::case_when(
          get(paste0(dplyr::cur_column(), "_parsed_")) ~ stringr::str_squish(stringr::str_c(get(paste0(dplyr::cur_column(), "_num_")), get(paste0(dplyr::cur_column(), "_body")), sep = " ")),
          .default = .x
        )
      )
    ) |>
    dplyr::select(-dplyr::ends_with("_"))
}

flow_assess_address_to_range <- function(df, site_id_col, site_addr_col, own_addr_col) {
  own_id_col <- stringr::str_replace(site_id_col, "site", "own")
  
  site_start <- paste0(site_addr_col, "_start")
  site_end <- paste0(site_addr_col, "_end")
  site_body <- paste0(site_addr_col, "_body")
  site_even <- paste0(site_addr_col, "_even")
  own_start <- paste0(own_addr_col, "_start")
  own_end <- paste0(own_addr_col, "_end")
  own_body <- paste0(own_addr_col, "_body")
  own_even <- paste0(own_addr_col, "_even")
  
  matched <- df |>
    dplyr::filter(!is.na(.data[[own_id_col]])) |>
    std_address_to_range(site_addr_col)
  
  df |>
    dplyr::filter(is.na(.data[[own_id_col]])) |>
    std_address_to_range(c(site_addr_col, own_addr_col)) |>
    dplyr::mutate(
      !!own_id_col := dplyr::case_when(
        (.data[[site_body]] == .data[[own_body]]) & 
          (dplyr::between(.data[[site_start]], .data[[own_start]], .data[[own_end]]) |
             dplyr::between(.data[[site_end]], .data[[own_start]], .data[[own_end]])) &
          .data[[site_even]] == .data[[own_even]]
        ~ .data[[site_id_col]],
        .default = NA_character_
      )
    ) |>
    dplyr::bind_rows(matched)
}

flow_postal <- function(df, col, fill_cols, zips, parcels, constraint) {
  df <- df |>
    std_zip_format(col, zips, constraint=constraint) |>
    dplyr::group_by(dplyr::across(fill_cols)) |>
    tidyr::fill(
      .data[[col]]
    ) |>
    dplyr::ungroup()
}


flow_assess_postal <- function(df, site_prefix, own_prefix, zips, parcels) {
  
  site_postal_col <- stringr::str_c(site_prefix, "_postal")
  site_loc_id <- stringr::str_c(site_prefix, "_loc_id")
  site_muni_id <- stringr::str_c(site_prefix, "_muni_id")
  
  own_postal_col <- stringr::str_c(own_prefix, "_postal")
  own_muni_col <- stringr::str_c(own_prefix, "_postal")
  own_loc_id <- stringr::str_c(own_prefix, "_loc_id")
  
  df <- df |>
    std_zip_format(site_postal_col, zips, constraint="ma") |>
    std_zip_format(own_postal_col, zips) |>
    dplyr::mutate(
      !!site_postal_col := dplyr::case_when(
        is.na(.data[[site_postal_col]]) & 
          !is.na(.data[[site_postal_col]]) & 
          !is.na(.data[[own_loc_id]]) ~ .data[[own_postal_col]],
        .default = .data[[site_postal_col]]
      )
    ) |>
    dplyr::group_by(dplyr::across(site_loc_id)) |>
    tidyr::fill(
      .data[[site_postal_col]]
    ) |>
    dplyr::ungroup() 
  
  df <- df |>
    dplyr::filter(is.na(.data[[site_postal_col]])) |>
    std_fill_ma_zip_sp(
      col=site_postal_col,
      site_loc_id=site_loc_id,
      site_muni_id=site_muni_id,
      parcels=parcels,
      zips=zips
    ) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(!is.na(.data[[site_postal_col]]))
    )
}

flow_assess_muni <- function(df, 
                             prefix, 
                             id_col,
                             zips,
                             places) {
  
  muni_col <- stringr::str_c(prefix, "_muni")
  postal_col <- stringr::str_c(prefix, "_postal")
  state_col <- stringr::str_c(prefix, "_state")
  loc_id <- stringr::str_c(prefix, "_state")
  
  df <- df |>
    dplyr::mutate(
      muni_match = is.na(.data[[loc_id]]) | !(.data[[muni_col]] %in% places$muni)
    )
  
  df |>
    dplyr::filter(!muni_match) |>
    std_remove_counties(
      muni_col,
      state_col=state_col,
      places=places
    ) |>
    std_muni_names(
      muni_col,
      state_col=state_col, 
      zip_col=postal_col,
      zips=zips
    )  |> 
    std_directions(muni_col) |>
    std_munis_by_places(
      muni_col,
      id_col=id_col,
      state_col=state_col,
      places=places,
      postal_col=postal_col,
      zips=zips
    ) |>
    std_fill_zip_by_muni(
      postal_col, 
      muni_col=muni_col, 
      zips=zips
    ) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(muni_match)
    ) |>
    dplyr::select(-muni_match)
}
