# Helpers ====

flow_add_to_col_list <- function(col_list, prefixes, new_cols) {
  for (prefix in prefixes) {
    cols <- as.list(paste(prefix, new_cols, sep="_"))
    names(cols) <- new_cols
    col_list[[prefix]] <- append(col_list[[prefix]], cols)
  }
  col_list
}

flow_cols_to_list <- function(df, prefix) {
  cols <- df |> 
    dplyr::select(dplyr::starts_with(prefix)) |>
    names() |>
    as.list()
  names(cols) <- stringr::str_remove(cols, paste0(prefix, "_"))
  cols
}

flow_assess_cols <- function(
    df,
    site_prefix = NULL, 
    own_prefix = NULL) {
  l <- list()
  if (!is.null(site_prefix)) {
    l[[site_prefix]] = flow_cols_to_list(df, site_prefix)
  }
  if (!is.null(own_prefix)) {
    l[[own_prefix]] = flow_cols_to_list(df, own_prefix)
  }
  l
}

# Preprocessors ====

flow_generic_preprocess <- function(df, cols, id_cols) {
  if(!missing(id_cols)) {
    df <- df |>
      dplyr::filter(
        dplyr::if_all({{id_cols}}, ~ !is.na(.))
        ) |>
      dplyr::group_by(dplyr::across(dplyr::all_of(id_cols))) |>
      dplyr::filter(dplyr::n() == 1) |>
      dplyr::ungroup()
  }
  df |>
    std_leading_zeros(cols, rmsingle = FALSE) |>
    std_uppercase(cols) |>
    std_replace_blank(cols) |>
    std_remove_special(cols) |>
    std_spacing_characters(cols) |>
    std_squish(cols)
}

flow_boston_address_preprocess <- function(df, cols, bos_id='035') {
  df <- df |>
    dplyr::filter(!is.na(street_body) & !is.na(street_full_suffix)) |>
    dplyr::mutate(
      muni = "BOSTON",
      muni_id = bos_id,
      is_range = as.logical(is_range),
      body = stringr::str_to_upper(stringr::str_c(street_body, street_full_suffix, sep = " ")),
      state = "MA",
    ) |>
    dplyr::select(
      addr2 = unit,
      body,
      state,
      muni,
      postal = zip_code,
      num = street_number,
      start = range_from,
      end = range_to,
      is_range
    ) |>
    flow_generic_preprocess(cols) |>
    dplyr::mutate(
      dplyr::across(
        c(start, end),
        ~ stringr::str_replace_all(
          .,
          " 1 ?\\/ ?2", "\\.5"
        )
      ),
      dplyr::across(
        c(start, end),
        ~ as.numeric(stringr::str_remove_all(., "[A-Z]"))
      ),
      range_fix = !is_range & stringr::str_detect(num, "[0-9\\.]+ ?[A-Z]{0,2} ?- ?[0-9\\.]+ ?[A-Z]{0,1}"),
      start_temp = dplyr::case_when(
        range_fix ~ abs(as.numeric(stringr::str_remove_all(stringr::str_extract(num, "^[0-9\\.]+"), "[A-Z]")))
      ),
      end_temp = dplyr::case_when(
        range_fix ~ abs(as.numeric(stringr::str_remove_all(stringr::str_extract(num, "(?<=[- ]{1,2})[0-9\\.]+ ?[A-Z]{0,1}(?= ?$)"), "[A-Z]")))
      ),
      is_range = dplyr::case_when(
        end_temp > start_temp ~ TRUE,
        .default = is_range
      ),
      start = dplyr::case_when(
        is_range & !(start > 0) ~ start_temp,
        .default = start
      ),
      end = dplyr::case_when(
        is_range & !(end > 0) ~ end_temp,
        .default = end
      ),
      dplyr::across(
        c(start, end),
        ~ dplyr::case_when(
          is.na(start) & !is_range ~ abs(as.numeric(stringr::str_remove_all(num, "[A-Z]"))),
          .default = .
        )
      )
    ) |>
    dplyr::filter(!is.na(start) & !is.na(end)) |>
    dplyr::select(-c(is_range, num, range_fix, start_temp, end_temp))
}

flow_nb_address_preprocess <- function(df, cols, munis) {
  df |>
    dplyr::filter(!is.na(streetname)) |>
    dplyr::mutate(
      state = "MA",
      start = dplyr::case_when(
        num1_sfx == "1/2" ~ num1 + 0.5,
        .default = num1
      ),
      end = dplyr::case_when(
        num2_sfx == "1/2" ~ num2 + 0.5,
        .default = num2
      ),
      muni_id = std_pad_muni_ids(addrtwn_id)
    ) |>
    dplyr::left_join(
      munis,
      dplyr::join_by(muni_id)
    ) |>
    dplyr::select(
      body = streetname,
      addr2 = unit,
      state,
      muni,
      postal = zipcode,
      start,
      end,
      muni_id
    ) |>
    flow_generic_preprocess(cols) |>
    dplyr::mutate(
      end = dplyr::case_when(
        end == 0 ~ start,
        end <= start ~ start,
        .default = end
      )
    )
}

flow_assess_preprocess <- function(df, path) {
  util_log_message(glue::glue("Preprocessing Assessors' Tables."))
  df <- df |>
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
    )  |>
    flow_generic_preprocess(
      c("site_addr", "site_muni", "site_postal", 
        "own_name",  "own_addr", "own_muni", 
        "own_postal", "own_state", "own_country"), 
      id_cols = c("site_id", "site_muni_id")
      ) |>
    tidyr::replace_na(list(site_units = 0)) |> 
    dplyr::group_by(site_addr, site_muni, site_postal) |>
    tidyr::fill(
      site_loc_id
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(!is.na(site_loc_id))
}

flow_officers_preprocess <- function(df) {
  # df |>
}



# Addresses ====

flow_address_text <- function(df, cols, rm_ma = TRUE) {
  df |>
    std_street_types(cols) |>
    std_directions(cols) |>
    std_frac_to_dec(cols) |>
    std_small_ordinals(cols) |>
    std_leading_zeros(cols, rmsingle = TRUE) |>
    std_hyphenate_range(cols) |>
    std_massachusetts(cols, rm_ma = rm_ma) |>
    std_squish(cols)
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
    std_addr2_alpha_num(cols) |>
    std_addr2_num_alpha(cols) |> 
    std_addr2_all_num(cols) |> 
    std_addr2_alpha(cols) |>
    std_addr2_words(cols) |>
    std_squish(cols)
}

flow_address_to_range <- function(df, cols, prefixes) {
  
  if (missing(prefixes)) {
    prefixes <- cols
  }
  
  parsed_cols <- c("start", "end", "body", "even")
  
  df <- df |>
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
            floor(get(paste0(dplyr::cur_column(), "_start"))) %% 2 == 1 ~ FALSE,
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
          )
        )
      ),
      dplyr::across(
        cols,
        ~ dplyr::case_when(
          !is.na(get(paste0(dplyr::cur_column(), "_num_"))) & !is.na(get(paste0(dplyr::cur_column(), "_body"))) ~ stringr::str_squish(stringr::str_c(get(paste0(dplyr::cur_column(), "_num_")), get(paste0(dplyr::cur_column(), "_body")), sep = " ")),
          .default = .x
        )
      )
    ) |>
    dplyr::select(-dplyr::ends_with("_")) |>
    dplyr::rename_with(
      ~ stringr::str_replace(
        .x, 
        stringr::str_c("(?<=(", stringr::str_c(prefixes, collapse="|"), "))[a-z\\_]+(?=_(", stringr::str_c(parsed_cols, collapse="|"), ")$)"),
        ""
      )
    )
}

flow_match_simp_street <- function(df, cols) {
  #' Standardize street types.
  #'
  #' @param df A dataframe.
  #' @param cols Column or columns to be processed.
  #' @param fill Whether one should be used to fill the other.
  #' @returns A dataframe.
  #' @export
  df <- df |>
    std_simp_street(cols)
  if (length(cols) == 2) {
    df <- df |>
      dplyr::mutate(
        first_longer = nchar(.data[[cols[1]]]) > nchar(.data[[cols[2]]]),
        match = .data[[paste0(cols[1], "_simp")]] == .data[[paste0(cols[2], "_simp")]],
        dplyr::across(
          cols,
          ~ dplyr::case_when(
            !first_longer & match ~ .data[[cols[2]]],
            first_longer & match ~ .data[[cols[1]]],
            .default = .x,
          )
        )
      ) |> 
      dplyr::select(-c(first_longer, match))
  }
  df |>
    dplyr::select(-paste0(cols, "_simp"))
}

flow_postal <- function(df, col, state_col, zips, state_val = "MA") {
  if(missing(state_col)) {
    df <- df |>
      std_zip_format(
        col,
        zips=zips
        )
  } else {
    df |>
      dplyr::filter(.data[[state_col]] == state_val) |>
      std_zip_format(
        col, 
        state_col=state_col,
        zips=zips, 
        constraint="MA"
        ) |>
      dplyr::bind_rows(
        df |>
          dplyr::filter(.data[[state_col]] != state_val | is.na(.data[[state_col]])) |>
          std_zip_format(
            col,
            state_col=state_col,
            zips=zips
            )
      )
  }
}

flow_muni <- function(df,
                      col, 
                      state_col,
                      postal_col,
                      zips,
                      places,
                      state_val = "MA") {
  state_zips <- zips |>
    dplyr::filter(unambig_state == state_val) |> 
    dplyr::pull(zip) |>
    unique()
  
  df <- df |>
    std_directions(col) |>
    dplyr::mutate(
      !!col := stringr::str_remove(
        .data[[col]], 
        stringr::str_c("(^| )", state_val, "$")
      ),
      temp_postal := stringr::str_extract(
        .data[[col]], 
        "(?<=[A-Z ^])[0-9]{5}(?=[A-Z $])"
      ),
      !!postal_col := dplyr::case_when(
        !is.na(temp_postal) & is.na(.data[[postal_col]]) ~ temp_postal,
        .default = .data[[postal_col]]
      ),
      !!col := stringr::str_remove_all(.data[[col]], " ?[0-9]+ ?"),
      state_unmatched = !(get(col) %in% places$muni) &
        get(state_col) == state_val
    ) |>
    dplyr::select(-temp_postal) |>
    std_replace_blank(col)
  
  df <- df |>
    dplyr::filter(state_unmatched) |>
    std_remove_counties(
      col,
      state_col=state_col,
      places=places
    ) |>
    std_muni_names(
      col,
      mass = TRUE
    ) |>
    std_munis_by_places(
      col,
      state_col=state_col,
      postal_col=postal_col,
      places=places
    ) |>
    dplyr::bind_rows(
      df |> 
        dplyr::filter(!state_unmatched | is.na(state_unmatched)) |>
        std_muni_names(
          col,
          mass = FALSE
        )
    ) |>
    dplyr::select(-state_unmatched)
}


flow_address_omni <- function(df, col, postal_col, muni_col, state_col, zips, places, po_pmb = FALSE, state_val = "MA") {
  df |>
    flow_address_text(col) |>
    flow_addr2(col, po_pmb) |>
    flow_address_to_range(col) |>
    flow_postal(
      postal_col, 
      state_col=state_col, 
      zips, 
      state_val
    ) |>
    flow_muni(
      muni_col, 
      state_col=state_col, 
      postal_col=postal_col, 
      zips=zips, 
      places=places
    )
}

# Assessors-Specific Workflows ====

flow_assess_address_text <- function(df, site_prefix, own_prefix) {
  
  cols <- flow_assess_cols(df, site_prefix = site_prefix, own_prefix = own_prefix)
  
  cols$own$loc_id <- stringr::str_replace(cols$site$loc_id, site_prefix, own_prefix)
  
  df <- df |>
    dplyr::mutate(
      !!cols$own$loc_id := dplyr::case_when(
        get(cols$site$addr) == get(cols$own$addr) ~ get(cols$site$loc_id),
        .default = NA_character_
      )
    )
  
  matched <- df |>
    dplyr::filter(!is.na(get(cols$own$loc_id))) |>
    flow_address_text(cols$site$addr)
  
  
  df |>
    dplyr::filter(is.na(get(cols$own$loc_id))) |>
    flow_address_text(c(cols$site$addr, cols$own$addr)) |>
    dplyr::mutate(
      !!cols$own$loc_id := dplyr::case_when(
        get(cols$site$addr) == get(cols$own$addr) ~ get(cols$site$loc_id),
        .default = get(cols$own$loc_id)
      )
    ) |>
    dplyr::bind_rows(matched)
}

flow_assess_addr2 <- function(df, site_prefix, own_prefix) {
  
  cols <- flow_assess_cols(df, site_prefix = site_prefix, own_prefix = own_prefix)
  
  matched <- df |>
    dplyr::filter(!is.na(get(cols$own$loc_id))) |>
    flow_addr2(cols$site$addr, po_pmb = FALSE)
  
  df |>
    dplyr::filter(is.na(get(cols$own$loc_id))) |>
    flow_addr2(c(cols$site$addr, cols$own$addr), po_pmb = TRUE) |>
    flow_match_simp_street(c(cols$site$addr, cols$own$addr)) |>
    dplyr::mutate(
      !!cols$own$loc_id := dplyr::case_when(
        get(cols$site$addr) == get(cols$own$addr) ~ get(cols$site$loc_id),
        .default = get(cols$own$loc_id)
      )
    ) |>
    dplyr::bind_rows(matched)
}


flow_assess_address_to_range <- function(df, site_prefix, own_prefix) {
  
  cols <- flow_assess_cols(df, site_prefix = site_prefix, own_prefix = own_prefix)
  
  new_cols <- c("start", "end", "body", "even")
  cols <- flow_add_to_col_list(cols,  c(own_prefix, site_prefix), c("start", "end", "body", "even"))
  
  matched <- df |>
    dplyr::filter(!is.na(.data[[cols$own$loc_id]])) |>
    flow_address_to_range(cols$site$addr, prefix=site_prefix)
  
  df |>
    dplyr::filter(is.na(.data[[cols$own$loc_id]])) |>
    flow_address_to_range(c(cols$site$addr, cols$own$addr), prefixes=c(site_prefix, own_prefix)) |>
    dplyr::mutate(
      !!cols$own$loc_id := dplyr::case_when(
        (get(cols$site$body) == get(cols$own$body)) &
          (dplyr::between(get(cols$site$start), get(cols$own$start), get(cols$own$end)) |
             dplyr::between(get(cols$site$end), get(cols$own$start), get(cols$own$end))) &
          get(cols$site$even) == get(cols$own$even)
        ~ get(cols$site$loc_id),
        .default = NA_character_
      )
    ) |>
    dplyr::bind_rows(matched)
}

flow_assess_postal <- function(df, site_prefix, own_prefix, zips, parcels) {
  
  cols <- flow_assess_cols(df, site_prefix = site_prefix, own_prefix = own_prefix)
  
  df <- df |>
    flow_postal(
      cols$site$postal,
      state_col=cols$site$state, 
      zips=zips
      )
  
  df <- df |>
    dplyr::mutate(
      !!cols$own$state := dplyr::case_when(
        !is.na(get(cols$own$loc_id)) ~ "MA",
        .default = get(cols$own$state)
      )
    ) |>
    flow_postal(
      cols$own$postal,
      state_col=cols$own$state, 
      zips=zips
      ) |>
    dplyr::mutate(
      !!cols$site$postal := dplyr::case_when(
        is.na(get(cols$site$postal)) &
          !is.na(get(cols$own$postal)) &
          !is.na(get(cols$own$loc_id)) ~ get(cols$own$postal),
        .default = get(cols$site$postal)
      )
    ) |>
    dplyr::group_by(dplyr::across(cols$site$loc_id)) |>
    tidyr::fill(
      dplyr::all_of(cols$site$postal)
    ) |>
    dplyr::ungroup()

  df |>
    dplyr::filter(is.na(get(cols$site$postal))) |>
    std_fill_ma_zip_sp(
      col=cols$site$postal,
      site_loc_id=cols$site$loc_id,
      site_muni_id=cols$site$muni_id,
      parcels=parcels,
      zips=zips
    ) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(!is.na(get(cols$site$postal)))
    )
}

flow_assess_muni <- function(df, own_prefix, places, zips) {
  cols <- flow_assess_cols(df, own_prefix = own_prefix)
  df |> 
    flow_muni(
      col = cols$own$muni,
      state_col = cols$own$state,
      postal_col = cols$own$postal,
      places=places,
      zips=zips
    )
}
