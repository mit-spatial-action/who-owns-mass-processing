source('R/standardizers.R')
source('R/deduplicaters.R')

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
      muni_id,
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

flow_address_text <- function(df, cols, rm_ma = TRUE, numbers = TRUE) {
  df <- df |>
    std_street_types(cols) |>
    std_directions(cols) |>
    std_frac_to_dec(cols) |>
    std_small_ordinals(cols) |>
    std_leading_zeros(cols, rmsingle = TRUE)
  
  if (numbers) {
    df <- df |>
      std_small_numbers(cols)
  }
  df |>
    std_fix_concatenated_ranges(cols) |>
    std_hyphenate_range(cols) |>
    std_massachusetts(cols, rm_ma = rm_ma) |>
    std_squish(cols)
}

flow_address_addr2 <- function(df, cols, prefixes=c(), po_pmb = FALSE) {
  
  if (po_pmb) {
    df <- df |> 
      std_addr2_po_pmb(cols)
  }
  
  df |>
    std_addr2_remove_keywords(cols) |>
    std_addr2_floor(cols) |>
    std_addr2_twr(cols) |>
    std_addr2_bldg(cols) |>
    std_addr2_and(cols) |>
    std_addr2_alpha_num(cols) |>
    std_addr2_num_alpha(cols) |>
    std_addr2_all_num(cols) |>
    std_addr2_alpha(cols) |>
    std_addr2_words(cols) |>
    std_squish(cols) |>
    std_col_prefixes(prefixes, parsed_cols=c("po", "pmb", "addr2"))
}

flow_address_to_range <- function(df, cols, prefixes=c()) {
  
  df <- df |>
    dplyr::mutate(
      dplyr::across(
        dplyr::all_of(cols),
        list(
          num_ = ~ stringr::str_extract(.x, "^[0-9\\.]+[A-Z]{0,2}([ \\-][0-9\\.]+[A-Z]{0,1})*(?= ([0-9]{1,3}(ST|ND|RD|TH)|[A-Z]))")
        )
      ),
      dplyr::across(
        dplyr::all_of(cols),
        list(
          body = ~ dplyr::case_when(
            !is.na(get(paste0(dplyr::cur_column(), "_num_"))) ~
              stringr::str_squish(stringr::str_remove(.x, get(paste0(dplyr::cur_column(), "_num_")))),
            .default = NA_character_
          )
        )
      ),
      dplyr::across(
        dplyr::all_of(cols),
        list(
          start = ~ dplyr::case_when(
            !is.na(get(paste0(dplyr::cur_column(), "_num_"))) ~ 
              as.numeric(stringr::str_extract(get(paste0(dplyr::cur_column(), "_num_")), "^[0-9\\.]+")),
            .default = NA
          )
        )
      ),
      dplyr::across(
        dplyr::all_of(cols),
        list(
          end_init_ = ~ dplyr::case_when(
            !is.na(get(paste0(dplyr::cur_column(), "_num_"))) ~ as.numeric(
              stringr::str_extract(
                get(paste0(dplyr::cur_column(), "_num_")), 
                "[0-9\\.]+(?=[A-Z]?$)"
                )
            ),
            .default = NA
          )
        )
      ),
      dplyr::across(
        dplyr::all_of(cols),
        list(
          end = ~ dplyr::case_when(
            (get(paste0(dplyr::cur_column(), "_end_init_")) > get(paste0(dplyr::cur_column(), "_start"))) | is.na(get(paste0(dplyr::cur_column(), "_end_init_")))
            ~ get(paste0(dplyr::cur_column(), "_end_init_")),
            .default = get(paste0(dplyr::cur_column(), "_start"))
          )
        )
      ),
      dplyr::across(
        dplyr::all_of(cols),
        list(
          even = ~ dplyr::case_when(
            floor(get(paste0(dplyr::cur_column(), "_start"))) %% 2 == 0 ~ TRUE,
            floor(get(paste0(dplyr::cur_column(), "_start"))) %% 2 == 1 ~ FALSE,
            .default = FALSE
          )
        )
      )
    ) |>
    dplyr::select(-dplyr::ends_with("_")) |>
    std_col_prefixes(prefixes=prefixes, parsed_cols=c("start", "end", "body", "even"))
}

flow_address_match_simp <- function(df, cols) {
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
          dplyr::all_of(cols),
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

flow_address_postal <- function(df, col, state_col, zips, state_constraint = "") {
  if (state_constraint != "") {
    df <- df |>
      dplyr::filter(.data[[state_col]] == state_constraint) |>
      std_zip_format(
        col, 
        state_col=state_col,
        zips=zips, 
        state_constraint=state_constraint
      ) |>
      dplyr::bind_rows(
        df |>
          dplyr::filter(.data[[state_col]] != state_constraint | is.na(.data[[state_col]])) |>
          std_zip_format(
            col,
            state_col=state_col,
            zips=zips
          )
      )
  } else {
    df <- df |>
      std_zip_format(
        col,
        state_col=state_col,
        zips=zips
      )
  }
  df
}

flow_address_muni <- function(df,
                      col, 
                      state_col,
                      postal_col,
                      places,
                      state_val = "MA") {
  
  df <- df |>
    std_directions(col) |>
    dplyr::mutate(
      !!col := stringr::str_remove(
        .data[[col]], 
        stringr::str_c(" ", state_val, "$")
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
  
  df |>
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

flow_address_to_address_seq <- function(a1, sites, addresses) {
  if ("sf" %in% class(sites)) {
    sites <- sites |>
      sf::st_drop_geometry()
  }
  
  if ("sf" %in% class(addresses)) {
    addresses <- addresses |>
      sf::st_drop_geometry()
  }
  
  if (!("loc_id" %in% names(a1))) {
    a1 <- a1 |>
      dplyr::mutate(
        loc_id = NA_character_
      )
  }
  
  a1 |>
    std_match_address_to_address(
      sites,
      fill_col="loc_id",
      body, muni, postal
    )  |>
    std_match_address_to_address(
      addresses,
      fill_col="loc_id",
      body, muni, postal
    )  |>
    std_match_address_to_address(
      addresses,
      fill_col="loc_id",
      body, muni, postal
    ) |>
    std_match_address_to_address(
      addresses |> dplyr::filter(unique_in_muni),
      fill_col=c("loc_id", "postal"),
      body, muni
    ) |>
    std_match_address_to_address(
      addresses |> dplyr::filter(unique_in_postal),
      fill_col=c("loc_id", "muni"),
      body, postal
    ) |>
    std_simp_street("body") |>
    std_match_address_to_address(
      addresses |> dplyr::filter(unique_in_muni_simp),
      fill_col=c("loc_id", "postal", "body"),
      body_simp, muni
    ) |>
    std_match_address_to_address(
      addresses |> dplyr::filter(unique_in_postal_simp),
      fill_col=c("loc_id", "postal", "body"),
      body_simp, postal
    ) |>
    dplyr::select(-body_simp)
}

flow_address <- function(df, col, postal_col, muni_col, state_col, zips, places, po_pmb = FALSE, state_constraint = "") {
  df |>
    flow_address_text(col) |>
    flow_address_addr2(
      col, 
      po_pmb=po_pmb
    ) |>
    flow_address_to_range(col) |>
    flow_address_postal(
      postal_col, 
      state_col=state_col, 
      zips, 
      state_constraint
    ) |>
    flow_address_muni(
      muni_col, 
      state_col=state_col, 
      postal_col=postal_col,
      places=places
    )
}

# Names ====

flow_name <- function(df, col, address_col, type="") {
  df <- df |>
    std_trailing_leading(c(col)) |>
    std_street_types(c(col)) |>
    std_mass_corp(c(col)) |>
    std_inst_types(c(col)) |>
    std_small_numbers(c(col))
  
  if (type == "mixed") {
    df <- df |> 
      std_flag_inst(c(col)) |>
      std_flag_trust(c(col)) |>
      dplyr::mutate(
        !!col := dplyr::case_when(
          trustees ~ stringr::str_remove(.data[[col]], "[\\-\\s](TRUSTEES|FOR LIFE)$"),
          trust ~ stringr::str_remove(.data[[col]], "^TRUSTEES (OF )?"),
          trust | trustees ~ stringr::str_remove(.data[[col]], "(?<=TRUST )TRUSTEES ?"),
          .default = .data[[col]]
        )
      )
  } else if (type == "inst") {
    df <- df |>
      dplyr::mutate(
        inst = TRUE,
        trust = FALSE,
        trustees = FALSE
      )
  } else if (type == "ind") {
    df <- df |>
      dplyr::mutate(
        inst = FALSE,
        trust = FALSE,
        trustees = FALSE
      )
  }
  
  inds <- df |>
    dplyr::filter(!inst & !trust) |>
    std_remove_titles(c(col)) |>
    std_multiname(col) |>
    std_remove_middle_initial(col, restrictive = FALSE)
  
  
  df |>
    dplyr::filter(inst | trust) |>
    std_massachusetts(c(col)) |>
    dplyr::bind_rows(inds) |>
    std_replace_blank(c(col)) |>
    std_squish(c(col))
}

flow_name_co_dba_attn <- function(df, col, target, clear_cols = c()) {
  df |>
    std_separate_and_label(
      col = col,
      target_col = target,
      regex = "(^(CO |C O ?))|( C O (?=[A-Z]+))",
      label = "co",
      clear_cols = clear_cols
    ) |>
    std_separate_and_label(
      col = col,
      target_col = target,
      regex = "(^(ATTN|A T T N) ?)|( (ATTN|A T T N) (?=[A-Z]+))",
      label = "attn",
      clear_cols = clear_cols
    ) |>
    std_separate_and_label(
      col = col,
      target_col = target,
      regex = "(^(DBA|D B A) ?)|( (DBA|D B A) (?=[A-Z]+))",
      label = "dba",
      clear_cols = clear_cols
    )
}

# OpenCorporates ====

flow_oc_fix_officer_addresses <- function(df) {
  parsed_or_none <- df |>
    dplyr::filter(is.na(addr) | !is.na(str)) |>
    dplyr::select(-addr) |>
    dplyr::rename(addr = str)
  
  df |>
    dplyr::filter(!is.na(addr) & is.na(str)) |>
    std_extract_zip("addr", "postal") |>
    std_street_types("addr") |>
    dplyr::mutate(
      addr = stringr::str_replace(addr, " [A-Z]{3}$", ""),
      addr = stringr::str_replace(addr, "(?<= [A-Z]{2}) [A-Z]{2}$", ""),
      state = stringr::str_extract(addr, "(?<= |^)[A-Z]{2}$"),
      addr = stringr::str_replace(addr, paste0("[ \\-]", state, "$"), ""),
      new_addr = std_extract_address_vector(addr, start = TRUE),
      addr = stringr::str_replace(addr, paste0("(?<= |^)", new_addr, "([ -]|$)"), ""),
      muni = stringr::str_extract(addr, "(?<= |^)[A-Z\\s]+$"),
      addr = stringr::str_replace(addr, paste0("(?<= |^)", muni, "([ -]|$)"), ""),
      addr = dplyr::case_when(
        !is.na(addr) & !is.na(new_addr) ~ stringr::str_c(new_addr, addr, sep=" "),
        !is.na(addr) | addr == "" ~ new_addr,
        .default = new_addr
      )
    ) |>
    dplyr::select(-c(new_addr, str)) |>
    dplyr::bind_rows(parsed_or_none)
}

flow_oc_generic <- function(df, zips, places, type) {
  
  df <- df |>
    dplyr::mutate(
      type = type
    ) |>
    flow_name_co_dba_attn(
      "addr",
      "name"
    )
  
  df <- df |>
    dplyr::filter(type == "co")  |>
    flow_address_text("name") |>
    flow_address_addr2("name", po_pmb=TRUE) |>
    std_extract_address(
      col="name",
      target_col="addr"
    ) |>
    std_assemble_addr(range = FALSE) |>
    dplyr::select(-c(pmb, po, addr2)) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(type != "co")
    ) |>
    dplyr::filter(!is.na(addr)) |>
    flow_address(
      "addr",
      postal_col="postal",
      muni_col="muni",
      state_col="state",
      zips=zips,
      places=places,
      po_pmb=TRUE,
      state_constraint = ""
    ) |>
    std_assemble_addr() |>
    dplyr::select(-c(pmb, po, addr2)) |>
    flow_name(
      "name",
      address_col="addr",
      type="mixed"
    )
}

flow_oc_officers <- function(df, zips, places, type_name="officer") {
  df <- df |>
    flow_generic_preprocess(
      c("addr", "name", "position", "str", "muni", "state", "postal", "country")
    ) |>
    flow_oc_fix_officer_addresses() |>
    flow_oc_generic(zips=zips, places=places, type=type_name)
  
  df |>
    dplyr::filter(!inst & !trust) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(inst | trust)
    ) |>
    dplyr::mutate(
      id = stringr::str_c(type_name, "-", dplyr::row_number())
    )
}

flow_oc_companies <- function(df, zips, places, type_name="company") {
  df <- df |>
    flow_generic_preprocess(
      c("name", "addr", "company_type", "muni", "state", "postal", "country"),
      id_cols = c("id")
    )
  
  df |>
    dplyr::rename(
      company_id = id
    ) |>
    flow_oc_generic(zips=zips, places=places, type=type_name) |>
    dplyr::mutate(
      id = stringr::str_c(type_name, "-", dplyr::row_number())
    )
}

# Assessors-Specific Workflows ====

flow_assess_split <- function(df, site_prefix, own_prefix) {
  sites <- df |>
    dplyr::select(
      dplyr::starts_with(site_prefix)
    ) |>
    dplyr::rename_with(
      ~ stringr::str_remove(.x, stringr::str_c(site_prefix, "_"))
    ) |>
    dplyr::select(-c(country))
  
  res_col <- stringr::str_c(site_prefix, "res", sep="_")
  
  owners <- df |> 
    dplyr::filter(.data[[res_col]]) |>
    dplyr::select(
      c(site_id, dplyr::starts_with(own_prefix))
    ) |>
    dplyr::rename_with(
      ~ stringr::str_remove(.x, stringr::str_c(own_prefix, "_"))
    ) |>
    dplyr::select(-c(country))
  
  owners <- owners |>
    dplyr::filter(!is.na(loc_id)) |>
    dplyr::select(site_id, loc_id, name) |>
    dplyr::left_join(
      sites |>
        dplyr::select(id, addr, start, end, body, even, muni, postal, state),
      by = dplyr::join_by(site_id == id)
    ) |>
    dplyr::bind_rows(
      owners |>
        dplyr::filter(is.na(loc_id))
    )

  list(
    sites = sites,
    owners = owners
  )
}

flow_assess_sites_condos <- function(df, luc_col, id_cols, units_col) {
  df <- df |>
    std_flag_condos(
      luc_col,
      id_cols=id_cols
    ) 
  
  df |>
    dplyr::filter(condo) |>
    dplyr::mutate(
      !!units_col := dplyr::case_when(
        .data[[luc_col]] %in% c("970", "908", "0xxR") ~ 1,
        .default = .data[[units_col]]
      )
    ) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(!condo)
    )
}

flow_assess_sites_units <- function(df, luc_col, addresses) {
  # Identify unit counts for unambiguous cases where there is one property
  # on a parcel and that property is single-, two-, or three-family.
  
  df <- df |> 
    std_units_from_luc(
      "luc",
      muni_id_col="muni_id",
      units_col="units"
    ) |>
    std_test_units(
      "units",
      muni_id_col="muni_id",
      luc_col = "luc"
    )
  
  df <- df |>
    dplyr::filter(!units_valid) |>
    std_estimate_units(
      "units", 
      luc_col="luc", 
      muni_id_col="muni_id",
      count_col="addr_count",
      addresses=addresses
      ) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(units_valid)
    ) |>
    dplyr::select(-c(units_valid))
}

flow_assess_sites <- function(df, addresses) {
  df |>
    flow_assess_sites_condos(
      luc_col="luc", 
      id_cols=c("loc_id", "body"), 
      units_col="units"
      ) |>
    flow_assess_sites_units(
      "luc",
      addresses
    )
}

flow_assess_owners <- function(df, name_col, address_col, type_name = "owners") {
  df |>
    dplyr::mutate(
      type = type_name
    ) |>
    flow_name_co_dba_attn(
      address_col,
      target=name_col
    ) |>
    flow_name(
      col=name_col,
      address_col=address_col,
      type="mixed"
    ) |>
    std_assemble_addr() |>
    dplyr::select(-c(addr2, po, pmb)) |>
    dplyr::mutate(
      id = stringr::str_c(type_name, "-", dplyr::row_number())
    )
}

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

flow_assess_address_addr2 <- function(df, site_prefix, own_prefix) {
  
  cols <- flow_assess_cols(df, site_prefix = site_prefix, own_prefix = own_prefix)
  
  matched <- df |>
    dplyr::filter(!is.na(get(cols$own$loc_id))) |>
    flow_address_addr2(
      cols$site$addr, 
      po_pmb = FALSE, 
      prefixes=c(site_prefix)
      )
  
  df |>
    dplyr::filter(is.na(get(cols$own$loc_id))) |>
    flow_address_addr2(
      c(cols$site$addr, cols$own$addr),
      prefixes=c(site_prefix, own_prefix),
      po_pmb = TRUE
      ) |>
    flow_address_match_simp(c(cols$site$addr, cols$own$addr)) |>
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

flow_assess_address_postal <- function(df, site_prefix, own_prefix, zips, parcels, state_constraint = "MA") {
  
  cols <- flow_assess_cols(df, site_prefix = site_prefix, own_prefix = own_prefix)
  
  df <- df |>
    flow_address_postal(
      cols$site$postal,
      state_col=cols$site$state, 
      zips=zips,
      state_constraint=state_constraint
      )
  
  df <- df |>
    dplyr::mutate(
      !!cols$own$state := dplyr::case_when(
        !is.na(get(cols$own$loc_id)) ~ state_constraint,
        .default = get(cols$own$state)
      )
    ) |>
    flow_address_postal(
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
    dplyr::group_by(dplyr::across(dplyr::all_of(cols$site$loc_id))) |>
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

flow_assess_address_muni <- function(df, own_prefix, places, zips) {
  cols <- flow_assess_cols(df, own_prefix = own_prefix)
  df |> 
    flow_address_muni(
      col = cols$own$muni,
      state_col = cols$own$state,
      postal_col = cols$own$postal,
      places=places
    )
}

flow_assess_luc <- function(df, path=DATA_PATH) {
  df |>
    std_luc(
      "site_use_code",
      muni_id_col="site_muni_id", 
      name="site_luc",
      path=path) |>
    std_flag_residential(
      "site_luc", 
      muni_id_col="site_muni_id", 
      name="site_res")
}

flow_assess <- function(df, 
                        site_prefix,
                        own_prefix,
                        zips,
                        parcels,
                        places,
                        state_constraint) {
  util_log_message("Processing assessors table.")
  df |>
    flow_assess_address_text(
      site_prefix = site_prefix,
      own_prefix = own_prefix
    ) |>
    flow_assess_address_addr2(
      site_prefix = site_prefix,
      own_prefix = own_prefix
    ) |>
    flow_assess_address_to_range(
      site_prefix = site_prefix,
      own_prefix = own_prefix
    ) |>
    flow_assess_address_postal(
      site_prefix=site_prefix,
      own_prefix=own_prefix,
      zips=zips,
      parcels=parcels,
      state_constraint=state_constraint
    ) |>
    flow_assess_address_muni(
      own_prefix=own_prefix,
      places=places,
      zips=zips
    ) |>
    flow_assess_luc()
}

# Omnibus Data Process ====

flow_process_all <- function(assess,
                             companies,
                             officers,
                             addresses,
                             zips,
                             parcels,
                             places) {
  assess <- assess |>
    flow_assess(
      site_prefix="site",
      own_prefix="own",
      zips=zips,
      parcels=parcels,
      places=places,
      state_constraint="MA"
    )
  
  flow_assess_split(
    assess,
    site_prefix="site",
    own_prefix="own"
    ) |>
    wrapr::unpack(
      sites <- sites,
      owners <- owners
    )
  
  sites <- sites |>
    flow_assess_sites(
      addresses=addresses
    )

  owners <- owners |>
    flow_assess_owners(
      name_col="name",
      address_col="addr"
    )
  
  companies <- companies |>
    flow_oc_companies(
      zips=zips,
      places=places
    )
  
  officers <- officers |>
    flow_oc_officers(
      zips=zips,
      places=places
    )

  list(
    sites = sites,
    owners = owners,
    companies = companies,
    officers = officers
  )
}
