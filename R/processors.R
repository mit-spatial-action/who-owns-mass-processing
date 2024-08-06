source('R/standardizers.R')
source('R/loaders.R')
source("R/utilities.R")

# Helpers ====

proc_add_to_col_list <- function(col_list, prefixes, new_cols) {
  for (prefix in prefixes) {
    cols <- as.list(paste(prefix, new_cols, sep="_"))
    names(cols) <- new_cols
    col_list[[prefix]] <- append(col_list[[prefix]], cols)
  }
  col_list
}

proc_cols_to_list <- function(df, prefix) {
  cols <- df |> 
    dplyr::select(dplyr::starts_with(prefix)) |>
    names() |>
    as.list()
  names(cols) <- stringr::str_remove(cols, paste0(prefix, "_"))
  cols
}

proc_assess_cols <- function(
    df,
    site_prefix = NULL, 
    own_prefix = NULL) {
  l <- list()
  if (!is.null(site_prefix)) {
    l[[site_prefix]] = proc_cols_to_list(df, site_prefix)
  }
  if (!is.null(own_prefix)) {
    l[[own_prefix]] = proc_cols_to_list(df, own_prefix)
  }
  l
}

# Addresses ====

proc_address_text <- function(df, cols, rm_ma = TRUE, numbers = TRUE) {
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

proc_address_addr2 <- function(df, cols, prefixes=c(), po_pmb = FALSE) {
  
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

proc_address_to_range <- function(df, cols, prefixes=c()) {
  
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

proc_address_match_simp <- function(df, cols) {
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

proc_address_postal <- function(df, col, state_col, muni_col, zips, state_constraint = "") {
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
  df |>
    std_fill_zip_by_muni(
      col, 
      muni_col=muni_col, 
      zips
    ) |>
    std_fill_muni_by_zip(
      muni_col, 
      postal_col=col, 
      state_col=state_col,
      zips
    ) |>
    std_fill_state_by_zip(
      state_col,
      postal_col=col, 
      zips
    )
}

proc_address_muni <- function(df,
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

proc_address_to_address_seq <- function(a1, sites, addresses) {
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

proc_address <- function(df, col, postal_col, muni_col, state_col, zips, places, po_pmb = FALSE, state_constraint = "") {
  df |>
    proc_address_text(col) |>
    proc_address_addr2(
      col, 
      po_pmb=po_pmb
    ) |>
    proc_address_to_range(col) |>
    proc_address_postal(
      postal_col, 
      state_col=state_col, 
      muni_col=muni_col,
      zips, 
      state_constraint
    ) |>
    proc_address_muni(
      muni_col, 
      state_col=state_col, 
      postal_col=postal_col,
      places=places
    )
}

# Names ====

proc_name <- function(df, col, multiname = TRUE, type="") {
  df <- df |>
    std_trailing_leading(c(col)) |>
    std_street_types(c(col)) |>
    std_inst_types(c(col))
  
  if (type == "company") {
    df <- df |>
      dplyr::mutate(
        inst = TRUE,
        trust = FALSE,
        trustees = FALSE
      )
  } else {
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
  }
  
  inds <- df |>
    dplyr::filter(!inst & !trust) |>
    # This also removes roman numerals.
    std_remove_titles(c(col))
    
  if (multiname) {
    inds <- inds |>
      std_multiname(col) 
  }
  inds <- inds |>
    std_remove_middle_initial(col, restrictive = FALSE)
  
  
  df |>
    dplyr::filter(inst | trust) |>
    std_mass_corp(c(col)) |>
    std_small_numbers(c(col)) |>
    std_massachusetts(c(col)) |>
    dplyr::bind_rows(inds) |>
    std_replace_blank(c(col)) |>
    std_squish(c(col))
}

proc_name_co_dba_attn <- function(df, col, target, clear_cols = c(), retain = TRUE) {
  df |>
    std_separate_and_label(
      col = col,
      target_col = target,
      regex = "(^(CO |C O ?))|( C O (?=[A-Z]+))",
      label = "co",
      clear_cols = c("addr", "muni", "state", "postal", "country", "body", "start", "end", "even"),
      retain = retain
    ) |>
    std_separate_and_label(
      col = col,
      target_col = target,
      regex = "(^(ATTN|A T T N) ?)|( (ATTN|A T T N) (?=[A-Z]+))",
      label = "attn",
      clear_cols = clear_cols,
      retain = FALSE
    ) |>
    std_separate_and_label(
      col = col,
      target_col = target,
      regex = "(^(DBA|D B A) ?)|( (DBA|D B A) (?=[A-Z]+))",
      label = "dba",
      clear_cols = clear_cols,
      retain = FALSE
    ) |>
    std_separate_and_label(
      col = col,
      target_col = target,
      regex = "(^(FBO|F B O) ?)|( (FBO|F B O) (?=[A-Z]+))",
      label = "fbo",
      clear_cols = clear_cols,
      retain = FALSE
    )
}

# OpenCorporates ====

proc_oc_generic <- function(df, zips, places, type, retain= TRUE, quiet=FALSE) {
  if(!quiet) {
    util_log_message(glue::glue("PROCESSING: Parsing {type} C/O, DBA, ATTN:, etc."))
  }
  df <- df |>
    dplyr::mutate(
      type = type
    ) |>
    proc_name_co_dba_attn(
      "addr",
      "name",
      retain = retain
    ) |>
    proc_name_co_dba_attn(
      "name",
      "name",
      retain = retain
    )
  
  if (retain) {
    df <- df |>
      dplyr::filter(type == "co")  |>
      proc_address_text("name") |>
      proc_address_addr2("name", po_pmb=TRUE) |>
      std_extract_address(
        col="name",
        target_col="addr"
      ) |>
      std_assemble_addr(range = FALSE) |>
      dplyr::select(-c(pmb, po, addr2)) |>
      dplyr::bind_rows(
        df |>
          dplyr::filter(type != "co")
      )
  }
  
  if(!quiet) {
    util_log_message(glue::glue("PROCESSING: Standardizing {type} addresses."))
  }
  df <- df |>
    dplyr::filter(!is.na(addr)) |>
    proc_address(
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
    dplyr::bind_rows(
      df |>
        dplyr::filter(is.na(addr))
    )
  
  if(!quiet) {
    util_log_message(glue::glue("PROCESSING: Standardizing {type} names."))
  }
  
  df |>
    dplyr::filter(!is.na(name)) |>
    proc_name(
      "name",
      multiname = FALSE,
      type=type
    ) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(is.na(name))
    ) |>
    tibble::rowid_to_column("id")
}

proc_oc_officers <- function(df, zips, places, type_name="officer", quiet = FALSE) {
  if(!quiet) {
    util_log_message("BEGIN OPENCORPORATES OFFICERS SEQUENCE", header=TRUE)
  }
  df <- df |>
    dplyr::select(-id) |>
    proc_oc_generic(zips=zips, places=places, type=type_name, quiet=quiet) |>
    dplyr::filter(!is.na(name)) |>
    dplyr::distinct(dplyr::pick(-c(id, type)), .keep_all = TRUE) |>
    dplyr::mutate(
      position = dplyr::case_when(
        type == "co" ~ "C/O",
        .default = position
      ),
      type = type_name
    ) |>
    dplyr::select(-id) |>
    tibble::rowid_to_column("id")
}

proc_oc_companies <- function(df, zips, places, type_name="company", quiet = FALSE) {
  if(!quiet) {
    util_log_message("BEGIN OPENCORPORATES COMPANIES SEQUENCE", header=TRUE)
  }
  
  df |>
    proc_oc_generic(zips=zips, places=places, type=type_name, quiet=quiet, retain = FALSE) |>
    dplyr::filter(!is.na(name)) |>
    dplyr::select(-id) |>
    tibble::rowid_to_column("id")
}

# Parcels ====

proc_parcels_to_dry_points <- function(parcels, hydro, crs, quiet = FALSE) {
  
  if(!quiet) {
    util_log_message("PROCESSING: Relocating points that lie within water bodies.")
  }
  
  if(missing(hydro)) {
    hydro <- load_ma_hydro(crs, quiet=quiet)
  }
  
  points <- parcels |>
    sf::st_set_agr("constant") |>
    sf::st_point_on_surface()
  
  wet_points <- points |>
    sf::st_filter(hydro, .predicate = sf::st_intersects)
  
  wet_polys <- parcels |>
    dplyr::mutate(
      wet = loc_id %in% wet_points$loc_id
    ) |>
    dplyr::filter(wet)
  
  dry_points <- wet_polys |>
    sf::st_set_agr("constant") |>
    sf::st_difference(
      hydro |>
        sf::st_set_agr("constant") |>
        sf::st_union()
      ) |>
    sf::st_set_agr("constant") |>
    sf::st_point_on_surface()
  
  points |>
    dplyr::filter(
      !(loc_id %in% wet_points$loc_id)
    ) |>
    dplyr::bind_rows(
      dry_points,
      wet_points |>
        dplyr::filter(!(loc_id %in% dry_points$loc_id))
    )
}



# Assessor's Database ====

proc_assess_split <- function(df, site_prefix, own_prefix, quiet = FALSE) {
  if(!quiet) {
    util_log_message("PROCESSING: Splitting assessors table into sites and owners.")
  }
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
      c(site_id, site_muni_id, dplyr::starts_with(own_prefix))
    ) |>
    dplyr::rename_with(
      ~ stringr::str_remove(.x, stringr::str_c(own_prefix, "_"))
    ) |>
    dplyr::select(-c(country))
  
  owners <- owners |>
    dplyr::filter(!is.na(loc_id)) |>
    dplyr::select(site_id, site_muni_id, loc_id, name) |>
    dplyr::left_join(
      sites |>
        dplyr::select(id, muni_id, addr, start, end, body, even, muni, postal, state),
      by = dplyr::join_by(site_id == id, site_muni_id == muni_id),
      multiple="any",
      na_matches="never"
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

proc_assess_sites_condos <- function(df, luc_col, id_cols, units_col) {
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

proc_assess_sites_units <- function(df, luc_col, addresses) {
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

proc_assess_sites <- function(df, addresses, quiet=FALSE) {
  if(!quiet) {
    util_log_message("PROCESSING: Standardizing land uses and estimating unit counts.")
  }
  df |>
    proc_assess_sites_condos(
      luc_col="luc", 
      id_cols=c("loc_id", "body"), 
      units_col="units"
      ) |>
    proc_assess_sites_units(
      "luc",
      addresses
    )
}

proc_assess_owners <- function(df, name_col, address_col, type = "owners", quiet=FALSE) {
  if(!quiet) {
    util_log_message("PROCESSING: Standardizing owner names and addresses.")
  }
  
  df <- df |>
    dplyr::mutate(
      type = type
    )
  df <- df |>
    proc_name_co_dba_attn(
      address_col,
      target=name_col
    )
  df <- df |>
    proc_name_co_dba_attn(
      name_col,
      target=name_col
    )
  df <- df |>
    proc_name(
      col=name_col
    )
  df |>
    std_assemble_addr() |>
    dplyr::select(-c(addr2, po, pmb)) |>
    tibble::rowid_to_column("id")
}

proc_assess_address_text <- function(df, site_prefix, own_prefix, quiet = FALSE) {
  
  if(!quiet) {
    util_log_message("PROCESSING: Standardizing address text.")
  }
  
  cols <- proc_assess_cols(df, site_prefix = site_prefix, own_prefix = own_prefix)
  
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
    proc_address_text(cols$site$addr)
  
  
  df |>
    dplyr::filter(is.na(get(cols$own$loc_id))) |>
    proc_address_text(c(cols$site$addr, cols$own$addr)) |>
    dplyr::mutate(
      !!cols$own$loc_id := dplyr::case_when(
        get(cols$site$addr) == get(cols$own$addr) ~ get(cols$site$loc_id),
        .default = get(cols$own$loc_id)
      )
    ) |>
    dplyr::bind_rows(matched)
}

proc_assess_address_addr2 <- function(df, site_prefix, own_prefix, quiet = FALSE) {
  
  if(!quiet) {
    util_log_message("PROCESSING: Standardizing address second lines and PO Boxes.")
  }
  
  cols <- proc_assess_cols(df, site_prefix = site_prefix, own_prefix = own_prefix)
  
  matched <- df |>
    dplyr::filter(!is.na(get(cols$own$loc_id))) |>
    proc_address_addr2(
      cols$site$addr, 
      po_pmb = FALSE, 
      prefixes=c(site_prefix)
      )
  
  df |>
    dplyr::filter(is.na(get(cols$own$loc_id))) |>
    proc_address_addr2(
      c(cols$site$addr, cols$own$addr),
      prefixes=c(site_prefix, own_prefix),
      po_pmb = TRUE
      ) |>
    proc_address_match_simp(c(cols$site$addr, cols$own$addr)) |>
    dplyr::mutate(
      !!cols$own$loc_id := dplyr::case_when(
        get(cols$site$addr) == get(cols$own$addr) ~ get(cols$site$loc_id),
        .default = get(cols$own$loc_id)
      )
    ) |>
    dplyr::bind_rows(matched)
}


proc_assess_address_to_range <- function(df, site_prefix, own_prefix, quiet = FALSE) {
  
  if(!quiet) {
    util_log_message("PROCESSING: Parsing address ranges.")
  }
  
  cols <- proc_assess_cols(df, site_prefix = site_prefix, own_prefix = own_prefix)
  
  new_cols <- c("start", "end", "body", "even")
  cols <- proc_add_to_col_list(cols,  c(own_prefix, site_prefix), c("start", "end", "body", "even"))
  
  matched <- df |>
    dplyr::filter(!is.na(.data[[cols$own$loc_id]])) |>
    proc_address_to_range(cols$site$addr, prefix=site_prefix)
  
  df |>
    dplyr::filter(is.na(.data[[cols$own$loc_id]])) |>
    proc_address_to_range(c(cols$site$addr, cols$own$addr), prefixes=c(site_prefix, own_prefix)) |>
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

proc_assess_address_postal <- function(df, site_prefix, own_prefix, zips, parcels_point, state_constraint = "MA", quiet = FALSE) {
  
  if(!quiet) {
    util_log_message("PROCESSING: Standardizing postal codes.")
  }
  
  cols <- proc_assess_cols(df, site_prefix = site_prefix, own_prefix = own_prefix)
  
  df <- df |>
    proc_address_postal(
      cols$site$postal,
      state_col=cols$site$state, 
      muni_col=cols$site$muni, 
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
    proc_address_postal(
      cols$own$postal,
      state_col=cols$own$state, 
      muni_col=cols$own$muni, 
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
      parcels_point=parcels_point,
      zips=zips
    ) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(!is.na(get(cols$site$postal)))
    )
}

proc_assess_address_muni <- function(df, own_prefix, places, zips, quiet = FALSE) {
  
  if(!quiet) {
    util_log_message("PROCESSING: Standardizing municipality names.")
  }
  
  cols <- proc_assess_cols(df, own_prefix = own_prefix)
  df |> 
    proc_address_muni(
      col = cols$own$muni,
      state_col = cols$own$state,
      postal_col = cols$own$postal,
      places=places
    )
}

proc_assess_luc <- function(df, quiet = FALSE, path=DATA_PATH) {
  
  if(!quiet) {
    util_log_message("PROCESSING: Standardizing land use codes.")
  }
  
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

proc_assess <- function(df, 
                        site_prefix,
                        own_prefix,
                        zips,
                        parcels_point,
                        places,
                        state_constraint,
                        quiet = FALSE) {
  if(!quiet) {
    util_log_message("BEGIN ASSESSORS TABLE SEQUENCE", header=TRUE)
  }
  
  places <- places |>
    dplyr::select(-id)
  
  df <- df |>
    proc_assess_address_text(
      site_prefix = site_prefix,
      own_prefix = own_prefix,
      quiet=quiet
    )
  
  df <- df |>
    proc_assess_address_addr2(
      site_prefix = site_prefix,
      own_prefix = own_prefix,
      quiet=quiet
    )
  
  df <- df |>
    proc_assess_address_to_range(
      site_prefix = site_prefix,
      own_prefix = own_prefix,
      quiet=quiet
    )
  
  df <- df |>
    proc_assess_address_postal(
      site_prefix=site_prefix,
      own_prefix=own_prefix,
      zips=zips,
      parcels_point=parcels_point,
      state_constraint=state_constraint,
      quiet=quiet
    )
  
  df <- df |>
    proc_assess_address_muni(
      own_prefix=own_prefix,
      places=places,
      zips=zips,
      quiet=quiet
    )
  
  df |>
    proc_assess_luc(quiet=quiet)
}

# Omnibus Data Process ====

proc_all <- function(assess,
                     companies,
                     officers,
                     addresses,
                     zips,
                     parcels,
                     places,
                     tables,
                     crs,
                     push_db = "",
                     refresh = FALSE,
                     quiet = FALSE
                     ) {
  
  if (is.null(tables)) {
    if (!quiet) {
      util_log_message("NO PROCESSING TABLES REQUESTED. SKIPPING SUBROUTINE.", header=TRUE)
    }
  } else {
    if (!quiet) {
      util_log_message("BEGINNING DATA PROCESSING SUBROUTINE.", header=TRUE)
    }
  }
  
  out <- list(
    parcels_point = NULL,
    assess = NULL,
    sites = NULL,
    owners = NULL,
    companies = NULL,
    officers = NULL
  )
  
  if ("parcels_point" %in% tables) {
    parcels_point <- load_read_write(
      util_conn(push_db),
      "parcels_point",
      loader=proc_parcels_to_dry_points(
        parcels, 
        crs=crs
        ),
      id_col="loc_id",
      refresh=refresh
    )
    # load_add_fk(util_conn(push_db), "parcels_point", "block_groups", "block_group_id", "id")
    # load_add_fk(util_conn(push_db), "parcels_point", "tracts", "tract_id", "id")
    # load_add_fk(util_conn(push_db), "parcels_point", "munis", "muni_id", "muni_id")
    out[['parcels_point']] <- parcels_point
  }
  
  
  if ("proc_assess" %in% tables) {
    assess <- load_read_write(
      util_conn(push_db),
      "proc_assess",
      loader=proc_assess(
        assess,
        site_prefix="site",
        own_prefix="own",
        zips=zips,
        parcels_point=parcels_point,
        places=places,
        quiet=quiet,
        state_constraint="MA"
      ),
      id_col=c("site_id", "site_muni_id"),
      refresh=refresh
    )
    # load_add_fk(util_conn(push_db), "proc_assess", "munis", "site_muni_id", "muni_id")
    # load_add_fk(util_conn(push_db), "proc_assess", "parcels_point", "site_loc_id", "loc_id")
    
    out[['assess']] <- assess
    
    assess |>
      proc_assess_split(
        site_prefix="site",
        own_prefix="own",
        quiet=quiet
      ) |>
      wrapr::unpack(
        sites <- sites,
        owners <- owners
      )
  }
  
  rm(assess, parcels_point) |> suppressWarnings()
  
  if ("proc_sites" %in% tables) {
    sites <- load_read_write(
      util_conn(push_db),
      "proc_sites",
      loader=proc_assess_sites(
        sites,
        addresses=addresses,
        quiet=quiet
      ),
      id_col=c("id", "muni_id"),
      refresh=refresh
      )
    
    # load_add_fk(util_conn(push_db), "proc_sites", "munis", "muni_id", "muni_id")
    # load_add_fk(util_conn(push_db), "proc_sites", "parcels_point", "loc_id", "loc_id")
    out[['sites']] <- sites
  }
  
  rm(sites) |> suppressWarnings()
  
  if("proc_owners" %in% tables) {
    owners <- load_read_write(
      util_conn(push_db),
      "proc_owners",
      loader=proc_assess_owners(
        owners,
        name_col="name",
        address_col="addr",
        quiet=quiet
      ),
      id_col="id",
      refresh=refresh
    )
    
    # load_add_fk(util_conn(push_db), "proc_owners", "munis", "site_muni_id", "muni_id")
    # load_add_fk(util_conn(push_db), "proc_owners", "parcels_point", "loc_id", "loc_id")
    # load_add_fk(util_conn(push_db), "proc_owners", "proc_sites", c("site_id", "site_muni_id"), c("id", "muni_id"))
    out[['owners']] <- owners
  }
  
  rm(owners) |> suppressWarnings()
  
  if("proc_companies" %in% tables) {
    companies <- load_read_write(
      util_conn(push_db),
      "proc_companies",
      loader=proc_oc_companies(
        companies,
        zips=zips,
        places=places,
        quiet=quiet
      ),
      id_col="id",
      refresh=refresh
    )
    out[['companies']] <- companies
  }
  
  rm(companies) |> suppressWarnings()
  
  if("proc_officers" %in% tables) {
    officers <- load_read_write(
      util_conn(push_db),
      "proc_officers",
      loader=proc_oc_officers(
        officers,
        zips=zips,
        places=places,
        quiet=quiet
      ),
      id_col="id",
      refresh=refresh
    )
    # load_add_fk(util_conn(push_db), "proc_officers", "proc_companies", "company_id", "company_id")
    out[['officers']] <- officers
  }
  
  rm(officers) |> suppressWarnings()
  out
}
