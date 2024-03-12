source("R/globals.R")
source("R/standardizers.R")
# source("R/deduplicaters.R")
# source("R/filing_linkers.R")
source("R/loaders.R")
source("R/run_utils.R")

residential_filter <- function(df, col) {
  #' Filter assessors records by MA residential use codes.
  #' Massachusetts Codebook
  #' https://www.mass.gov/files/documents/2016/08/wr/classificationcodebook.pdf
  #' Boston Codebook
  #' https://www.cityofboston.gov/Images_Documents/MA_OCCcodes_tcm3-16189.pdf
  #' @param df A dataframe.
  #' @param cols The columns containing the use codes.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::filter(
      stringr::str_detect(
        get({{ col }}), stringr::str_c(c(
          # Residential use codes.
          "^0?10[13-59][0-9A-Z]?$",
          # Apartments.
          "^0?11[1-5][0-9A-Z]?$",
          # Subsidized Housing.
          "^0?12[5-7]",
          # Mixed use codes.
          "^0((1[0-9])|([1-9]1))[A-Z]?$",
          # Boston Housing Authority.
          "^908[A-Z]?",
          # Housing authority outside Boston.
          "^0?970[A-Z]?",
          # Section 121-A Property...
          # (Tax-exempt 'blight' redevelopment.)
          # in Boston
          "^0?907[A-Z]?",
          # outside Boston.
          "^990[A-Z]?",
          # 'Other' Housing.
          "^959[A-Z]?",
          "^000"
        ), collapse = "|")
      )
    )
}

prepare_owner_name <- function(df) {
  df |>
    std_flow_strings(c("name")) |>
    std_street_types("name") |>
    std_inst_types("name") |>
    std_massachusetts("name") |>
    dplyr::mutate(
      city_of = stringr::str_extract(name, "^(CITY|TOWN|VILLAGE) OF "),
      name = dplyr::case_when(
        !is.na(city_of) ~ stringr::str_c(
          stringr::str_remove(name, stringr::str_c("^", city_of, sep="")),
          stringr::str_extract(city_of, "^[A-Z]+"),
          sep = " "
        ),
        .default = name
      )
    ) |>
    dplyr::select(-city_of)
}

detect_addr2 <- function(df, number = FALSE) {
  if (number) {
    regex <- "[- ]+(([A-Z]?[0-9\\/ \\-]+[A-Z]?)|([A-Z]( [A-Z])?)|(U \\-))$"
  } else {
    terms <- c(
      "UNITS?", 
      "SUITES?", 
      "STE[ -\\#]+", 
      "AP(AR)?T(MENT)?", 
      "ROOM", 
      "OFFICE",
      " RM[ -#]*[0-9]+", 
      " PH[ -#]*[0-9]+", 
      " U(PH)?([ -#]*[A-Z]?[0-9]+[A-Z]?)+", 
      " (NO |NUM )",  
      " PMB", 
      "\\#", 
      "PENT([HOUSE]{4,6})?"
      ) |>
      stringr::str_c(collapse="|")
    regex <- glue::glue("[- ]*({terms}).*$")
  }
  df |>
    dplyr::mutate(
      match = !po_box & stringr::str_detect(addr, regex) & is.na(addr2),
      addr2 = dplyr::case_when(
        match ~ stringr::str_extract(addr, regex),
        .default = addr2
      ),
      addr = dplyr::case_when(
        !is.na(addr2) ~ stringr::str_squish(stringr::str_remove(addr, stringr::str_c(addr2, "$", sep=""))),
        .default = addr
      ),
      addr2 = stringr::str_squish(addr2)
    ) |>
    dplyr::select(-match)
}

detect_floor <- function(df) {
  df |>
    dplyr::mutate(
      floor = stringr::str_extract(addr, "[- ]?([0-9]{1,3}(TH|ST|RD|ND) FL[OR]{0,3}|FL[OR]{0,3} [0-9]{1,3})"),
      addr = dplyr::case_when(
        !is.na(floor) ~ stringr::str_squish(stringr::str_remove(addr, floor)),
        .default = addr
      ),
      floor = stringr::str_squish(floor)
    )
}

std_parse_addr2 <- function(df) {
  df |>
    dplyr::mutate(
      po_box = stringr::str_detect(addr, "P ?O BO?X [0-9\\/\\- ]+$"),
      addr2 = NA_character_
    ) |>
    detect_floor() |>
    detect_addr2(number = FALSE) |>
    detect_addr2(number = TRUE) |>
    std_street_types("addr")
}

std_spatial_match <- function(df, parcel_points, ma_munis, ma_postals) {
  df |>
    dplyr::left_join(parcel_points, by=c("loc_id" = "loc_id")) |>
    sf::st_as_sf() |>
    # All parcels in MA are in MA...
    dplyr::mutate(
      # When muni doesn't appear in a list of names, join it to the spatial df.
      muni = dplyr::case_when(
        !((state == "MA") & (muni %in% ma_munis$pl_name)) ~
          sf::st_join(sf::st_as_sf(geometry), dplyr::select(ma_munis, pl_name)) |> dplyr::pull(pl_name),
        .default = muni
      ),
      # When postal doesn't appear in a list of zip codes, join it to the spatial df.
      postal = dplyr::case_when(
        !((state == "MA") & (postal %in% ma_postals$zip)) ~
          sf::st_join(sf::st_as_sf(geometry), dplyr::select(ma_postals, zip)) |> dplyr::pull(zip),
        .default = postal
      )
    ) |>
    sf::st_drop_geometry()
}

std_address_range <- function(df) {
  df |>
    dplyr::mutate(
      # Extract address, accounting for possible presence of half-addresses.
      addr_range = stringr::str_detect(
        addr, 
        "^[0-9]+[A-Z]{0,1} *(1 \\/ 2)?([ -]+[0-9]+[A-Z]{0,1} *(1 \\/ 2)?)+ +(?=[A-Z0-9])"
        ),
      addr_num = stringr::str_extract(
        addr, 
        "^[0-9]+[A-Z]{0,1} *(1 \\/ 2)?([ -]+(([0-9]+[A-Z]{0,1} *(1 \\/ 2)?)*|[A-Z]))? +(?=[A-Z0-9])"
        ),
      # Extract address body.
      addr_body = stringr::str_remove(
        addr, 
        addr_num
        ),
      addr_temp = stringr::str_replace_all(
        addr_num, 
        " 1 \\/ 2", "\\.5"
        ),
      addr_start = as.numeric(
        stringr::str_extract(
          addr_temp, 
          "^[0-9\\.]+"
          )
        ),
      addr_end = dplyr::case_when(
        addr_range ~ 
          as.numeric(
            stringr::str_extract(
              addr_temp, 
              "(?<=[- ])[0-9\\.]+"
              )
            ),
        .default = addr_start
      ),
      addr_num = dplyr::case_when(
        (addr_start == addr_end) & addr_range ~ 
          stringr::str_extract(addr_num, "^[0-9]+[A-Z]{0,1}( ?1 / 2)?"),
        .default = addr_num
      ),
      addr_end = dplyr::case_when(
        (addr_end < addr_start) | is.na(addr_end) ~ addr_start,
        .default = addr_end
      ),
      even = dplyr::case_when(
        floor(addr_start) %% 2 == 0 ~ TRUE,
        floor(addr_start) %% 2 == 1 ~ FALSE,
        .default = NA
      ),
      parsed = !is.na(addr_start) & !is.na(addr_end) & !is.na(addr_body),
      addr = dplyr::case_when(
        parsed & (addr_start == addr_end) ~ stringr::str_c(addr_start, addr_body, sep = " "),
        parsed ~ stringr::str_c(
          stringr::str_c(addr_start, addr_end, sep = " - "),
          addr_body, sep = " "),
        .default = addr
      )
    ) |>
    dplyr::select(-c(parsed, addr_temp, addr_range))
}

fill_muni_by_postal <- function(df) {
  df |>
    dplyr::filter(!is.na(addr)) |>
    dplyr::add_count(addr, muni,  postal, name = "cnt_all") |>
    dplyr::add_count(addr, muni,  name = "cnt_muni") |>
    dplyr::add_count(addr, postal,  name = "cnt_postal") |>
    dplyr::mutate(
      cnt_postal = dplyr::case_when(
        is.na(postal) ~ 0,
        .default = cnt_postal
      ),
      cnt_muni = dplyr::case_when(
        is.na(muni) ~ 0,
        .default = cnt_muni
      )
    ) |>
    dplyr::group_by(addr, postal) |>
    dplyr::mutate(
      muni = dplyr::case_when(
        (cnt_postal > cnt_muni) & (cnt_muni != cnt_all) ~ muni[[which.max(cnt_muni)]],
        .default = muni
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-c(cnt_postal, cnt_muni, cnt_all)) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(is.na(addr))
    )
}

fill_postal_by_muni <- function(df) {
  df |>
    dplyr::filter(!is.na(addr)) |>
    dplyr::add_count(addr, muni,  postal, name = "cnt_all") |>
    dplyr::add_count(addr, muni,  name = "cnt_muni") |>
    dplyr::add_count(addr, postal,  name = "cnt_postal") |>
    dplyr::mutate(
      cnt_postal = dplyr::case_when(
        is.na(postal) ~ 0,
        .default = cnt_postal
      ),
      cnt_muni = dplyr::case_when(
        is.na(muni) ~ 0,
        .default = cnt_muni
      )
    ) |>
    dplyr::group_by(addr, muni) |>
    dplyr::mutate(
      postal = dplyr::case_when(
        (cnt_muni > cnt_postal) ~ postal[[which.max(cnt_postal)]],
        .default = postal
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-c(cnt_postal, cnt_muni, cnt_all)) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(is.na(addr))
    )
}

fill_state_by_muni <- function(df) {
  df |>
    dplyr::filter(!is.na(addr)) |>
    dplyr::add_count(addr, muni, state, name = "cnt_all") |>
    dplyr::add_count(addr, muni, name = "cnt_muni") |>
    dplyr::add_count(addr, state, name = "cnt_state") |>
    dplyr::mutate(
      cnt_muni = dplyr::case_when(
        is.na(muni) ~ 0,
        .default = cnt_muni
      ),
      cnt_state = dplyr::case_when(
        is.na(state) ~ 0,
        .default = cnt_state
      )
    ) |>
    dplyr::group_by(addr, muni) |>
    dplyr::mutate(
      state = dplyr::case_when(
        (cnt_muni > cnt_state) & (cnt_muni != cnt_all) ~ state[[which.max(cnt_state)]],
        .default = state
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-c(cnt_muni, cnt_state, cnt_all)) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(is.na(addr))
    )
}

std_postal <- function(df, zips_unambig, state_abb) {
  df |>
    dplyr::left_join(
      zips_unambig |> dplyr::select(zip, known_state = state), by = c("postal" = "zip")
    ) |>
    dplyr::mutate(
      state = dplyr::case_when(
        !is.na(known_state) & !(state %in% state_abb) ~ known_state,
        .default = state
      )
    ) |>
    dplyr::select(-known_state)
}

flag_inst_types <- function(df, cols) {
  #' Standardize street types.
  #'
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  inst_types <- c(
    "CORPORATION","INC", "LLC", "LTD", "COMPANY", "LP", "PROPERT(IES|Y)", 
    "GROUP", "MANAGEMENT", "PARTNERS", "REALTY", "DEVELOPMENT", "EQUITIES", 
    "HOLDING", "INSTITUTE", "DIOCESE", "PARISH", "HOUSING", "AUTHORITY"
    )
  trust_types <- c(
    "TRUST","(IR)?REVOCABLE","LIVING TR","REALTY T","REAL ESTATE TR","ESTATE"
    )
  df |>
    dplyr::mutate(
      corp = stringr::str_detect(
        name, 
        stringr::str_c(inst_types, sep = "|")
        ),
      name_simp = stringr::str_squish(
        stringr::str_remove_all(
          name, 
          "CORPORATION|INC|LLC|LTD|LP|COMPANY|FEE OWNER"
          )
        ),
      trust = stringr::str_detect(
        name, 
        stringr::str_c(trust_types, sep = "|")
        ),
    )
}

std_assess_addr <- function(df, 
                            zips, 
                            states_abb, 
                            bos_neighs, 
                            states = FALSE, 
                            postal = FALSE,
                            country = FALSE) {
  df <- df |>
    std_street_types("addr") |>
    dplyr::mutate(
      addr = dplyr::case_when(
        stringr::str_detect(addr, "(?<=[02-9]) ST ") ~ stringr::str_replace(addr, "(?<=[02-9]) ST ", " SAINT "),
        .default = addr
      )
    ) |> 
    std_leading_zeros("addr") |>
    std_directions(c("addr", "muni")) |>
    std_massachusetts(c("addr")) |>
    std_postal_format(c("postal"), zips$all$zip) |>
    std_muni_names(c("muni"), bos_neighs) |>
    dplyr::mutate(
      addr = dplyr::case_when(
        addr == "" ~ NA_character_,
        .default = addr
      )
    ) |>
    std_parse_addr2() |>
    dplyr::mutate(
      addr = stringr::str_replace(addr, "^$", NA_character_)
    ) |>
    std_address_range() |>
    fill_postal_by_muni() |>
    fill_muni_by_postal()
  if (states) {
    df <- df |>
      fill_state_by_muni()
  } 
  if (postal) {
    df <- df |>
      std_postal(zips$unambig, states_abb)
  }
  if (country) {
    df <- df |>
      dplyr::mutate(
        country = stringr::str_squish(stringr::str_remove_all(country, "[:digit:]|\\/|//-")),
        country = dplyr::case_when(
          country == "USA" ~ "US",
          stringr::str_starts(country, "UNITED STAT") | stringr::str_ends(country, "OF AMERICA") ~ "US",
          state %in% STATES$abb ~ "US",
          postal %in% ZIPS$all$zip ~ "US",
          .default = countrycode::countrycode(country, 'country.name.en.regex', 'iso2c', warn = FALSE)
        )
      )
  }
  df
}

std_use_codes <- function(df, col) {
  df |>
    std_replace_generic(
      c("use_code"), 
      pattern = c("[A-Z]|0(?=[1-9][0-9][1-9])|(?<=[1-9][0-9]{2})0|(?<=0[1-9][1-9])0|(?<=[1-9][0-9]{2})[1-9]" = "")
    ) |>
    dplyr::filter(
      stringr::str_starts(use_code, "1") | 
        (stringr::str_detect(use_code, "^(01|0[2-9]1|959|9[79]0)") & muni != "BOSTON") |
        (stringr::str_detect(use_code, "^90[78]") & muni == "BOSTON") | 
        (use_code != "199")
    ) |>
    # Appears to be some kind of condo quirk in Cambridge...|>
    dplyr::mutate(
      area = dplyr::case_when(
        stringr::str_starts(use_code, "0") | use_code %in% c('113', '114') ~ res_area,
        .default = pmax(res_area, bld_area, na.rm = TRUE)
      )
    ) |>
    dplyr::filter(area > 0) |>
    dplyr::mutate(
      units = dplyr::case_when(
        units == 0 ~ NA,
        .default = units
      ),
      missing_units = is.na(units),
      units = dplyr::case_when(
        # Single-family.
        use_code == '101' ~ 1,
        # Condo.
        use_code == '102' & (land_val == 0 | is.na(land_val)) ~ 1,
        # Two-family.
        use_code == '104' ~ 2,
        # Three-family.
        use_code == '105' ~ 3,
        # Multiple houses.
        use_code == '109' & is.na(units) ~ 
          as.integer(round(area * 0.7 / 1200)),
        use_code == '111' & (!dplyr::between(units, 4, 8) | missing_units) ~ 
          as.integer(round(pmax(4, pmin(8, area * 0.7 / 1000)))),
        # 8+ MA, 7-30 in Boston
        use_code == '112' & muni != "BOSTON" & (!(units >= 8) | missing_units) ~ 
          as.integer(round(pmax(8, area * 0.7 / 1000))),
        use_code == '112' & muni == "BOSTON" & (!(units >= 7) | missing_units) ~ 
          as.integer(round(pmax(7, pmin(30, area * 0.7 / 1000)))),
        # 113: Boston, 39-99
        use_code == '113' & muni == "BOSTON" & (!dplyr::between(units, 30, 99) | missing_units) ~ 
          as.integer(round(pmax(30, pmin(99, area * 0.7 / 1000)))),
        # 114: Boston, 100+
        use_code == '114' & muni == "BOSTON" & (!(units >= 100) | missing_units) ~ 
          as.integer(round(pmax(100, area * 0.7 / 1000))),
        # Unclear what these are outside of Boston.
        use_code %in% c('113', '114') & muni != "BOSTON" & missing_units ~ 
          as.integer(round(area * 0.7 / 1000)),
        # Boston Housing Authority
        use_code == '908' & muni == "BOSTON" & missing_units ~ 
          as.integer(round(area * 0.7 / 1000)),
        missing_units ~ 
          pmax(1, as.integer(round(area * 0.7 / 1000))),
        .default = units
      )
    )
}


std_co_dba <- function(df, fill_owner = TRUE) {
  df <- df |>
    dplyr::mutate(
      co = stringr::str_extract(
        name, 
        "(C[ \\/]{1,3}O|D[ \\/]{0,3}B[ \\/]{0,3}A) .*"
        ),
      owner = dplyr::case_when(
        !is.na(co) ~ 
          stringr::str_replace(
            stringr::str_remove(name, co), 
            "^ *$", 
            NA_character_
          )
      ),
      likely_addr = dplyr::case_when(
        !is.na(owner) ~
          stringr::str_detect(
            owner, 
            "^ ?[0-9]+ ([1-9]{0,2}[A-Z]+ )+(?!(LLC|L?LP|LTD|CORP|INC|COMP)).*$"
          ),
        .default = FALSE
      ),
      co = stringr::str_squish(
        stringr::str_replace(
          co, 
          "(C[ \\/]{1,3}O|[ \\/]{0,3}B[ \\/]{0,3}A) ?", 
          ""
        )
      )
    )
  if (fill_owner) {
    df <- df |>
      dplyr::mutate(
        owner = dplyr::case_when(
          is.na(owner) ~ co,
          .default = owner
        ),
        co = dplyr::case_when(
          owner == co ~ NA_character_,
          .default = co
        )
      )
  } else {
    df <- df |>
      dplyr::mutate(
        dplyr::across(
          tidyselect::any_of(c("muni", "postal", "state", "addr")),
          ~ dplyr::case_when(
            !likely_addr ~ NA_character_,
            .default = .
          )
        )
      )
  }
  df |>
    dplyr::mutate(
      addr = dplyr::case_when(
        likely_addr ~ owner,
        .default = NA_character_
      ),
      owner = dplyr::case_when(
        likely_addr ~ NA_character_,
        .default = owner
      )
    ) |>
    dplyr::select(-c(likely_addr, name)) |>
    tidyr::pivot_longer(
      cols = c("owner", "co"), 
      values_to = "name", 
      names_to = "relation",
      names_repair = "minimal", 
      values_drop_na = TRUE
    )
}

std_flow_assess_parcel <- function(assess) {
  assess |>
    dplyr::select(
      prop_id, loc_id, fy, addr = site_addr, muni = city, postal = zip, 
      use_code, units, bldg_val, land_val, total_val, ls_date, ls_price, bld_area,
      res_area
    ) |>
    # All parcels are in MA, in the US...
    dplyr::mutate(
      state = "MA", country = "US"
    ) |>
    # Apply string standardization workflow to string columns.
    std_flow_strings(c("addr", "muni", "postal")) |>
    # Standardize addresses.
    std_assess_addr(
      zips = ZIPS, bos_neighs = BOS_NEIGHS$name, states_abb = STATES$abb, 
      states = FALSE, postal = TRUE
    ) |>
    # Spatially match those addresses that can't be completely standardized with
    # reference to the table.
    std_spatial_match(PARCELS_POINTS, ma_munis = MA_MUNIS, ma_postals = ZIPS$ma) |>
    # Remove all unparseable addresses. (Most of these are street names with no 
    # associated number.)
    dplyr::filter(!is.na(addr_body)) |>
    # Standardize use codes.
    std_use_codes() |>
    dplyr::group_by(loc_id, addr_body, fy, state, country, muni, even) |>
    dplyr::summarize(
      addr_start = min(addr_start, na.rm = TRUE),
      addr_end = max(addr_end, na.rm = TRUE),
      postal = collapse::fmode(postal, na.rm = TRUE),
      use_code = collapse::fmode(use_code, na.rm = TRUE),
      dplyr::across(
        dplyr::any_of(c("units", "total_val", "bldg_val", "land_val", "total_val", "area")),
        ~ sum(., na.rm = TRUE)
      )
    ) |>
    dplyr::ungroup()
}

std_flow_assess_owner <- function(assess) {
  assess <- assess |>
    dplyr::select(
      loc_id, prop_id, name = owner1, addr = own_addr, muni = own_city, 
      state = own_state, postal = own_zip, country = own_co
    ) |>
    std_flow_strings(c("addr", "muni", "state", "postal", "country")) |>
    prepare_owner_name()
  
  cos <- assess |>
    prepare_co_dba() |>
    dplyr::bind_rows(
      assess |>
        dplyr::select(-c(name), name = addr) |>
        prepare_owner_name() |>
        prepare_co_dba(fill_owner = FALSE)
    )
  
  assess <- assess |>
    dplyr::mutate(
      relation = "owner"
    ) |>
    dplyr::filter(!(prop_id %in% cos$prop_id)) |>
    dplyr::bind_rows(cos) |>
    dplyr::filter(!is.na(name) | !is.na(addr)) |>
    flag_inst_types() |>
    std_assess_addr(
      zips = ZIPS, bos_neighs = BOS_NEIGHS$name, states_abb = STATES$abb, 
      states = TRUE, postal = TRUE, country = TRUE
    ) 
  
  assess |>
    dplyr::filter(!is.na(name)) |>
    dplyr::group_by(name_simp) |>
    dplyr::mutate(
      name = collapse::fmode(name),
      corp_id = dplyr::cur_group_id()
    ) |>
    dplyr::ungroup() |>
    dplyr::bind_rows(
      assess |>
        dplyr::filter(is.na(name))
    )
}

std_link_owner_parcel <- function(owner, parcel) {
  owner <- owner |>
    dplyr::left_join(
      parcel |>
        dplyr::select(
          address_id = loc_id, 
          addr_start_y = addr_start, 
          addr_end_y = addr_end, 
          addr_body, 
          even, 
          muni, 
          state
        ), 
      by = dplyr::join_by(
        addr_body, 
        even, 
        muni, 
        state, 
        between(
          addr_start, 
          addr_start_y, 
          addr_end_y
        )
      ),
      na_matches = "never"
    ) |>
    dplyr::select(-c(addr_start_y, addr_end_y))
  
  owener <- owner |>
    dplyr::filter(!is.na(addr_start) & !is.na(corp_id)) |>
    dplyr::group_by(corp_id, addr_start) |>
    dplyr::mutate(
      dplyr::across(
        c(addr, postal, state, country, addr_num, addr_body, address_id),
        ~ collapse::fmode(.)
      ),
      dplyr::across(
        c(corp, trust, even),
        ~ as.logical(max(., na.rm = TRUE))
      ),
      addr_end = max(na.omit(addr_end), na.rm = TRUE)
    ) |>
    dplyr::ungroup() |>
    dplyr::bind_rows(
      owner |> 
        dplyr::filter(is.na(addr_start))
    )
  
  owner |>
    dplyr::filter(!is.na(corp_id)) |>
    dplyr::group_by(corp_id, corp, trust) |>
    dplyr::mutate(
      dplyr::across(
        c(addr, postal, po_box, state, country, addr_num, addr_body, address_id),
        ~ dplyr::case_when(
          is.na(.) ~ collapse::fmode(.),
          .default = .
        )
      ),
      dplyr::across(
        c(even),
        ~ dplyr::case_when(
          is.na(.) ~ as.logical(max(., na.rm = TRUE)),
          .default = .
        )
      ),
      dplyr::across(
        c(addr_end, addr_start),
        ~ dplyr::case_when(
          is.na(.) ~ max(na.omit(.), na.rm = TRUE),
          .default = .
        )
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::bind_rows(
      owner |> 
        dplyr::filter(is.na(corp_id))
    )
}

# RUN ----

ASSESS <- load_assess(
  path = file.path(DATA_DIR, ASSESS_GDB), 
  town_ids = subset_town_ids("test")
)

PARCELS <- load_parcels(
  path = file.path(DATA_DIR, ASSESS_GDB), 
  town_ids = subset_town_ids("test"),
  crs = 2249
)

PARCELS_POINTS a<- PARCELS |>
  sf::st_point_on_surface()

MA_MUNIS <- load_ma_munis(2249)

BOS_NEIGHS <- load_bos_neighs()

STATES <- load_state_list()

ZIPS <- load_zips_by_state(STATES$abb)

assess_parcel_addr <- ASSESS |>
  std_flow_assess_parcel()

assess_owner_addr <- ASSESS |>
  std_flow_assess_owner()

owner_parcel_link <- assess_owner_addr |>
  std_link_owner_parcel(assess_parcel_addr)



test <- owner_parcel_link |>
  dplyr::select(-c(addr_num, floor, addr2)) |>
  dplyr::group_by(addr, muni, postal, state, country, addr_start, addr_end, even) |>
  dplyr::mutate(
    address_id = dplyr::case_when(
      is.na(address_id) ~ stringr::str_c("address", dplyr::cur_group_id(), sep = "_"),
      .default = address_id
    )
  ) |>
  dplyr::ungroup()

owner_to_address <- test |> 
  dplyr::distinct(address_id, corp_id)

unique_owners <- test |>
  dplyr::distinct(corp_id, name, relation, corp, trust)


unique_addresses <- test  |>
  dplyr::distinct(
    address_id, addr, muni, postal, state, country, po_box, addr_start, 
    addr_end, even
    )

unique_addresses |>
  dplyr::filter(!po_box) |>
  nrow()

addr_owner3 |>
  dplyr::filter(!po_box) |>
  



test <- addr_owner3 |>
  dplyr::filter(!po_box) |>
  


addresses <- addr_parcel_reduced |>
  dplyr::left_join(
    match |>
      dplyr::select(loc_id, owner_id = parcel_id),
    by = c("loc_id" = "loc_id"),
    na_matches = "never"
  )
  



run_all <- function(subset = "test", return_results = FALSE) {
  # Create and open log file with timestamp name.
  lf <- logr::log_open(
      format(Sys.time(), "%Y-%m-%d_%H%M%S"),
      logdir = TRUE
    )
  process_deduplication(
    town_ids = subset_town_ids(subset),
    return_results = return_results
    )
  process_link_filings(town_ids = subset_town_ids(subset))
  # Close logs.
  logr::log_close()
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  run_all(subset = "all", return_results = FALSE)
}