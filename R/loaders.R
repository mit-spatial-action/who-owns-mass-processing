source("R/run_utils.R")

load_corps <- function(path) {
  #' Load corporations, sourced from the MA Secretary of the Commonwealth.
  #'
  #' @param path Path to delimited text Corporations file
  #' @returns A dataframe.
  #' @export
  readr::read_delim(
      path,
      delim = "|",
      col_select = c(
        DataID, EntityName,
        AgentName, AgentAddr1, AgentAddr2, AgentCity,
        AgentState, AgentPostalCode, ActiveFlag
        ),
      show_col_types = FALSE
    ) |>
    dplyr::rename(
      id_corp = DataID
    ) |>
    dplyr::rename_with(stringr::str_to_lower)
}

load_agents <- function(df, cols, drop_na_col) {
  #' Load agents, which are listed alongside corporations.
  #'
  #' @param df Dataframe created by `load_corps`
  #' @param cols Columns containing fields describing agents.
  #' @param drop_na_col Column for which NA rows should be dropped.
  #' @returns A dataframe of corporate agents.
  #' @export
  df |>
    dplyr::select(all_of(cols)) |>
    dplyr::filter(!is.na(get({{ drop_na_col }})))
}

load_parcels <- function(path, town_ids=NULL, crs = 2249) {
  #' Load assessing table from MassGIS geodatabase.
  #' https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/gdbs/l3parcels/MassGIS_L3_Parcels_gdb.zip
  #' 
  #' @param path Path to MassGIS Parcels GDB.
  #' @param test Whether to only load a sample subset of rows.
  #' @export
  q <- "SELECT LOC_ID FROM L3_TAXPAR_POLY"
  if (!is.null(town_ids)) {
    q <- stringr::str_c(
        q, 
        "WHERE TOWN_ID IN (", 
        stringr::str_c(town_ids, collapse=", "), 
        ")",
        sep = " "
      )
  }
  sf::st_read(path, query = q, quiet = TRUE) |>
    dplyr::rename_with(stringr::str_to_lower) |> 
    # Correct weird naming conventions of GDB.
    sf::st_set_geometry("shape") |>
    sf::st_set_geometry("geometry") |>
    # Reproject to specified CRS.
    sf::st_transform(crs) |>
    # Cast from MULTISURFACE to MULTIPOLYGON.
    dplyr::mutate(
      geometry = sf::st_cast(geometry, "MULTIPOLYGON")
    )
  
}

load_inds <- function(path) {
  #' Load individuals from corporate db, 
  #' sourced from the MA Secretary of the Commonwealth.
  #'
  #' @param path Path to delimited text Corporations file
  #' @returns A dataframe.
  #' @export
  readr::read_delim(
      path,
      delim = "|",
      col_select = c(
        DataID, FirstName, LastName, BusAddr1,
        ResAddr1
      ),
      show_col_types = FALSE
    ) |>
    dplyr::rename(
      id_corp = DataID
    ) |>
    dplyr::rename_with(stringr::str_to_lower)
}

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

load_assess <- function(path = ".", town_ids = FALSE) {
  #' Load assessing table from MassGIS geodatabase.
  #' https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/gdbs/l3parcels/MassGIS_L3_Parcels_gdb.zip
  #'
  #' @param path Path to MassGIS Parcels GDB.
  #' @param town_ids list of town IDs
  #' @export
  cols <- c(
      # Property metadata.
      "PROP_ID", "LOC_ID", "FY", 
      # Parcel address.
      "SITE_ADDR", "ADDR_NUM", "FULL_STR", "CITY", "ZIP", 
      # Owner name and address.
      "OWNER1", "OWN_ADDR", "OWN_CITY", "OWN_STATE", "OWN_ZIP", "OWN_CO",
      # Last sale date and price.
      "LS_DATE", "LS_PRICE",
      # Necessary to estimate unit counts.
      "BLD_AREA", "RES_AREA", "UNITS",
      # Assessed values.
      "BLDG_VAL", "LAND_VAL", "TOTAL_VAL",
      # Land use code.
      "USE_CODE"
      )
  cols <- stringr::str_c(cols, collapse = ", ")
  q <- stringr::str_c("SELECT", cols, "FROM L3_ASSESS", sep = " ")
  if (!isFALSE(town_ids)) {
    q <- stringr::str_c(
      q,
      "WHERE TOWN_ID IN (",
      paste(town_ids, collapse = ", "),
      ")",
      sep = " "
    )
  }
  sf::st_read(
      path,
      query = q,
      quiet = TRUE
    ) |>
    dplyr::rename_with(stringr::str_to_lower) |>
    # residential_filter("use_code") |>
    dplyr::mutate(
      site_addr = dplyr::case_when(
        is.na(site_addr) & 
          !is.na(addr_num) & 
          !is.na(full_str) ~ stringr::str_c(addr_num, full_str, sep = " "),
        TRUE ~ site_addr
      )
    ) |>
    dplyr::select(-c(addr_num, full_str))
}

get_remote_zip <- function(url, path) {
  httr::GET(
    paste0(url), 
    httr::write_disk(path, overwrite = TRUE)
  )
}

read_shp_from_zip <- function(path, layer) {
  path <- stringr::str_c("/vsizip/", path, "/", layer)
  sf::st_read(path, quiet=TRUE)
}

get_shp_from_remote_zip <- function(url, shpfile, crs) {
  message(
    glue::glue("Downloading {shpfile} from {url}...")
  )
  temp <- base::tempfile(fileext = ".zip")
  get_remote_zip(
    url = url,
    path = temp
  )
  read_shp_from_zip(temp, shpfile) |>
    dplyr::rename_with(tolower) |>
    sf::st_transform(crs)
}

load_place_names <- function(crs, ma_munis) {
  
  ma_munis <- dplyr::select(ma_munis, muni_name = pl_name)
  
  df <- get_shp_from_remote_zip(
    "https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/geonames_shp.zip",
    shpfile = "GEONAMES_PT_PLACES.shp",
    crs = crs
    ) |>
    dplyr::rename(pl_name = labeltext) |>
    dplyr::select(-angle) |>
    dplyr::mutate(
      pl_name = stringr::str_replace(pl_name, "BORO$", "BOROUGH"),
      pl_name = stringr::str_remove_all(
        pl_name, "(?<=\\b\\w)[ ]{1,2}(?=\\w\\b)"
      ),
      pl_name = stringr::str_replace_all(
        pl_name, "([\\s]+)", " "
      )
    ) |>
    sf::st_join(
      ma_munis, join = sf::st_intersects
    )
  
  df <- df |>
    dplyr::filter(is.na(muni_name)) |>
    dplyr::select(-muni_name) |>
    sf::st_join(
      ma_munis, join = sf::st_nearest_feature
    ) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(!is.na(muni_name))
    ) |>
    sf::st_drop_geometry() |>
    dplyr::distinct()
  
  df |>
    dplyr::bind_rows(
      ma_munis |>
        sf::st_drop_geometry() |>
        dplyr::filter(!(muni_name %in% df$pl_name)) |>
        dplyr::mutate(
          pl_name = muni_name
        )
    ) |>
    dplyr::mutate(
      tmp = dplyr::case_when(
        stringr::str_detect(pl_name, "^(SOUTH|EAST|WEST|NORTH)\\w|PORT$") &
          !stringr::str_detect(pl_name, " ") ~
          stringr::str_c(
            stringr::str_sub(pl_name, start = 1, end = 2),
            " ",
            stringr::str_sub(pl_name, start = 3)
          ),
        .default = pl_name
      ),
      pl_name_fuzzy = fuzzify_string(tmp)
    ) |>
    dplyr::select(-tmp) |>
    dplyr::filter(!(pl_name == "CAMBRIDGE" & muni_name == "WORCESTER"))
}

get_from_arc <- function(dataset, crs) {
  prefix <- "https://opendata.arcgis.com/api/v3/datasets/"
  suffix <- "/downloads/data?format=geojson&spatialRefId=4326&where=1=1"
  sf::st_read(
    glue::glue("{prefix}{dataset}{suffix}")
  ) |>
    dplyr::rename_with(tolower) |>
    sf::st_transform(crs)
}

load_ma_munis <- function(crs) {
  #' Downloads MA municipalities from MassGIS ArcGIS Hub.
  #'
  #' @param crs Coordinate reference system for output.
  #' @export
  message("Downloading Massachusetts municipal boundaries...")
  get_from_arc("43664de869ca4b06a322c429473c65e5_0", crs = crs) |>
    dplyr::select(town_id, pl_name = town) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::where(is.character), 
        stringr::str_to_upper)
      ) |>
    dplyr::mutate(
      pl_name = dplyr::case_when(
        pl_name == "MANCHESTER" ~ "MANCHESTER BY THE SEA",
        .default = pl_name
      ),
      pl_name = stringr::str_replace(pl_name, "BORO$", "BOROUGH")
    )
}

load_bos_neighs <- function() {
  readr::read_csv(
    "https://raw.githubusercontent.com/mit-spatial-action/utility_datasets/main/bos_neigh.csv",
    show_col_types = FALSE
  ) |>
    dplyr::mutate(
      name = toupper(name)
    )
}


load_lu_lookup <- function() {
  readr::read_csv(
    "https://raw.githubusercontent.com/MAPC/landparcels/master/land_use_lookup.csv",
    show_col_types = FALSE
  )
}

overlap_analysis <- function(x, y, threshold = 0) {
  x |>
    dplyr::mutate(
      area = sf::st_area(geometry)
    ) |>
    sf::st_intersection(y) |>
    dplyr::mutate(
      overlap = units::drop_units(sf::st_area(geometry) / area)
    ) |>
    sf::st_drop_geometry() |>
    dplyr::filter(
      overlap > threshold
    )
}

load_zips_by_state <- function(crs, ma_munis, threshold = 0.95) {
  all <- list()
  for (s in state.abb) {
    all[[s]] <- tigris::zctas(cb = FALSE, year = 2010, state = s) |>
      dplyr::mutate(state = s) |>
      dplyr::select(zip = ZCTA5CE10, state)
  }
  zips <- dplyr::bind_rows(all) |> 
    dplyr::group_by(zip) |>
    dplyr::mutate(
      state_unambig = dplyr::case_when(
        dplyr::n() == 1 ~ TRUE,
        .default = FALSE
      )
    ) |>
    dplyr::ungroup()
  
  zips_ma <- zips |>
    dplyr::filter(state == "MA") |>
    sf::st_transform(crs)
  
  zips <- sf::st_drop_geometry(zips)
  
  ma_munis_clip <- ma_munis |>
    sf::st_intersection(
      sf::st_union(zips_ma)
    )
  
  zips_ma_clip <- zips_ma |>
    sf::st_intersection(
      sf::st_union(ma_munis_clip)
    )
    
  # Identify cases where ZIPS can be unambiguously assigned from munis (i.e.,
  # where the vast majority of a zip is contained w/in a single muni.)
  ma_umanbig_zip_from_muni <- overlap_analysis(ma_munis_clip, 
                                               zips_ma_clip, 
                                               threshold = threshold) |>
    dplyr::select(zip, state, pl_name)
  
  # Identify cases where munis can be unambiguously assigned
  # from ZIPS (i.e., where the vast majority of a muni is contained w/in
  # a single zip).
  ma_unambig_muni_from_zip <- overlap_analysis(zips_ma_clip, 
                                               ma_munis_clip, 
                                               threshold = threshold) |>
    dplyr::select(zip, state, pl_name)
  
  list(
    all = dplyr::pull(zips, zip) |> unique(),
    unambig = dplyr::filter(zips, state_unambig),
    ma = zips_ma,
    ma_unambig = dplyr::filter(zips_ma, state_unambig),
    ma_unambig_zip_from_muni = ma_umanbig_zip_from_muni,
    ma_unambig_muni_from_zip = ma_unambig_muni_from_zip
  )
}

load_filings <- function(ma_munis, bos_neighs, town_ids = FALSE, crs = 2249) {
  #' Pulls eviction filings from database.
  #'
  #' @return A dataframe.
  #' @export
  # Construct SQL query.
  docket_col <- "docket_id"
  filings_table <- "filings"
  plaintiffs_table <- "plaintiffs"
  cols <- stringr::str_c(
    c(docket_col, "add1", "city", "zip", "state", "match_type", "geometry"), 
    collapse = ","
    )
  q <- stringr::str_c(
    "SELECT", cols, 
    "FROM", filings_table, "AS f",
    sep = " "
  )
  # Set limit if test = TRUE
  if (!isFALSE(town_ids)) {
    q_filter <- ma_munis |>
      dplyr::filter(town_id %in% town_ids) |>
      dplyr::pull(id) 
    if (35 %in% town_ids) {
      neighs <- bos_neighs |>
        dplyr::pull(Name)
      q_filter <- c(q_filter, neighs)
    }
    q_filter <- q_filter |>
      stringr::str_c("UPPER(f.city) = '", ., "'") |>
      stringr::str_c(., collapse = " OR ") |>
      stringr::str_c("WHERE", ., sep = " ")
    q <- stringr::str_c(q, q_filter, sep = " ")
  }
  # Pull filings.
  conn <- DBI::dbConnect(
      RPostgres::Postgres(),
      dbname = Sys.getenv("DB_NAME"),
      host = Sys.getenv("DB_HOST"),
      port = Sys.getenv("DB_PORT"),
      user = Sys.getenv("DB_USER"),
      password = Sys.getenv("DB_PASS"),
      sslmode = "allow"
    ) 
  filings <- conn |>
    sf::st_read(
      query=q,
      quiet = TRUE
      )
  
  DBI::dbDisconnect(conn)
  
  filings |> 
    dplyr::select(-tidyselect::contains('..')) |>
    sf::st_transform(crs) |>
    dplyr::rename_with(stringr::str_to_lower) |>
    dplyr::filter(!is.na(add1))
}