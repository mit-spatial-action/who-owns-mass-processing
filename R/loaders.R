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

load_gdb_vintages <- function(path) {
  gdb_list <- list.files(path)
  vintages <- data.frame(
    muni_id = stringr::str_extract(gdb_list, "(?<=M)[0-9]{3}"),
    fy = as.numeric(stringr::str_extract(gdb_list, "(?<=_FY)[0-9]{2}")),
    cy = as.numeric(stringr::str_extract(gdb_list, "(?<=_CY)[0-9]{2}"))
  )
  
  most_complete_recent <- vintages |>
    dplyr::filter(fy > 21) |>
    dplyr::group_by(fy) |>
    dplyr::tally() |>
    dplyr::ungroup() |>
    dplyr::filter(
      n == max(n)
    ) |>
    dplyr::filter(
      fy == max(fy)
    ) |>
    dplyr::pull(fy)
  
  vintages <- vintages |>
    dplyr::group_by(muni_id) |>
    dplyr::mutate(
      year_diff = abs(fy - most_complete_recent),
      load = dplyr::case_when(
        year_diff == 0 ~ TRUE,
        .default = FALSE
      )
    )
  
  exact_matches <- vintages |>
    dplyr::filter(load) |>
    dplyr::mutate(
      count = dplyr::n()
    ) |>
    dplyr::filter(count == 1 | (count > 1 & cy == max(cy))) |>
    dplyr::select(-c(year_diff, load, count))
  
  results <- vintages |>
    dplyr::filter(!load & min(year_diff) > 0) |>
    dplyr::mutate(
      min_diff = year_diff == min(year_diff)
    ) |>
    dplyr::filter(min_diff) |>
    dplyr::mutate(
      count = dplyr::n()
    ) |>
    dplyr::filter(count == 1 | (count > 1 & cy == max(cy))) |>
    dplyr::select(-c(year_diff, load, min_diff, count)) |>
    dplyr::bind_rows(exact_matches) |>
    dplyr::ungroup()
}

load_parcels_all_vintages <- function(path, town_ids=NULL, crs = CRS) {
  #' Load assessing table from MassGIS geodatabase.
  #' https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/gdbs/l3parcels/MassGIS_L3_Parcels_gdb.zip
  #' 
  #' @param path Path to MassGIS Parcels GDB.
  #' @param test Whether to only load a sample subset of rows.
  #' @export
  vintages <- load_gdb_vintages(path)
  
  if (!is.null(town_ids)) {
    town_ids <- town_ids |>
      stringr::str_pad(3, side = "left", pad = "0")
    vintages <- vintages |>
      dplyr::filter(muni_id %in% town_ids)
  }
  
  all <- list()
  for (row in 1:nrow(vintages)) {
    
    muni_id <- vintages |> dplyr::slice(row) |> dplyr::pull(muni_id)
    cy <- vintages |> dplyr::slice(row) |> dplyr::pull(cy)
    fy <-vintages |> dplyr::slice(row) |> dplyr::pull(fy)
    
    message(glue::glue("Loading parcels for muni {muni_id}."))
    
    
    file <- glue::glue("M{muni_id}_parcels_CY{cy}_FY{fy}_sde.gdb")
    q <- glue::glue("SELECT LOC_ID, TOWN_ID FROM M{muni_id}TaxPar")
    
    all[[muni_id]] <- sf::st_read(
      glue::glue("{path}{file}"), 
      query = q, 
      quiet = TRUE
      )
  }
  dplyr::bind_rows(all) |>
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

load_parcels <- function(path, town_ids=NULL, crs = CRS) {
  #' Load assessing table from MassGIS geodatabase.
  #' https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/gdbs/l3parcels/MassGIS_L3_Parcels_gdb.zip
  #' 
  #' @param path Path to MassGIS Parcels GDB.
  #' @param test Whether to only load a sample subset of rows.
  #' @export
  q <- "SELECT LOC_ID, TOWN_ID FROM L3_TAXPAR_POLY"
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

load_assess_all_vintages <- function(path, town_ids=NULL, ma_munis) {
  #' Load assessing table from MassGIS complete vintage tax parcel collection.
  #'
  #' @param path Path to collection ofc MassGIS Parcels GDBs.
  #' @param town_ids list of town IDs
  #' @export
  
  vintages <- load_gdb_vintages(path)
  if (!is.null(town_ids)) {
    town_ids <- town_ids |>
      stringr::str_pad(3, side = "left", pad = "0")
    print(town_ids)
    vintages <- vintages |>
      dplyr::filter(muni_id %in% town_ids)
  }
  cols <- c(
    # Property metadata.
    "PROP_ID", "LOC_ID", "FY", 
    # Parcel address.
    "SITE_ADDR", "ADDR_NUM", "FULL_STR", "TOWN_ID", "ZIP", 
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
  all <- list()
  for (row in 1:nrow(vintages)) {
    muni_id <- vintages |> dplyr::slice(row) |> dplyr::pull(muni_id)
    cy <- vintages |> dplyr::slice(row) |> dplyr::pull(cy)
    fy <-vintages |> dplyr::slice(row) |> dplyr::pull(fy)
    
    message(glue::glue("Loading assessors table for muni {muni_id}."))
    
    file <- glue::glue("M{muni_id}_parcels_CY{cy}_FY{fy}_sde.gdb")
    q <- stringr::str_c("SELECT", cols, glue::glue("FROM M{muni_id}Assess"), sep = " ")
    all[[muni_id]] <- sf::st_read(
      glue::glue("{path}{file}"),
      query = q,
      quiet = TRUE
    )
  }
  df <- dplyr::bind_rows(all) |>
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
    dplyr::select(-c(addr_num, full_str)) |>
    std_use_codes("use_code") |>
    # All parcels are in MA, in the US...
    dplyr::mutate(
      state = "MA", country = "US"
    ) |>
    std_fill_missing_units() |>
    std_residential_flag() |>
    dplyr::left_join(sf::st_drop_geometry(ma_munis), by = dplyr::join_by(town_id)) |>
    dplyr::rename(muni = pl_name)
}

load_assess <- function(path = ".", town_ids=NULL, ma_munis) {
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
      "SITE_ADDR", "ADDR_NUM", "FULL_STR", "TOWN_ID", "ZIP", 
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
  if (!is.null(town_ids)) {
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
    dplyr::select(-c(addr_num, full_str)) |>
    std_use_codes("use_code") |>
    # All parcels are in MA, in the US...
    dplyr::mutate(
      state = "MA", country = "US"
    ) |>
    std_fill_missing_units() |>
    std_residential_flag() |>
    dplyr::left_join(sf::st_drop_geometry(ma_munis), by = dplyr::join_by(town_id)) |>
    dplyr::rename(muni = pl_name)
}

load_shp_from_zip <- function(path, layer) {
  path <- stringr::str_c("/vsizip/", path, "/", layer)
  sf::st_read(path, quiet=TRUE)
}

load_shp_from_remote_zip <- function(url, shpfile, crs = CRS) {
  message(
    glue::glue("Downloading {shpfile} from {url}...")
  )
  temp <- base::tempfile(fileext = ".zip")
  httr::GET(
    paste0(url), 
    httr::write_disk(temp, overwrite = TRUE)
  )
  load_shp_from_zip(temp, shpfile) |>
    dplyr::rename_with(tolower) |>
    sf::st_transform(crs)
}

load_address_points <- function(ma_munis, parcels, crs = CRS, test = FALSE) {
  
  muni_ids <- ma_munis |>
    dplyr::pull(town_id)
  
  if (test) {
    muni_ids <- c('274', '035')
  }
  muni_ids <- muni_ids |>
    stringr::str_pad(3, side = "left", pad = "0")
  
  url_base <- "https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/mad/town_exports/addr_pts/"
  
  all <- list()
  for (id in muni_ids) {
    if (id == '035') {
      # Boston handler---MassGIS does not maintain the Boston Address list.
      all[[id]] <- load_from_arc("b6bffcace320448d96bb84eabb8a075f_0", crs) |>
        dplyr::filter(!is.na(street_body) & !is.na(street_full_suffix)) |>
        dplyr::mutate(
          muni = "BOSTON",
          is_range = as.logical(is_range),
          addr_body = stringr::str_to_upper(stringr::str_c(street_body, street_full_suffix, sep = " ")),
          state = "MA",
        ) |>
        dplyr::select(
          addr_body,
          state,
          muni,
          postal = zip_code,
          addr_num = street_number,
          addr_start = range_from,
          addr_end = range_to,
          is_range
        ) |>
        dplyr::mutate(
          dplyr::across(
            c(addr_body, postal),
            ~ dplyr::case_when(
              . == "" ~ NA_character_,
              .default = .
            )
          ),
          dplyr::across(
            c(addr_num, addr_start, addr_end),
            ~ stringr::str_replace_all(
              .,
              " 1 ?\\/ ?2", "\\.5"
            )
          ),
          dplyr::across(
            c(addr_start, addr_end),
            ~ as.numeric(stringr::str_remove_all(., "[A-Z]"))
          ),
          range_fix = !is_range & stringr::str_detect(addr_num, "[0-9\\.]+[A-Z]{0,1} ?- ?[0-9\\.]+[A-Z]{0,1}"),
          addr_start_temp = dplyr::case_when(
            range_fix ~ abs(as.numeric(stringr::str_remove_all(stringr::str_extract(addr_num, "^[0-9\\.]+"), "[A-Z]")))
          ),
          addr_end_temp = dplyr::case_when(
            range_fix ~ abs(as.numeric(stringr::str_remove_all(stringr::str_extract(addr_num, "(?<=[- ]{1,2})[0-9\\.]+[A-Z]{0,1}(?= ?$)"), "[A-Z]")))
          ),
          viable_range = addr_start_temp <= addr_end_temp,
          addr_start = dplyr::case_when(
            viable_range ~ addr_start_temp,
            .default = addr_start
          ),
          addr_end = dplyr::case_when(
            viable_range ~ addr_end_temp,
            .default = addr_end
          ),
          dplyr::across(
            c(addr_start, addr_end),
            ~ dplyr::case_when(
              is.na(addr_start) & !is_range ~ abs(as.numeric(stringr::str_remove_all(addr_num, "[A-Z]"))),
              .default = .
            )
          )
        ) |>
        dplyr::filter(!is.na(addr_start) & !is.na(addr_end)) |>
        dplyr::select(-c(is_range, viable_range, range_fix, addr_start_temp, addr_end_temp))
    } else {
      filename <- glue::glue("AddressPts_M{id}")
      url <- glue::glue("{url_base}{filename}.zip")
      all[[id]] <- load_shp_from_remote_zip(
          url,
          shpfile = glue::glue("{filename}.shp"),
          crs = crs
        ) |>
        dplyr::filter(!is.na(streetname)) |>
        dplyr::mutate(
          state = "MA",
          num1 = dplyr::case_when(
            num1_sfx == "1/2" ~ num1 + 0.5,
            .default = num1
          ),
          num2 = dplyr::case_when(
            num1_sfx == "1/2" ~ num2 + 0.5,
            .default = num2
          ),
        ) |>
        dplyr::left_join(
          sf::st_drop_geometry(ma_munis),
          dplyr::join_by(addrtwn_id == town_id)
        ) |>
        dplyr::select(
          addr_num,
          addr_body = streetname,
          state,
          muni = pl_name,
          postal = zipcode,
          addr_start = num1,
          addr_end = num2,
          -addrtwn_id
        ) |>
        dplyr::mutate(
          addr_end = dplyr::case_when(
            addr_end == 0 ~ addr_start,
            addr_end <= addr_start ~ addr_start,
            .default = addr_end
          )
        )
    }
  }
  
  df <- dplyr::bind_rows(all) |>
    std_flow_strings("addr_body") |>
    std_street_types("addr_body") |>
    std_directions("addr_body") |>
    dplyr::mutate(
      addr_num = dplyr::case_when(
        addr_start != addr_end ~ stringr::str_c(addr_start, addr_end, sep = " - "),
        .default = addr_num
      )
    ) |>
    tibble::rowid_to_column("id")
  
  df_id <- df |>
    dplyr::select(id)
  
  parcels_id <- parcels |>
    dplyr::select(loc_id)
  
  relation <- parcels_id |>
    sf::st_join(df_id, join = sf::st_contains_properly, left = FALSE) |>
    sf::st_drop_geometry()
  
  df <- df |>
    dplyr::left_join(relation, by = dplyr::join_by(id))
  
  df |>
    sf::st_drop_geometry() |>
    dplyr::group_by(addr_body, state, muni, postal, loc_id) |>
    dplyr::summarize(
      addr_start = min(addr_start),
      addr_end = max(addr_end),
      addr_count = dplyr::n()
    ) |>
    dplyr::ungroup() |>
    dplyr::left_join(
      df |> 
        dplyr::select(-c(addr_start, addr_end, id)),
      multiple = "first",
      by = dplyr::join_by(addr_body, state, muni, postal, loc_id)
    ) |>
    dplyr::mutate(
      even = dplyr::case_when(
        floor(addr_start) %% 2 == 0 ~ TRUE,
        floor(addr_start) %% 2 == 1 ~ FALSE,
        .default = NA
      )
    ) |>
    sf::st_set_geometry("geometry") |>
    tibble::rowid_to_column("id")
}

load_places <- function(crs = CRS, ma_munis) {
  
  ma_munis <- dplyr::select(ma_munis, muni_name = pl_name)
  
  df <- load_shp_from_remote_zip(
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
    dplyr::filter(!(pl_name == "CAMBRIDGE" & muni_name == "WORCESTER")) |>
    # Remove placenames that appear in multiple places.
    # These are not useful for deduplication.
    dplyr::add_count(pl_name) |>
    dplyr::filter(n == 1) |>
    dplyr::select(-n)
}

load_from_arc <- function(dataset, crs = CRS) {
  prefix <- "https://opendata.arcgis.com/api/v3/datasets/"
  suffix <- "/downloads/data?format=geojson&spatialRefId=4326&where=1=1"
  sf::st_read(
    glue::glue("{prefix}{dataset}{suffix}")
  ) |>
    dplyr::rename_with(tolower) |>
    sf::st_transform(crs)
}

load_conn <- function(remote=FALSE) {
  if (remote) {
    dbname <- "DB_NAME"
    host <- "DB_HOST"
    port <- "DB_PORT"
    user <- "DB_USER"
    password <- "DB_PASS"
  } else {
    dbname <- "LOCAL_DB_NAME"
    host <- "LOCAL_DB_HOST"
    port <- "LOCAL_DB_PORT"
    user <- "LOCAL_DB_USER"
    password <- "LOCAL_DB_PASS"
  }
  DBI::dbConnect(
    RPostgres::Postgres(),
    dbname = Sys.getenv(dbname),
    host = Sys.getenv(host),
    port = Sys.getenv(port),
    user = Sys.getenv(user),
    password = Sys.getenv(password),
    sslmode = "allow"
  )
}

load_check_for_table <- function(conn, table) {
  if(table %in% DBI::dbListTables(conn)) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}

load_postgis_ingest <- function(df, conn, layer_name, overwrite=FALSE) {
  sf::st_write(
    df, 
    dsn = conn, 
    layer = layer_name,
    delete_layer = overwrite
  )
}

load_postgis_read <- function(conn, layer_name) {
  sf::st_read(
    dsn = conn,
    layer = layer_name,
    quiet = TRUE
  )
}

load_layer_flow <- function(conn, layer_name, loader, refresh=FALSE) {
  table_exists <- load_check_for_table(conn, layer_name)
  if(!table_exists | refresh) {
    if(!table_exists) {
      message(glue::glue("Table {layer_name} does not exist in PostGIS."))
    } else {
      message(glue::glue("Table {layer_name} exists, but user specified refresh."))
    }
    df <- loader
    message(glue::glue("Writing {layer_name} to PostGIS database."))
    df <- df |>
      load_postgis_ingest(conn, layer_name=layer_name, overwrite=refresh)
  } else {
    message(glue::glue("Reading {layer_name} from PostGIS database."))
    df <- load_postgis_read(conn, layer_name=layer_name)
  }
  df
}

load_ma_munis <- function(crs = CRS) {
  #' Downloads MA municipalities from MassGIS ArcGIS Hub.
  #'
  #' @param crs Coordinate reference system for output.
  #' @export
  message("Downloading Massachusetts municipal boundaries...")
  load_from_arc("43664de869ca4b06a322c429473c65e5_0", crs = crs) |>
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

load_zips <- function(crs = CRS, ma_munis, threshold = 0.95) {
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
    dplyr::ungroup() |>
    dplyr::mutate(
      ma = state == "MA",
      ma_unambig = ma & state_unambig
    )
  
  zips_ma <- zips |>
    dplyr::filter(ma) |>
    sf::st_transform(crs)
  
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
  ma_unambig_zip_from_muni <- overlap_analysis(ma_munis_clip,
                                               zips_ma_clip,
                                               threshold = threshold) |>
    dplyr::select(zip, muni_unambig_from = pl_name)
  
  zips <- zips |>
    dplyr::left_join(ma_unambig_zip_from_muni, by=dplyr::join_by(zip))

  # Identify cases where munis can be unambiguously assigned
  # from ZIPS (i.e., where the vast majority of a muni is contained w/in
  # a single zip).
  ma_unambig_muni_from_zip <- overlap_analysis(zips_ma_clip,
                                               ma_munis_clip,
                                               threshold = threshold) |>
    dplyr::select(zip, muni_unambig_to = pl_name)
  
  zips |>
    dplyr::left_join(ma_unambig_muni_from_zip, by=dplyr::join_by(zip))
}

load_filings <- function(ma_munis, bos_neighs, town_ids = FALSE, crs = CRS) {
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