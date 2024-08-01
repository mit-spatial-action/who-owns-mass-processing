# Load Helpers ====

load_muni_table <- function(path, file="muni_ids.csv") { 
  readr::read_csv(
    file.path(path, file), 
    progress=TRUE,
    show_col_types = FALSE)
}

load_test_muni_ids <- function(muni_ids, path, quiet=FALSE) {
  #' Test Validity of Muni IDs and Pad
  #' 
  #' Tests whether provided municipality ids are valid (`stop()` if they are 
  #' not) and pads them out to three characters with zeroes to the left.
  #'
  #' If need to create file...
  #' MUNIS |> 
  #'     sf::st_drop_geometry() |> 
  #'     dplyr::select(muni_id, muni) |>
  #'     readr::write_csv("data/muni_ids.csv")
  #'
  #' @param muni_ids Vector of municipality IDs.
  #' @param path Path to data directory.
  #' @param file CSV file containing municipality IDs.
  #' 
  #' @return A transformed vector of municipality IDs.
  #' 
  #' @export
  ids <- load_muni_table(path)  |>
    dplyr::pull(muni_id)
  if (is.null(muni_ids)) {
    muni_ids <- std_pad_muni_ids(ids)
  } else if (muni_ids == "hns") {
    muni_ids <- c("163", "057", "044", "095", "035", "201", "274", "049")
  } else {
    if(!all(std_pad_muni_ids(muni_ids) %in% ids)) {
      stop("VALIDATION: Provided invalid test municipality ids. ❌❌❌")
    } else {
      if(!quiet) {
        util_log_message("VALIDATION: Municipality IDs are valid. 🚀🚀🚀")
      }
    }
    muni_ids <- std_pad_muni_ids(muni_ids)
  }
  muni_ids
}

load_vintage_select <- function(gdb_path, muni_ids=NULL, recent = 3) {
  #' Select Parcel Vintage
  #' 
  #' Decide which vintage to use per MA municipality based on a simple 
  #' algorithm.
  #'
  #' @param gdb_path Path to collection of MassGIS parcel geodatabases.
  #' @param recent Integer. How many years back algorithm should look in
  #'    identifying most complete vintages.
  #' 
  #' @return A dataframe containing both `fy` and `cy`. Both are necessary
  #'    because (strangely), there is no fixed relationship between them.
  #' 
  #' @export
  
  gdb_list <- list.files(gdb_path)
  vintages <- data.frame(
    muni_id = stringr::str_extract(gdb_list, "(?<=M)[0-9]{3}"),
    fy = as.numeric(stringr::str_extract(gdb_list, "(?<=_FY)[0-9]{2}")) + 2000,
    cy = as.numeric(stringr::str_extract(gdb_list, "(?<=_CY)[0-9]{2}")) + 2000
  )
  
  if (!is.null(muni_ids)) {
    vintages <- vintages |>
      dplyr::filter(muni_id %in% muni_ids)
  }
  
  most_complete_recent <- vintages |>
    dplyr::filter(fy > lubridate::year(Sys.Date()) - recent) |>
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
    dplyr::filter(count == 1 | (count > 1 & cy == max(cy)))

  unmatched <- vintages |>
    dplyr::filter(!load & min(year_diff) > 0)
  
  if (nrow(unmatched) > 0) {
    unmatched <- unmatched |>
      dplyr::mutate(
        min_diff = year_diff == min(year_diff)
      ) |>
      dplyr::filter(min_diff) |>
      dplyr::mutate(
        count = dplyr::n()
      ) |>
      dplyr::filter(count == 1 | (count > 1 & cy == max(cy))) |>
      dplyr::select(-min_diff)
  }
  exact_matches |>
    dplyr::bind_rows(unmatched) |>
    dplyr::ungroup() |>
    dplyr::select(-c(year_diff, load, count))
}

load_gdb_is_file <- function(path) {
  file <- file.exists(path) && !dir.exists(path)
  if (file) {
    util_log_message("VALIDATION: Single geodatabase provided.")
  } else {
    util_log_message("VALIDATION: Folder of geodatabases provided.")
  }
  file
}

load_rename_geometry <- function(g, name){
  # Thank you StackExchange GIS user Spacedman!
  # https://gis.stackexchange.com/questions/386584/sf-geometry-column-naming-differences-r
  current <- attr(g, "sf_column")
  names(g)[names(g)==current] <- name
  sf::st_geometry(g) <- name
  g
}

# Load from Services ====

load_shp_from_zip <- function(path, shpfile, crs) {
  #' Load Shapefile from .zip file.
  #' 
  #' Reads a shapefile from a .zip file without requiring that the file be
  #' unarchived. Uses vsizip from GDAL. 
  #' 
  #' N.B., seems like the kind of thing that could be OS dependent. Only tested
  #' on macOS.
  #'
  #' @param path Path to .zip file.
  #' @param shpfile Name of .shp file to be loaded.
  #' @param crs Coordinate reference system for resulting `sf` data frame.
  #' 
  #' @return `sf` data frame.
  #' 
  #' @export
  
  path <- stringr::str_c("/vsizip/", path, "/", shpfile)
  sf::st_read(path, quiet=TRUE) |>
    sf::st_transform(crs) |>
    dplyr::rename_with(tolower)
}

load_shp_from_remote_zip <- function(url, shpfile, crs) {
  #' Load Shapefile from a Remote .zip
  #' 
  #' Downloads a remote ZIP file and reads a shapefile from a remote .zip file 
  #' without requiring that the file be unarchived. Uses vsizip from GDAL. 
  #' 
  #' N.B., seems like the kind of thing that could be OS dependent. Only tested
  #' on macOS.
  #'
  #' @param url URL of remote ZIP>
  #' @param shpfile Name of .shp file to be loaded.
  #' @param crs Coordinate reference system for resulting `sf` data frame.
  #' 
  #' @return A dataframe containing both `fy` and `cy`. Both are necessary
  #'    because (strangely), there is no fixed relationship between them.
  #' 
  #' @export
  
  temp <- base::tempfile(fileext = ".zip")
  on.exit(file.remove(temp))
  httr::GET(
    paste0(url), 
    httr::write_disk(temp, overwrite = TRUE)
  )
  load_shp_from_zip(temp, shpfile, crs)
}

load_from_arc <- function(dataset, crs) {
  #' Load Shapefile from .zip file.
  #' 
  #' Reads spatial dataset from ArcGIS open data portal.
  #'
  #' @param dataset ID of ArcGIS layer (generally listed after
  #'    https://opendata.arcgis.com/api/v3/datasets/).
  #' @param crs Coordinate reference system for resulting `sf` data frame.
  #' 
  #' @return `sf` data frame.
  #' 
  #' @export
  
  prefix <- "https://opendata.arcgis.com/api/v3/datasets/"
  suffix <- "/downloads/data?format=geojson&spatialRefId=4326&where=1=1"
  sf::st_read(
    glue::glue("{prefix}{dataset}{suffix}"),
    quiet=TRUE
  ) |>
    dplyr::rename_with(tolower) |>
    sf::st_transform(crs)
}

# Database Functions ====

load_conn <- function(remote=FALSE) {
  #' Load DBMS Connection
  #' 
  #' Creates connection to remote or local PostGIS connection. Requires a
  #' variables to be set in `.Renviron`.
  #'
  #' @param remote If `TRUE`, creates connection to remote db. If `FALSE`,
  #'    creates connection to local PostGIS instance.
  #' 
  #' @return dbConnect() returns an S4 object that inherits from DBIConnection.
  #'    This object is used to communicate with the database engine.
  #' 
  #' @export
  
  if (remote) {
    dbname <- "REMOTE_DB_NAME"
    host <- "REMOTE_DB_HOST"
    port <- "REMOTE_DB_PORT"
    user <- "REMOTE_DB_USER"
    password <- "REMOTE_DB_PASS"
  } else {
    dbname <- "DB_NAME"
    host <- "DB_HOST"
    port <- "DB_PORT"
    user <- "DB_USER"
    password <- "DB_PASS"
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

load_check_for_tables <- function(conn, table_names) {
  #' Check Whether Database Table Exists
  #' 
  #' Checks whether a specified table exists in a PostGIS database.
  #'
  #' @param conn A `DBIConnection`.
  #' @param table_name Name of table to check for existence of.
  #' 
  #' @return `TRUE` if table exists, `FALSE` if it does not.
  #' 
  #' @export
  
  all(table_names %in% DBI::dbListTables(conn))
}

load_column_name_lookup <- function(table_name) {
  if (table_name == "init_assess") {
    col <- "site_muni_id"
  } else if (table_name %in% c("init_addresses", "init_parcels")) {
    col <- "muni_id"
  } else {
    stop("Invalid table to check for present muni IDs.")
  }
  col
}

load_check_for_muni_ids <- function(conn, table_name, muni_ids) {
  col <- load_column_name_lookup(table_name)
  cols <- conn |>
    DBI::dbGetQuery(
      glue::glue("SELECT * FROM {table_name} WHERE FALSE")
    ) |> 
    names()
  
  if(col %in% cols) {
    muni_ids_present <- conn |>
      DBI::dbGetQuery(
        glue::glue("SELECT DISTINCT {col} AS m FROM {table_name}")
      ) |>
      dplyr::pull(m)
    
    result <- all(muni_ids %in% muni_ids_present)
  } else {
    result <- FALSE
  }
  result
}

load_write <- function(df, conn, table_name, other_formats = c(), overwrite=FALSE, quiet=FALSE, dir=RESULTS_PATH) {
  #' Write Table to PostGIS
  #' 
  #' Writes table to PostGIS.
  #'
  #' @param conn A `DBIConnection`.
  #' @param table_name Name of table to write.
  #' @overwrite If `TRUE`, overwrite table.
  #' 
  #' @return Unmodified data frame.
  #' 
  #' @export
  if(!quiet) {
    util_log_message(
      glue::glue(
        "INPUT/OUTPUT: Writing table '{table_name}' to PostGIS database."
      )
    )
  }
  if ("sf" %in% class(df)) {
    sf::st_write(
      df, 
      dsn=conn, 
      layer=table_name,
      delete_layer=overwrite
    )
  } else {
    DBI::dbWriteTable(
      conn=conn,
      name=table_name,
      value=df,
      overwrite=overwrite
    )
  }
  path <- stringr::str_c(dir, table_name, sep="/")
  if ("csv" %in% other_formats) {
    file <- stringr::str_c(path, "csv", sep=".")
    if(!quiet) {
      util_log_message(
        glue::glue(
          "INPUT/OUTPUT: Writing '{table_name}' to {file} as well."
        )
      )
    }
    readr::write_csv(df, file, append=!overwrite)
  }
  if ("gpkg" %in% other_formats) {
    file <- stringr::str_c(glue::glue("{dir}/{dir}"), "gpkg", sep=".")
    if(!quiet) {
      util_log_message(
        glue::glue(
          "INPUT/OUTPUT: Writing '{table_name}' to layer '{table_name}' in {file} as well."
        )
      )
    }
    sf::st_write(df, file, layer=table_name, delete_layer=overwrite)
  }
  if ("r" %in% other_formats) {
    file <- stringr::str_c(path, "Rda", sep=".")
    if(!quiet) {
      util_log_message(
        glue::glue(
          "INPUT/OUTPUT: Writing '{table_name}' to to {file} as well."
        )
      )
    }
    save(df, file=file)
  }
  df
}

load_postgis_read <- function(conn, table_name, muni_ids = NULL) {
  #' Read table from PostGIS.
  #' 
  #' Reads table from PostGIS.
  #'
  #' @param conn A `DBIConnection`.
  #' @param table_name Name of table to write.
  #' 
  #' @return Table as a data frame.
  #' 
  #' @export
  q <- glue::glue("SELECT * FROM {table_name}")
  if (!is.null(muni_ids)) {
    col <- load_column_name_lookup(table_name)
    q <- q |>
      stringr::str_c(
        glue::glue(" WHERE {col} ~ '("),
        stringr::str_c(muni_ids, collapse="|"),
        ")'",
        sep = ""
      )
  }
  geo_tables <- DBI::dbGetQuery(
      conn, 
      "SELECT * FROM geometry_columns"
      ) |> 
    dplyr::pull(f_table_name)
  
  if (table_name %in% geo_tables) {
    sf::st_read(
      dsn = conn,
      query = q,
      quiet = TRUE
    )
  } else {
    DBI::dbGetQuery(
      conn = conn,
      statement = q
    )
  }
}

load_read_write <- function(conn, table_name, loader, muni_ids=NULL, refresh=FALSE, quiet=FALSE) {
  #' Ingest table to or read table from PostGIS.
  #' 
  #' Either writes or reads a table, depending on parameter settings and table
  #' existence.
  #'
  #' @param conn A `DBIConnection`.
  #' @param table_name Name of table to write or read.
  #' @param loader A dataframe or function to read dataframe.
  #' @param refresh Whether to re-ingest data from source.
  #' 
  #' @return Unmodified data frame.
  #' 
  #' @export
  
  # Disconnect on function exit.
  on.exit(DBI::dbDisconnect(conn))
  
  table_exists <- load_check_for_tables(conn, table_name)
  if (!is.null(muni_ids) & table_exists) {
    muni_ids_exist <- load_check_for_muni_ids(conn, table_name, muni_ids)
  } else {
    muni_ids_exist <- TRUE
  }
  
  if(!table_exists | refresh) {
    if(!quiet){
      if(!table_exists) {
        util_log_message(
          glue::glue(
            "VALIDATION: Table '{table_name}' does not exist in PostGIS."
          )
        )
      } else {
        util_log_message(
          glue::glue(
            "VALIDATION: Table '{table_name}' exists in PostGIS, but user specified refresh."
          )
        )
      }
    }
    df <- loader |>
      load_write(conn, table_name=table_name, overwrite=refresh, quiet=quiet)
  } else if (muni_ids_exist) {
    if(!quiet) {
      util_log_message(
        glue::glue(
          "INPUT/OUTPUT: Reading table '{table_name}' from PostGIS database."
        )
      )
    }
    df <- load_postgis_read(conn, table_name=table_name, muni_ids)
  } else {
    if(!quiet) {
      util_log_message(
        glue::glue(
          "INPUT/OUTPUT: Table '{table_name}' exists, but not all municipalities were found. Refreshing."
        )
      )
    }
    df <- loader |>
      load_write(conn, table_name=table_name, overwrite=TRUE, quiet=quiet)
  }
  df
}

# Preprocess Layers ====

load_generic_preprocess <- function(df, cols, id_cols) {
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

load_boston_address_preprocess <- function(df, cols, bos_id='035') {
  df <- df |>
    dplyr::filter(!is.na(street_body) & !is.na(street_full_suffix)) |>
    dplyr::mutate(
      muni = "BOSTON",
      muni_id = bos_id,
      is_range = dplyr::case_when(
        !is.na(is_range) ~ as.logical(is_range),
        .default = FALSE
      ),
      body = dplyr::case_when(
        !is.na(street_body) & !is.na(street_full_suffix) ~ stringr::str_to_upper(stringr::str_c(street_body, street_full_suffix, sep = " ")),
        .default = NA_character_
      ),
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
    load_generic_preprocess(cols) |>
    dplyr::mutate(
      dplyr::across(
        c(start, end),
        ~ dplyr::case_when(
          !is.na(.x) ~ stringr::str_replace_all(
            .,
            " 1 ?\\/ ?2", "\\.5"
          ),
          .default = NA_character_
        )
      ),
      dplyr::across(
        c(start, end),
        ~ dplyr::case_when(
          !is.na(.x) & stringr::str_detect(.x, "[0-9]") ~ as.numeric(stringr::str_remove_all(., "[A-Z]")),
          .default = NA
        )
      ),
      range_fix = dplyr::case_when(
        !is.na(num) & !is_range ~ stringr::str_detect(num, "[0-9\\.]+ ?[A-Z]{0,2} ?- ?[0-9\\.]+ ?[A-Z]{0,1}"),
        .default = FALSE
      ),
      start_temp = dplyr::case_when(
        range_fix & !is.na(num) ~ abs(as.numeric(stringr::str_remove_all(stringr::str_extract(num, "^[0-9\\.]+"), "[A-Z]"))),
        .default = NA
      ),
      end_temp = dplyr::case_when(
        range_fix & !is.na(num) ~ abs(as.numeric(stringr::str_remove_all(stringr::str_extract(num, "(?<=[- ]{1,2})[0-9\\.]+ ?[A-Z]{0,1}(?= ?$)"), "[A-Z]"))),
        .default = NA
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
          is.na(start) & !is_range & !is.na(num) ~ abs(as.numeric(stringr::str_remove_all(num, "[A-Z]"))),
          .default = .x
        )
      )
    ) |>
    dplyr::filter(!is.na(start) & !is.na(end)) |>
    dplyr::select(-c(is_range, num, range_fix, start_temp, end_temp)) |>
    suppressWarnings()
}

load_nonboston_address_preprocess <- function(df, cols, munis) {
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
    load_generic_preprocess(cols) |>
    dplyr::mutate(
      end = dplyr::case_when(
        end == 0 ~ start,
        end <= start ~ start,
        .default = end
      )
    )
}

load_assess_preprocess <- function(df, path) {
  util_log_message(glue::glue("LOADING: Preprocessing Assessors' Tables."))
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
    load_generic_preprocess(
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

# Load Layers from Source ====

load_block_groups <- function(state, crs, year=NULL, quiet=FALSE) {
  if (!quiet) {
    util_log_message(glue::glue("INPUT/OUTPUT: Downloading {state} block groups."))
  }
  tigris::block_groups(state=state, year=year) |>
    sf::st_transform(crs) |>
    dplyr::select(id = GEOID) |>
    suppressMessages()
}

load_tracts <- function(state, crs, year=NULL, quiet=FALSE) {
  if (!quiet) {
    util_log_message(glue::glue("INPUT/OUTPUT: Downloading {state} census tracts."))
  }
  tigris::tracts(state=state, year=year) |>
    sf::st_transform(crs) |>
    dplyr::select(id = GEOID)  |>
    suppressMessages()
}

load_assess <- function(path, gdb_path, muni_ids=NULL, quiet=FALSE) {
  #' Load Assessors' Tables from MassGIS Parcel GDB(s)
  #' 
  #' Load assessing table from MassGIS tax parcel collection, presented either
  #' as a single GDB or a folder of many vintages.
  #' See https://www.mass.gov/info-details/massgis-data-property-tax-parcels
  #'
  #' @param path Path to data folder.
  #' @param gdb_path Path to collection of MassGIS Parcel GDBs or single GDB.
  #' @param muni_ids Vector of municipality IDs.
  #' @param quiet If `TRUE`, print incremental loading messages.
  #' 
  #' @return A data frame of assessors' records for specified municipalities.
  #' 
  #' @export
  
  cols <- c(
    # Property metadata.
    "PROP_ID AS SITE_ID", "LOC_ID AS SITE_LOC_ID", "FY AS SITE_FY", 
    # Parcel address.
    "SITE_ADDR", "ADDR_NUM", "FULL_STR", "TOWN_ID AS SITE_MUNI_ID", 
    "ZIP AS SITE_POSTAL", 
    # Owner name and address.
    "OWNER1 AS OWN_NAME", "OWN_ADDR", "OWN_CITY AS OWN_MUNI", "OWN_STATE", 
    "OWN_ZIP AS OWN_POSTAL", "OWN_CO AS OWN_COUNTRY",
    # Last sale date and price.
    "LS_DATE AS SITE_LS_DATE", "LS_PRICE AS SITE_LS_PRICE",
    # Necessary to estimate unit counts.
    "BLD_AREA AS SITE_BLD_AREA", "RES_AREA AS SITE_RES_AREA", "UNITS AS SITE_UNITS",
    # Assessed values.
    "BLDG_VAL AS SITE_BLDG_VAL", "LAND_VAL AS SITE_LAND_VAL", "TOTAL_VAL AS SITE_TOT_VAL",
    # Land use code.
    "USE_CODE AS SITE_USE_CODE"
  )
  cols <- stringr::str_c(cols, collapse = ", ")
  
  if(!quiet) {
    util_log_message(glue::glue("INPUT/OUTPUT: Loading assessors' records."))
  }
  
  single_gdb <- load_gdb_is_file(gdb_path)
  
  if (single_gdb) {
    q <- stringr::str_c("SELECT", cols, "FROM L3_ASSESS", sep = " ")
    if (!is.null(muni_ids)) {
      q <- stringr::str_c(
        q,
        "WHERE TOWN_ID IN (",
        paste(as.integer(muni_ids), collapse = ", "),
        ")",
        sep = " "
      )
    }
    
    df <- sf::st_read(
        gdb_path,
        query = q,
        quiet = TRUE
      )
  } else {
    if (!quiet) {
      util_log_message(glue::glue("INPUT/OUTPUT: Reading from collection of GDBs.")) 
    }
    
    vintages <- load_vintage_select(gdb_path, muni_ids)
    
    all <- list()
    for (row in 1:nrow(vintages)) {
      
      muni_id <- vintages[[row, 'muni_id']]
      cy <- vintages[[row, 'cy']] - 2000
      fy <- vintages[[row, 'fy']] - 2000
      
      if (!quiet) {
        util_log_message(glue::glue("INPUT/OUTPUT: Loading assessors records for muni {muni_id} (FY{fy}, CY{cy}).")) 
      }
      
      file <- glue::glue("M{muni_id}_parcels_CY{cy}_FY{fy}_sde.gdb")
      if (!file.exists(file.path(gdb_path, file))) {
        stop("You've passed an invalid GDB directory.")
      }
      q <- stringr::str_c("SELECT", cols, glue::glue("FROM M{muni_id}Assess"), sep = " ")
      
      all[[muni_id]] <- sf::st_read(
        file.path(gdb_path, file),
        query = q,
        quiet = TRUE
      )
    }
  }
  
  dplyr::bind_rows(all) |>
    dplyr::rename_with(stringr::str_to_lower) |>
    load_assess_preprocess(path)
}

load_parcels <- function(gdb_path, crs, assess, block_groups, muni_ids=NULL, quiet=FALSE) {
  #' Load Parcels from MassGIS Parcel GDB(s)
  #' 
  #' Load parcels from MassGIS tax parcel collection, presented either
  #' as a single GDB or a folder of many vintages.
  #' See https://www.mass.gov/info-details/massgis-data-property-tax-parcels
  #'
  #' @param gdb_path Path to collection of MassGIS Parcel GDBs or single GDB.
  #' @param crs Coordinate reference system for output.
  #' @param muni_ids Vector of municipality IDs.
  #' @param quiet If `TRUE`, print incremental loading messages.
  #' 
  #' @return An `sf` dataframe containing MULTIPOLYGON parcels for specified 
  #' municipalities.
  #' 
  #' @export
  
  if (!quiet){
    util_log_message("INPUT/OUTPUT: Loading parcels.")
  }
  
  
  single_gdb <- load_gdb_is_file(gdb_path)
  
  if (single_gdb) {
    q <- "SELECT LOC_ID, TOWN_ID AS MUNI_ID FROM L3_TAXPAR_POLY"
    
    if (!is.null(muni_ids)) {
      q <- stringr::str_c(
        q, 
        "WHERE TOWN_ID IN (", 
        stringr::str_c(as.integer(muni_ids), collapse=", "), 
        ")",
        sep = " "
      )
    }
    
    sf::st_read(gdb_path, query = q, quiet = TRUE)
  } else {
    vintages <- load_vintage_select(gdb_path, muni_ids)
    
    all <- list()
    for (row in 1:nrow(vintages)) {
      
      muni_id <- vintages[[row, 'muni_id']]
      cy <- vintages[[row, 'cy']] - 2000
      fy <- vintages[[row, 'fy']] - 2000
      
      if (!quiet) {
        util_log_message(glue::glue("INPUT/OUTPUT: Loading parcels for muni {muni_id} (FY{fy}, CY{cy}).")) 
      }
      
      file <- glue::glue("M{muni_id}_parcels_CY{cy}_FY{fy}_sde.gdb")
      q <- glue::glue("SELECT LOC_ID, TOWN_ID AS MUNI_ID FROM M{muni_id}TaxPar")
      
       sdf <- sf::st_read(
        file.path(gdb_path, file), 
        query = q, 
        quiet = TRUE
        ) |>
         load_rename_geometry("geometry")
        
       all[[muni_id]] <- sdf
    }
    df <- dplyr::bind_rows(all)
    rm(all)
  }
  df <- df |>
    dplyr::rename_with(stringr::str_to_lower) |>
    dplyr::semi_join(assess, by=dplyr::join_by(loc_id==site_loc_id))

  df <- df |>
    # Reproject to specified CRS.
    sf::st_transform(crs) |>
    # Cast from MULTISURFACE to MULTIPOLYGON.
    dplyr::mutate(
      muni_id = std_pad_muni_ids(muni_id),
      geometry = sf::st_cast(geometry, "MULTIPOLYGON")
    )

  df |>
    dplyr::mutate(
      point = sf::st_point_on_surface(geometry)
    ) |>
    sf::st_set_geometry("point") |>
    sf::st_join(
      block_groups |>
        dplyr::select(block_group_id = id)
    ) |>
    sf::st_set_geometry("geometry") |>
    dplyr::select(-point) |>
    dplyr::mutate(
      tract_id = stringr::str_sub(block_group_id, start = 1L, end = 11L)
    )
}

load_addresses <- function(path, parcels, crs, muni_ids=NULL, quiet=FALSE) {
  #' Load MassGIS Master Address Data Points
  #' 
  #' Load Basic Address Points data prodoct from MassGIS Master Address Data.
  #' See https://www.mass.gov/info-details/massgis-data-master-address-data-basic-address-points
  #'
  #' @param parcels Municipalities, as loaded by `load_parcels()`.
  #' @param crs Coordinate reference system for output.
  #' @param muni_ids Vector of municipality IDs.
  #' @param quiet If `TRUE`, print incremental loading messages.
  #' 
  #' @return An `sf` dataframe containing POINTs for each address per parcel.
  #' 
  #' @export
  
  munis <- load_muni_table(path)
  if (is.null(muni_ids)) {
    muni_ids <- munis |>
      dplyr::pull(muni_id)
  }
  if(!quiet) {
    util_log_message("INPUT/OUTPUT: Downloading addresses.")
  }
  
  all <- list()
  nb <- list()
  bos_id = "035"
  for (id in muni_ids) {
    if (id == bos_id) {
      if (!quiet) {
        util_log_message(
          "INPUT/OUTPUT: Downloading Boston addresses from ArcGIS service."
        )
      }
      # Boston handler---MassGIS does not maintain the Boston Address list.
      all[[id]] <- load_from_arc("b6bffcace320448d96bb84eabb8a075f_0", crs) |>
        load_boston_address_preprocess(
          c("body", "muni", "state", "postal", "addr2", "num")
          ) |>
        dplyr::mutate(
          muni_id = id
        )
    } else {
      url_base <- "https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/mad/town_exports/addr_pts/"
      filename <- glue::glue("AddressPts_M{id}")
      url <- glue::glue("{url_base}{filename}.zip")
      if (!quiet) {
        util_log_message(
          glue::glue("INPUT/OUTPUT: Downloading addresses for muni id {id}.")
        )
      }
      nb[[id]] <- load_shp_from_remote_zip(
          url,
          shpfile = glue::glue("{filename}.shp"),
          crs = crs
        ) |>
        dplyr::mutate(
          muni_id = id
        )
    }
  }
  
  all[['nb']] <- dplyr::bind_rows(nb) |>
    load_nonboston_address_preprocess(
      c("body", "muni", "state", "postal", "addr2"),
      munis=munis
    )
  
  df <- dplyr::bind_rows(all) |>
    flow_address_text(c("body"), numbers=FALSE) |>
    dplyr::mutate(
      addr = dplyr::case_when(
        end > start ~ stringr::str_c(start, "-", end, " ", body, sep=""),
        .default = stringr::str_c(start, body, sep=" "),
      ),
      even = dplyr::case_when(
        floor(start) %% 2 == 0 ~ TRUE,
        floor(start) %% 2 != 0 ~ FALSE
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
    dplyr::left_join(
      relation, 
      by = dplyr::join_by(id)
      )
  
  df |>
    sf::st_drop_geometry() |>
    dplyr::group_by(body, state, muni_id, muni, even, postal, loc_id) |>
    dplyr::summarize(
      start = min(start),
      end = max(end),
      addr_count = dplyr::n()
    ) |>
    dplyr::ungroup() |>
    dplyr::left_join(
      df |>
        dplyr::select(body, state, muni_id, muni, even, postal, loc_id),
      multiple = "first",
      by = dplyr::join_by(body, state, muni_id, even, muni, postal, loc_id)
    ) |>
    sf::st_set_geometry("geometry") |>
    tibble::rowid_to_column("id") |>
    dplyr::group_by(body, start, end, muni, even) |>
    dplyr::mutate(
      unique_in_muni = dplyr::n() == 1
    ) |>
    dplyr::ungroup() |>
    dplyr::group_by(body, start, end, postal, even) |>
    dplyr::mutate(
      unique_in_postal = dplyr::n() == 1
    ) |>
    dplyr::ungroup() |>
    std_simp_street("body") |>
    dplyr::group_by(body_simp, start, end, muni, even) |>
    dplyr::mutate(
      unique_in_muni_simp = dplyr::n() == 1
    ) |>
    dplyr::ungroup() |>
    dplyr::group_by(body_simp, start, end, postal, even) |>
    dplyr::mutate(
      unique_in_postal_simp = dplyr::n() == 1
    ) |>
    dplyr::ungroup()
}

load_places <- function(munis, zips, crs, quiet=FALSE) {
  #' Load Massachusetts Geographic Place Names
  #' 
  #' Downloads Geographic Place Names from MassGIS Servers and performs some basic
  #' standardization. Municipalities are required to link placenames to the name of
  #' their containing municipality.
  #'
  #' @param munis Municipal boundaries loaded by `load_munis()`.
  #' @param crs Coordinate reference system for output.
  #' 
  #' @return An dataframe depicting relationships between place names and
  #'    municipalities, including a version of placename that can be used for 
  #'    fuzzy matching.
  #' 
  #' @export
  
  
  if(!quiet) {
    util_log_message("INPUT/OUTPUT: Downloading placenames.")
  }
  
  munis <- dplyr::select(munis, muni)
  
  ma_zips <- zips |>
    dplyr::filter(state == "MA") |>
    dplyr::group_by(zip) |>
    dplyr::filter(dplyr::row_number() == 1) |>
    dplyr::ungroup() |>
    dplyr::select(zip)
  
  df <- load_shp_from_remote_zip(
      "https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/geonames_shp.zip",
      shpfile = "GEONAMES_PT_PLACES.shp",
      crs = crs
    ) |>
    dplyr::rename(name = labeltext) |>
    dplyr::select(-angle) |>
    dplyr::mutate(
      name = stringr::str_replace(name, "BORO$", "BOROUGH"),
      name = stringr::str_remove_all(
        name, "(?<=\\b\\w)[ ]{1,2}(?=\\w\\b)"
      ),
      name = stringr::str_replace_all(
        name, "([\\s]+)", " "
      )
    ) |>
    sf::st_join(
      munis, join = sf::st_intersects
    ) |>
    sf::st_join(
      ma_zips, join = sf::st_intersects
    )
  
  df <- df |>
    dplyr::filter(is.na(muni)) |>
    dplyr::select(-muni) |>
    sf::st_join(
      munis, join = sf::st_nearest_feature
    ) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(!is.na(muni))
    ) 
  
  df <- df |>
    dplyr::filter(is.na(zip)) |>
    dplyr::select(-zip) |>
    sf::st_join(
      ma_zips, join = sf::st_nearest_feature
    ) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(!is.na(zip))
    )
  
  df |>
    sf::st_drop_geometry() |>
    dplyr::distinct() |>
    dplyr::bind_rows(
      munis |>
        sf::st_drop_geometry() |>
        dplyr::filter(!(muni %in% df$name)) |>
        dplyr::mutate(
          name = muni
        )
    ) |>
    dplyr::mutate(
      tmp = dplyr::case_when(
        stringr::str_detect(name, "^(SOUTH|EAST|WEST|NORTH)\\w|PORT$") &
          !stringr::str_detect(name, " ") ~
          stringr::str_c(
            stringr::str_sub(name, start = 1, end = 2),
            " ",
            stringr::str_sub(name, start = 3)
          ),
        .default = name
      ),
      name_fuzzy = std_fuzzify_string(tmp)
    ) |>
    dplyr::select(-tmp)
}

load_munis <- function(crs, quiet=FALSE) {
  #' Load Massachusetts Municipal Boundaries
  #' 
  #' Downloads MA municipalities from MassGIS ArcGIS Hub and flags
  #' HNS municipalities.
  #'
  #' @param crs Coordinate reference system for output.
  #' 
  #' @return An `sf` dataframe containing municipal boundaries as MULTIPOLYGONs.
  #' 
  #' @export
  if(!quiet) {
    util_log_message("INPUT/OUTPUT: Downloading Massachusetts municipal boundaries.")
  }
  
  load_from_arc("43664de869ca4b06a322c429473c65e5_0", crs = crs) |>
    dplyr::select(
      muni_id = town_id, 
      muni = town
      ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::where(is.character), 
        stringr::str_to_upper
        ),
      muni_id = std_pad_muni_ids(muni_id)
      ) |>
    dplyr::mutate(
      muni = dplyr::case_when(
        muni == "MANCHESTER" ~ "MANCHESTER-BY-THE-SEA",
        .default = muni
      ),
      muni = stringr::str_replace(muni, "BORO$", "BOROUGH")
    ) |>
    std_flag_hns("muni")
}

load_zips <- function(munis, crs, thresh = 0.95, quiet=FALSE) {
  #' Load ZIP Boundaries
  #' 
  #' Downloads ZIP boundaries and attributes from US Census, subsequently
  #' identifying cases where ZIPS are unambiguously within single states,
  #' where ZIPS are unambiguously within single municipalities, and where 
  #' municipalities are unambiguously within single ZIPS.
  #'
  #' @param munis Municipalities as read by `load_muni()`.
  #' @param crs Coordinate reference system for output.
  #' @param thresh Number between 0 and 1 that sets a threshold for ambiguity.
  #' 
  #' @return An `sf` dataframe containing ZIP boundaries as MULTIPOLYGONs.
  #' 
  #' @export
  
  if(!quiet) {
    util_log_message("INPUT/OUTPUT: Downloading ZIP boundaries.")
  }
  
  if(!dplyr::between(thresh, 0, 1)) {
    stop("Threshold must be between 0 and 1.")
  }
  
  zips <- tigris::zctas(
      cb=FALSE, 
      progress_bar=FALSE
      ) |>
    dplyr::select(zip=ZCTA5CE20) |>
    sf::st_transform(5070) |>
    suppressMessages()
  
  states <- tigris::states() |>
    dplyr::select(state = STUSPS) |> 
    sf::st_transform(5070) |>
    dplyr::filter(state %in% state.abb) |>
    suppressMessages()
  
  if(!quiet) {
    util_log_message("INPUT/OUTPUT: Identifying ZIP code states by intersection.")
  }
  
  state_from_zip <- zips |>
    sf::st_set_agr("constant") |>
    sf::st_intersection(
      sf::st_set_agr(states, "constant")
    ) |> 
    sf::st_drop_geometry() |>
    dplyr::filter(!is.na(state)) |>
    dplyr::group_by(zip) |>
    dplyr::mutate(
      unambig_state = dplyr::case_when(
        dplyr::n() == 1 ~ state,
        .default = NA_character_
      ),
    ) |>
    dplyr::ungroup()
  
  zips <- zips |>
    dplyr::left_join(
      state_from_zip |>
        dplyr::distinct(),
      by = dplyr::join_by(zip)
    )
  
  if(!quiet) {
    util_log_message("INPUT/OUTPUT: Intersecting ZIP codes with municipal borders.")
  }
  
  zips_ma <- zips  |>
    dplyr::filter(state == "MA") |>
    sf::st_transform(crs) |>
    sf::st_set_agr("constant") |>
    sf::st_intersection(
      sf::st_set_agr(
        munis |> 
          sf::st_union() |>
          sf::st_as_sf() |>
          dplyr::select(), "constant")
    )
  
  if (thresh == 1) {
    if(!quiet) {
      util_log_message("INPUT/OUTPUT: Identifying ZIP codes that lie within a single municipal boundary by containment.")
    }
    ma_unambig_zip_from_muni <- munis |>
      sf::st_join(
        zips_ma, 
        join = sf::st_contains_properly, 
        left = FALSE
        ) |>
      sf::st_drop_geometry()
    if(!quiet) {
      util_log_message("INPUT/OUTPUT: Identifying municipal boundaries that lie within a single ZIP code by containment.")
    }
    ma_unambig_muni_from_zip <- zips_ma |>
      sf::st_join(
        munis, 
        join = sf::st_contains_properly, 
        left = FALSE
      ) |>
      sf::st_drop_geometry()
  } else {
    if(!quiet) {
      util_log_message("INPUT/OUTPUT: Identifying ZIP codes that lie within a single municipal boundary by intersection.")
    }
    ma_unambig_zip_from_muni <- munis |>
      std_calculate_overlap(
        zips_ma, 
        thresh=thresh
      )
    if(!quiet) {
      util_log_message("INPUT/OUTPUT: Identifying municipal boundaries that lie within a single ZIP code by intersection.")
    }
    ma_unambig_muni_from_zip <- zips_ma |>
      std_calculate_overlap(
        munis,
        thresh=thresh
      )
  }
  
  zips <- zips |>
    dplyr::left_join(
      ma_unambig_zip_from_muni |>
        dplyr::select(zip, muni_unambig_from=muni) |>
        dplyr::distinct(), 
      by=dplyr::join_by(zip)
    ) |>
    dplyr::left_join(
      ma_unambig_muni_from_zip |>
        dplyr::select(zip, muni_unambig_to=muni) |>
        dplyr::distinct(),
      by=dplyr::join_by(zip)
    ) |>
    sf::st_as_sf() |>
    sf::st_transform(crs)
}

load_oc_companies <- function(path, gdb_path, test_count=NULL, quiet=FALSE, filename = "companies.csv") {
  #' Load OpenCorporates Companies
  #' 
  #' Load OpenCorporates companies.
  #'
  #' @param path Name of directory containing OpenCorporates data.
  #' @param gdb_path Path to collection of MassGIS Parcel GDBs.
  #' @param filename Name of file containing companies.
  #' 
  #' @return A data frame of companies.
  #' 
  #' @export
  
  
  if(!quiet) {
    util_log_message(glue::glue("INPUT/OUTPUT: Loading companies from {path}/{filename}."))
  }
  
  min_year <- load_vintage_select(gdb_path) |>
    dplyr::pull(cy) |>
    min()
  
  df <- readr::read_csv(
      file.path(path, filename),
      col_select = c(name, company_type, registered_address.street_address,
                     registered_address.locality, registered_address.region,
                     registered_address.postal_code, registered_address.country,
                     company_number, dissolution_date),
      col_types = "ccccccccD",
      progress = FALSE
    ) |>
    dplyr::rename(
      id = company_number,
      addr = registered_address.street_address,
      muni = registered_address.locality,
      state = registered_address.region,
      postal = registered_address.postal_code,
      country = registered_address.country
    ) |>
    dplyr::filter(is.na(dissolution_date) | dissolution_date > glue::glue("{min_year}-01-01")) |>
    dplyr::select(-dissolution_date) |>
    suppressWarnings()
  
  if(!is.null(test_count)) {
    if (test_count <= nrow(df) & is.numeric(test_count)) {
      if(!quiet) {
        util_log_message(glue::glue("INPUT/OUTPUT: Loading only first {test_count} companies."))
      }
      df <- df |>
        dplyr::slice_head(n=test_count)
    } else if (test_count > nrow(df)) {
      if(!quiet) {
        util_log_message(glue::glue("VALIDATION: Test count larger than row count. Loading all companies."))
      }
    } else if (test_count == 0) {
      if(!quiet) {
        util_log_message(glue::glue("VALIDATION: Test count cannot be zero. Loading all companies."))
      }
    } else {
      stop("Invalid test count provided to load_oc_companies(). Must be numeric.")
    }
  }
  
  df |>
    load_generic_preprocess(
      c("name", "addr", "company_type", "muni", "state", "postal", "country"),
      id_cols = c("id")
    ) |>
    dplyr::rename(
      company_id = id
    )
}

load_oc_officer_fix_addresses <- function(df, quiet=FALSE) {
  
  if(!quiet) {
    util_log_message(glue::glue("INPUT/OUTPUT: Fixing officer addresses."))
  }
  
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

load_oc_officers <- function(path, companies, quiet=FALSE, filename = "officers.csv") {
  #' Load OpenCorporates Officers
  #' 
  #' Load OpenCorporates officers.
  #'
  #' @param path Name of directory containing OpenCorporates data.
  #' @param companies Companies as loaded by `load_companies()`.
  #' @param filename Name of file containing officers.
  #' 
  #' @return A data frame of officers.
  #' 
  #' @export
  
  if(!quiet) {
    util_log_message(glue::glue("INPUT/OUTPUT: Loading officers from {path}/{filename}."))
  }
  
  readr::read_csv(
    file.path(path, filename),
    col_select = c(name, position, address.in_full, address.street_address, 
                   address.locality, address.region, address.postal_code, 
                   address.country, company_number),
    col_types = "ccccccccc",
    progress = FALSE,
    show_col_types = FALSE
  ) |>
    dplyr::rename(
      addr = address.in_full, 
      str = address.street_address, 
      muni = address.locality,
      state = address.region,
      postal = address.postal_code,
      country = address.country,
      company_id = company_number
    ) |>
    dplyr::semi_join(companies, by = dplyr::join_by(company_id == company_id)) |>
    std_replace_newline("addr") |>
    load_generic_preprocess(
      c("addr", "name", "position", "str", "muni", "state", "postal", "country")
    ) |>
    dplyr::filter(!is.na(name)) |>
    load_oc_officer_fix_addresses(quiet=quiet) |>
    std_replace_blank(c("addr", "muni", "state", "postal")) |>
    suppressWarnings()
}

# Omnibus Ingestor/Loader ====

load_read_write_all <- function(
    data_path,
    muni_ids,
    gdb_path,
    oc_path,
    crs,
    zip_int_thresh,
    refresh,
    company_test_count=NULL,
    quiet=FALSE
    ) {
  #' Ingests/Read All Layers
  #' 
  #' Ingests or reads all layers, writing each to a globally scoped variable
  #' (e.g., COMPANIES, ADDRESSES)
  #'
  #' @param data_path Data folder path.
  #' @param muni_ids Vector of municipality IDs.
  #' @param gdb_path Collection of MassGIS Parcel GDBs path.
  #' @param oc_path OpenCorporates data path.
  #' @param filename Name of file containing companies.
  #' @param crs Coordinate reference system for output.
  #' @param refresh Whether to re-ingest data from source.
  #' 
  #' @return Nothing, though binds layers to globally scoped variables.
  #' 
  #' @export
  
  if(!quiet) {
    util_log_message("BEGINNING DATA LOADING SEQUENCE", header=TRUE)
  }
  
  # Read Municipalities
  munis <- load_read_write(
    load_conn(),
    "munis",
    loader=load_munis(
      crs=crs,
      quiet=quiet
    ),
    refresh=refresh,
    quiet=quiet
  )

  # Read ZIPs
  zips <- load_read_write(
    load_conn(),
    "zips",
    load_zips(
      munis=munis,
      crs=crs,
      thresh=zip_int_thresh,
      quiet=quiet
    ),
    refresh=refresh,
    quiet=quiet
  )

  # Read Places
  places <- load_read_write(
    load_conn(),
    "places",
    load_places(
      munis=munis,
      zips=zips,
      crs=crs,
      quiet=quiet
    ),
    refresh=refresh,
    quiet=quiet
  )

  # Read Assessors Tables
  assess <- load_read_write(
    load_conn(),
    "init_assess",
    load_assess(
      path=data_path,
      gdb_path=file.path(data_path, gdb_path),
      muni_ids=muni_ids,
      quiet=quiet
    ),
    refresh=refresh,
    muni_ids=muni_ids,
    quiet=quiet
  )
  
  # Read Census Tracts
  tracts <- load_read_write(
    load_conn(),
    "tracts",
    load_tracts(
      state="MA",
      crs=crs,
      quiet=quiet
    ),
    refresh=refresh
  )

  # Read Block Groups
  block_groups <- load_read_write(
    load_conn(),
    "block_groups",
    loader=load_block_groups(
      state="MA",
      crs=crs,
      quiet=quiet
    ),
    refresh=refresh
  )

  # Read Parcels
  parcels <- load_read_write(
    load_conn(),
    "init_parcels",
    loader=load_parcels(
      gdb_path=file.path(data_path, gdb_path),
      muni_ids=muni_ids,
      assess=assess,
      block_groups=block_groups,
      crs=crs,
      quiet=quiet
    ),
    refresh=refresh,
    muni_ids=muni_ids
  )
  
  # Read Master Address File
  addresses <- load_read_write(
    load_conn(),
    "init_addresses",
    load_addresses(
      path=data_path,
      muni_ids=muni_ids,
      parcels=parcels,
      crs=crs,
      quiet=quiet
    ),
    refresh=refresh,
    muni_ids=muni_ids
  )

  # Read OpenCorpoates Companies
  companies <- load_read_write(
    load_conn(),
    "init_companies",
    load_oc_companies(
      path=file.path(data_path, oc_path),
      gdb_path=file.path(data_path, gdb_path),
      quiet=quiet,
      test_count=company_test_count
    ),
    refresh=refresh
  )

  # Read OpenCorporates Officers
  officers <- load_read_write(
    load_conn(),
    "init_officers",
    load_oc_officers(
      path=file.path(data_path, oc_path),
      companies=companies,
      quiet=quiet
    ),
    refresh=refresh
  )
  
  list(
    munis = munis,
    zips = zips,
    places = places,
    assess = assess,
    parcels = parcels,
    addresses = addresses,
    companies = companies,
    officers = officers
  )
}

# Needs rework ====

load_filings <- function(munis, bos_neighs, crs, town_ids = FALSE) {
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
    q_filter <- munis |>
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
