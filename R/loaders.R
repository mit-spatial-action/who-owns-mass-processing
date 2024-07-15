source("R/utilities.R")
source("R/standardizers.R")

# Load Helpers ====

load_muni_table <- function(path, file="muni_ids.csv") { 
  readr::read_csv(
    file.path(path, file), 
    progress=TRUE,
    show_col_types = FALSE)
}

load_test_muni_ids <- function(muni_ids, path) {
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
  
  if(!all(std_pad_muni_ids(muni_ids) %in% ids)) {
    stop("Provided invalid test municipality ids. ❌❌❌")
  } else {
    util_log_message("Municipality IDs are valid. 🚀🚀🚀")
  }
  std_pad_muni_ids(muni_ids)
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
    util_log_message("Single geodatabase provided.")
  } else {
    util_log_message("Folder of geodatabases provided.")
  }
  file
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

load_check_for_table <- function(conn, table_name) {
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
  
  if(table_name %in% DBI::dbListTables(conn)) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}

load_postgis_ingest <- function(df, conn, table_name, overwrite=FALSE) {
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
  df
}

load_postgis_read <- function(conn, table_name) {
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
  
  q <- DBI::dbSendQuery(conn, "SELECT * FROM geometry_columns")
  
  geo_tables <- DBI::dbFetch(q) |> 
    dplyr::pull(f_table_name)
  
  if (layer_name %in% geo_tables) {
    sf::st_read(
      dsn = conn,
      layer = table_name,
      quiet = TRUE
    )
  } else {
    DBI::dbReadTable(
      conn = conn,
      name = table_name
    )
  }
}

load_ingest_read <- function(conn, table_name, loader, refresh=FALSE) {
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
  
  on.exit(DBI::dbDisconnect(conn))
  table_exists <- load_check_for_table(conn, table_name)
  if(!table_exists | refresh) {
    if(!table_exists) {
      util_log_message(
        glue::glue(
          "Table '{table_name'} does not exist in PostGIS."
          )
        )
    } else {
      util_log_message(
        glue::glue(
          "Table '{table_name}' exists in PostGIS, but user specified refresh."
          )
        )
    }
    df <- loader
    util_log_message(
      glue::glue(
        "Writing table '{table_name}' to PostGIS database."
        )
      )
    df <- df |>
      load_postgis_ingest(conn, table_name=table_name, overwrite=refresh)
  } else {
    util_log_message(
      glue::glue(
        "Reading table '{table_name}' from PostGIS database."
        )
      )
    df <- load_postgis_read(conn, table_name=table_name)
  }
  df
}

# Load Layers from Source ====

load_parcels <- function(gdb_path, crs, muni_ids=NULL, quiet=FALSE) {
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
  
  util_log_message(glue::glue("Loading parcels."))
  
  single_gdb <- load_gdb_is_file(gdb_path)
  
  if (single_gdb) {
    # If single gdb passed... ====
    
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
    
    util_log_message(glue::glue("Loading parcels."))
    
    sf::st_read(gdb_path, query = q, quiet = TRUE)
  } else {
    # If folder of gdbs passed... ====
    vintages <- load_vintage_select(gdb_path, muni_ids)
    
    all <- list()
    for (row in 1:nrow(vintages)) {
      
      muni_id <- vintages[[row, 'muni_id']]
      cy <- vintages[[row, 'cy']] - 2000
      fy <- vintages[[row, 'fy']] - 2000
      
      if (!quiet) {
        util_log_message(glue::glue("Loading parcels for muni {muni_id} (FY{fy}, CY{cy}).")) 
      }
      
      file <- glue::glue("M{muni_id}_parcels_CY{cy}_FY{fy}_sde.gdb")
      q <- glue::glue("SELECT LOC_ID, TOWN_ID AS MUNI_ID FROM M{muni_id}TaxPar")
      
      all[[muni_id]] <- sf::st_read(
          file.path(gdb_path, file), 
          query = q, 
          quiet = TRUE
        )
    }
    df <- dplyr::bind_rows(all)
    rm(all)
  }
  df |>
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

load_assess_preprocess <- function(df, path) {
  simp_cols <- c(
    "addr", "muni", "postal", "own_addr", "own_muni", 
    "own_postal", "own_state", "own_country")
  df |>
    dplyr::rename_with(stringr::str_to_lower) |>
    dplyr::filter(!is.na(loc_id)) |>
    dplyr::mutate(
      addr = dplyr::case_when(
        is.na(addr) & 
          !is.na(addr_num) & 
          !is.na(full_str) ~ stringr::str_c(addr_num, full_str, sep = " "),
        .default = addr
      )
    ) |>
    dplyr::select(-c(addr_num, full_str)) |>
    # All parcels are in MA, in the US...
    dplyr::mutate(
      muni_id = std_pad_muni_ids(muni_id),
      ls_date = lubridate::fast_strptime(ls_date, "%Y%m%d", lt=FALSE),
      state = "MA", 
      country = "US"
    ) |>
    dplyr::left_join(
      load_muni_table(path),
      by = dplyr::join_by(muni_id)
    ) |>
    tidyr::replace_na(list(units = 0)) |>
    std_luc(path=path) |>
    std_flag_residential("luc") |>
    std_units_from_luc("luc", "muni_id") |>
    std_uppercase(simp_cols) |>
    std_replace_blank(simp_cols) |>
    std_remove_special(simp_cols) |>
    std_spacing_characters(simp_cols) |>
    std_leading_zeros(simp_cols, rmsingle = FALSE) |>
    std_squish(simp_cols)
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
    "PROP_ID", "LOC_ID", "FY", 
    # Parcel address.
    "SITE_ADDR AS ADDR", "ADDR_NUM", "FULL_STR", "TOWN_ID AS MUNI_ID", 
    "ZIP AS POSTAL", 
    # Owner name and address.
    "OWNER1 AS OWN_NAME", "OWN_ADDR", "OWN_CITY AS OWN_MUNI", "OWN_STATE", 
    "OWN_ZIP AS OWN_POSTAL", "OWN_CO AS OWN_COUNTRY",
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
  
  util_log_message(glue::glue("Loading assessors' records."))
  
  single_gdb <- load_gdb_is_file(gdb_path)
  
  # If single gdb passed... ====
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
    # If folder of gdbs passed... ====
    util_log_message(glue::glue("Reading from collection of GDBs.")) 
    vintages <- load_vintage_select(gdb_path, muni_ids)
    
    all <- list()
    for (row in 1:nrow(vintages)) {
      
      muni_id <- vintages[[row, 'muni_id']]
      cy <- vintages[[row, 'cy']] - 2000
      fy <- vintages[[row, 'fy']] - 2000
      
      if (!quiet) {
        util_log_message(glue::glue("Loading assessors records for muni {muni_id} (FY{fy}, CY{cy}).")) 
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
    df <- dplyr::bind_rows(all)
  }
  
  df |>
    load_assess_preprocess(path)
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
  
  url_base <- "https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/mad/town_exports/addr_pts/"
  
  util_log_message(glue::glue("Loading addresses."))
  
  all <- list()
  bos_id = "035"
  for (id in muni_ids) {
    if (id == bos_id) {
      if (!quiet) {
        util_log_message(
          glue::glue("Downloading Boston addresses from ArcGIS service.")
        )
      }
      # Boston handler---MassGIS does not maintain the Boston Address list.
      all[[id]] <- load_from_arc("b6bffcace320448d96bb84eabb8a075f_0", crs) |>
        dplyr::filter(!is.na(street_body) & !is.na(street_full_suffix)) |>
        dplyr::mutate(
          muni = "BOSTON",
          muni_id = bos_id,
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
      if (!quiet) {
        util_log_message(
          glue::glue("Downloading {filename} from {url}...")
        )
      }
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
          addrtwn_id = std_pad_muni_ids(addrtwn_id)
        ) |>
        dplyr::left_join(
          munis,
          dplyr::join_by(addrtwn_id == muni_id)
        ) |>
        dplyr::select(
          addr_num,
          addr_body = streetname,
          state,
          muni,
          postal = zipcode,
          addr_start = num1,
          addr_end = num2,
          muni_id = addrtwn_id
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

load_places <- function(munis, crs) {
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
  
  munis <- dplyr::select(munis, muni)
  
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
    ) |>
    sf::st_drop_geometry() |>
    dplyr::distinct()
  
  df |>
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
    dplyr::select(-tmp) |>
    dplyr::filter(!(name == "CAMBRIDGE" & muni == "WORCESTER")) |>
    # Remove placenames that appear in multiple places.
    # These are not useful for deduplication.
    dplyr::add_count(name) |>
    dplyr::filter(n == 1) |>
    dplyr::select(-n)
}

load_munis <- function(crs) {
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
  
  util_log_message("Downloading Massachusetts municipal boundaries...")
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

load_zips <- function(munis, crs, threshold = 0.95) {
  #' Load ZIP Boundaries
  #' 
  #' Downloads ZIP boundaries and attributes from US Census, subsequently
  #' identifying cases where ZIPS are unambiguously within single states,
  #' where ZIPS are unambiguously within single municipalities, and where 
  #' municipalities are unambiguously within single ZIPS.
  #'
  #' @param munis Municipalities as read by `load_muni()`.
  #' @param crs Coordinate reference system for output.
  #' @param threshold Number between 0 and 1 that sets a threshold for ambiguity.
  #' 
  #' @return An `sf` dataframe containing ZIP boundaries as MULTIPOLYGONs.
  #' 
  #' @export
  
  if(!dplyr::between(threshold, 0, 1)) {
    stop("Threshold must be between 0 and 1.")
  }
  
  all <- list()
  for (state in state.abb) {
    all[[state]] <- tigris::zctas(
        cb=FALSE, 
        year=2010, 
        state=state,
        progress_bar=FALSE) |>
      dplyr::mutate(state=state) |>
      dplyr::select(zip=ZCTA5CE10, state) |>
      suppressMessages()
  }
  zips <- dplyr::bind_rows(all) |> 
    sf::st_transform(crs) |>
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
    dplyr::filter(ma)
  
  munis_clip <- munis |>
    sf::st_set_agr("constant") |>
    sf::st_intersection(
      sf::st_union(sf::st_set_agr(zips_ma, "constant"))
    )
  
  zips_ma_clip <- zips_ma |>
    sf::st_set_agr("constant") |>
    sf::st_intersection(
      sf::st_union(sf::st_set_agr(munis_clip, "constant"))
    )

  # Identify cases where ZIPS can be unambiguously assigned from munis (i.e.,
  # where the vast majority of a zip is contained w/in a single muni.)
  ma_unambig_zip_from_muni <- munis_clip |>
    std_calculate_overlap(
      zips_ma_clip, 
      threshold=threshold
      ) |>
    dplyr::select(
      zip, 
      muni_unambig_from=muni
      )
  
  zips <- zips |>
    dplyr::left_join(
      ma_unambig_zip_from_muni, 
      by=dplyr::join_by(zip)
      )

  # Identify cases where munis can be unambiguously assigned
  # from ZIPS (i.e., where the vast majority of a muni is contained w/in
  # a single zip).
  ma_unambig_muni_from_zip <- zips_ma_clip |>
    std_calculate_overlap(
      munis_clip,
      threshold=threshold
      ) |>
    dplyr::select(
      zip, 
      muni_unambig_to=muni
      )
  
  zips |>
    dplyr::left_join(
      ma_unambig_muni_from_zip, 
      by=dplyr::join_by(zip)
      )
}

# Derive Layers ====

load_props_from_assess <- function(df) {
  # WIP
  
  df |> 
    dplyr::select(
      prop_id, loc_id, fy, addr, muni, postal, 
      state, country, luc, units, res, ls_date, ls_price, bld_area,
      res_area, bldg_val, total_val
    )
}


load_owners_from_assess <- function(df) {
  # WIP
  
  df |>
    dplyr::select(
      prop_id, loc_id, fy, name = own_name, addr = own_addr, muni = own_city, 
      state = own_state, postal = own_zip, country = own_co, res
    )
}

# In-Progress ====

load_companies <- function(path, gdb_path, filename = "companies.csv") {
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
  
  min_year <- load_vintage_select(gdb_path) |>
    dplyr::pull(cy) |>
    min()
  
  # print(min_year)
  readr::read_csv(
    file.path(path, filename),
    progress = FALSE,
    show_col_types = FALSE
  ) |>
    dplyr::filter(is.na(dissolution_date) | dissolution_date > glue::glue("{min_year}-01-01")) |>
    dplyr::select(
      id = company_number,
      name,
      company_type,
      nonprofit,
      addr = registered_address.street_address,
      muni = registered_address.locality,
      state = registered_address.region,
      postal = registered_address.postal_code,
      country = registered_address.country
    )
}

load_officers <- function(path, companies, filename = "officers.csv") {
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
  
  readr::read_csv(
    file.path(path, filename),
    progress = FALSE,
    show_col_types = FALSE
  ) |>
    dplyr::select(
      name, 
      position, 
      addr = address.in_full, 
      str = address.street_address, 
      muni = address.locality,
      state = address.region,
      postal = address.postal_code,
      country = address.country,
      company_id = company_number
    ) |>
    dplyr::semi_join(companies, by = dplyr::join_by(company_id == id)) |>
    std_replace_newline("addr")
}

# Omnibus Ingestor/Loader ====

load_ingest_read_all <- function(
    data_path,
    muni_ids,
    gdb_path,
    oc_path,
    crs,
    refresh,
    tables = NULL
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
  
  # Test Validity of Municipality IDs ====
  muni_ids <- load_test_muni_ids(
    muni_ids=muni_ids,
    path=data_path
  )
  
  # Read Municipalities ====
  if (any(c("munis", "zips", "places") %in% tables) | is.null(tables)) {
    MUNIS <<- load_ingest_read(
      load_conn(),
      "munis",
      loader=load_munis(
        crs=crs
      ),
      refresh=refresh
    )
  }

  # Read ZIPs ====
  if ("zips" %in% tables | is.null(tables)) {
    ZIPS <<- load_ingest_read(
      load_conn(),
      "zips",
      load_zips(
        munis=MUNIS,
        crs=crs
      ),
      refresh=refresh
    )
  }

  # Read Places ====
  if ("places" %in% tables | is.null(tables)) {
    PLACES <<- load_ingest_read(
      load_conn(),
      "places",
      load_places(
        munis=MUNIS,
        crs=crs
      ),
      refresh=refresh
    )
  }

  # Read Assessors Tables ====
  if ("assess" %in% tables | is.null(tables)) {
    ASSESS <<- load_ingest_read(
      load_conn(),
      "assess",
      load_assess(
        path=data_path,
        gdb_path=file.path(data_path, gdb_path),
        muni_ids=muni_ids,
        quiet=TRUE
      ),
      refresh=refresh
    )
  }

  # Read Parcels ====
  if (any(c("parcels", "addresses") %in% tables) | is.null(tables)) {
    PARCELS <<- load_ingest_read(
      load_conn(),
      "parcels",
      loader=load_parcels(
        gdb_path=file.path(data_path, gdb_path),
        muni_ids=muni_ids,
        crs=crs,
        quiet=TRUE
      ),
      refresh=refresh
    )
  }

  # Read Master Address File ====
  if ("addresses" %in% tables | is.null(tables)) {
    ADDRESSES <<- load_ingest_read(
      load_conn(),
      "addresses",
      load_addresses(
        path=data_path,
        muni_ids=muni_ids,
        parcels=PARCELS,
        crs=crs,
        quiet=TRUE
      ),
      refresh=refresh
    )
  }
  
  # Read OpenCorpoates Companies ====
  if (any(c("companies", "officers") %in% tables) | is.null(tables)) {
    COMPANIES <<- load_ingest_read(
      load_conn(),
      "companies",
      load_companies(
        path=file.path(data_path, oc_path),
        gdb_path=file.path(data_path, gdb_path)
      ),
      refresh=refresh
    )
  }
  
  # Read OpenCorporates Officers ====
  if ("officers" %in% tables | is.null(tables)) {
    OFFICERS <<- load_ingest_read(
      load_conn(),
      "officers",
      load_officers(
        path=file.path(data_path, oc_path),
        companies=COMPANIES
      ),
      refresh=refresh
    )
  }
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

# Deprecated ====

#' load_corps_deprec <- function(path) {
#'   #' Load corporations, sourced from the MA Secretary of the Commonwealth.
#'   #'
#'   #' @param path Path to delimited text Corporations file
#'   #' @return A dataframe.
#'   #' @export
#'   readr::read_delim(
#'     path,
#'     delim = "|",
#'     col_select = c(
#'       DataID, EntityName,
#'       AgentName, AgentAddr1, AgentAddr2, AgentCity,
#'       AgentState, AgentPostalCode, ActiveFlag
#'     ),
#'     show_col_types = FALSE
#'   ) |>
#'     dplyr::rename(
#'       id_corp = DataID
#'     ) |>
#'     dplyr::rename_with(stringr::str_to_lower)
#' }
#' 
#' load_agents_deprec <- function(df, cols, drop_na_col) {
#'   #' Load agents, which are listed alongside corporations.
#'   #'
#'   #' @param df Dataframe created by `load_corps`
#'   #' @param cols Columns containing fields describing agents.
#'   #' @param drop_na_col Column for which NA rows should be dropped.
#'   #' @return A dataframe of corporate agents.
#'   #' @export
#'   df |>
#'     dplyr::select(all_of(cols)) |>
#'     dplyr::filter(!is.na(get({{ drop_na_col }})))
#' }
#' 
#' load_inds_deprec <- function(path) {
#'   #' Load individuals from corporate db, 
#'   #' sourced from the MA Secretary of the Commonwealth.
#'   #'
#'   #' @param path Path to delimited text Corporations file
#'   #' @return A dataframe.
#'   #' @export
#'   readr::read_delim(
#'     path,
#'     delim = "|",
#'     col_select = c(
#'       DataID, FirstName, LastName, BusAddr1,
#'       ResAddr1
#'     ),
#'     show_col_types = FALSE
#'   ) |>
#'     dplyr::rename(
#'       id_corp = DataID
#'     ) |>
#'     dplyr::rename_with(stringr::str_to_lower)
#' }
