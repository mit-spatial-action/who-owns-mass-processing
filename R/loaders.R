source("R/globals.R")

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
        AgentState, AgentPostalCode, ActiveFlag)
    ) %>%
    dplyr::rename(
      id_corp = DataID
    ) %>%
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
  df %>%
    dplyr::select(all_of(cols)) %>%
    dplyr::filter(!is.na(get({{ drop_na_col }})))
}

load_parcels <- function(path, town_ids=FALSE, crs = 2249) {
  #' Load assessing table from MassGIS geodatabase.
  #' https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/gdbs/l3parcels/MassGIS_L3_Parcels_gdb.zip
  #' 
  #' @param path Path to MassGIS Parcels GDB.
  #' @param test Whether to only load a sample subset of rows.
  #' @export
  q <- "SELECT OBJECTID, MAP_PAR_ID, LOC_ID, TOWN_ID FROM L3_TAXPAR_POLY"
  if (!isFALSE(town_ids)) {
    q <- stringr::str_c(
        parcel_query, 
        "WHERE TOWN_ID IN (", 
        stringr::str_c(town_ids, collapse=", "), 
        ")",
        sep = " "
      )
  }
  sf::st_read(path, query = q) %>%
    dplyr::rename_with(stringr::str_to_lower) %>% 
    # Correct weird naming conventions of GDB.
    sf::st_set_geometry("shape") %>%
    sf::st_set_geometry("geometry") %>%
    # Select only unique id.
    dplyr::select(c(loc_id)) %>%
    # Reproject to specified CRS.
    sf::st_transform(crs) %>%
    # Cast from MULTISURFACE to MULTIPOLYGON.
    dplyr::mutate(
      geometry = st_cast(geometry, "MULTIPOLYGON")
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
      )
    ) %>%
    dplyr::rename(
      id_corp = DataID
    ) %>%
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
  df %>%
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
      "PROP_ID", "LOC_ID", "FY", "USE_CODE", 
      "SITE_ADDR", "CITY", "ZIP", "OWNER1", 
      "OWN_ADDR", "OWN_CITY", "OWN_STATE", "OWN_ZIP"
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
      query = q
    ) %>%
    dplyr::rename_with(stringr::str_to_lower) %>%
    residential_filter("use_code")
}

load_filings <- function(test = FALSE, crs = 2249) {
  #' Pulls eviction filings from database.
  #'
  #' @returns A dataframe.
  #' @export
  # Construct SQL query.
  docket_col <- "docket"
  filings_table <- "filings"
  plaintiffs_table <- "plaintiffs"
  cols <- stringr::str_c(docket_col, "add1", "city", "zip", "state", "match_type", "geometry", sep = ",")
  q <- stringr::str_c(
    "SELECT", cols, 
    "FROM", filings_table, "AS f",
    sep = " "
  )
  # Set limit if test = TRUE
  if (test) {
    q <- stringr::str_c(q, "LIMIT 1000", sep = " ")
  }
  # Pull filings.
  DBI::dbConnect(
      RPostgres::Postgres(),
      dbname = Sys.getenv("DB_NAME"),
      host = Sys.getenv("DB_HOST"),
      port = Sys.getenv("DB_PORT"),
      user = Sys.getenv("DB_USER"),
      password = Sys.getenv("DB_PASS"),
      sslmode = "allow"
    ) %>%
    sf::st_read(query=q) %>% 
    dplyr::select(-contains('..')) %>%
    sf::st_transform(crs) %>%
    dplyr::rename_with(str_to_lower) %>%
    dplyr::filter(!is.na(add1)) 
}