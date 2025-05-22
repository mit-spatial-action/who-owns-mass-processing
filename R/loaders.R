# Load Helpers ====

#' Select Parcel Vintage
#' 
#' Decide which vintage to use per MA municipality based on a simple 
#' algorithm.
#'
#' @param path Path to collection of MassGIS parcel geodatabases.
#' @param recent Integer. How many years back algorithm should look in
#'    identifying most complete vintages.
#' 
#' @return A dataframe containing both `fy` and `cy`. Both are necessary
#'    because (strangely), there is no fixed relationship between them.
load_vintage_select <- function(path, most_recent = FALSE, muni_ids = NULL, recent = 3) {
  
  gdb_list <- list.files(path)
  
  vintages <- data.frame(
    muni_id = stringr::str_extract(gdb_list, "(?<=M)[0-9]{3}"),
    fy = as.numeric(stringr::str_extract(gdb_list, "(?<=_FY)[0-9]{2}")) + 2000,
    cy = as.numeric(stringr::str_extract(gdb_list, "(?<=_CY)[0-9]{2}")) + 2000
  )
  
  if (!is.null(muni_ids)) {
    vintages <- vintages |>
      dplyr::filter(muni_id %in% muni_ids)
  }
  
  if (most_recent) {
    vintages <- vintages |>
      dplyr::group_by(muni_id) |>
      dplyr::arrange(muni_id, dplyr::desc(fy), dplyr::desc(cy)) |>
      dplyr::slice_head(n=1) |>
      dplyr::ungroup()
    return(vintages)
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

load_gdb_type <- function(path) {
  if (tools::file_ext(path) == "gdb" && dir.exists(path)) {
    util_log_message("VALIDATION: Single geodatabase provided.")
    type <- "gdb"
  } else if (dir.exists(path) && length(list.files(path = path, pattern = "\\.gdb$")) > 0) {
    util_log_message("VALIDATION: Folder of geodatabases provided.")
    type <- "dir"
  } else {
    stop("VALIDATION: Invalid geodatabase provided.")
  }
  type
}

load_sql_query <- function(from, select = NULL, where = NULL, limit = NULL) {
  if (!is.null(select)) {
    s <- stringr::str_c(select, collapse = ',')
  } else {
    s <- "*"
  }
  q <- glue::glue("SELECT {s}
                FROM {from}")
  if (!is.null(where)) {
    q <- glue::glue("{q}
                    WHERE {where}")
  }
  if (!is.null(limit)) {
    q <- glue::glue("{q}
                    LIMIT {limit}")
  }
  q
}

load_rename_with_cols <- function(df, from, to) {
  df |>
    dplyr::rename(
      dplyr::any_of(
        setNames(
          from,
          to
        )
      )
    )
}

load_from_gdb <- function(path, from, select = NULL, where_ids = NULL, where_col = NULL) {
  if (!is.null(where_ids)) {
    where <- glue::glue(
      "{where_col} IN ({stringr::str_c(where_ids, collapse = ',')})"
    )
  } else {
    where <- NULL
  }
  sf::st_read(
    path,
    query = load_sql_query(
      select = select, 
      from = from, 
      where = where
    ),
    quiet = TRUE
  )
}

# **********************************************************
# Copied from ographiesresearch/urbanplanr. 
# Decided import was too heavy.
# **********************************************************

#' Read Remote ArcGIS Open Data Layer
#'
#' @param id Character. ArcGIS online ID.
#'
#' @returns Object of class `sf`.
#' @export
load_arc <- function(id) {
  prefix <- "https://opendata.arcgis.com/api/v3/datasets/"
  suffix <- "/downloads/data?format=geojson&spatialRefId=4326&where=1=1"
  sf::st_read(
    glue::glue("{prefix}{id}{suffix}")
  )
}

load_remote <- function(url, path) {
  httr::GET(
    url, 
    httr::write_disk(path, overwrite = TRUE)
  )
}

load_zipped_shp <- function(path, layer) {
  path <- stringr::str_c("/vsizip", path, layer, sep="/")
  sf::st_read(path, quiet=TRUE)
}

load_remote_shp <- function(url, layer) {
  temp <- base::tempfile(fileext = ".zip")
  load_remote(
    url = url,
    path = temp
  )
  load_zipped_shp(temp, layer)
}

# Database Helpers ====

load_column_name_lookup <- function(table_name) {
  if (table_name == "init_assess") {
    col <- "site_muni_id"
  } else if (table_name %in% c("init_addresses", "parcels")) {
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

load_add_fk <- function(conn, table1, table2, table1_col, table2_col) {
  on.exit(DBI::dbDisconnect(conn))
  DBI::dbExecute(
    conn,
    statement=glue::glue("ALTER TABLE {table1} 
                         DROP CONSTRAINT IF EXISTS {table1}_{table2}_fk;")
  )
  DBI::dbExecute(
    conn,
    statement=glue::glue("ALTER TABLE {table1} 
                         ADD CONSTRAINT {table1}_{table2}_fk
                         FOREIGN KEY ({stringr::str_c(table1_col, collapse=', ')})
                         REFERENCES {table2} ({stringr::str_c(table2_col, collapse=', ')})
                         ON DELETE CASCADE;")
  )
}

load_write <- function(df, 
                       conn, 
                       table_name, 
                       id_col=NULL, 
                       other_formats = c(), 
                       overwrite=FALSE, 
                       append=FALSE,
                       quiet=FALSE, 
                       dir=RESULTS_PATH) {
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
  path <- stringr::str_c(dir, table_name, sep="/")
  csv_file <- stringr::str_c(path, "csv", sep=".")
  gpkg_file <- stringr::str_c(glue::glue("{dir}/{dir}"), "gpkg", sep=".")
  r_file <- stringr::str_c(path, "Rda", sep=".")
  if(!quiet) {
    addl_f <- ""
    if ("csv" %in% other_formats) {
      addl_f <- stringr::str_c(addl_f, glue::glue(", {csv_file}"))
    }
    if ("gpkg" %in% other_formats) {
      addl_f <- stringr::str_c(addl_f, glue::glue(", to layer '{table_name}' of {gpkg_file}"))
    }
    if ("r" %in% other_formats) {
      addl_f <- stringr::str_c(addl_f, glue::glue(", and to {r_file}"))
    }
    util_log_message(
      glue::glue(
        "INPUT/OUTPUT: Writing table '{table_name}' to PostGIS database{addl_f}."
      )
    )
  }
  if ("sf" %in% class(df)) {
    drop_idx_q <- glue::glue("DROP INDEX IF EXISTS {table_name}_geom_idx")
    DBI::dbExecute(
      conn,
      drop_idx_q
    )
    sf::st_write(
      df, 
      dsn=conn, 
      layer=table_name,
      delete_layer=overwrite,
      append=append
    )
    geom_type <- df |>
      sf::st_geometry_type(by_geometry = FALSE) |> 
      as.character()
    epsg <- sf::st_crs(df)$epsg
    geom_q <- glue::glue("ALTER TABLE {table_name} ALTER COLUMN geometry TYPE geometry({geom_type}, {epsg})")
    DBI::dbExecute(
      conn,
      geom_q
    )
    idx_q <- glue::glue("CREATE INDEX {table_name}_geom_idx ON {table_name} USING GIST(geometry)")
    DBI::dbExecute(
      conn,
      idx_q
    )
  } else {
    DBI::dbWriteTable(
      conn=conn,
      name=table_name,
      value=df,
      overwrite=overwrite,
      append=append
    )
  }
  if(!is.null(id_col)) {
    DBI::dbExecute(
      conn,
      statement=glue::glue("ALTER TABLE {table_name} ADD PRIMARY KEY ({stringr::str_c(id_col, collapse=', ')});")
    )
  }
  if ("csv" %in% other_formats) {
    readr::write_csv(df, csv_file, append=!overwrite)
  }
  if ("gpkg" %in% other_formats) {
    sf::st_write(df, gpkg_file, layer=table_name, delete_layer=overwrite)
  }
  if ("r" %in% other_formats) {
    save(df, file=r_file)
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

load_read_write <- function(conn, table_name, loader, id_col=NULL, muni_ids=NULL, refresh=FALSE, quiet=FALSE) {
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
  
  table_exists <- util_check_for_tables(conn, table_name)
  
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
      load_write(conn, table_name=table_name, id_col=id_col, overwrite=refresh, quiet=quiet)
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
      load_write(conn, table_name=table_name, id_col=id_col, overwrite=TRUE, quiet=quiet)
  }
  df
}

# Load Layers from Source ====

## Lil' guys ====

load_muni_helper <- function(path = file.path("data", "munis.csv")) { 
  readr::read_csv(
    path, 
    progress=TRUE,
    show_col_types = FALSE)
}

load_luc_crosswalk <- function(file = file.path("data", "mhp_luc_cw.csv")) {
  readr::read_csv(file) |>
    dplyr::select(muni_id, use_code, luc) |>
    dplyr::mutate(
      muni_id = stringr::str_pad(muni_id, width = 3, side="left", pad="0")
    ) |>
    dplyr::filter(!is.na(luc)) |>
    dplyr::distinct()
}

## Primary ====

#' Load Assessors' Tables from MassGIS Parcel GDB(s)
#' 
#' Load assessing table from MassGIS tax parcel collection, presented either
#' as a single GDB or a folder of many vintages.
#' See https://www.mass.gov/info-details/massgis-data-property-tax-parcels
#'
#' @param path Path to collection of MassGIS Parcel GDBs or single GDB.
#' @param layer Name of layer from which to read assessors table.
#' @param col_cw Path to column crosswalk.
#' @param state Two-character abbrevation of state.
#' @param muni_ids Vector of municipality IDs.
#' @param fy = Preferred fiscal year.
#' @param cy = Preferred calendar year.
#' @param most_recent If `TRUE`, selects most recent year when presented with
#' a folder of vintages.
#' 
#' @return A data frame of assessors' records for specified municipalities.
#' 
#' @export
load_assess <- function(path, 
                        layer, 
                        state, 
                        col_cw = file.path("data", "col_cw.csv"),
                        muni_ids=NULL, 
                        fy = NULL, 
                        cy = NULL, 
                        most_recent=FALSE
                        ) {
  state <- stringr::str_to_lower(state)
  
  col_cw <- col_cw |>
    readr::read_csv() |>
    dplyr::filter(table == "assess")
  
  col_list <- col_cw |> 
    tidyr::drop_na(dplyr::matches(state))
  
  type <- load_gdb_type(path)
  
  if (type == "gdb") {
    df <- load_from_gdb(
      path = path, 
      select = dplyr::pull(col_list, dplyr::matches(state)),
      from = layer, 
      where_ids = muni_ids, 
      where_col = col_cw |>
        dplyr::filter(name == "site_muni_id") |> 
        dplyr::pull(dplyr::all_of(state))
    )
  } else if (type == "dir") {
    if (is.null(fy) & is.null(cy)) {
      vintages <- load_vintage_select(path, muni_ids, most_recent=most_recent)
    } else {
      vintages <- data.frame(
        muni_id = muni_ids
      ) |>
        dplyr::mutate(
          fy = fy,
          cy = cy
        )
    }
    
    all <- list()
    for (row in 1:nrow(vintages)) {
      muni_id <- vintages[[row, 'muni_id']]
      cy <- vintages[[row, 'cy']] - 2000
      fy <- vintages[[row, 'fy']] - 2000
      
      file <- glue::glue("M{muni_id}_parcels_CY{cy}_FY{fy}_sde.gdb")
      if (!load_gdb_type(file.path(path, file))) {
        stop("You've passed an invalid GDB directory.")
      }
      
      q <- stringr::str_c("SELECT", cols, glue::glue("FROM M{muni_id}Assess"), sep = " ")
      
      all[[muni_id]] <- sf::st_read(
        file.path(path, file),
        query = q,
        quiet = TRUE
      )
    }
    df <- dplyr::bind_rows(all)
  }
  
  df |>
    load_rename_with_cols(from = col_list[[state]], to = col_list[["name"]])
}

#' Load Parcels from MassGIS Parcel GDB(s)
#' 
#' Load parcels from MassGIS tax parcel collection, presented either
#' as a single GDB or a folder of many vintages.
#' See https://www.mass.gov/info-details/massgis-data-property-tax-parcels
#'
#' @param gdb_path Path to collection of MassGIS Parcel GDBs or single GDB.
#' @param muni_ids Vector of municipality IDs.
#' 
#' @return An `sf` dataframe containing MULTIPOLYGON parcels for specified 
#' municipalities.
#' 
#' @export
load_parcels <- function(path, 
                         layer, 
                         col_cw, 
                         state, 
                         muni_ids=NULL, 
                         most_recent = FALSE
                         ) {
  state <- stringr::str_to_lower(state)
  
  col_cw <- col_cw |>
    dplyr::filter(table == "parcel")
  
  col_list <- col_cw |> 
    tidyr::drop_na(dplyr::matches(state))
  
  type <- load_gdb_type(path)
  
  if (type == "gdb") {
    df <- load_from_gdb(
      path = path, 
      select = dplyr::pull(col_list, dplyr::matches(state)),
      from = layer, 
      where_ids = muni_ids, 
      where_col = col_cw |>
        dplyr::filter(name == "site_muni_id") |> 
        dplyr::pull(dplyr::all_of(state))
    )
  } else if (type == "dir") {
    vintages <- load_vintage_select(path, muni_ids, most_recent=most_recent)
    
    all <- list()
    for (row in 1:nrow(vintages)) {
      
      muni_id <- vintages[[row, 'muni_id']]
      cy <- vintages[[row, 'cy']] - 2000
      fy <- vintages[[row, 'fy']] - 2000
      
      file <- glue::glue("M{muni_id}_parcels_CY{cy}_FY{fy}_sde.gdb")
      q <- glue::glue("SELECT LOC_ID, TOWN_ID AS MUNI_ID FROM M{muni_id}TaxPar")
      
       sdf <- sf::st_read(
        file.path(path, file), 
        query = q, 
        quiet = TRUE
        ) |>
         load_rename_geometry("geometry")
        
       all[[muni_id]] <- sdf
    }
    df <- dplyr::bind_rows(all)
    rm(all)
  }
  df
}

#' Load Live Street Address Management (SAM) Addresses
#'
#' @return An `sf` dataframe containing `"POINT"`s.
load_boston_addresses <- function() {
  file <- "Live_Street_Address_Management_SAM_Addresses"
  url_base <- "https://data.boston.gov/dataset/fc9562ca-02df-40bf-b4db-a36effb52ccc/resource/b7f6856f-0e69-40e5-8c27-0a40c132802a/download"
  url <- stringr::str_c(url_base, glue::glue("{file}.zip"), sep="/")
  
  load_remote_shp(
    url = url,
    layer = glue::glue("{file}.shp")
  )
}


#' Load MassGIS Master Address Data Points
#' 
#' Load Basic Address Points data product from MassGIS Master Address Data.
#' See https://www.mass.gov/info-details/massgis-data-master-address-data-basic-address-points
#'
#' @param muni_ids Optional. Vector of municipality IDs.
#' 
#' @return An `sf` dataframe containing `"POINT"`s.
load_massgis_addresses <- function(muni_ids=NULL) {
  if (is.null(muni_ids)) {
    muni_ids <- load_muni_helper() |>
      dplyr::pull(id)
  }
  
  url_base <- "https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/mad/town_exports/addr_pts"
  
  muni_ids |>
    purrr::map(\(x) {
      # Boston has a different address database...
      if (x != '035') {
        file <- glue::glue("AddressPts_M{x}")
        url <- paste(url_base, glue::glue("{file}.zip"), sep = "/")
        load_remote_shp(
          url = url,
          layer = glue::glue("{file}.shp")
        )
      }
    }) |>
    purrr::list_rbind()
}

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
load_geonames <- function(state) {
  if (state == "MA") {
    g  <- load_remote_shp(
      url = "https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/geonames_shp.zip",
      layer = "GEONAMES_PT_PLACES.shp"
    )
  } else {
    g <- NULL
  }
  g
}

#' Load Massachusetts Municipal Boundaries
#' 
#' @return An `sf` dataframe containing municipal boundaries as MULTIPOLYGONs.
load_munis <- function(state) {
  if (state == "MA") {
    m <- load_arc("43664de869ca4b06a322c429473c65e5_0")
  } else {
    m <- NULL
  }
  m
}

load_muni_helper <- function(path = file.path("data", "munis.csv")) { 
  readr::read_csv(
    path, 
    progress=TRUE,
    show_col_types = FALSE)
}

load_luc_crosswalk <- function(file) {
  readr::read_csv(file) |>
    dplyr::select(muni_id, use_code, luc) |>
    dplyr::mutate(
      muni_id = stringr::str_pad(muni_id, width = 3, side="left", pad="0")
    ) |>
    dplyr::filter(!is.na(luc)) |>
    dplyr::distinct()
}

# Needs rework ====

#' load_filings <- function(munis, bos_neighs, crs, town_ids = FALSE) {
#'   #' Pulls eviction filings from database.
#'   #'
#'   #' @return A dataframe.
#'   #' @export
#'   # Construct SQL query.
#'   docket_col <- "docket_id"
#'   filings_table <- "filings"
#'   plaintiffs_table <- "plaintiffs"
#'   cols <- stringr::str_c(
#'     c(docket_col, "add1", "city", "zip", "state", "match_type", "geometry"), 
#'     collapse = ","
#'   )
#'   q <- stringr::str_c(
#'     "SELECT", cols, 
#'     "FROM", filings_table, "AS f",
#'     sep = " "
#'   )
#'   # Set limit if test = TRUE
#'   if (!isFALSE(town_ids)) {
#'     q_filter <- munis |>
#'       dplyr::filter(town_id %in% town_ids) |>
#'       dplyr::pull(id) 
#'     if (35 %in% town_ids) {
#'       neighs <- bos_neighs |>
#'         dplyr::pull(Name)
#'       q_filter <- c(q_filter, neighs)
#'     }
#'     q_filter <- q_filter |>
#'       stringr::str_c("UPPER(f.city) = '", ., "'") |>
#'       stringr::str_c(., collapse = " OR ") |>
#'       stringr::str_c("WHERE", ., sep = " ")
#'     q <- stringr::str_c(q, q_filter, sep = " ")
#'   }
#'   # Pull filings.
#'   conn <- DBI::dbConnect(
#'     RPostgres::Postgres(),
#'     dbname = Sys.getenv("DB_NAME"),
#'     host = Sys.getenv("DB_HOST"),
#'     port = Sys.getenv("DB_PORT"),
#'     user = Sys.getenv("DB_USER"),
#'     password = Sys.getenv("DB_PASS"),
#'     sslmode = "allow"
#'   ) 
#'   filings <- conn |>
#'     sf::st_read(
#'       query=q,
#'       quiet = TRUE
#'     )
#'   
#'   DBI::dbDisconnect(conn)
#'   
#'   filings |> 
#'     dplyr::select(-tidyselect::contains('..')) |>
#'     sf::st_transform(crs) |>
#'     dplyr::rename_with(stringr::str_to_lower) |>
#'     dplyr::filter(!is.na(add1))
#' }
