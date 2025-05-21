# Load Helpers ====

load_vintage_select <- function(gdb_path, most_recent = FALSE, muni_ids=NULL, recent = 3) {
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

# Database Functions ====

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

# Preprocess Layers ====

load_generic_preprocess <- function(df, cols, id_cols = NULL) {
  if(!is.null(id_cols)) {
    df <- df |>
      dplyr::filter(
        dplyr::if_all({{id_cols}}, ~ !is.na(.))
      ) |>
      dplyr::group_by(dplyr::across(dplyr::all_of(id_cols))) |>
      dplyr::slice_head(n=1) |>
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
    dplyr::select(-c(is_range, num, range_fix, start_temp, end_temp))
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

# Load Layers from Source ====

load_sql_query <- function(select, from, where = NULL, limit = NULL) {
  q <- glue::glue("SELECT {stringr::str_c(select, collapse = ',')}
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

load_from_gdb <- function(path, from, select, where_ids, where_col) {
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
#' @param crs Coordinate reference system for output.
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
                         crs, 
                         muni_ids=NULL, 
                         most_recent = FALSE
                         ) {
  state <- stringr::str_to_lower(state)
  
  col_cw <- col_cw |>
    dplyr::filter(table == "parcel")
  
  col_list <- col_cw |> 
    tidyr::drop_na(dplyr::matches(state))
  
  muni_col <- col_cw |>
    dplyr::filter(name == "muni_id") |> 
    dplyr::pull(dplyr::all_of(state))
  
  type <- load_gdb_type(path)
  
  if (type == "gdb") {
    
    where <- NULL
    if (!is.null(muni_ids)) {
      where <- glue::glue(
        "{muni_col} IN ({stringr::str_c(as.integer(muni_ids), collapse=',')})"
      )
    }
    
    df <- sf::st_read(
      path, 
      query = load_sql_query(
        select = dplyr::pull(col_list, dplyr::matches(state)),
        from = layer,
        where = where
      ), 
      quiet = TRUE
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
  
  df |>
    load_rename_with_cols(
      from = col_list[[state]], 
      to = col_list[["name"]]
    ) |>
    st_preprocess(crs) |>
    dplyr::mutate(
      muni_id = std_pad_muni_ids(muni_id),
      geometry = sf::st_cast(geometry, "MULTIPOLYGON")
    )
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
    purrr::list_rbind() |>
    dplyr::rename_with(tolower)
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
load_geonames <- function() {
  load_remote_shp(
    url = "https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/geonames_shp.zip",
    layer = "GEONAMES_PT_PLACES.shp"
  )
}

load_munis <- function(crs, path) {
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
  
  load_arc("43664de869ca4b06a322c429473c65e5_0") |>
    sf::st_preprocess() |>
    dplyr::select(
      id = town_id, 
      muni = town
      ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::where(is.character), 
        stringr::str_to_upper
        ),
      id = std_pad_muni_ids(id)
      ) |>
    dplyr::mutate(
      muni = dplyr::case_when(
        muni == "MANCHESTER" ~ "MANCHESTER-BY-THE-SEA",
        .default = muni
      ),
      muni = stringr::str_replace(muni, "BORO$", "BOROUGH")
    ) |>
    dplyr::left_join(
      load_muni_helper() |> 
        dplyr::select(id, hns, mapc),
      by = dplyr::join_by(id == id)
    )
}

load_ma_hydro <- function(crs, thresh = 0.98, quiet=FALSE) {
  if(!quiet) {
    util_log_message("INPUT/OUTPUT: Downloading and processing Massachusetts Hydrology...")
  }
  df <- load_remote_shp(
    "https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/hydro25k.zip",
    layer = "HYDRO25K_POLY.shp"
  ) |>
    st_preprocess(crs)
  
  ocean <- df |> 
    dplyr::filter(poly_code == 8)
  
  rivers_lakes <- df |> 
    dplyr::filter(poly_code == 6)
  
  union <- rivers_lakes |>
    sf::st_union() |>
    sf::st_cast("POLYGON") |>
    sf::st_as_sf() |>
    sf::st_set_geometry("geometry") |>
    dplyr::mutate(
      area = sf::st_area(geometry)
    ) |>
    dplyr::filter(area > quantile(area, 0.98))
  
  rivers_lakes |>
    sf::st_filter(union, .predicate = sf::st_intersects) |>
    dplyr::bind_rows(
      ocean
    )
}

load_zips <- function(munis, crs, thresh = 0.95) {
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
  
  if(!dplyr::between(thresh, 0, 1)) {
    stop("Threshold must be between 0 and 1.")
  }
  
  zips <- tigris::zctas(
      cb=FALSE, 
      progress_bar=FALSE
      ) |>
    dplyr::select(zip=ZCTA5CE20) |>
    sf::st_transform(5070)
  
  states <- tigris::states() |>
    dplyr::select(state = STUSPS) |> 
    sf::st_transform(5070) |>
    dplyr::filter(state %in% state.abb)
  
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
  
  zips_ma <- zips  |>
    dplyr::filter(state == "MA") |>
    sf::st_transform(crs) |>
    sf::st_set_agr("constant") |>
    sf::st_intersection(
      sf::st_set_agr(
        munis |> 
          sf::st_union() |>
          sf::st_as_sf() |>
          dplyr::select(), 
        "constant"
        )
    )
  
  if (thresh == 1) {
    ma_unambig_zip_from_muni <- munis |>
      sf::st_join(
        zips_ma, 
        join = sf::st_contains, 
        left = FALSE
        ) |>
      sf::st_drop_geometry()
    
    ma_unambig_muni_from_zip <- zips_ma |>
      sf::st_join(
        munis, 
        join = sf::st_contains, 
        left = FALSE
      ) |>
      sf::st_drop_geometry()
  } else {
    ma_unambig_zip_from_muni <- munis |>
      std_calculate_overlap(
        zips_ma, 
        thresh=thresh
      )
    
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
    sf::st_transform(crs) |>
    dplyr::filter(!is.na(state))
}

load_oc_companies <- function(path, muni_ids, company_count=NULL) {
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
  if (!is.numeric(company_count)) {
    company_count <- Inf
  }
  
  path |>
    readr::read_csv(
      col_select = c(name, company_type, registered_address.street_address,
                     registered_address.locality, registered_address.region,
                     registered_address.postal_code, registered_address.country,
                     company_number, dissolution_date),
      col_types = "ccccccccD",
      n_max = company_count,
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
    tibble::rowid_to_column("id")
}

# Omnibus Ingestor/Loader ====

load_read_write_all <- function(
    data_path,
    muni_ids,
    gdb_path,
    assess_layer,
    parcel_layer,
    oc_path,
    crs,
    zip_int_thresh,
    most_recent,
    refresh,
    tables,
    company_count=NULL,
    quiet=FALSE,
    push_db=""
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
  
  if (is.null(tables)) {
    if (!quiet) {
      util_log_message("NO LOADER TABLES REQUESTED. SKIPPING SUBROUTINE.", header=TRUE)
    }
  } else {
    if (!quiet) {
      util_log_message("BEGINNING DATA LOADING SUBROUTINE.", header=TRUE)
    }
  }
  
  out <- list(
    munis = NULL,
    zips = NULL,
    block_groups = NULL,
    tracts = NULL,
    places = NULL,
    parcels = NULL,
    assess = NULL,
    addresses = NULL,
    companies = NULL,
    officers = NULL
  )
  
  # # Read Municipalities
  # if ("munis" %in% tables) {
  #   munis <- load_read_write(
  #     util_conn(push_db),
  #     "munis",
  #     loader=load_munis(
  #       crs=crs,
  #       path=data_path,
  #       quiet=quiet
  #     ),
  #     id_col="muni_id",
  #     refresh=refresh,
  #     quiet=quiet
  #   )
  #   out[['munis']] <- munis
  # }

  # # Read ZIPs
  # if ("zips" %in% tables) {
  #   zips <- load_read_write(
  #     util_conn(push_db),
  #     "zips",
  #     load_zips(
  #       munis=munis,
  #       crs=crs,
  #       thresh=zip_int_thresh,
  #       quiet=quiet
  #     ),
  #     id_col=c("zip", "state"),
  #     refresh=refresh,
  #     quiet=quiet
  #   )
  #   out[['zips']] <- zips
  # }

  # # Read Places
  # if ("places" %in% tables) {
  #   places <- load_read_write(
  #     util_conn(push_db),
  #     "places",
  #     load_places(
  #       munis=munis,
  #       zips=zips,
  #       crs=crs,
  #       quiet=quiet
  #     ),
  #     id_col = "id",
  #     refresh=refresh,
  #     quiet=quiet
  #   )
  #   out[['places']] <- places
  #   
  # }
  # rm(places, zips, munis)
  
  # # Read Census Tracts
  # if ("tracts" %in% tables) {
  #   tracts <- load_read_write(
  #     util_conn(push_db),
  #     "tracts",
  #     load_tracts(
  #       state="MA",
  #       crs=crs,
  #       quiet=quiet
  #     ),
  #     id_col="id",
  #     refresh=refresh
  #   )
  #   out[['tracts']] <- tracts
  # }
  # rm(tracts)
  
  # # Read Block Groups
  # if ("block_groups" %in% tables) {
  #   block_groups <- load_read_write(
  #     util_conn(push_db),
  #     "block_groups",
  #     loader=load_block_groups(
  #       state="MA",
  #       crs=crs,
  #       quiet=quiet
  #     ),
  #     id_col="id",
  #     refresh=refresh
  #   )
  #   out[['block_groups']] <- block_groups
  # }
  

  # # Read Assessors Tables
  # if ("init_assess" %in% tables) {
  #   assess <- load_read_write(
  #     util_conn(push_db),
  #     "init_assess",
  #     load_assess(
  #       path=data_path,
  #       gdb_path=file.path(data_path, gdb_path),
  #       assess_layer=assess_layer,
  #       muni_ids=muni_ids,
  #       most_recent=most_recent,
  #       quiet=quiet
  #     ),
  #     id_col=c("site_id", "site_muni_id"),
  #     refresh=refresh,
  #     muni_ids=muni_ids,
  #     quiet=quiet
  #   )
  #   # load_add_fk(util_conn(push_db), "init_assess", "munis", "site_muni_id", "muni_id")
  #   out[['assess']] <- assess
  # }

  # # Read Parcels
  # if ("parcels" %in% tables) {
  #   parcels <- load_read_write(
  #     util_conn(push_db),
  #     "parcels",
  #     loader=load_parcels(
  #       gdb_path=file.path(data_path, gdb_path),
  #       layer=parcel_layer,
  #       muni_ids=muni_ids,
  #       assess=assess,
  #       block_groups=block_groups,
  #       crs=crs,
  #       most_recent=most_recent,
  #       quiet=quiet
  #     ),
  #     id_col="loc_id",
  #     refresh=refresh,
  #     muni_ids=muni_ids
  #   )
  #   out[['parcels']] <- parcels
  #   # load_add_fk(util_conn(push_db), "init_assess", "parcels", "site_loc_id", "loc_id")
  #   # load_add_fk(util_conn(push_db), "parcels", "block_groups", "block_group_id", "id")
  #   # load_add_fk(util_conn(push_db), "parcels", "tracts", "tract_id", "id")
  #   # load_add_fk(util_conn(push_db), "parcels", "munis", "muni_id", "muni_id")
  # }
  # rm(assess, block_groups)
  
  # Read Master Address File
  if ("init_addresses" %in% tables) {
    addresses <- load_read_write(
      util_conn(push_db),
      "init_addresses",
      load_addresses(
        path=data_path,
        muni_ids=muni_ids,
        parcels=parcels,
        crs=crs,
        quiet=quiet
      ),
      id_col="id",
      refresh=refresh,
      muni_ids=muni_ids
    )
    out[['addresses']] <- addresses
    # load_add_fk(util_conn(push_db), "init_addresses", "munis", "muni_id", "muni_id")
    # load_add_fk(util_conn(push_db), "init_addresses", "parcels", "loc_id", "loc_id")
  }
  rm(addresses, parcels)
  
  # Read OpenCorpoates Companies
  if ("init_companies" %in% tables) {
    companies <- load_read_write(
      util_conn(push_db),
      "init_companies",
      load_oc_companies(
        path=file.path(data_path, oc_path),
        gdb_path=file.path(data_path, gdb_path),
        muni_ids=muni_ids,
        most_recent=most_recent,
        quiet=quiet,
        test_count=company_count
      ),
      id_col="company_id",
      refresh=refresh
    )
    out[['companies']] <- companies
  }

  # Read OpenCorporates Officers
  if ("init_officers" %in% tables) {
    officers <- load_read_write(
      util_conn(push_db),
      "init_officers",
      load_oc_officers(
        path=file.path(data_path, oc_path),
        companies=companies,
        quiet=quiet
      ),
      id_col="id",
      refresh=refresh
    )
    out[['officers']] <- officers
  }
  rm(officers, companies)
  
  out
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
