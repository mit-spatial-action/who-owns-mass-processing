df_parcels <- sf::read_sf("data/Feb_2025_Parcels.gdb", layer = "ParcelBoundaries")

load_assess_md <- function(df_parcels, state, col_cw_path, unit_cw_path, nrows = NULL, data_path) {
  #' @seealso [std_site_units()]
  if (!is.null(nrows)) {
    df <- df_parcels |> dplyr::slice_head(n = nrows)
  } 
  
  col_cw <- readr::read_csv(file.path(data_path, col_cw_path), show_col_types = FALSE)
  unit_cw <- readr::read_csv(file.path(data_path, unit_cw_path), show_col_types = FALSE)
  
  # Get columns for this state
  state_cols <- col_cw |>
    dplyr::select(!!state) |>
    tidyr::drop_na() |>
    dplyr::pull() |>
    stringr::str_split("\\|") |>
    unlist() |>
    unique()
  # Get land use codes for this state
  state_lu_codes <- unit_cw  |>
    dplyr::filter(state == !!state) |>
    dplyr::pull(code) |>
    stringr::str_replace("^([^-]+).*$", "\\1") |> #capture before the first hyphen, land use code without residential type
    unique()
  
  # Filter to residential parcels and select relevant columns
  df <- df |> 
    sf::st_drop_geometry() |> 
    dplyr::rename_with(stringr::str_to_lower)|>
    dplyr::filter(lu %in% state_lu_codes) |>
    dplyr::select(dplyr::any_of(state_cols))
  
  
  std_df <- data.frame(matrix(nrow = nrow(df), ncol = 0))
  
  for (row_index in 1:nrow(col_cw)) {
    ma_col <- col_cw$ma[row_index]
    md_col <- col_cw$md[row_index]
    
    if (is.na(md_col) || md_col == "") {
      std_df[[ma_col]] <- NA
    }
    
    # --- Special case: SITE_LS_PRICE ---
    if (ma_col == "site_ls_price" && grepl("\\|", md_col)) {
      column_parts <- trimws(strsplit(md_col, "\\|")[[1]])
      std_df[[ma_col]] <- dplyr::coalesce(df[[column_parts[1]]], 0) +
        dplyr::coalesce(df[[column_parts[2]]], 0)
    }
    
    # --- Special case: SITE_USE_CODE ---
    if (ma_col == "site_use_code" && grepl("\\|", md_col)) {
      column_parts <- trimws(strsplit(md_col, "\\|")[[1]])
      std_df[[ma_col]] <- ifelse(
        is.na(df[[column_parts[2]]]),
        as.character(df[[column_parts[1]]]),
        paste0(df[[column_parts[1]]], "-", df[[column_parts[2]]])
      )
    }
    
    
    # --- Standard copying ---
    if (md_col %in% names(df)) {
      std_df[[ma_col]] <- df[[md_col]]
    } else {
      std_df[[ma_col]] <- NA
    }
  }
  
  
  std_df
}

std_site_units <- function(df, unit_cw_path, lu_col = "site_use_code", units_col = "site_units", area_col = "site_bld_area", data_path = "data", est_size = 900) {
  unit_cw <- readr::read_csv(file.path(data_path, unit_cw_path), show_col_types = FALSE) |>
    dplyr::select(code, units_low, units_high)
  
  df |>
    dplyr::left_join(unit_cw, by = setNames("code", lu_col)) |>
    
    # Estimate unit count
    dplyr::mutate(
      unit_by_area = as.integer(ceiling(.data[[area_col]] / est_size)),
      
      unit_valid = dplyr::case_when(
        !is.na(.data[[units_col]]) & !is.na(.data[["units_low"]]) &
          (is.na(.data[["units_high"]]) | .data[[units_col]] <= .data[["units_high"]]) &
          .data[[units_col]] >= .data[["units_low"]] ~ TRUE,
        TRUE ~ NA
      ),
      
      "{units_col}" := dplyr::case_when(
        unit_valid == TRUE ~ .data[[units_col]],
        TRUE ~ dplyr::case_when(
          !is.na(.data[["units_low"]]) & !is.na(.data[["units_high"]]) &
            .data[["units_low"]] == .data[["units_high"]] ~ .data[["units_low"]],
          !is.na(.data[["units_low"]]) & !is.na(.data[["units_high"]]) &
            (.data[["unit_by_area"]] >= .data[["units_low"]] &
               .data[["unit_by_area"]] <= .data[["units_high"]]) ~ .data[["unit_by_area"]],
          TRUE ~ .data[["units_low"]]
        )
      )
    ) |>
    
    
    dplyr::select(-unit_by_area, -unit_valid, -units_low, -units_high)
}

df <- df_parcels |>
  load_assess_md(state = 'md', col_cw_path = 'col_cw.csv', unit_cw_path = 'unit_cw.csv', nrows = 1000, data_path = 'data') |>
  std_site_units(unit_cw_path = 'unit_cw.csv')