data_path <- "data"

gdb_path <- "Feb_2025_Parcels.gdb"


parcels <- sf::st_read(
  file.path(data_path, gdb_path), 
  # layer = "ParcelBoundaries"
  query = "SELECT * FROM ParcelBoundaries LIMIT 5000"
  )

col_cw <- readr::read_csv("data/col_cw.csv")
unit_cw <- readr::read_csv("data/unit_cw.csv")
state <- "MD"

lu_by_state <- function(state, cw_file = "unit_cw.csv", data_path = "data") {
  readr::read_csv(
    file.path(data_path, cw_file),
    show_col_types= FALSE
    ) |>
     dplyr::filter(state == state) |>
     dplyr::pull(code) |>
     unique()
}

cols_by_state <- function(state, cw_file = "col_cw.csv", data_path = "data") {
  state <- stringr::str_to_lower(state)
  readr::read_csv(
    file.path(data_path, cw_file),
    show_col_types= FALSE
  ) |>
    tidyr::drop_na(state) |>
    dplyr::pull(state) |>
    stringr::str_split("\\|") |>
    unlist() |>
    unique()
}

p <- parcels |> 
  dplyr::rename_with(stringr::str_to_lower) |>
  dplyr::filter(lu %in% lu_by_state(state)) |>
  dplyr::select(dplyr::any_of(cols_by_state(state)))

cols_by_state("MD")

# Filter parcel_boundaries to include only the residential properties

# Filter to include only relevant columns
filtercol <- function(dataset, col_crosswalk, additional_cols = NULL) {
  md_columns <- col_crosswalk$md_col
  md_columns <- md_columns[!is.na(md_columns) & md_columns != ""]
  split_columns <- unlist(strsplit(md_columns, "|")) 
  split_columns <- trimws(split_columns)
  
  # Add additional columns if provided
  if (!is.null(additional_cols)) {
    split_columns <- c(split_columns, additional_cols)
  }
  # Remove duplicates before checking existence in dataset
  split_columns <- unique(split_columns)
  existing_columns <- split_columns[split_columns %in% names(dataset)]
  if (inherits(dataset, "sf")) {
    # Convert to regular data frame (drops geometry)
    result <- as.data.frame(dataset)[, existing_columns, drop = FALSE]
  } else {
    result <- dataset[, existing_columns, drop = FALSE]
  }
  return(result)
}

# Apply Filtering
parcels <- filtercol(
  parcels, 
  col_crosswalk,
  additional_cols = NULL)
names(parcels)

#' Title
#'
#' @param data Dataframe to link to unit crosswalk estimes.
#' @param unit_cw_path Path to crosswalk. By default `file.path(DATA_PATH, "unit_cw.csv")`
#' @param lu_col 
#' @param resi_col 
#'
#' @returns
#' @export
#'
#' @examples
std_units_by_lu <- function(data, unit_cw_path = file.path(DATA_PATH, "unit_cw.csv"), lu_col = "lu", resi_col = "resityp") {
  data |>
    # Flagged: hardcoded resityp on left will break unless column names match
    dplyr::left_join(
      unit_cw_path |>
        readr::read_csv() |>
        dplyr::rename_with(tolower) |>
        tidyr::separate_longer_delim("resityp", "|"), 
      by=c(
        resi_col = "resityp",
        lu_col = "code"
      )
    )
}


  
std_test_units <- function(data, col, units_low_col, units_high_col) {
  data |>
    dplyr::mutate(
      unit_valid = dplyr::case_when(
        # When unit_high column is not NA, check if col is between unit_low and unit_high columns
        !is.na({{ col }}) & !is.na({{ units_low_col }}) & !is.na({{ units_high_col }}) ~ 
          ({{ col }} >= {{ units_low_col }} & {{ col }} <= {{ units_high_col }}),
        # When unit_high column is NA, check if col is >= unit_low column
        !is.na({{ col }}) & !is.na({{ units_low_col }}) & is.na({{ units_high_col }}) ~ 
          ({{ col }} >= {{ units_low_col }}),
        TRUE ~ NA
      )
    )
}
std_estimate_units <- function(data, col, units_low_col, units_high_col, area_col, units_col, est_size = 900) {
  data |>
    # calculate units by area
    dplyr::mutate(
      unit_by_area = as.integer(ceiling({{ area_col }} / est_size))
    ) |>
    # apply the logic for unit estimation
    dplyr::mutate(
      # Using proper tidyeval syntax for dynamic column naming
      !!rlang::ensym(units_col) := dplyr::case_when(
        # A. If unit_valid is TRUE, use existing column value
        unit_valid == TRUE ~ {{ col }},
        
        # B. For all other cases (unit_valid is FALSE or NA)
        # Apply the sub-conditions
        TRUE ~ dplyr::case_when(
          # a. If units_high_col equals units_low_col, use that value
          !is.na({{ units_low_col }}) & !is.na({{ units_high_col }}) & {{ units_low_col }} == {{ units_high_col }} ~ 
            {{ units_low_col }},
          # b. If unit_by_area is between units_low_col and units_high_col, use explicit comparison
          !is.na({{ units_low_col }}) & !is.na({{ units_high_col }}) & 
            (unit_by_area >= {{ units_low_col }} & unit_by_area <= {{ units_high_col }}) ~ 
            unit_by_area,
          # c. Default to units_low_col in all other cases (since we know it exists)
          TRUE ~ {{ units_low_col }}
        )
      )
    )
}
# standardization of column names ====

#Calculate SITE_LS_PRICE Value by aggregating the values from CONSIDR1 and MORTGAG columns.
calculate_site_ls_price <- function(data, md_col) {
  column_parts <- strsplit(md_col, "|")[[1]]
  # Ensure both columns exist, if not, use 0
  considr1_values <- if(column_parts[1] %in% names(data)) data[[column_parts[1]]] else 0
  mortgage_values <- if(column_parts[2] %in% names(data)) data[[column_parts[2]]] else 0
  # Combine with NA handling
  combined_values <- ifelse(is.na(considr1_values), 0, considr1_values) + 
    ifelse(is.na(mortgage_values), 0, mortgage_values)
  return(combined_values)
}
# Combine land use code and residential type as reference for site use code 
combine_site_use_code <- function(data, md_col) {
  column_parts <- strsplit(md_col, "|")[[1]]
  column_parts <- trimws(column_parts)  
  first_col_values <- if(column_parts[1] %in% names(data)) data[[column_parts[1]]] else rep(NA, nrow(data))
  second_col_values <- if(column_parts[2] %in% names(data)) data[[column_parts[2]]] else rep(NA, nrow(data))
  # Combine values with semicolon
  combined_values <- character(length = nrow(data))
  for (i in 1:nrow(data)) {
    # Convert NA to "NA" string for combination
    first_val <- if(is.na(first_col_values[i])) "NA" else as.character(first_col_values[i])
    second_val <- if(is.na(second_col_values[i])) "NA" else as.character(second_col_values[i])
    combined_values[i] <- paste(first_val, second_val, sep = "|")
  }
  return(combined_values)
}

#' Standardize Parcel Columns According to Mapping
#' This function takes a parcels dataset with original column names (md_col) and 
#' standardizes them according to a mapping table to the desired column names (ma_col).
#' @param data A data frame containing the original parcel data with md_col column names
#' @param crosswalk A data frame with the mapping between md_col and ma_col
#'
#' @return A data frame with standardized column names according to ma_col
std_rename_col<- function(data, crosswalk) {
  std_data <- data.frame(matrix(nrow = nrow(data), ncol = 0))
  
  for (row_index in 1:nrow(crosswalk)) {
    ma_col <- crosswalk$ma[row_index]
    md_col <- crosswalk$md[row_index]
    
    # Case 1: ma_col has empty md_col - add empty column
    if (is.na(md_col) || md_col == "") {
      std_data[[ma_col]] <- NA
      next
    }
    
    # Case 2: Special case for SITE_LS_PRICE (sum of CONSIDR1 and MORTGAG)
    if (ma_col == "SITE_LS_PRICE" && grepl("|", md_col)) {
      std_data[[ma_col]] <- calculate_site_ls_price(data, md_col)
      next
    }
    
    # Case 3: Special case for SITE_USE_CODE (handle LU; RESITYP)
    if (ma_col == "SITE_USE_CODE" && grepl("|", md_col)) {
      std_data[[ma_col]] <- combine_site_use_code(data, md_col)
      next
    }
    
    # Case 4: Special case for SITE_UNITS
    if (ma_col == "SITE_UNITS") {
      if ("UNITS" %in% names(data)) {
        std_data[[ma_col]] <- data[["UNITS"]]
      } else {
        std_data[[ma_col]] <- NA
      }
      next
    }
    
    
    # Case 5: Standard renaming
    if (md_col %in% names(data)) {
      std_data[[ma_col]] <- data[[md_col]]
    } else {
      # Column doesn't exist in original data
      std_data[[ma_col]] <- NA
    }
  }
  
  
  return(std_data)
}

# run with samples ====
samples <- parcels |> dplyr::slice(1:100)
units_samples <- std_units_by_luresityp(
  data = samples, 
  csv_path = "data/unit_crosswalk.csv", 
  filter_state = "md")
units_samples <- std_test_units(
  data = units_samples, 
  col = BLDG_UNITS, 
  units_low_col = estimated_unit_low, 
  units_high_col = estimated_unit_high)

units_samples <- std_estimate_units(
  data = units_samples, 
  col = BLDG_UNITS, 
  units_low_col = estimated_unit_low, 
  units_high_col = estimated_unit_high,
  units_col= "UNITS",
  area_col = SQFTSTRC, 
  est_size = 900)

std_parcels <- std_rename_col(units_samples, col_crosswalk)