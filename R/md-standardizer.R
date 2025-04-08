gdb_path <- "data/Nov_2024_Parcels.gdb"
sf::st_layers(gdb_path)
parcel_boundaries <- sf::st_read(gdb_path, layer = "ParcelBoundaries")
col_crosswalk <- readr::read_csv("data/col_crosswalk.csv")
unit_crosswalk <- readr::read_csv("data/unit_crosswalk.csv")

# preliminary filtering ====

# Filter parcel_boundaries to include only the residential properties
parcels <- parcel_boundaries |> dplyr::filter(LU %in% c("R", "TH", "E", "M", "U", "CC", "CR", "RC"))
# Filter to include only relevant columns
filtercol <- function(dataset, col_crosswalk, additional_cols = NULL) {
  mar_columns <- col_crosswalk$MAR_col
  mar_columns <- mar_columns[!is.na(mar_columns) & mar_columns != ""]
  split_columns <- unlist(strsplit(mar_columns, ";")) 
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

# unit estimation ====
std_units_by_luresityp <- function(data, csv_path, filter_state = "md") {
  reference_data <- utils::read.csv(csv_path, stringsAsFactors = FALSE)
  names(reference_data) <- tolower(names(reference_data))
  state_data <- reference_data[tolower(reference_data$state) == tolower(filter_state), ]
  
  # Build lookup tables 
  # Exact match lookup (code + resityp)
  exact_lookup <- data.frame(
    code = state_data$code,
    resityp = state_data$resityp,
    unit_low = state_data$unit_low,
    unit_high = state_data$unit_high,
    stringsAsFactors = FALSE
  )
  # Split multi-value resityp entries (handling the either or case)
  expanded_lookup <- data.frame(
    code = character(),
    resityp = character(),
    unit_low = numeric(),
    unit_high = numeric(),
    stringsAsFactors = FALSE
  )
  
  for(i in 1:nrow(exact_lookup)) {
    code_val <- exact_lookup$code[i]
    resityp_val <- exact_lookup$resityp[i]
    low_val <- exact_lookup$unit_low[i]
    high_val <- exact_lookup$unit_high[i]
    
    if(is.na(resityp_val)) {
      resityp_val <- "<NA>"
    }
    resityp_options <- strsplit(resityp_val, "[;:]")[[1]]
    for(option in resityp_options) {
      option <- trimws(option)  
      expanded_lookup <- rbind(
        expanded_lookup,
        data.frame(
          code = code_val,
          resityp = option,
          unit_low = low_val,
          unit_high = high_val,
          stringsAsFactors = FALSE
        )
      )
    }
  }
  
  
  data$estimated_unit_low <- NA
  data$estimated_unit_high <- NA
  
  for(i in 1:nrow(data)) {
    lu_val <- data$LU[i]
    resityp_val <- data$RESITYP[i]
    if(is.na(resityp_val)) {
      resityp_val <- "<NA>"
    }
    # Look for exact match
    exact_matches <- expanded_lookup[
      expanded_lookup$code == lu_val & 
        expanded_lookup$resityp == resityp_val, 
    ]
    if(nrow(exact_matches) > 0) {
      data$estimated_unit_low[i] <- exact_matches$unit_low[1]
      data$estimated_unit_high[i] <- exact_matches$unit_high[1]
    }
  }
 
  return(data)
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
calculate_site_ls_price <- function(data, mar_col) {
  column_parts <- strsplit(mar_col, ";")[[1]]
  # Ensure both columns exist, if not, use 0
  considr1_values <- if(column_parts[1] %in% names(data)) data[[column_parts[1]]] else 0
  mortgage_values <- if(column_parts[2] %in% names(data)) data[[column_parts[2]]] else 0
  # Combine with NA handling
  combined_values <- ifelse(is.na(considr1_values), 0, considr1_values) + 
    ifelse(is.na(mortgage_values), 0, mortgage_values)
  return(combined_values)
}
# Combine land use code and residential type as reference for site use code 
combine_site_use_code <- function(data, mar_col) {
  column_parts <- strsplit(mar_col, ";")[[1]]
  column_parts <- trimws(column_parts)  
  first_col_values <- if(column_parts[1] %in% names(data)) data[[column_parts[1]]] else rep(NA, nrow(data))
  second_col_values <- if(column_parts[2] %in% names(data)) data[[column_parts[2]]] else rep(NA, nrow(data))
  # Combine values with semicolon
  combined_values <- character(length = nrow(data))
  for (i in 1:nrow(data)) {
    # Convert NA to "NA" string for combination
    first_val <- if(is.na(first_col_values[i])) "NA" else as.character(first_col_values[i])
    second_val <- if(is.na(second_col_values[i])) "NA" else as.character(second_col_values[i])
    combined_values[i] <- paste(first_val, second_val, sep = ";")
  }
  return(combined_values)
}

#' Standardize Parcel Columns According to Mapping
#' This function takes a parcels dataset with original column names (MAR_col) and 
#' standardizes them according to a mapping table to the desired column names (MAS_col).
#' @param data A data frame containing the original parcel data with MAR_col column names
#' @param crosswalk A data frame with the mapping between MAR_col and MAS_col
#'
#' @return A data frame with standardized column names according to MAS_col
std_rename_col<- function(data, crosswalk) {
  std_data <- data.frame(matrix(nrow = nrow(data), ncol = 0))
  
  for (row_index in 1:nrow(crosswalk)) {
    mas_col <- crosswalk$MAS_col[row_index]
    mar_col <- crosswalk$MAR_col[row_index]
    
    # Case 1: MAS_col has empty MAR_col - add empty column
    if (is.na(mar_col) || mar_col == "") {
      std_data[[mas_col]] <- NA
      next
    }
    
    # Case 2: Special case for SITE_LS_PRICE (sum of CONSIDR1 and MORTGAG)
    if (mas_col == "SITE_LS_PRICE" && grepl(";", mar_col)) {
      std_data[[mas_col]] <- calculate_site_ls_price(data, mar_col)
      next
    }
    
    # Case 3: Special case for SITE_USE_CODE (handle LU; RESITYP)
    if (mas_col == "SITE_USE_CODE" && grepl(";", mar_col)) {
      std_data[[mas_col]] <- combine_site_use_code(data, mar_col)
      next
    }
    
    # Case 4: Special case for SITE_UNITS
    if (mas_col == "SITE_UNITS") {
      if ("UNITS" %in% names(data)) {
        std_data[[mas_col]] <- data[["UNITS"]]
      } else {
        std_data[[mas_col]] <- NA
      }
      next
    }
    
    
    # Case 5: Standard renaming
    if (mar_col %in% names(data)) {
      std_data[[mas_col]] <- data[[mar_col]]
    } else {
      # Column doesn't exist in original data
      std_data[[mas_col]] <- NA
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