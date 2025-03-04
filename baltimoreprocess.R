source('load_results.R')
load_results("", load_boundaries=TRUE, summarize=TRUE)

#' Clean Baltimore Assessment Data Using Column Crosswalk
#'
#' @param assess_path Path to CSV file containing the Baltimore assessment data
#' @param crosswalk_path Path to the CSV file containing column mapping 
#' @param output_path Optional path to save the cleaned data 
#'
#' @return A dataframe with columns selected and renamed according to the crosswalk
rename_assess <- function(assess_path, crosswalk_path, output_path = NULL) {
  # Input validation? 
  if (!file.exists(assess_path)) {
    stop("Assess file not found: ", assess_path)
  }
  
  if (!file.exists(crosswalk_path)) {
    stop("Crosswalk file not found: ", crosswalk_path)
  }
  
  assess <- readr::read_csv(assess_path, show_col_types = FALSE)
  col_crosswalk <- readr::read_csv(crosswalk_path, show_col_types = FALSE)
  
  # Identify MAR columns that exist in the input data
  available_mar_cols <- base::intersect(col_crosswalk$MAR_col, colnames(assess))
  
  # Create mapping of MAR columns to MAS columns (key is MAR, value is MAS)
  col_mapping <- col_crosswalk |>
    dplyr::filter(MAR_col %in% available_mar_cols) |>
    dplyr::select(MAS_col, MAR_col) |>
    dplyr::mutate(MAR_col = as.character(MAR_col), 
                  MAS_col = as.character(MAS_col)) |>
    tibble::deframe()
  
  
  clean_balti <- assess |>
    dplyr::select(dplyr::all_of(available_mar_cols), SUBTYPE_GEODB) |>
    dplyr::rename(!!!rlang::set_names(col_mapping, names(col_mapping)))
  
  # MAS columns that don't have MAR equivalents
  all_mas_cols <- col_crosswalk$MAS_col
  existing_mas_cols <- names(clean_balti)
  missing_cols <- base::setdiff(all_mas_cols, existing_mas_cols)
  
  # Add missing columns with NA values
  for (col in missing_cols) {
    clean_balti[[col]] <- NA
  }
  
  # Reorder columns to match the order in col_crosswalk
  ordered_cols <- base::intersect(all_mas_cols, names(clean_balti))
  clean_balti <- clean_balti|>
    dplyr::select(dplyr::all_of(ordered_cols), dplyr::everything())
  
  if (!is.null(output_path)) {
    readr::write_csv(clean_balti, output_path)
  }
  
  # add MUNI_OD values 
  clean_balti <- clean_balti |>
    mutate(SITE_MUNI_ID = 1)
  
  return(clean_balti)
}


clean_balti <- rename_assess(
  assess_path = "data/balti_assess.csv",
  crosswalk_path = "data/col_crosswalk.csv",
  output_path = "data/clean_balti_assess.csv"
)

# filter residential 
usecode_crosswalk <- readr::read_csv("data/usecode_crosswalk.csv", show_col_types = FALSE)
mar_codes <- usecode_crosswalk$MAR_code
mar_codes <- mar_codes[!is.na(mar_codes)]
mas_codes <- usecode_crosswalk$MAS_code

filtered_balti <- clean_balti |>
  dplyr::filter(SITE_USE_CODE %in% mar_codes)


unit_add_balti <- filtered_balti |>
  dplyr::mutate(UNIT_LOW = dplyr::case_when(
    SITE_USE_CODE== "R" & SUBTYPE_GEODB == 1 ~ "1",
    SITE_USE_CODE == "R" & SUBTYPE_GEODB == 2 ~ "2",
    SITE_USE_CODE == "M" ~ "4",
    SITE_USE_CODE == "U" ~ "1",
    SITE_USE_CODE == "CC" ~"1",
    TRUE ~ NA_character_
  )) |>
  dplyr::mutate(UNIT_HIGH = dplyr::case_when(
    SITE_USE_CODE== "R" & SUBTYPE_GEODB == 1 ~ "1",
    SITE_USE_CODE == "U" ~ "1",
    SITE_USE_CODE == "CC" ~"1",
    TRUE ~ NA_character_
  ))

readr::write_csv(unit_add_balti, "data/mapped_balti_assess.csv")



# search llc
owners |>
  dplyr::slice_head(n=1000) |>
  dplyr::mutate(has_llc = stringr::str_detect(name, "LLC"))


std_flag <- function(df, col, string, flag_col) {
  #' Flags provided text in provided column using regex defined in global 
  #' 
  #' @param df A dataframe
  #' @param col A column containing characters. 
  #' @param string A string to flag the presence of.
  #' @param flag_col A name for the new column. If not provided, name is generated based on `string` text. .
  #' @returns A dataframe with added flag column.
  #' @export
  #' @details If no `flag_col` provided, its name will be generated based on `string` text.
  
  if (missing(flag_col)) {
    flag_col <- string |>
      stringr::str_split("[\\s\\,\\.]+", simplify = TRUE) |> 
      stringr::str_sub(1, 3) |>                   
      stringr::str_to_lower() |>                   
      paste(collapse = "_")
  }
  
  df |>
    dplyr::mutate(
      !!flag_col := stringr::str_detect(.data[[col]], string)
      )
}



#test
owners <- std_flag (owners, col = "name", string = "LLC", flag_col = "llllll")
owners <- owners |> 
  dplyr::select(-age_uni)

