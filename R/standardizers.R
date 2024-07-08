source("R/globals.R")

std_uppercase <- function(df, cols) {
  #' Uppercase string values
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param except Column or columns to remain untouched.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        stringr::str_to_upper
      ),
    )
}

std_remove_middle_initial <- function(df, cols) {
  #' Replace middle initial when formatted like "ERIC R HUNTLEY"
  #' 
  #' @param df A dataframe.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ stringr::str_replace(., "(?<=[A-Z] )[A-Z] (?=[A-Z])", "")
      )
    )
}

collapse_regex <- function(c, full_string = FALSE) {
  str <- stringr::str_c(c, collapse="|")
  if (full_string) {
    str <- stringr::str_c("^(", str, ")$")
  }
  str
}

std_replace_generic <- function(df, cols, pattern, replace) {
  if (missing(replace)) {
    df |>
      dplyr::mutate(
        dplyr::across(
          tidyselect::where(is.character) & tidyselect::all_of(cols),
          ~ stringr::str_squish(stringr::str_replace_all(., pattern))
        )
      )
  } else {
    df |>
      dplyr::mutate(
        dplyr::across(
          tidyselect::where(is.character) & tidyselect::all_of(cols),
          ~ stringr::str_squish(stringr::str_replace_all(., pattern, replace))
        )
      )
  }
}

std_remove_special <- function(df, cols) {
  #' Removes all special characters from columns, except slash
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  
  std_replace_generic(
    df, 
    cols, 
    pattern = "[^[:alnum:][:space:]\\/\\-\\#\\'\\`\\,([0-9].[0-9])]", 
    replace = " "
    ) |>
    std_replace_generic(
      cols, 
      pattern = "\\'|\\`", 
      replace = ""
    )
}

std_remove_commas <- function(df, cols) {
  #' Removes all special characters from columns, except slash
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  
  std_replace_generic(
    df, 
    cols, 
    pattern = "\\,", 
    replace = " "
  )
}

std_trailing_leading <- function(df, cols) {
  #' Standardizes slashes to have a space on either side and
  #' replaces all instances of an ampersand with the word "AND"
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  
  leading_trailing <- c(
    " OF ?$", 
    " AND ?$", 
    " THE ?$",
    "^ ?OF ",
    "^ ?AND ",
    "^ ?THE ",
    "[ \\,]+$",
    "^[ \\,]+"
  ) |>
    collapse_regex()
  std_replace_generic(
    df,
    cols, 
    pattern = leading_trailing,
    replace = ""
  )
}

std_leading_zeros <- function(df, cols, rmsingle = TRUE) {
  #' Remove leading zeros
  #'
  #' @param df A dataframe.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  #' 
  if (rmsingle) {
    lz <- c(
      "^0+(?=[1-9])",
      "^[\\- ]+"
    )
  } else {
    lz <- c(
      "^0+(?=0)",
      "^[\\- ]+"
      )
  }
  std_replace_generic(
    df,
    cols, 
    pattern = collapse_regex(lz, full_string = FALSE),
    replace = ""
  )
}

std_directions <- function(df, cols) {
  #' Standardizes abbreviated cardinal directions.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  directions <- c(
    # Directions
    "(?<=^| )N(?= |$)" = "NORTH",
    "^NO(?= )" = "NORTH",
    "(?<=^| )NW(?= |$)" = "NORTHWEST",
    "(?<=^| )NE(?= |$)" = "NORTHEAST",
    "^SO(?= )" = "SOUTH",
    "(?<=^| )S(?= |$)" = "SOUTH",
    "(?<=^| )SW(?= |$)" = "SOUTHWEST",
    "(?<=^| )SE(?= |$)" = "SOUTHEAST",
    "(?<=^| )E(?= |$)" = "EAST",
    "(?<=^| )W(?= |$)" = "WEST",
    "(?<=^| )GT(?= |$)" = "GREAT",
    "(?<=^| )MT(?= |$)" = "MOUNT",
    "(?<=^| )(CENTRE|CTR)(?= |$)" = "CENTER",
    "(?<=^| )VLLY(?= |$)" = "VALLEY"
  )
  std_replace_generic(
    df,
    cols, 
    pattern = directions
  )
}

std_replace_blank <- function(df, cols) {
  #' Replace blank string with NA and remove leading and trailing whitespace.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @returns A dataframe.
  #' @export
  blank <- c(
      ",?([:alnum:])\\2*",
      "[\\_\\-]+", 
      "N(ONE)?", 
      "N / A",
      "UNKNOWN",
      " *"
      # "^[- ]*SAME( ADDRESS)?"
      # "ABOVE"
    ) |>
    collapse_regex(full_string = TRUE)
  std_replace_generic(
    df,
    cols, 
    pattern = blank,
    replace = NA_character_
  )
}

std_street_types <- function(df, cols) {
  #' Standardize street types.
  #'
  #' @param df A dataframe.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  street <-c(
    # Correct for spaces between numbers and suffixes.
    # (Prevents errors like 3 RD > 3 ROAD after type std.)
    # So many saint addresses... thus the below comment-out.
    # "(?<= 1) (?=ST( |$))" = "",
    "(?<=[02-9]) ST " = " SAINT ",
    "(?<= 2) (?=ND( |$))" = "",
    "(?<= 3) (?=RD( |$))" = "",
    "(?<= [1-9]?[04-9]) (?=TH( |$))" = "",
    "(?<= )(ST|ST[RET]{3,5})(?=$| (#|U|PO))" = "STREET",
    "(?<= )STREE(?=$| )" = "STREET",
    "(?<= )AVE?(?=$| )" = "AVENUE",
    "(?<= )LA?N(?=$| )" = "LANE",
    "(?<= )BLV?R?D?(?=$| )" = "BOULEVARD",
    "(?<= )P(A?R?KWA?)?Y(?=$| )" = "PARKWAY",
    "(?<= )EXT(?=$| )" = "EXTENSION",
    "(?<= )PR?K(?=$| )" = "PARK",
    "(?<= )DRV?(?=$| )" = "DRIVE",
    "(?<= )RD(?=$| )" = "ROAD",
    "(?<= )T[ER]+R+(CE)?(?=$| )" = "TERRACE",
    "(?<= )PLC?E?(?=$| )" = "PLACE",
    "(?<= )WY(?=$| )" = "WAY",
    "(?<= )(CI?RC?)(?=$| )" = "CIRCLE",
    "(?<= )A[L]+E?Y(?=$| )" = "ALLEY",
    "(?<= )SQR?(?=$| )" = "SQUARE",
    "(?<= )HG?WY(?=$| )" = "HIGHWAY",
    "(?<= )CNTR(?=$| )" = "CENTER",
    "(?<= )FR?WY(?=$| )" = "FREEWAY",
    "(?<= )CR?T(?=$| )" = "COURT",
    "(?<= )PLZ?(?=$| )" = "PLAZA",
    "(?<= )W[HR]+F(?=$| )" = "WHARF",
    "(?<= )DEPT(?=$| )" = "DEPARTMENT",
    "(?<= |^)P ?O( ?BO?X)?[ \\#\\-]*(?=[A-Z]{0,1}[0-9])" = "PO BOX ",
    "(?<= |^)(?<!PO )BO?X[ \\#\\-]+(?=[A-Z]{0,1}[0-9])" = "PO BOX "
  )
  std_replace_generic(
    df,
    cols, 
    pattern = street
  )
}

std_small_numbers <- function(df, cols) {
  #' Standardize small leading numbers.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  sm_num <- c(
    "(?<=^|#| )ZERO(?= |$)" = "0",
    "(?<=^|#| )ONE(?= |$)" = "1",
    "(?<=^|#| )TWO(?= |$)" = "2",
    "(?<=^|#| )THREE(?= |$)" = "3",
    "(?<=^|#| )FOUR(?= |$)" = "4",
    "(?<=^|#| )FIVE(?= |$)" = "5",
    "(?<=^|#| )SIX(?= |$)" = "6",
    "(?<=^|#| )SEVEN(?= |$)" = "7",
    "(?<=^|#| )EIGHT(?= |$)" = "8",
    "(?<=^|#| )NINE(?= |$)" = "9",
    "(?<=^|#| )TEN(?= |$)" = "10",
    "(?<=^|#| )ELEVEN(?= |$)" = "11",
    "(?<=^|#| )TWELVE(?= |$)" = "12",
    "(?<=^|#| )THIRTEEN(?= |$)" = "13",
    "(?<=^|#| )FOURTEEN(?= |$)" = "14",
    "(?<=^|#| )FIFTEEN(?= |$)" = "15",
    "(?<=^|#| )SIXTEEN(?= |$)" = "16",
    "(?<=^|#| )SEVENTEEN(?= |$)" = "17",
    "(?<=^|#| )EIGHTEEN(?= |$)" = "18",
    "(?<=^|#| )NINETEEN(?= |$)" = "19",
    "(?<=^|#| )TWENTY(?= |$)" = "20",
    "(?<=^| )FIRST(?= )" = "1ST",
    "(?<=^| )SECOND(?= )" = "2ND",
    "(?<=^| )THIRD(?= )" = "3RD",
    "(?<=^| )FOURTH(?= )" = "4TH",
    "(?<=^| )FIFTH(?= )" = "5TH",
    "(?<=^| )SIXTH(?= )" = "6TH",
    "(?<=^| )SEVENTH(?= )" = "7TH",
    "(?<=^| )EIGHTH(?= )" = "8TH",
    "(?<=^| )NINTH(?= )" = "9TH",
    "(?<=^| )TENTH(?= )" = "10TH",
    "(?<=^| )ELEVENTH(?= )" = "11TH",
    "(?<=^| )TWELTH(?= )" = "12TH",
    "(?<=^| )THIRTEENTH(?= )" = "13TH",
    "(?<=^| )FOURTEENTH(?= )" = "14TH",
    "(?<=^| )FIFTEENTH(?= )" = "15TH",
    "(?<=^| )SIXTEENTH(?= )" = "16TH",
    "(?<=^| )SEVENTEENTH(?= )" = "17TH",
    "(?<=^| )EIGHTEENTH(?= )" = "18TH",
    "(?<=^| )NINTEENTH(?= )" = "19TH",
    "(?<=^| )TWENTIETH(?= )" = "20TH"
  )
  std_replace_generic(
    df,
    cols, 
    pattern = sm_num
  )
}

std_spacing_characters <- function(df, cols) {
  #' Standardizes slashes to have a space on either side and
  #' replaces all instances of an ampersand with the word "AND"
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  spacing_characters <- c(
    # Put space around slashes
    " ?/ ?" = " / ",
    # Replace & with AND
    " ?& ?" = " AND ",
    " ?(-|–|—) ?" = " - ",
    " ?' ?" = "",
    " ?, ?" = " , "
  )
  std_replace_generic(
    df,
    cols, 
    pattern = spacing_characters
  )
}

std_postal_format <- function(df, cols, postals) {
  #' Standardize and simplify (i.e., remove 4-digit suffix) US Postal codes.
  #'
  #' @param df A dataframe.
  #' @param cols Column or columns containing the ZIP code to be simplified.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
          ~ dplyr::case_when(
            stringr::str_detect(.x, "^O(?=[0-9]{4})") ~ 
              stringr::str_replace(.x, "^O", '0'),
            .default = .x
            )
        ),
        dplyr::across(
          tidyselect::where(is.character) & tidyselect::all_of(cols),
          ~ dplyr::case_when(
            stringr::str_detect(.x, "[0-9]{5} ?[\\-\\/]* ?[0-9]{4}") ~ 
              stringr::str_extract(
                .x,
                "[0-9]{5}(?=[ \\-])"
              ),
            stringr::str_detect(.x, "[0-9]{9}") ~ 
              stringr::str_extract(
                .x,
                "[0-9]{5}"
              ),
            .default = .x
          )
        ),
        dplyr::across(
          tidyselect::where(is.character) & tidyselect::all_of(cols),
            ~ dplyr::case_when(
              stringr::str_sub(
                stringr::str_replace_all(.x, "\\-", ""),
                1,
                5
                ) %in% postals$all ~ 
                  stringr::str_sub(
                    stringr::str_replace_all(.x, "\\-", ""),
                    1,
                    5
                    ),
              stringr::str_sub(
                stringr::str_replace_all(.x, "\\-", ""),
                -5,
                -1
                ) %in% postals$all ~ 
                  stringr::str_sub(
                    stringr::str_replace_all(.x, "\\-", ""),
                    -5,
                    -1
                    ),
              .default = .x
            )
          )
        )
}

std_muni_names <- function(df, cols) {
  #' Replace Boston neighborhoods with Boston
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  #' 
  muni_corrections <- c(
    "BORO$" = "BOROUGH",
    "^ACT$" = "ACTON",
    "^GLOUSTER$" = "GLOUCESTER",
    "^NEW TOWN$" = "NEWTON",
    "^DENNISPORT$" = "DENNIS PORT",
    "^HARWICHPORT$" = "HARWICH PORT",
    "^[A-Z]AMBRIDGE$" = "CAMBRIDGE",
    "^NEWBURY PORT$" = "NEWBURYPORT",
    "^DEVEN$" = "DEVENS",
    "^PRIDE CROSSING$" = "PRIDES CROSSING",
    "( ?MA(SSACHUSETTS)?| ST)$" = "",
    "FY[0-9]{4}$" = "",
    "(MAN.* SEA|MANCHESTER)" = "MANCHESTER BY THE SEA",
    "^ ?$" = ""
  )
  df <- df |>
    dplyr::mutate(
      muni_postal = stringr::str_extract(muni, "[0-9]+"),
      postal = dplyr::case_when(
        !is.na(muni_postal) & is.na(postal) ~ muni_postal,
        .default = postal
      ),
      muni = stringr::str_squish(stringr::str_remove_all(muni, "[0-9]+"))
    ) |>
    dplyr::select(-muni_postal)
  ma <- df |>
    dplyr::filter(state == "MA")
  ma |>
    std_replace_generic(
      cols, 
      pattern = muni_corrections
      ) |>
    std_replace_blank(cols) |>
    dplyr::bind_rows(
      df |> dplyr::filter(state != "MA" | is.na(state))
    )
}

# std_use_codes_deprec <- function(df, col) {
#   df |>
#     # Simplify by removing prefixes/suffixes.
#     std_replace_generic(
#       c("use_code"), 
#       pattern = c("[A-Z]|0(?=[1-9][0-9][1-9])|(?<=[1-9][0-9]{2})0|(?<=0[1-9][1-9])0|(?<=[1-9][0-9]{2})[1-9]" = "")
#     ) |>
#     dplyr::filter(
#       stringr::str_starts(use_code, "1") | 
#         (stringr::str_detect(use_code, "^(01|0[2-9]1|959|9[79]0)") & muni != "BOSTON") |
#         (stringr::str_detect(use_code, "^90[78]") & muni == "BOSTON") | 
#         (use_code != "199")
#     ) |>
#     # Appears to be some kind of condo quirk in Cambridge...|>
#     dplyr::mutate(
#       area = dplyr::case_when(
#         stringr::str_starts(use_code, "0") | use_code %in% c('113', '114') ~ res_area,
#         .default = pmax(res_area, bld_area, na.rm = TRUE)
#       )
#     ) |>
#     dplyr::filter(area > 0) |>
#     dplyr::mutate(
#       units = dplyr::case_when(
#         units == 0 ~ NA,
#         .default = units
#       ),
#       missing_units = is.na(units),
#       units = dplyr::case_when(
#         # Single-family.
#         use_code == '101' ~ 1,
#         # Condo.
#         use_code == '102' & (land_val == 0 | is.na(land_val)) ~ 1,
#         # Two-family.
#         use_code == '104' ~ 2,
#         # Three-family.
#         use_code == '105' ~ 3,
#         # Multiple houses.
#         use_code == '109' & is.na(units) ~ 
#           as.integer(round(area * 0.7 / 1200)),
#         use_code == '111' & (!dplyr::between(units, 4, 8) | missing_units) ~ 
#           as.integer(round(pmax(4, pmin(8, area * 0.7 / 1000)))),
#         # 8+ MA, 7-30 in Boston
#         use_code == '112' & muni != "BOSTON" & (!(units >= 8) | missing_units) ~ 
#           as.integer(round(pmax(8, area * 0.7 / 1000))),
#         use_code == '112' & muni == "BOSTON" & (!(units >= 7) | missing_units) ~ 
#           as.integer(round(pmax(7, pmin(30, area * 0.7 / 1000)))),
#         # 113: Boston, 39-99
#         use_code == '113' & muni == "BOSTON" & (!dplyr::between(units, 30, 99) | missing_units) ~ 
#           as.integer(round(pmax(30, pmin(99, area * 0.7 / 1000)))),
#         # 114: Boston, 100+
#         use_code == '114' & muni == "BOSTON" & (!(units >= 100) | missing_units) ~ 
#           as.integer(round(pmax(100, area * 0.7 / 1000))),
#         # Unclear what these are outside of Boston.
#         use_code %in% c('113', '114') & muni != "BOSTON" & missing_units ~ 
#           as.integer(round(area * 0.7 / 1000)),
#         # Boston Housing Authority
#         use_code == '908' & muni == "BOSTON" & missing_units ~ 
#           as.integer(round(area * 0.7 / 1000)),
#         missing_units ~ 
#           pmax(1, as.integer(round(area * 0.7 / 1000))),
#         .default = units
#       )
#     )
# }

std_use_codes <- function(df, col){
  
  # Read Land Use Codes
  lu <- readr::read_csv('data/comprehensive_adj_use.code_xwalk.csv', show_col_types = FALSE) |>
    dplyr::rename_with(tolower) |>
    dplyr::select(c(use_code, luc_assign))
  
  df <- df |> 
    dplyr::left_join(
      lu,
      by = c("use_code" = "use_code"),
      na_matches = "never"
    )
  
  unmatched <- df |>
    dplyr::filter(is.na(luc_assign)) |>
    dplyr::select(-luc_assign) |>
    dplyr::mutate(
      use_code_simp = substr(use_code, 1, 3)
    ) |>
    dplyr::left_join(
      lu,
      by = c("use_code_simp" = "use_code"),
      na_matches = "never"
    ) |>
    dplyr::select(-use_code_simp)
  
  df |>
    dplyr::filter(!is.na(luc_assign)) |>
    dplyr::bind_rows(unmatched) |>
    dplyr::rename(luc = luc_assign)
}

std_estimate_units <- function(df) {
  df |>
    dplyr::mutate(
      units = dplyr::case_when(
        is.na(units) ~ 0,
        .default = units
      ),
      units = dplyr::case_when(
        # Single-family.
        luc == '101' ~ 1,
        # Two-family.
        luc == '104' ~ 2,
        # Three-family.
        luc == '105' ~ 3,
        # 4-8 Unit
        # ===
        luc == '111' & units == 0 & dplyr::between(unit_count, 4, 8) ~
          unit_count,
        luc == '111' & units == 0 & !dplyr::between(unit_count, 4, 8) ~
          4,
        luc == '111' & dplyr::between(units, 4, 8) ~
          units,
        luc == '111' & units != 0 & !dplyr::between(units, 4, 8) & dplyr::between(unit_count, 4, 8)~
          unit_count,
        luc == '111' & units != 0 & !dplyr::between(units, 4, 8) & !dplyr::between(unit_count, 4, 8)~
          4,
        # 112: >8 Unit
        # ===
        luc == '112' & units == 0 & unit_count > 8 ~
          unit_count,
        luc == '112' & units == 0 & unit_count <= 8 ~
          9,
        luc == '112' & units > 8 & units == unit_count ~
          units,
        luc == '112' & units > 8 & units > unit_count ~
          units,
        luc == '112' & units > 8 & units < unit_count ~
          unit_count,
        luc == '112' & dplyr::between(units, 1, 8) & unit_count <= 8 ~
          9,
        luc == '112' & dplyr::between(units, 1, 8) & unit_count > 8 ~
          unit_count,
        # 102: Condos
        # ===
        luc == '102' & units == 0 ~
          unit_count,
        luc == '102' & units != 0 & units == unit_count ~
          units - 1,
        luc == '102' & units != 0 & units > unit_count ~
          units - 1,
        luc == '102' & units != 0 & units < unit_count ~
          unit_count,
        # 109: Multi House, 0xx: Multi-Use, 103: Mobile Home
        # ===
        luc %in% c('109', '0xx', '103', '970', '908', '959') & units == 0 ~
          unit_count,
        luc %in% c('109', '0xx', '103', '970', '908', '959') & units != 0 & units == unit_count ~
          units,
        luc %in% c('109', '0xx', '103', '970', '908', '959') & units != 0 & units > unit_count ~
          units,
        luc %in% c('109', '0xx', '103', '970', '908', '959') & units != 0 & units < unit_count ~
          unit_count,
        .default = units
      )
    ) |>
    dplyr::select(-unit_count)
}

std_massachusetts <- function(df, cols, street_name = FALSE) {
  #' Replace variations of Massachusetts with MA
  #'
  #' @param df A dataframe.
  #' @param cols Column or columns in which to replace "MASS"
  #' @returns A dataframe.
  #' @export
  if (street_name) {
    p <- "(?<= |^)(MASS)(?= |$)"
    r <- "MASSACHUSETTS"
  } else {
    p <- "(?<= |^)(MASS([ACHUSETTS]{8,10})?)(?= |$)"
    r <- "MA"
  }
  std_replace_generic(
    df,
    cols, 
    pattern = p,
    replace = r
  )
}

std_hyphenated_numbers <- function(df, cols) {
  #' Strips away second half of hyphenated number
  #'
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  df |>
    # Remove "C / O" prefix.
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ stringr::str_replace_all(
            stringr::str_replace_all(., "(?<=[0-9]{1,4}[A-Z]?)-[0-9]+[A-Z]?", ""),
            "(?<=[0-9]{1,4}[A-Z]?)-(?=[A-Z]{1,2})",
            ""
        )
      )
    )
}

std_select_address <- function(df,
                               addr_col1,
                               addr_col2,
                               output_col = "address") {
  #' Choose address column on simple criteria.
  #'
  #' @param df A dataframe.
  #' @param addr_col1 First address column to be compared.
  #' @param addr_col2 Second address column to be compared.
  #' @param output_col Name of column that stores selected address.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::mutate(
      !!output_col := dplyr::case_when(
        stringr::str_detect(
          get({{ addr_col2 }}), "^[0-9]") &
          !stringr::str_detect(get({{ addr_col1 }}), "^[0-9]")
        ~ get({{ addr_col2 }}),
        stringr::str_detect(
          get({{ addr_col2 }}), "^[0-9]") &
          stringr::str_detect(get({{ addr_col1 }}), "LLC")
        ~ get({{ addr_col2 }}),
        TRUE ~ get({{ addr_col1 }})
      )
    )
}

st_get_censusgeo <- function(sdf, state = "MA", crs = 2249) {
  #' Bind census geography IDs to geometries of interest.
  #'
  #' @param sdf A sf dataframe.
  #' @param state State of your study. (TODO: multiple states?)
  #' @param crs EPSG code of appropriate coordinate reference system.
  #' @returns A dataframe.
  #' @export
  censusgeo <- tigris::block_groups(state = state) |>
    sf::st_transform(crs) |>
    dplyr::rename(
      geoid_bg = GEOID
    ) |>
    dplyr::select(geoid_bg)
  sdf |>
    dplyr::mutate(
      point = sf::st_point_on_surface(geometry)
    ) |>
    sf::st_set_geometry("point") |>
    sf::st_join(
      censusgeo
    ) |>
    sf::st_set_geometry("geometry") |>
    dplyr::mutate(
      geoid_t = stringr::str_sub(geoid_bg, start = 1L, end = 11L)
    ) |>
    dplyr::select(
      -c(point)
    )
}

std_corp_rm_sys <- function(df, cols) {
  #' Replace variations on "CORPORATION SYSTEMS" with NA in corp addresses.
  #'
  #' @param df A dataframe.
  #' @param cols The columns containing the addresses to be standardized.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ dplyr::case_when(
          stringr::str_detect(
            .,
            "(((CORP(ORATION)?)|(LLC)|) (SYS)|(SER))|(AGENT)|(BUSINESS FILINGS)"
          ) ~ NA_character_,
          TRUE ~ .
        )
      )
    )
}

std_squish <- function(df, cols) {
  df |> 
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ stringr::str_squish(.)
      )
    )
}

std_flow_strings <- function(df, cols) {
  #' Generic string standardization workflow.
  #'
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  df |>
    std_uppercase(cols) |>
    std_replace_blank(cols) |>
    std_spacing_characters(cols) |>
    std_remove_special(cols) |>
    std_small_numbers(cols) |>
    std_trailing_leading(cols) |>
    std_leading_zeros(cols, rmsingle = FALSE) |>
    std_squish(cols)
}

std_flow_names <- function(df, cols) {
  #' Name standardization workflow.
  #'
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  df |>
    std_corp_types(cols) |>
    std_corp_rm_sys(cols) |>
    std_remove_middle_initial(cols) 
}



std_owner_name <- function(df, col) {
  df |>
    std_street_types(col) |>
    std_massachusetts(col) |>
    dplyr::mutate(
      city_of = stringr::str_extract(name, "^(CITY|TOWN|VILLAGE) OF "),
      !!col := dplyr::case_when(
        !is.na(city_of) ~ stringr::str_c(
          stringr::str_remove(.data[[col]], stringr::str_c("^", city_of, sep="")),
          stringr::str_extract(city_of, "^[A-Z]+"),
          sep = " "
        ),
        .default = .data[[col]]
      ),
      !!col := stringr::str_replace_all(.data[[col]], "^0+(?=0 )|^# ?", "")
    ) |>
    dplyr::select(-city_of)
}

std_multiline_address <- function(df, number = FALSE) {
  if (number) {
    regex <- "[- ]+(([A-Z]?[0-9\\/ \\-]+[A-Z]?)|([A-Z]( [A-Z])?)|(U \\-))$"
  } else {
    terms <- c(
      "UNITS?", 
      "SUITES?", 
      "STE[ -\\#]+", 
      "AP(AR)?T(MENT)?", 
      "ROOM", 
      "OFFICE",
      " RM[ -#]*[0-9]+", 
      " PH[ -#]*[0-9]+", 
      " U(PH)?([ -#]*[A-Z]?[0-9]+[A-Z]?)+", 
      " (NO |NUM )",  
      " PMB", 
      "\\#", 
      "PENT([HOUSE]{4,6})?"
    ) |>
      stringr::str_c(collapse="|")
    regex <- glue::glue("[- ]*({terms}).*$")
  }
  df |>
    dplyr::mutate(
      match = !po_box & stringr::str_detect(addr, regex) & is.na(addr2),
      addr2 = dplyr::case_when(
        match ~ stringr::str_extract(addr, regex),
        .default = addr2
      ),
      addr = dplyr::case_when(
        !is.na(addr2) ~ stringr::str_squish(stringr::str_remove(addr, stringr::str_c(addr2, "$", sep=""))),
        .default = addr
      ),
      addr2 = stringr::str_squish(addr2)
    ) |>
    dplyr::select(-match)
}

std_flow_multiline_address <- function(df) {
  df |>
    dplyr::mutate(
      po_box = stringr::str_detect(addr, "PO BOX"),
      addr2 = NA_character_,
      box = stringr::str_extract(addr, "PO BOX ([A-Z]?[0-9\\/\\-\\# ]+[A-Z]?$|[A-Z]?[0-9\\#]+[A-Z]?(?=[\\/\\- ]{1,3}))"),
      addr = stringr::str_remove(addr, "PO BOX ([A-Z]?[0-9\\/\\-\\# ]+[A-Z]?$|[A-Z]?[0-9\\#]+[A-Z]?[\\/\\- ]{1,3})")
    ) |>
    dplyr::mutate(
      floor = stringr::str_extract(addr, "[- ]?([0-9]FL[OR]{0,3}|[0-9]{1,3}(TH|ST|RD|ND) FL[OR]{0,3}|FL[OR]{0,3} [0-9]{1,3})"),
      addr = dplyr::case_when(
        !is.na(floor) ~ stringr::str_squish(stringr::str_remove(addr, floor)),
        .default = addr
      ),
      floor = stringr::str_squish(floor)
    ) |>
    std_multiline_address(number = FALSE) |>
    std_multiline_address(number = TRUE) |>
    std_replace_blank("addr")
}

std_fill_postal_sp <- function(df, parcels, zips, crs=CRS) {
  ma_zips <- zips |>
    dplyr::filter(ma) |>
    sf::st_transform(crs)
  
  df <- df |> 
    dplyr::filter(!(postal %in% ma_zips$zip)) |>
    dplyr::left_join(sf::st_point_on_surface(parcels), by=dplyr::join_by(loc_id)) |>
    sf::st_as_sf() |>
    sf::st_join(dplyr::select(ma_zips, zip)) |>
    dplyr::mutate(
      postal = zip
    ) |>
    dplyr::select(-zip) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(postal %in% ma_zips$zip)
    ) |>
    sf::st_drop_geometry()
}

std_fill_muni_sp <- function(df, parcels, ma_munis, ma_postals) {
  df <- df |>
    dplyr::left_join(sf::st_point_on_surface(parcels), by=c("loc_id" = "loc_id")) |>
    sf::st_as_sf() 
  
  df <- df |>
    dplyr::filter(!(muni %in% ma_munis$pl_name)) |>
    sf::st_join(dplyr::select(ma_munis, pl_name)) |>
    dplyr::mutate(
      muni = pl_name
    ) |>
    dplyr::select(-pl_name) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(muni %in% ma_munis$pl_name)
    )
}

std_address_range <- function(df) {
  df |>
    dplyr::mutate(
      # Extract address, accounting for possible presence of half-addresses.
      addr_range = stringr::str_detect(
        addr, 
        "^[0-9]+[A-Z]{0,1} *(1 \\/ 2)?([ -]+[0-9]+[A-Z]{0,1} *(1 \\/ 2)?)+ +(?=[A-Z0-9])"
      ),
      addr_num = stringr::str_extract(
        addr, 
        "^[0-9]+[A-Z]{0,1} *(1 \\/ 2)?([ -]+(([0-9]+[A-Z]{0,1} *(1 \\/ 2)?)*|[A-Z]))? +(?=[A-Z0-9])"
      ),
      # Extract address body.
      addr_body = stringr::str_remove(
        addr, 
        addr_num
      ),
      addr_temp = stringr::str_replace_all(
        addr_num, 
        " 1 \\/ 2", "\\.5"
      ),
      addr_start = as.numeric(
        stringr::str_extract(
          addr_temp, 
          "^[0-9\\.]+"
        )
      ),
      addr_end = dplyr::case_when(
        addr_range ~ 
          as.numeric(
            stringr::str_extract(
              addr_temp, 
              "(?<=[- ])[0-9\\.]+"
            )
          ),
        .default = addr_start
      ),
      addr_num = dplyr::case_when(
        (addr_start == addr_end) & addr_range ~ 
          stringr::str_extract(addr_num, "^[0-9]+[A-Z]{0,1}( ?1 / 2)?"),
        .default = addr_num
      ),
      addr_end = dplyr::case_when(
        (addr_end < addr_start) | is.na(addr_end) ~ addr_start,
        .default = addr_end
      ),
      even = dplyr::case_when(
        floor(addr_start) %% 2 == 0 ~ TRUE,
        floor(addr_start) %% 2 == 1 ~ FALSE,
        .default = NA
      ),
      parsed = !is.na(addr_start) & !is.na(addr_end) & !is.na(addr_body),
      addr = dplyr::case_when(
        parsed & (addr_start == addr_end) ~ stringr::str_c(addr_start, addr_body, sep = " "),
        parsed ~ stringr::str_c(
          stringr::str_c(addr_start, addr_end, sep = " - "),
          addr_body, sep = " "),
        .default = addr
      )
    ) |>
    dplyr::select(-c(parsed, addr_temp, addr_range))
}

std_fill_state_by_zip <- function(df, zips) {
  df <- df |>
    dplyr::filter(is.na(state), !is.na(postal)) |>
    dplyr::left_join(
      zips |>
        dplyr::filter(state_unambig) |>
        dplyr::select(zip, zip_state = state), 
      by = c("postal" = "zip")) |>
    dplyr::mutate(
      condition = !is.na(zip_state) & (country == "US" | is.na(country)),
      state = dplyr::case_when(
        condition ~ zip_state,
        .default = state
      )
    ) |>
    dplyr::select(-c(condition, zip_state)) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(!is.na(state) | is.na(postal))
    )
}

std_fill_zip_by_muni <- function(df, zips) {
  df <- df |>
    dplyr::filter(is.na(postal), !is.na(muni)) |>
    dplyr::left_join(
      zips |>
        sf::st_drop_geometry() |>
        dplyr::filter(!is.na(muni_unambig_from)) |>
        dplyr::select(pl_name = muni_unambig_from, zip),
      by = c("muni" = "pl_name")
    ) |>
    dplyr::mutate(
      postal = dplyr::case_when(
        !is.na(zip) ~ zip,
        .default = postal
      )
    ) |>
    dplyr::select(-zip) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(!is.na(postal) | is.na(muni))
    )
}

std_address_by_matching <- function(df, zips, places) {
  #' Walks through a series of possible matching steps using, e.g.,
  
  # Direct matches to municipality names.
  df_ma <- df |>
    dplyr::filter(state == "MA") |>
    dplyr::mutate(
      match = muni %in% places$muni_name
    )
  
  # Match on places (incl municipalities).
  df_ma <- df_ma |>
    dplyr::filter(!match, !is.na(muni)) |>
    dplyr::left_join(
      places |>
        dplyr::select(pl_name, muni_name),
      by = c("muni" = "pl_name")
    ) |>
    dplyr::mutate(
      match = !is.na(muni_name),
      muni = dplyr::case_when(
        match ~ muni_name,
        .default = muni
      )
    ) |>
    dplyr::select(-c(muni_name)) |>
    dplyr::bind_rows(
      df_ma |>
        dplyr::filter(match | is.na(muni))
    )
  
  # Match on unambiguous ZIP codes.
  df_ma <- df_ma |>
    dplyr::filter(!match, !is.na(postal)) |>
    dplyr::left_join(
      zips |>
        dplyr::filter(!is.na(muni_unambig_to)) |>
        dplyr::select(zip, pl_name = muni_unambig_to),
      by = c("postal" = "zip")
    ) |>
    dplyr::mutate(
      match = !is.na(pl_name),
      muni = dplyr::case_when(
        match ~ pl_name,
        .default = muni
      )
    ) |>
    dplyr::select(-c(pl_name)) |>
    dplyr::bind_rows(
      df_ma |>
        dplyr::filter(match | is.na(postal))
    )
  
  df_ma <- df_ma |>
    dplyr::filter(!match, !is.na(muni)) |>
    fuzzyjoin::regex_left_join(
      places |>
        dplyr::select(pl_name_fuzzy, muni_name),
      by = c("muni" = "pl_name_fuzzy")
    ) |>
    dplyr::mutate(
      dist = stringdist::stringdist(muni, muni_name)
    ) |>
    dplyr::group_by(dplyr::across(-c(dist, muni_name, pl_name_fuzzy))) |>
    dplyr::slice_min(dist, n=1) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      match = !is.na(muni_name),
      muni = dplyr::case_when(
        match ~ muni_name,
        .default = muni
      )
    ) |>
    dplyr::select(-c(dist, muni_name, pl_name_fuzzy)) |>
    dplyr::bind_rows(
      df_ma |>
        dplyr::filter(match | is.na(muni))
    )
  
  df_ma <- df_ma |>
    dplyr::filter(!match, !is.na(muni)) |>
    dplyr::mutate(
      muni_simp = stringr::str_remove(muni, "^(SOUTH(WEST|EAST)?|NORTH(WEST|EAST)?|EAST|WEST) ")
    ) |>
    fuzzyjoin::regex_left_join(
      places |>
        dplyr::select(pl_name_fuzzy, muni_name),
      by = c("muni_simp" = "pl_name_fuzzy")
    ) |>
    dplyr::mutate(
      dist = stringdist::stringdist(muni, muni_name)
    ) |>
    dplyr::group_by(dplyr::across(-c(dist, muni_name, pl_name_fuzzy))) |>
    dplyr::slice_min(dist, n=1) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      match = !is.na(muni_name),
      muni = dplyr::case_when(
        match ~ muni_name,
        .default = muni
      )
    ) |>
    dplyr::select(-c(dist, muni_name, pl_name_fuzzy, muni_simp)) |>
    dplyr::bind_rows(
      df_ma |>
        dplyr::filter(match | is.na(muni))
    )
  
  
  
  df_ma <- df_ma |>
    dplyr::filter(!match & !is.na(postal)) |>
    dplyr::left_join(
      zips |>
        dplyr::filter(state_unambig) |>
        dplyr::select(zip, zip_state = state),
      by = c("postal" = "zip")
    ) |>
    dplyr::mutate(
      state = dplyr::case_when(
        !is.na(zip_state) ~ zip_state,
        .default = state
      )
    ) |>
    dplyr::select(-zip_state) |>
    dplyr::bind_rows(
      df_ma |>
        dplyr::filter(match | is.na(postal))
    )
  
  
  df <- df_ma |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(state != "MA" | is.na(state))
    ) |>
    dplyr::select(-match)
  
  df <- df |>
    dplyr::filter(!is.na(addr), !is.na(muni)) |>
    dplyr::group_by(addr, muni) |>
    tidyr::fill(postal, .direction = "downup") |>
    dplyr::ungroup() |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(is.na(addr) | is.na(muni))
    )
  
  df <- df |>
    dplyr::filter(!is.na(addr), !is.na(postal)) |>
    dplyr::group_by(addr, postal) |>
    tidyr::fill(muni, .direction = "downup") |>
    dplyr::ungroup() |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(is.na(addr) | is.na(postal))
    )
}

SEARCH <- tibble::lst(
  estate = c(
    "ESTATE OF", "(A )?LIFE ESTATE", "FOR LIFE", "LE"
  ),
  inst = c(
    "CORPORATION", " INC( |$)", "LLC", "LTD", "COMPANY", 
    "LP", "PROPERT(IES|Y)", "GROUP", "MANAGEMENT", "PARTNERS", 
    "REALTY", "DEVELOPMENT", "EQUITIES", "HOLDING", "INSTITUTE", 
    "DIOCESE", "PARISH", "CITY", "HOUSING", "AUTHORITY", "SERVICES", 
    "LEGAL", "SERVICES", "LLP", "UNIVERSITY", "COLLEGE", "ASSOCIATION",
    "CONDOMINIUM", "HEALTH", "HOSPITAL", "SYSTEM"
  ),
  trust = "(?= \\bTRUST(EES?)?)",
  trustees = "\\bTRUST(EES?)( OF)?\\b",
  trust_definite = c(
    "(IR)?REVOCABLE", "NOMINEE", "INCOME ONLY", "UNDER DECLARATION OF"
  ),
  trust_types = c(
    trust_definite,
    "LIVING", "REALTY", "REAL ESTATE", 
    "FAMI(LY)?( SEC[URITY]{0,5})?", "JOINT",
    "PERSONAL", "(19|20)[0-9]{2}", "HOUSE", 
    "WEALTH", "HOLDINGS", "INVESTMENT", "GIFT",
    "UNDER"
  ),
  trust_regex = stringr::str_c(
    "(\\b(",
    stringr::str_c(trust_types, collapse = "|"),
    ")\\b(?:\\s+\\b(",
    stringr::str_c(trust_types, collapse = "|"),
    ")\\b)*)?",
    sep = "") |>
    stringr::str_c(trust, sep = ""),
  trust_definite_regex = stringr::str_c(
    "\\b(",
    stringr::str_c(c(trust_definite, c("TRUST(EES?)?")), collapse = "|"),
    ")\\b",
    sep = ""),
  titles = c(
    # Professional titles
    "ESQ(UIRE)?", "MD", "JD", "PHD", "PC",
    # Generations and numbers
    "JR", "SR", "I+", "I*[VX]I*",
    "(AND )?ET( - | )?ALL?"
  )
)

flag_inst <- function(df, col) {
  #' Flags institutional names in provided column using terms defined in global
  #' SEARCH list.
  #' @param df A dataframe.
  #' @param col The column to be flagged.
  #' @returns A dataframe with added columns 'trust_{col}' and 'trustee_{col}'.
  #' @export
  flag <- stringr::str_c("inst", col, sep = "_")
  df |>
    dplyr::mutate(
      !!flag := stringr::str_detect(
        .data[[col]],
        stringr::str_c(
          "\\b(",
          stringr::str_c(SEARCH$inst, collapse = "|"),
          ")\\b",
          sep = "")
      ),
      !!flag := tidyr::replace_na(
        .data[[flag]],
        FALSE
      )
    )
}

flag_trust <- function(df, col) {
  #' Flags trusts and trustees in provided column using regex defined in global
  #' SEARCH list.
  #' @param df A dataframe.
  #' @param col The column to be flagged.
  #' @returns A dataframe with added columns 'trust_{col}' and 'trustee_{col}'.
  #' @export
  
  trust_flag <- stringr::str_c("trust", col, sep ="_")
  trustee_flag <- stringr::str_c("trustee", col, sep ="_")
  
  df |>
    dplyr::mutate(
      !!col := dplyr::case_when(
        stringr::str_detect(.data[[col]], "(^TRUST\\b ?)|(TRUST (?=AND ))") ~ 
          stringr::str_c(
            stringr::str_remove(.data[[col]], "(^TRUST\\b ?)|(TRUST (?=AND ))"),
            "TRUST",
            sep = " "
          ),
        .default = .data[[col]]
      ),
      !!trust_flag := stringr::str_detect(
        .data[[col]], 
        SEARCH$trust_regex
      ) | stringr::str_detect(
        .data[[col]], 
        SEARCH$trust_definite_regex
      ),
      !!trust_flag := tidyr::replace_na(
        .data[[trust_flag]],
        FALSE
      ),
      !!trustee_flag := stringr::str_detect(
        .data[[col]], 
        SEARCH$trustee
      ),
      !!trustee_flag := tidyr::replace_na(
        .data[[trustee_flag]],
        FALSE
      )
    )
}

flag_estate <- function(df, col, estate_name = "estate") {
  #' Flags estates in provided column using terms defined in global
  #' SEARCH list.
  #' @param df A dataframe.
  #' @param col The column to be flagged.
  #' @returns A dataframe with added columns 'trust_{col}' and 'trustee_{col}'.
  #' @export
  
  flag <- stringr::str_c(estate_name, col, sep = "_")
  df |>
    dplyr::mutate(
      !!flag := stringr::str_detect(
        .data[[col]], 
        stringr::str_c(
          "\\b(",
          stringr::str_c(SEARCH$estate, collapse = "|"),
          ")\\b",
          sep = ""
        )
      ),
      !!flag := tidyr::replace_na(
        .data[[flag]],
        FALSE
      )
    )
}

std_remove_estate <- function(df, col, estate_name = "estate") {
  #' Standardize names by removing estate indicators.
  #'
  #' @param df A dataframe.
  #' @param cols Column to be processed.
  #' @param estate_name Column prefix for output.
  #' @returns A dataframe.
  #' @export
  flag <- stringr::str_c(estate_name, col, sep = "_")
  df |>
    dplyr::mutate(
      !!col := stringr::str_remove_all(
        .data[[col]],
        stringr::str_c(SEARCH$estate, collapse = "|")
      ) |> 
        stringr::str_squish() |>
        stringr::str_replace("^$", NA_character_)
    )
}

std_inst_types <- function(df, cols) {
  #' Standardize institution types and keywords.
  #'
  #' @param df A dataframe.
  #' @param cols Column to be processed.
  #' @returns A dataframe.
  #' @export
  inst_regex = c(
    # "(?<=[A-Z]) (?=[A-Z])" = "",
    "COMM OF" = "COMMONWEALTH OF",
    "MASSACHUSETTS COMMONWEALTH" = "COMMONWEALTH OF MASSACHUSETTS",
    "COMM" = "COMMUNITY",
    "CORP[ORATION]{0,8}" = "CORPORATION",
    "INC[ORPORATED]{0,10}" = "INC",
    "PRO?[PERTIE]{1,6}S" = "PROPERTIES",
    "PRO?[PERT]{1,4}Y?" = "PROPERTY",
    "L[IMI]{0,4}TE?D" = "LIMITED",
    "PA?RTN[ERS]{1,3}" = "PARTNERS",
    "(P[AR]{0,2}TN[ERS]{1,3}[HIP]{1,4}S?|PRTSHIP|PTSH)" = "PARTNERSHIP",
    "LANDMALL" = "LANDMARK",
    "LANDMALLS" = "LANDMARKS",
    "M[ANA]{0,4}G[EMENT]{0,6}" = "MANAGEMENT",
    "TECH" = "TECHNOLOGY",
    "INST[ITUT]{3,5}E?" = "INSTITUTE",
    "UNI[VERSITY]{6,8}" = "UNIVERSITY",
    "(COMP[ANY]{2,4}|CO(MP)*)" = "COMPANY",
    "GR[OU]{0,3}P" = "GROUP",
    "INV" = "INVESTMENT",
    "BK" = "BANK",
    "ESQ" = "ESQUIRE",
    "PRIV" = "PRIVATE",
    "(RLTY|RTY|RELTY|RALTY)" = "REALTY",
    "R / E" = "REAL ESTATE",
    "(LI?V[IN]{1,3}G|LIV)" = "LIVING",
    "FAM" = "FAMILY",
    "NOM[INEE]{3,5}" = "NOMINEE",
    "IRREV[OCABLE]{0,7}" = "IRREVOCABLE",
    "IRR(?= TR)" = "IRREVOCABLE",
    "REV[OCABLE]{0,7}" = "REVOCABLE",
    "CONDO[MINIU]{0,7}" = "CONDOMINIUM",
    "L L C" = "LLC",
    "L P" = "LP",
    "G P" = "GP",
    "L T D" = "LTD",
    "ET( (- )?)?AL" = "",
    "L[IMI]{0,4}TE?D" = "LTD",
    "LTD LIABILITY (COMPANY|CORPORATION)" = "LLC",
    "LTD LLC" = "LLC",
    "LTD (LIABILITY )?PARTNERS(HIP)?" = "LLP",
    "LP" = "LLP",
    "JU?ST[ \\-]*A[ \\-]*START" = "JUST A START",
    "GENERAL PARTNERS(HIP)?" = "GP",
    "AUTH[ORITY]{0,6}" = "AUTHORITY",
    "(ASSN?|ASSOC)" = "ASSOCATION",
    "DEPT" = "DEPARTMENT",
    "(TRU?ST|TR|TRT|TRUS|TRU|TRYST)" = "TRUST",
    "(CO( - )?)?(TRS|TRU?ST[ES]{1,4}|TRSTS)" = "TRUSTEES"
  )
  names(inst_regex) <- stringr::str_c("\\b", names(inst_regex), "\\b")
  std_replace_generic(
    df,
    cols, 
    pattern = inst_regex
  )
}

std_assess_addr <- function(df, 
                            zips,
                            places,
                            states = FALSE, 
                            postal = FALSE,
                            country = FALSE) {
  df <- df |>
    std_street_types("addr") |>
    std_directions(c("muni", "addr")) |>
    std_leading_zeros("addr", rmsingle = TRUE) |>
    std_massachusetts("addr", street_name = TRUE) |>
    std_postal_format(c("postal"), zips) |>
    std_muni_names(c("muni")) 
  
  if (country) {
    df <- df |>
      dplyr::mutate(
        country = stringr::str_squish(
          stringr::str_remove_all(country, "[:digit:]|\\/|//-")
        ),
        country = countrycode::countrycode(
          country,
          'country.name.en.regex',
          'iso2c',
          warn = FALSE
        ),
        country = dplyr::case_when(
          is.na(country) & stringr::str_detect(muni, "SINGAPORE") ~ "SG",
          is.na(country) & stringr::str_detect(muni, "JERUSALEM") ~ "IL",
          is.na(country) & stringr::str_detect(muni, "BEIJING") ~ "CN",
          is.na(country) & stringr::str_detect(muni, "LONDON") ~ "GB",
          is.na(country) & stringr::str_detect(muni, "TOKYO") ~ "JP",
          is.na(country) & state %in% state.abb & postal %in% zips$all ~ "US",
          .default = country
        ),
        muni = dplyr::case_when(
          country == "SG" ~ "SINGAPORE",
          .default = muni
        )
      )
  }
  
  df <- df |>
    std_address_by_matching(zips = zips, places = places)
  
  df
}

std_separate_and_label <- function(df, 
                                   col, 
                                   regex, 
                                   label,
                                   base_label = "owner",
                                   target_col = "name",
                                   clear_cols = c()) {
  regex <- stringr::regex(regex)
  df_base <- df |>
    dplyr::mutate(
      flag = stringr::str_detect(.data[[col]], regex),
      flag = dplyr::case_when(
        is.na(flag) ~ FALSE,
        .default = flag
      )
    )
  df <- df_base |>
    dplyr::filter(flag) |>
    tidyr::separate_longer_delim(
      tidyselect::all_of(col),
      regex
    ) |> 
    dplyr::filter(
      nzchar(.data[[col]])
    ) |>
    dplyr::group_by(dplyr::across(-dplyr::all_of(col))) |>
    dplyr::mutate(
      own_position = dplyr::row_number(),
      count = dplyr::n()
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      type_label = dplyr::case_when(
        own_position > 1 | own_position == count ~ label,
        .default = type_label
      )
    )
  
  if(target_col != col) {
    df <- df |>
      dplyr::filter(flag) |>
      tidyr::pivot_longer(
        cols = all_of(c(target_col, col)),
        names_to = "column"
      ) |>
      dplyr::mutate(
        type_label = dplyr::case_when(
          column == target_col ~ base_label,
          column == col ~ label
        ),
        !!col := NA_character_
      ) |>
      dplyr::rename(!!target_col := value) |>
      dplyr::select(-column)
  }
  df |> 
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(clear_cols),
        ~ dplyr::case_when(
          type_label == base_label ~
            NA_character_,
          .default = .
        )
      )
    ) |>
    dplyr::bind_rows(
      df_base |>
        dplyr::filter(!flag)
    ) |>
    dplyr::select(-c(own_position, count, flag))
}


std_co_dba_attn <- function(df, col = "name", target_col = "name") {
  df |>
    std_separate_and_label(
      col = col,
      target_col = target_col,
      regex = "( ?C / [O0]| C[O0] - (?!TR|OP))",
      label = "co",
      clear_cols = c("addr", "muni", "state", "postal", "country")
    ) |>
    std_separate_and_label(
      col = col,
      target_col = target_col,
      regex = "(^| )(D(( \\/ )| )?B(( \\/ )| )?A)($| )",
      label = "dba"
    ) |>
    std_separate_and_label(
      col = col,
      target_col = target_col,
      regex = "(^| )ATTN($| )|(^ATT )",
      label = "attn"
    )
}

std_flow_assess_parcel <- function(assess) {
  assess |>
    dplyr::select(
      prop_id, loc_id, fy, addr = site_addr, muni = city, postal = zip, 
      state, country, luc, units, bldg_val, land_val, total_val, ls_date, ls_price, 
      bld_area, res_area
    ) |>
    # Apply string standardization workflow to string columns.
    std_flow_strings(c("addr", "muni", "postal")) |>
    # Standardize addresses.
    std_assess_addr(
      zips = ZIPS, places = PLACES, 
      states = FALSE, postal = TRUE
    ) |>
    # Spatially match those addresses that can't be completely standardized with
    # reference to the table.
    # DEPRECATED - use std_fill_postal_sp, std_fill_muni_sp
    fill_muni_postal_spatial(PARCELS, ma_munis = MA_MUNIS, ma_postals = ZIPS$ma) |>
    std_flow_multiline_address() |>
    std_address_range() |>
    # Remove all unparseable addresses. (Most of these are street names with no
    # associated number.)
    dplyr::filter(!is.na(addr_body))
}

std_flow_owner_address <- function(df, zips, places) {
  df <- df |>
    dplyr::select(
      loc_id, prop_id, name = owner1, addr = own_addr, muni = own_city, 
      state = own_state, postal = own_zip, country = own_co, type_label
    ) |>
    std_flow_strings(c("name", "addr", "muni", "state", "postal", "country")) |> 
    dplyr::filter(!is.na(name) | !is.na(addr)) |>
    std_co_dba_attn(col = "addr", target_col = "name") |>
    std_remove_commas(c("addr", "muni", "state", "postal", "country")) |>
    std_assess_addr(
      zips = zips, places = places,
      states = TRUE, postal = TRUE, country = TRUE
    )
  
  # df |>
  #   dplyr::filter(!is.na(name)) |>
  #   dplyr::group_by(name_simp) |>
  #   dplyr::mutate(
  #     name = collapse::fmode(name),
  #     corp_id = dplyr::cur_group_id()
  #   ) |>
  #   dplyr::ungroup() |>
  #   dplyr::bind_rows(
  #     assess |>
  #       dplyr::filter(is.na(name))
  #   )
  df
}


# std_link_owner_parcel <- function(owner, parcel) {
#   owner <- owner |>
#     dplyr::left_join(
#       parcel |>
#         dplyr::select(
#           address_id = loc_id, 
#           addr_start_y = addr_start, 
#           addr_end_y = addr_end, 
#           addr_body, 
#           even, 
#           muni, 
#           state
#         ), 
#       by = dplyr::join_by(
#         addr_body, 
#         even, 
#         muni, 
#         state, 
#         between(
#           addr_start, 
#           addr_start_y, 
#           addr_end_y
#         )
#       ),
#       na_matches = "never"
#     ) |>
#     dplyr::select(-c(addr_start_y, addr_end_y))
#   
#   owner <- owner |>
#     dplyr::filter(!is.na(addr_start) & !is.na(corp_id)) |>
#     dplyr::group_by(corp_id, addr_start) |>
#     dplyr::mutate(
#       dplyr::across(
#         c(addr, postal, state, country, addr_num, addr_body, address_id),
#         ~ collapse::fmode(.)
#       ),
#       dplyr::across(
#         c(corp, trust, even),
#         ~ as.logical(max(., na.rm = TRUE))
#       ),
#       addr_end = max(na.omit(addr_end), na.rm = TRUE)
#     ) |>
#     dplyr::ungroup() |>
#     dplyr::bind_rows(
#       owner |> 
#         dplyr::filter(is.na(addr_start))
#     )
#   
#   owner |>
#     dplyr::filter(!is.na(corp_id)) |>
#     dplyr::group_by(corp_id, corp, trust) |>
#     dplyr::mutate(
#       dplyr::across(
#         c(addr, postal, po_box, state, country, addr_num, addr_body, address_id),
#         ~ dplyr::case_when(
#           is.na(.) ~ collapse::fmode(.),
#           .default = .
#         )
#       ),
#       dplyr::across(
#         c(even),
#         ~ dplyr::case_when(
#           is.na(.) ~ as.logical(max(., na.rm = TRUE)),
#           .default = .
#         )
#       ),
#       dplyr::across(
#         c(addr_end, addr_start),
#         ~ dplyr::case_when(
#           is.na(.) ~ max(na.omit(.), na.rm = TRUE),
#           .default = .
#         )
#       )
#     ) |>
#     dplyr::ungroup() |>
#     dplyr::bind_rows(
#       owner |> 
#         dplyr::filter(is.na(corp_id))
#     )
# }

fuzzify_string <- function(c) {
  #' Given a string vector, returns a vector of regex strings that will match
  #' anagrams that share starting and ending characters.
  #' @param c A vector to be anagramed.
  #' @returns A fuzzified vector
  #' @export
  
  innards <- c |> 
    stringr::str_extract("(?<=^[A-Z]).*(?=[A-Z]$)")
  space <- stringr::str_detect(innards, " ")
  chars <- nchar(innards)
  ceiling <- chars + 1
  floor <- ifelse(space, chars - 2,  chars - 1)
  innards <- innards |>
    stringr::str_extract_all(".") |>
    purrr::map_chr(~ stringr::str_c(unique(sort(.x)), collapse=""))
  stringr::str_c(
    "^",
    stringr::str_extract(c, "^[A-Z]"),
    "[",
    innards,
    "]{",
    glue::glue("{floor},{ceiling}"),
    "}",
    stringr::str_extract(c, "[A-Z]$"),
    "$"
  )
}

std_remove_titles <- function(df, col) {
  df |> 
    dplyr::mutate(
      !!col := stringr::str_remove_all(
        .data[[col]],
        stringr::str_c(
          "\\b(",
          stringr::str_c(SEARCH$titles, collapse = "|"),
          ")\\b",
          sep = ""
        )
      )
    )
}

std_flow_owner_name <- function(df, col = "name") {
  df <- df |> 
    std_flow_strings(col) |>
    std_owner_name(col)
  
  if (col == "name") {
    df <- df |>
      std_co_dba_attn(col = col, target_col = col)
  }
  df |>
    std_remove_titles(col) |>
    std_inst_types(col) |>
    std_trailing_leading(col)
}
