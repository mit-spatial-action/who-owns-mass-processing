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
    pattern = "[^[:alnum:][:space:]\\/\\-\\#\\'\\`\\,]", 
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

std_andslash <- function(df, cols) {
  #' Standardizes slashes to have a space on either side and
  #' replaces all instances of an ampersand with the word "AND"
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  andslash <- c(
    # Put space around slashes
    " ?/ ?" = " / ",
    # Replace & with AND
    " ?& ?" = " AND ",
    " ?(-|–|—) ?" = " - ",
    " ?' ?" = ""
  )
  std_replace_generic(
    df,
    cols, 
    pattern = andslash
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
  
  df |> 
    dplyr::left_join(
      lu,
      by = c("use_code" = "use_code"),
      na_matches = "never"
    ) |>
    dplyr::select(
      -c(use_code)
    ) |>
    dplyr::rename(
      use_code = luc_assign
    )
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

st_get_zips <- function(sdf, zip_col, state = "MA", crs = 2249) {
  #' Get ZIP codes based on actual parcel location.
  #' (These are frequently misreported.)
  #'
  #' @param sdf A sf dataframe.
  #' @param zip_col Name the column to which you want postal codes written.
  #' @param state State of your study. (TODO: multiple states?)
  #' @param crs EPSG code of appropriate coordinate reference system.
  #' @returns A dataframe.
  #' @export
  sdf |>
    dplyr::mutate(
      point = sf::st_point_on_surface(geometry)
    ) |>
    sf::st_set_geometry("point") |>
    sf::st_join(
      tigris::zctas(cb = FALSE, state = state, year = 2010) |>
        sf::st_transform(crs) |>
        dplyr::select(ZCTA5CE10)
    ) |>
    sf::st_set_geometry("geometry") |>
    dplyr::select(
      -c(point)
    ) |>
    dplyr::mutate(
      !!zip_col := ZCTA5CE10
    ) |>
    dplyr::select(-c(ZCTA5CE10))
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
    std_andslash(cols) |>
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
