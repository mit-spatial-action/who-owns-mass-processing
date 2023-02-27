library(dplyr)
library(tidyr)

std_uppercase_all <- function(df, cols, except_cols = c()) {
  #' Uppercase all strings
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param except Column or columns to remain untouched.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & !all_of({{ except_cols }}),
        str_to_upper
      ),
    )
}

std_directions <- function(df, cols) {
  #' Standardizes abbreviated cardinal directions.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & all_of({{ cols }}),
        ~str_trim(str_replace_all(., c(
          # Directions
          "(^| )N.? " = " NORTH ",
          "(^| )N.?W.? " = " NORTHWEST ",
          "(^| )N.?E.? " = " NORTHEAST ",
          "(^| )S.? " = " SOUTH ",
          "(^| )S.?W.? " = " SOUTHWEST ",
          "(^| )S.?E.? " = " SOUTHEAST ",
          "(^| )E.? " = " EAST",
          "(^| )W.? " = " WEST "
        )
        )
        )
      )
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
  df %>%
    mutate(
      across(
        where(is.character) & all_of({{ cols }}),
        ~str_replace_all(., c(
          # Put space around slashes
          " ?/ ?" = " / ",
          # Replace & with AND
          " ?& ?" = " AND "
        )
        )
      )
    )
}

std_onewordaddress <- function(df, cols) {
  #' Currently, std_simplify address just strips numbers from the end of
  #' address fields that contain only e.g., "APT" and #. This is a cludgy
  #' clean-up.
  #' TODO: Replace with better regex in std_simplify ::shrug::
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & all_of({{ cols }}),
        ~str_replace_all(., c(
          # Put space around slashes
          "^[A-Z0-9]+$" = NA_character_
        )
        )
      )
    )
}

std_trailingwords <- function(df, cols) {
  #' Standardizes slashes to have a space on either side and
  #' replaces all instances of an ampersand with the word "AND"
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & all_of({{ cols }}),
        ~str_replace_all(., c(
          # Put space around slashes
          " OF$" = "",
          # Replace & with AND
          " AND$" = "",
          "^THE " = ""
        )
        )
      )
    )
}

std_remove_special <- function(df, cols) {
  #' Removes all special characters from columns, except slash
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & all_of({{ cols }}),
        ~str_replace_all(., c(
          "[^[:alnum:][:space:]/-]" = ""
        )
        )
      )
    )
}

std_small_numbers <- function(df, cols) {
  #' Standardize small leading numbers.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & all_of({{ cols }}),
        ~str_replace_all(., c(
          "^ZERO(?=[ -])" = "0",
          "^ONE(?=[ -])" = "1",
          "^TWO(?=[ -])" = "2",
          "^THREE(?=[ -])" = "3",
          "^FOUR(?=[ -])" = "4",
          "^FIVE(?=[ -])" = "5",
          "^SIX(?=[ -])" = "6",
          "^SEVEN(?=[ -])" = "7",
          "^EIGHT(?=[ -])" = "8",
          "^NINE(?=[ -])" = "9",
          "^TEN(?=[ -])" = "10",
          "(?<= |^)FIRST(?= )" = "1ST",
          "(?<= |^)SECOND(?= )" = "2ND",
          "(?<= |^)THIRD(?= )" = "3RD",
          "(?<= |^)FOURTH(?= )" = "4TH",
          "(?<= |^)FIFTH(?= )" = "5TH",
          "(?<= |^)SIXTH(?= )" = "6TH",
          "(?<= |^)SEVENTH(?= )" = "7TH",
          "(?<= |^)EIGHTH(?= )" = "8TH",
          "(?<= |^)NINTH(?= )" = "9TH",
          "(?<= |^)TENTH(?= )" = "10TH"
        )
        )
      )
    )
}

std_remove_middle_initial <- function(df, cols) {
  #' Replace middle initial when formatted like "ERIC R HUNTLEY"
  #' 
  #' @param df A dataframe.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & all_of({{ cols }}),
        ~str_replace(., "(?<=[A-Z] )[A-Z] (?=[A-Z])", "")
      )
    )
}



std_replace_blank <- function(df, except_cols = c()) {
  #' Replace blank string with NA and remove leading and trailing whitespace.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & !all_of({{ except_cols }}),
        ~case_when(
          str_detect(
            .,
            "^X+$|^N(ONE)?$|^UNKNOWN$|ABOVE|^N / A$|^[- ]*SAME( ADDRESS)?") ~
            NA_character_,
          TRUE ~ str_squish(.)
        )
      )
    )
}

std_the <- function(df, cols) {
  #' Strips away leading or trailing the
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & all_of({{ cols }}),
        ~str_replace_all(
          .,
          c(
            " THE$" = "",
            "^THE " = "")
        )
      )
    )
}

std_and <- function(df, cols){
  #' Strips away leading or trailing and
  #' 
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & all_of(cols), 
        ~str_replace_all(
          .,
          c(
            " AND$" = "",
            "^AND " = "")
        )
      )
    )
}

std_street_types <- function(df, cols) {
  #' Standardize street types.
  #'
  #' @param df A dataframe.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & all_of({{ cols }}),
        ~ str_replace_all(
          .,
          c(
            # Correct for spaces between numbers and suffixes.
            # (Prevents errors like 3 RD > 3 ROAD after type std.)
            "(?<=[1-9 ][1-9]) (?=(ST|RD|TH|ND) |$)" = "",
            "(?<= )ST(?=$|\\s|\\.)" = "STREET",
            "(?<= )AVE?(?=$|\\s|\\.)" = "AVENUE",
            "(?<= )LA?N(?=$|\\s|\\.)" = "LANE",
            "(?<= )BLV?R?D?(?=$|\\s|\\.)" = "BOULEVARD",
            "(?<= )PR?KWA?Y(?=$|\\s|\\.)" = "PARKWAY",
            "(?<= )DRV?(?=$|\\s|\\.)" = "DRIVE",
            "(?<= )RD(?=$|\\s|\\.)" = "ROAD",
            "(?<= )TE?[R]+CE?(?=$|\\s|\\.)" = "TERRACE",
            "(?<= )PLC?E?(?=$|\\s|\\.)" = "PLACE",
            "(?<= )(CI?RC?)(?=$|\\s|\\.)" = "CIRCLE",
            "(?<= )A[L]+E?Y(?=$|\\s|\\.)" = "ALLEY",
            "(?<= )SQR?(?=$|\\s|\\.)" = "SQUARE",
            "(?<= )HG?WY(?=$|\\s|\\.)" = "HIGHWAY",
            "(?<= )FR?WY(?=$|\\s|\\.)" = "FREEWAY",
            "(?<= )CR?T(?=$|\\s|\\.)" = "COURT",
            "(?<= )PLZ?(?=$|\\s|\\.)" = "PLAZA",
            "(?<= )W[HR]+F(?=$|\\s|\\.)" = "WHARF",
            "(?<= |^)P.? ?O.? ?BO?X(?=$|\\s|\\.)" = "PO BOX"
          )
        )
      )
    )
}

std_simplify_zip <- function(df, cols) {
  #' Standardize and simplify (i.e., remove 4-digit suffix) US Postal codes.
  #'
  #' @param df A dataframe.
  #' @param cols Column or columns containing the ZIP code to be simplified.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & all_of({{ cols }}), ~ case_when(
          str_detect(., "[0-9] [0-9]") ~ str_extract(
            .,
            ".*(?=\\ )"
          ),
          str_detect(., "-") ~ str_extract(
            .,
            ".*(?=\\-)"
          ),
          str_detect(., "^0+$") ~ NA_character_,
          TRUE ~ .
        )
      )
    )
}

std_massachusetts <- function(df, cols) {
  #' Replace "MASS" with "MASSACHUSETTS"
  #'
  #' @param df A dataframe.
  #' @param cols Column or columns in which to replace "MASS"
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        where(is.character) & all_of({{ cols }}),
        ~str_replace_all(., c(
          "MASS " = "MASSACHUSETTS "
        )
        )
      )
    )
}

std_cities <- function(df, cols) {
  #' Replace Boston neighborhoods with Boston
  #' @param df A dataframe.
  #' @param col Columns to be processed.
  #' @returns A dataframe.
  neighs <- std_uppercase_all(
    read_delim(file.path(DATA_DIR, BOS_NBHD), delim = ",")
  ) %>%
    pull(Name)
  df %>%
    mutate(
      across(
        where(is.character) & all_of({{ cols }}),
        ~case_when(
          . %in% c(
            neighs,
            c("ROXBURY CROSSING", "DORCHESTER CENTER")
          ) ~ "BOSTON",
          . %in% c("NORTHWEST BEDFORD") ~ "BEDFORD",
          TRUE ~ .
        )
      )
    )
}

std_simplify_address <- function(df, cols) {
  #' Standardize street types.
  #'
  #' @param df A dataframe.
  #' @param col Columns to be processed.
  #' @returns A dataframe.
  #' @export
  replace <- c(
    # Matching unit word.
    "[ -]+((BLDG)|(UN?I?T)|(S(UI)?T?E)|(AP(ARTMEN)?T)|(NO)|(P ?O BOX)|(FLO?O?R)|(R(OO)?M)|(PMB))( *#?[A-Z]?[0-9-]*([A-Z]|([A-Z][A-Z])|(ABC))? ?$)" = "",
    # NTH FLOOR
    "[ -]+[1-9]+((ND)|(ST)|(RD)|(TH))? (FLO?O?R?)" = "",
    # Ends with series of letters and numbers.
    "[ -]+[A-Z]?[0-9-]+([A-Z]|(ABC))? ?$" = "",
    # Ends with a single number or letter.
    "[ -]+[A-Z0-9-]$" = ""
  )
  for (col in cols) {
    # Flag PO Boxes.
    df <- df %>%
      mutate(
        pobox = case_when(
          str_detect(get({{ col }}), "^PO BOX") ~ TRUE,
          TRUE ~ FALSE
        )
      )
    po_box <- filter(df, pobox)
    df <- filter(df, !pobox) %>%
      mutate(
        across(
          matches(col),
          ~str_replace_all(., replace)
        )
      ) %>%
      bind_rows(po_box)
  }
  df %>%
    select(-c(pobox))
}

std_corp_types <- function(df, cols) {
  #' Standardize street types.
  #'
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        all_of({{ cols }}),
        ~str_replace_all(., c(
          "LIMITED PARTNER?(SHIP)?" = "LP",
          "LIMITED LIABILITY PARTNER?(SHIP)?" = "LLP",
          "LIMITED LIABILITY (COMPANY|CORPORATION)" = "LLC",
          "PRIVATE LIMITED" = "LTD",
          "INCO?R?P?O?R?A?T?E?D ?$" = "INC",
          "CORPO?R?A?T?I?O?N ?$" = "CORP",
          "COMP(ANY)? ?$" = "CO",
          "LIMITED$" = "LTD",
          " TRU?S?T?E?E?S?( OF)?$" = " TRUST"
        )
        )
      )
    )
}

std_remove_co <- function(df, cols) {
  df %>%
    # Remove "C / O" prefix.
    mutate(
      across(
        {{ cols }},
        ~ str_replace_all(., " ?C / O? ?", "")
      )
    )
}

std_hyphenated_numbers <- function(df, cols) {
  df %>%
    # Remove "C / O" prefix.
    mutate(
      across(
        {{ cols }},
        ~ str_replace_all(
          str_replace_all(., "(?<=[0-9]{1,4}[A-Z]?)-[0-9]+[A-Z]?", ""),
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
  df %>%
    mutate(
      !!output_col := case_when(
        str_detect(
          get({{ addr_col2 }}), "^[0-9]") &
          !str_detect(get({{ addr_col1 }}), "^[0-9]")
        ~ get({{ addr_col2 }}),
        str_detect(
          get({{ addr_col2 }}), "^[0-9]") &
          str_detect(get({{ addr_col1 }}), "LLC")
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
  library(tigris)
  options(tigris_use_cache = TRUE)
  sdf %>%
    mutate(
      point = st_point_on_surface(geometry)
    ) %>%
    st_set_geometry("point") %>%
    st_join(
      zctas(year = 2010, state = state, zip_col) %>%
        st_transform(crs) %>%
        select(ZCTA5CE10)
    ) %>%
    st_set_geometry("geometry") %>%
    select(
      -c(point)
    ) %>%
    mutate(
      !!zip_col := ZCTA5CE10
    ) %>%
    select(-c(ZCTA5CE10))
}

st_get_censusgeo <- function(sdf, state = "MA", crs = 2249) {
  #' Bind census geography IDs to geometries of interest.
  #'
  #' @param sdf A sf dataframe.
  #' @param state State of your study. (TODO: multiple states?)
  #' @param crs EPSG code of appropriate coordinate reference system.
  #' @returns A dataframe.
  #' @exportx
  library(tigris)
  options(tigris_use_cache = TRUE)
  censusgeo <- block_groups(state = state) %>%
    st_transform(crs) %>%
    rename(
      geoid_bg = GEOID
    ) %>%
    select(geoid_bg)
  sdf %>%
    mutate(
      point = st_point_on_surface(geometry)
    ) %>%
    st_set_geometry("point") %>%
    st_join(
      censusgeo
    ) %>%
    st_set_geometry("geometry") %>%
    mutate(
      geoid_t = str_sub(geoid_bg, start = 1L, end = 11L)
    ) %>%
    select(
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
  df %>%
    mutate(
      across(
        all_of({{ cols }}),
        ~ case_when(
          str_detect(
            .,
            "(((CORP(ORATION)?)|(LLC)|) (SYS)|(SER))|(AGENT)|(BUSINESS FILINGS)"
          ) ~ NA_character_,
          TRUE ~ .
        )
      )
    )
}

std_owner_name <- function(df, col) {
  print(col)
  df %>% std_remove_special(col) %>%
    std_remove_middle_initial(col) %>%
    std_the(col) %>%
    std_and(col)
}

process_records <- function(df,
                            cols,
                            zip_cols = FALSE,
                            city_cols = FALSE,
                            addr_cols = FALSE,
                            name_cols = FALSE,
                            keep_cols = FALSE) {
  #' Run a series of string standardizing functions.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed by
  #'  `std_directions`, `std_andslash`, `std_remove_special`,
  #'  and `std_the`.
  #' @param zip_cols Column or columns to be processed by
  #'  `std_directions`, `std_andslash`, `std_remove_special`,
  #'  and `std_the`.
  #' @returns A dataframe.
  #' @export
  all_cols <- cols
  if (!isFALSE(keep_cols)) {
    all_cols <- c(keep_cols, all_cols)
  } else {
    keep_cols <- c()
  }
  if (!isFALSE(zip_cols)) {
    all_cols <- c(all_cols, zip_cols)
  }
  if (!isFALSE(city_cols)) {
    all_cols <- c(all_cols, city_cols)
  }
  if (!isFALSE(addr_cols)) {
    all_cols <- c(all_cols, addr_cols)
  }
  if (!isFALSE(name_cols)) {
    all_cols <- c(all_cols, name_cols)
  }
  all_cols <- unique(all_cols)
  df <- df %>%
    select(
      all_of(all_cols)
    ) %>%
    std_andslash(cols) %>%
    std_remove_special(cols) %>%
    std_replace_blank(keep_cols) %>%
    std_the(cols) %>%
    std_massachusetts(cols) %>%
    std_small_numbers(cols) %>%
    std_trailingwords(cols) %>%
    std_uppercase_all(cols)
  if (!isFALSE(zip_cols)) {
    df <- df %>%
      std_simplify_zip(zip_cols)
  }
  if (!isFALSE(city_cols)) {
    df <- df %>%
      std_cities(city_cols)
  }
  if (!isFALSE(addr_cols)) {
    df <- df %>%
      std_street_types(addr_cols) %>%
      std_simplify_address(addr_cols) %>%
      std_directions(addr_cols) %>%
      std_hyphenated_numbers(addr_cols) %>%
      std_onewordaddress(addr_cols)
  }
  if (!isFALSE(name_cols)) {
    df <- df %>%
      std_corp_types(name_cols) %>%
      std_corp_rm_sys(name_cols)
  }
  df
}
