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
          ~ stringr::str_replace_all(., pattern)
        )
      )
  } else {
    df |>
      dplyr::mutate(
        dplyr::across(
          tidyselect::where(is.character) & tidyselect::all_of(cols),
          ~ stringr::str_replace_all(., pattern, replace)
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
    pattern = "[^[:alnum:][:space:]\\/\\-\\#\\']", 
    replace = " "
    )
}

std_trailing_words <- function(df, cols) {
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
    "^ ?THE "
  ) |>
    collapse_regex()
  std_replace_generic(
    df,
    cols, 
    pattern = leading_trailing,
    replace = ""
  )
}

std_leading_zeros <- function(df, cols) {
  #' Remove leading zeros
  #'
  #' @param df A dataframe.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  #' 
  leading_zeros <- c(
    "^0+(?=[0-9])",
    "^[\\-]+") |>
    collapse_regex(full_string = FALSE)
  std_replace_generic(
    df,
    cols, 
    pattern = leading_zeros,
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
    "(?<=^| )NW(?= |$)" = "NORTHWEST",
    "(?<=^| )NE(?= |$)" = "NORTHEAST",
    "(?<=^| )S(?= |$)" = "SOUTH",
    "(?<=^| )SW(?= |$)" = "SOUTHWEST",
    "(?<=^| )SE(?= |$)" = "SOUTHEAST",
    "(?<=^| )E(?= |$)" = "EAST",
    "(?<=^| )W(?= |$)" = "WEST",
    "(?<=^| )GT(?= |$)" = "GREAT",
    "(?<=^| )MT(?= |$)" = "MOUNT",
    "(?<=^| )CENTRE(?= |$)" = "CENTER",
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
      ",?[:alnum:]{1}",
      "[\\_\\-]+", 
      "(0|9|X)+", 
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

std_and <- function(df, cols){
  #' Strips away leading or trailing and
  #' 
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  #' 
  and <- c(" AND ?$", "^ ?AND ") |>
    collapse_regex(full_string = FALSE)
  std_replace_generic(
    df,
    cols, 
    pattern = and,
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
    "(?<= 2) (?=ND( |$))" = "",
    "(?<= 3) (?=RD( |$))" = "",
    "(?<= [1-9]?[04-9]) (?=TH( |$))" = "",
    "(?<= )ST(?=$| )" = "STREET",
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
    "(?<= |^)P ?O ?BO?X[ \\#]+" = "PO BOX "
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
            stringr::str_detect(.x, "^O(?=[0-9]{4})") ~ stringr::str_replace(.x, "^O", '0'),
            .default = .x
            )
        ),
        dplyr::across(
          tidyselect::where(is.character) & tidyselect::all_of(cols),
          ~ dplyr::case_when(
            stringr::str_detect(.x, "[0-9]{5} ?[\\-\\/]* ?[0-9]{4}") ~ stringr::str_extract(
              .x,
              "[0-9]{5}(?=[ \\-])"
            ),
            stringr::str_detect(.x, "[0-9]{9}") ~ stringr::str_extract(
              .x,
              "[0-9]{5}"
            ),
            .default = .x
          )
        ),
        dplyr::across(
          tidyselect::where(is.character) & tidyselect::all_of(cols),
            ~ dplyr::case_when(
              stringr::str_sub(stringr::str_replace_all(.x, "\\-", ""),1,5) %in% postals ~ stringr::str_sub(stringr::str_replace_all(.x, "\\-", ""),1,5),
              stringr::str_sub(stringr::str_replace_all(.x, "\\-", ""),-5,-1) %in% postals ~ stringr::str_sub(stringr::str_replace_all(.x, "\\-", ""),-5,-1),
              .default = .x
            )
          )
        )
}

std_muni_names <- function(df, cols, bos_neighs) {
  #' Replace Boston neighborhoods with Boston
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::filter(state == "MA") |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ dplyr::case_when(
          . %in% stringr::str_to_upper(bos_neighs) ~ "BOSTON",
          stringr::str_detect(., "MAN.*SEA") ~ "MANCHESTER BY THE SEA",
          stringr::str_detect(., "MANCHESTER") ~ "MANCHESTER BY THE SEA",
          . %in% c("NORTHWEST BEDFORD") ~ "BEDFORD",
          .default = .
        )
      )
    ) |>
    dplyr::bind_rows(
      df |> dplyr::filter(state != "MA" | is.na(state))
    )
}

std_massachusetts <- function(df, cols) {
  #' Replace "MASS" with "MASSACHUSETTS"
  #'
  #' @param df A dataframe.
  #' @param cols Column or columns in which to replace "MASS"
  #' @returns A dataframe.
  #' @export
  std_replace_generic(
    df,
    cols, 
    pattern = "MASS[ACHUSETTS]{0,10}(?=[ $])",
    replace = "MASSACHUSETTS"
  )
}

std_inst_types <- function(df, cols) {
  #' Standardize street types.
  #'
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  
  inst_regex = c(
    "(?<=( |^)[A-Z]) (?=[A-Z]( |$))" = "",
    "(?<= |^)COMM(?= |$)" = "COMMUNITY",
    "(?<= |^)CORP[ORATION]{0,8}(?= |$)" = "CORPORATION",
    "(?<= |^)INC[ORPORATED]{0,10}(?= |$)" = "INC",
    "(?<= |^)PRO?[PERTIE]{1,6}S(?= |$)" = "PROPERTIES",
    "(?<= |^)PRO?[PERT]{1,4}Y?(?= |$)" = "PROPERTY",
    "(?<= |^)L[IMI]{0,4}TE?D(?= |$)" = "LIMITED",
    "(?<= |^)PA?RTN[ERS]{1,3}|PTN?RS(?=(H| |$))" = "PARTNERS",
    "LANDMALL" = "LANDMARK",
    "(?<= |^)M[ANA]{0,4}G[EMENT]{0,6}(?= |$)" = "MANAGEMENT",
    "(?<= |^)TECH(?= |$)" = "TECHNOLOGY",
    "(?<= |^)INST[ITUT]{3,5}E?(?= |$)" = "INSTITUTE",
    "(?<= |^)UNI[VERSITY]{6,8}(?= |$)" = "UNIVERSITY",
    "(?<= |^)(COMP[ANY]{2,4}|CO(MP)*)(?= |$)" = "COMPANY",
    "(?<= |^)GR[OU]{0,3}P(?= |$)" = "GROUP",
    "(?<= |^)TRU?ST?(?= |$)" = "TRUST",
    "(?<= |^)BK(?= |$)" = "BANK",
    "(?<= |^)PRIV(?= |$)" = "PRIVATE",
    "(?<= |^)R[EAL]{1,4}[TY]{1,3}(?= |$)" = "REALTY",
    "(?<= |^)LI?V[IN]{1,3}G(?= |$)" = "LIVING",
    "(?<= |^)NOM[INEE]{3,5}(?= |$)" = "NOMINEE",
    "(?<= |^)IRR[EVOCABLE]{0,9}(?= |$)" = "IRREVOCABLE",
    "(?<= |^)REV[OCABLE]{0,7}(?= T|$)" = "REVOCABLE",
    "(?<= |^)CONDO[MINIUM]{0,7}(?= |S|$)" = "CONDOMINIUM",
    "(?<= |^)L[IMI]{0,4}TE?D LLC(?= |$)" = "LLC",
    "(?<= |^)L[IMI]{0,4}TE?D(?=$)" = "LTD",
    "(?<= |^)LIMITED (LIABILITY )?PARTNERS(HIP)?(?= |$)" = "LP",
    "(?<= |^)JU?ST[ \\-]*A[ \\-]*START(?= |$)" = "JUST A START",
    "(?<= |^)GENERAL PARTNERS(HIP)?(?= |$)" = "GP",
    "(?<= |^)AUTH[ORITY]{0,6}(?= |$)" = "AUTHORITY",
    "(?<= |^)(ASSN?|ASSOC)(?= |$)" = "ASSOCATION",
    "(?<= |^)DEPT(?= |$)" = "DEPARTMENT",
    "LIMITED LIABILITY (COMPANY|CORPORATION)" = "LLC"
  )
  std_replace_generic(
    df,
    cols, 
    pattern = inst_regex
  )
}

std_remove_co <- function(df, cols) {
  #' Remove "C / O" prefix.
  #'
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ stringr::str_replace_all(., " ?C / O? ?", "")
      )
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
    std_remove_special(cols) |>
    std_andslash(cols) |>
    std_small_numbers(cols) |>
    std_trailing_words(cols) |>
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
