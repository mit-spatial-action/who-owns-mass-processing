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

# Standardizer Helpers ====

std_collapse_regex <- function(c, full_string = FALSE) {
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

std_fuzzify_string <- function(c) {
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

std_counts_in_group <- function(df, cols, col_distinct) {
  df |>
    dplyr::group_by(dplyr::across(dplyr::all_of(cols))) |>
    dplyr::mutate(
      count = dplyr::n(), 
      distinct := dplyr::n_distinct(get(col_distinct))
    ) |>
    dplyr::ungroup()
}

std_pad_muni_ids <- function(ids) {
  ids |>
    stringr::str_pad(3, side="left", pad="0")
}

# General Character Handlers ====

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
        tidyselect::where(is.character) & tidyselect::any_of(cols),
        stringr::str_to_upper
      ),
    )
}

std_squish_special <- function(df,cols) {
  df |>
    # Remove special characters and spaces at front and back of string.
    std_replace_generic(
      cols, 
      pattern = "(^[^[:alnum:]]+)|([^[:alnum:]]+$)", 
      replace = ""
    )
}


std_squish <- function(df, cols) {
  df |> 
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ dplyr::na_if(stringr::str_squish(.), "")
      )
    ) |>
    std_squish_special(cols)
}

std_remove_special <- function(df, cols, rm_commas=TRUE) {
  #' Removes all special characters from columns, except slash
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  
  # Remove (almost all) special characters.
  if (rm_commas) {
    string <- "[^[:alnum:][:space:]\\-\\.\\&\\']"
  } else {
    string <- "[^[:alnum:][:space:]\\-\\.\\&\\'\\,]"
  }
  df <- std_replace_generic(
    df, 
    cols, 
    pattern = string,
    replace = " "
  ) |> 
    std_replace_generic(
      cols, 
      pattern = " ?\\- ?",
      replace = "-"
    ) |> 
    std_replace_generic(
      cols, 
      pattern = " ?\\' ?", 
      replace = ""
    ) |>
    std_replace_generic(
      cols, 
      pattern = c(
        "(?<=[0-9]) *\\. *(?=5)" = "\\.",
        "(?<=[0-9]) *\\. *(?!5)" = "\\-",
        "(?<![0-9]) *\\. *(?!5)" = " "
        )
    ) |>
    std_replace_generic(
      cols, 
      pattern = c(
        "(?<=[0-9]) [1] *(\\/ *)?[2](?= |$)" = ".5",
        " *\\/ *" = " " 
      )
    )
  
  if (!rm_commas) {
    df <- df |>
      std_replace_generic(
        cols, 
        pattern = " ?, ?", 
        replace = " , "
      )
  }
  
  df |>
    std_squish(cols)
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
    "^ ?THE "
  ) |>
    std_collapse_regex()
  std_replace_generic(
    df,
    cols, 
    pattern = leading_trailing,
    replace = ""
  ) |>
    std_squish(cols)
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
    pattern = std_collapse_regex(lz, full_string = FALSE),
    replace = ""
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
    "[\\_\\-\\;\\:\\, ]+", 
    "N(ONE)?", 
    "N( /)? ?A",
    "UNKNOWN",
    " *",
    ""
    # "^[- ]*SAME( ADDRESS)?"
    # "ABOVE"
  ) |>
    std_collapse_regex(full_string = TRUE)
  std_replace_generic(
    df,
    cols, 
    pattern = blank,
    replace = NA_character_
  ) |>
    dplyr::mutate(
      dplyr::across(
        cols,
        ~ dplyr::case_when(
          stringr::str_detect(., "^SAME|NONE|UNKNOWN(?=\\s|\\,|$)") ~ NA_character_,
          .default = .
        )
    )
    )
}

std_replace_newline <- function(df, cols) {
  #' Replace blank string with NA and remove leading and trailing whitespace.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @returns A dataframe.
  #' @export
  newline <- c(
    ",?\\\\n"
    ) |>
    std_collapse_regex(full_string = FALSE)
  std_replace_generic(
    df,
    cols, 
    pattern = newline,
    replace = ", "
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
    " ?/ ?" = "/",
    "& ?$" = "",
    # Replace & with AND
    " ?& ?" = " AND ",
    " ?(-|–|—) ?" = "-",
    " ?, ?" = ","
  )
  std_replace_generic(
    df,
    cols, 
    pattern = spacing_characters
  )
}

# Word Standardizers / Crosswalks ====

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
    "(^NO(?= ))|((?<= )NO(?= [A-Z]{3,}))" = "NORTH",
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
    "(?<=^| )(CR?G)$" = "CROSSING",
    "(?<=^| )SQ$" = "SQUARE",
    "(?<=^| )VLLY(?= |$)" = "VALLEY"
  )
  std_replace_generic(
    df,
    cols, 
    pattern = directions
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
    "(?<=[02-9]) ST (?=[A-Z]{4,15})" = " SAINT ",
    "(?<= 1) (?=ST )" = "",
    "(?<= 2) (?=ND )" = "",
    "(?<= 3) (?=RD )" = "",
    "(?<= [1-9]?[04-9]) (?=TH )" = "",
    "(?<= )(ST|ST[RET]{3,5})(?=$| |\\-)" = "STREET",
    "(?<= )STREE(?=$| |\\-)" = "STREET",
    "(?<= )AVE?(?=$| |\\-)" = "AVENUE",
    "(?<= )LA?N(?=$| |\\-)" = "LANE",
    "(?<= )BLV?R?D?(?=$| |\\-)" = "BOULEVARD",
    "(?<= )P(A?R?KWA?)?Y(?=$| |\\-)" = "PARKWAY",
    "(?<= )PW(?=$| |\\-)" = "PARKWAY",
    "(?<= )EXT(?=$| |\\-)" = "EXTENSION",
    "(?<= )PR?K(?=$| |\\-)" = "PARK",
    "(?<= )DRV?(?=$| |\\-)" = "DRIVE",
    "(?<= )RD(?=$| |\\-)" = "ROAD",
    "(?<= )T[ER]+R+(CE)?(?=$| |\\-)" = "TERRACE",
    "(?<= )TE(?=$| |\\-)" = "TERRACE",
    "(?<= )PLC?E?(?=$| |\\-)" = "PLACE",
    "(?<= )WY(?=$| |\\-)" = "WAY",
    "(?<= )(CI?RC?|CI)(?=$| |\\-)" = "CIRCLE",
    "(?<= )A[L]+E?Y(?=$| |\\-)" = "ALLEY",
    "(?<= )SQR?(?=$| |\\-)" = "SQUARE",
    "(?<= )HG?WY?(?=$| |\\-)" = "HIGHWAY",
    "(?<= )CNTR(?=$| |\\-)" = "CENTER",
    "(?<= )FR?WY(?=$| |\\-)" = "FREEWAY",
    "(?<= |^)MSGR(?=$| |\\-)" = "MONSIGNOR",
    "(?<= )O ?BRI(?=[EA]N[$ \\-])" = "OBRI",
    "(?<= )(MONSIGNOR)?( P ?J)? LYDON(?=$| |\\-)" = " MONSIGNOR PJ LYDON",
    "(?<= )(MONSIGNOR)? OBRI[AE]N(?= HIGH[$ \\-])" = " MONSIGNOR OBRIEN",
    "(?<= )CR?T(?=$| |\\-)" = "COURT",
    "(?<= )PL?Z(?=$| |\\-)" = "PLAZA",
    "(?<= )W[HR]+F(?=$| |\\-)" = "WHARF",
    "(?<= )DEPT(?=$| |\\-)" = "DEPARTMENT",
    "(?<= )P ?O SQUARE(?=$| |\\-)" = "POST OFFICE SQUARE",
    "(?<= )PRUDENTIAL TOWER(?=$| |\\-)" = "",
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
    "(?<=^)ZERO(?= |$)" = "0",
    "(?<=^)ONE(?= |$)" = "1",
    "(?<=^)TWO(?= |$)" = "2",
    "(?<=^)THREE(?= |$)" = "3",
    "(?<=^)FOUR(?= |$)" = "4",
    "(?<=^)FIVE(?= |$)" = "5",
    "(?<=^)SIX(?= |$)" = "6",
    "(?<=^)SEVEN(?= |$)" = "7",
    "(?<=^)EIGHT(?= |$)" = "8",
    "(?<=^)NINE(?= |$)" = "9",
    "(?<=^)TEN(?= |$)" = "10",
    "(?<=^)ELEVEN(?= |$)" = "11",
    "(?<=^)TWELVE(?= |$)" = "12",
    "(?<=^)THIRTEEN(?= |$)" = "13",
    "(?<=^)FOURTEEN(?= |$)" = "14",
    "(?<=^)FIFTEEN(?= |$)" = "15",
    "(?<=^)SIXTEEN(?= |$)" = "16",
    "(?<=^)SEVENTEEN(?= |$)" = "17",
    "(?<=^)EIGHTEEN(?= |$)" = "18",
    "(?<=^)NINETEEN(?= |$)" = "19",
    "(?<=^)TWENTY(?= |$)" = "20"
  )
  std_replace_generic(
    df,
    cols, 
    pattern = sm_num
  )
}

std_small_ordinals <- function(df, cols) {
  #' Standardize small leading numbers.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  sm_num <- c(
    "(?<=^| )FIRST(?= |$)" = "1ST",
    "(?<=^| )SECOND(?= |$)" = "2ND",
    "(?<=^| )THIRD(?= |$)" = "3RD",
    "(?<=^| )FOURTH(?= |$)" = "4TH",
    "(?<=^| )FIFTH(?= |$)" = "5TH",
    "(?<=^| )SIXTH(?= |$)" = "6TH",
    "(?<=^| )SEVENTH(?= |$)" = "7TH",
    "(?<=^| )EIGHTH(?= |$)" = "8TH",
    "(?<=^| )NINTH(?= |$)" = "9TH",
    "(?<=^| )TENTH(?= |$)" = "10TH",
    "(?<=^| )ELEVENTH(?= |$)" = "11TH",
    "(?<=^| )TWELTH(?= |$)" = "12TH",
    "(?<=^| )THIRTEENTH(?= |$)" = "13TH",
    "(?<=^| )FOURTEENTH(?= |$)" = "14TH",
    "(?<=^| )FIFTEENTH(?= |$)" = "15TH",
    "(?<=^| )SIXTEENTH(?= |$)" = "16TH",
    "(?<=^| )SEVENTEENTH(?= |$)" = "17TH",
    "(?<=^| )EIGHTEENTH(?= |$)" = "18TH",
    "(?<=^| )NINTEENTH(?= |$)" = "19TH",
    "(?<=^| )TWENTIETH(?= |$)" = "20TH"
  )
  std_replace_generic(
    df,
    cols, 
    pattern = sm_num
  )
}

std_extract_zip <- function(df, col, postal_col) {
  df <- df |>
    dplyr::mutate(
      temp = stringr::str_extract(.data[[col]], "[0-9]{5}([ \\-]*[0-9]{4})?"),
      !!postal_col := dplyr::case_when(
        !is.na(temp) & 
          (is.na(.data[[postal_col]]) | 
             !stringr::str_detect(.data[[postal_col]], "[0-9]{5}([ \\-]*[0-9]{4})?") ) ~ temp,
        .default = .data[[postal_col]]
      ),
      !!col := stringr::str_squish(
        stringr::str_remove(.data[[col]], "[0-9]{5}([ \\-]*[0-9]{4})?")
      ),
    ) |>
    dplyr::select(-temp) |>
    std_replace_blank(col)
}

std_remove_counties <- function(df, col, state_col, places, state_val="MA") {
  counties <- tigris::counties(state=state_val, cb=TRUE, resolution="20m") |>
    dplyr::rename_with(stringr::str_to_lower) |>
    sf::st_drop_geometry() |>
    dplyr::select(name) |>
    std_uppercase(c("name")) |>
    dplyr::filter(!(name %in% places$name) &
                    !(name %in% places$muni)) |>
    dplyr::pull(name) |>
    std_collapse_regex() |>
    suppressMessages()
  
    df |>
      dplyr::filter(.data[[state_col]] == state_val) |>
      std_replace_generic(
        col,
        stringr::str_c("(?<=[A-Z] )(", counties, ")$", collapse=""),
        ""
      ) |>
      dplyr::bind_rows(
        df |>
          dplyr::filter(.data[[state_col]] != state_val | is.na(.data[[state_col]]))
      )
}

std_match_state_and_zips <- function(df, state_col, zip_col, zips, state_val = "MA") {
  state_zips <- zips |> 
    dplyr::filter(unambig_state == state_val) |> 
    dplyr::pull(zip)
  df <- df |>
    dplyr::mutate(
      state_match = .data[[state_col]] == state_val | 
        .data[[zip_col]] %in% state_zips
    )
}

std_muni_names <- function(df, col, mass = FALSE) {
  #' Replace Boston neighborhoods with Boston
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @param postal_col Columns to be processed.
  #' @returns A dataframe.
  #' @export
  #' 
  if (mass) {
    muni_corrections <- c(
      "BORO$" = "BOROUGH",
      "^ACT$" = "ACTON",
      "^GLOUSTER$" = "GLOUCESTER",
      "^NEW TOWN$" = "NEWTON",
      "^[A-Z]AMBRIDGE$" = "CAMBRIDGE",
      "^NEWBURY PORT$" = "NEWBURYPORT",
      " HLDS( |$)" = "HIGHLANDS",
      "^DEVEN$" = "DEVENS",
      "^PRIDE CROSSING$" = "PRIDES CROSSING",
      "^MANCHESTER$" = "MANCHESTER-BY-THE-SEA",
      "^MANC[A-Z \\/]+SEA$" = "MANCHESTER-BY-THE-SEA"
    )
    
    df |>
      std_replace_generic(
        col, 
        pattern = muni_corrections
      )
  } else {
    nyc <- c(
      "^QUEENS( |$)",
      "^(BROOKLYN|BKLY?N)( |$)",
      "^STATEN (ISL(AND)?)?( |$)",
      "^NEW YORK$",
      "^HARLEM$( |$)",
      "^MANHATTAN( |$)",
      "^(THE )?BRONX( |$)"
    ) |>
      std_collapse_regex()
    
    df |>
      dplyr::mutate(
        dplyr::across(
          dplyr::all_of(col),
          ~ dplyr::case_when(
            stringr::str_detect(.x, nyc) ~ "NEW YORK CITY",
            .x == "LA" ~ "LOS ANGELES",
            .default = .x
          )
        )
      )
  }
}

std_zip_format <- function(df, col, state_col, zips, constraint = "") {
  #' Standardize and simplify (i.e., remove 4-digit suffix) US ZIP codes.
  #'
  #' @param df A dataframe.
  #' @param col Column containing the ZIP code to be simplified.
  #' @param zips Dataframe of ZIP codes.
  #' @returns A dataframe.
  #' @export
  
  zips <- zips |>
    sf::st_drop_geometry()
  
  zips_unambig <- zips |>
    dplyr::filter(!is.na(unambig_state)) |>
    dplyr::distinct()
  
  if (constraint == "ma") {
    zips <- zips |>
      dplyr::filter(state == "MA")
  } 
  
  zips <- zips |>
    dplyr::pull(zip) |>
    unique()
  
  df <- df |>
    dplyr::mutate(
      # Deal with suffixes.
      # Replace  "O"s that should be 0s.
      temp = stringr::str_replace_all(
        .data[[col]], 
        c(
          "(?<=^[0-9O]{5})[ \\-\\/][0-9]{4}$" = "",
          "(^O|-)" = "0", 
          "[A-Z]" = ""
        )
      ),
      five = stringr::str_length(temp) == 5,
      in_zips = temp %in% zips,
      # # In cases where only ZIP codes are expected, set any ill-formatted
      # # codes to NA.
      !!col := dplyr::case_when(
        in_zips & five ~ temp,
        five & constraint %in% c("MA", "USA") ~ NA_character_,
        .default = .data[[col]]
      )
    ) 

  if (!missing(state_col)) {
    df <- df |>
      dplyr::filter(is.na(.data[[state_col]])) |>
      dplyr::left_join(
        zips_unambig |>
          dplyr::select(zip, new_state = unambig_state),
        by = dplyr::join_by(
          !!col == zip
        ),
        na_matches="never"
      ) |>
      dplyr::mutate(
        !!state_col := dplyr::case_when(
          is.na(.data[[state_col]]) ~ new_state,
          .default = .data[[state_col]]
        )
      ) |>
      dplyr::select(-new_state) |>
      dplyr::bind_rows(
        df |>
          dplyr::filter(!is.na(.data[[state_col]]))
      )
  }
  
  df |>
    dplyr::select(-c(five, temp, in_zips))
}

std_massachusetts <- function(df, cols, rm_ma = TRUE) {
  #' Replace variations of Massachusetts with MA
  #'
  #' @param df A dataframe.
  #' @param cols Column or columns in which to replace "MASS"
  #' @returns A dataframe.
  #' @export
  if (rm_ma) {
    string <- "(?<= |^)(MASS|MA)(?= |$)"
  } else {
    string <- "(?<= |^)MASS(?= |$)"
  }
  
  std_replace_generic(
    df,
    cols, 
    pattern = string,
    replace = "MASSACHUSETTS"
  )
}


std_luc <- function(
    df, 
    path, 
    muni_id_col, 
    use_code_col,
    luc_col,
    file = "luc_crosswalk.csv"
    ){
  
  # Read Land Use Codes
  lu <- readr::read_csv(file.path(path, file), show_col_types = FALSE) |>
    dplyr::rename_with(tolower) |>
    dplyr::select(c(use_code, luc_assign))
  
  nonboston <- df |> 
    dplyr::filter(.data[[muni_id_col]] != "035")
  
  boston <- df |> 
    dplyr::filter(.data[[muni_id_col]] == "035")
  
  nonboston <- nonboston |>
    dplyr::left_join(
      lu,
      by = dplyr::join_by(
        !!use_code_col == use_code
        ),
      na_matches = "never"
    )
  
  unmatched <- nonboston |>
    dplyr::filter(is.na(luc_assign)) |>
    dplyr::select(-luc_assign) |>
    dplyr::mutate(
      !!stringr::str_c(use_code_col, "_simp") := 
        substr(.data[[use_code_col]], 1, 3)
    ) |>
    dplyr::left_join(
      lu,
      by = dplyr::join_by(
        !!stringr::str_c(use_code_col, "_simp") == use_code
        ),
      na_matches = "never"
    ) |>
    dplyr::select(-dplyr::ends_with("_simp"))
  
  boston <- boston |>
    dplyr::mutate(
      !!luc_col := dplyr::case_when(
        .data[[use_code_col]] %in% c('012', '013', '019', '031') ~ '0xxR',
        .default = .data[[use_code_col]]
      )
    )
  
  nonboston |>
    dplyr::filter(!is.na(luc_assign)) |>
    dplyr::bind_rows(unmatched) |>
    dplyr::rename(!!luc_col := luc_assign) |>
    dplyr::bind_rows(
      boston
    ) |>
    dplyr::mutate(
      !!luc_col := dplyr::case_when(
        is.na(.data[[luc_col]]) ~ .data[[use_code_col]],
        .default = .data[[luc_col]]
      )
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
    "(TRUST|TRU?ST|TR|TRT|TRUS|TRU|TRYST|T[RUS]{3}T)( (OF )?[0-9]{4})?" = "TRUST",
    "(CO( - )?)?(TRS|TRU?ST[ES]{1,4}|TRSTS|T[RUSTEE]{6}S|TS$)" = "TRUSTEES"
  )
  names(inst_regex) <- stringr::str_c("\\b", names(inst_regex), "\\b")
  std_replace_generic(
    df,
    cols, 
    pattern = inst_regex
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

# Unit Estimation ====

std_units_from_luc <- function(df, col, muni_id_col, units_col) {
  non_boston <- df |>
    dplyr::filter(.data[[muni_id_col]] != '035')
  
  df <- df |>
    dplyr::filter(.data[[muni_id_col]] == '035') |>
    dplyr::mutate(
      !!units_col := dplyr::case_when(
        # RC: One Residential Unit
        .data[[col]] == '025' ~ 1,
        # RC: Two Residential Unit
        .data[[col]] == '026' ~ 2,
        # RC: Three Residential Unit
        .data[[col]] == '027' ~ 3,
        .default = .data[[units_col]]
      )
    ) |>
    dplyr::bind_rows(non_boston)
  
  df |>
    dplyr::mutate(
      !!units_col := dplyr::case_when(
        # Single-family.
        .data[[col]] == '101' ~ 1,
        # Two-family.
        .data[[col]] == '104' ~ 2,
        # Three-family.
        .data[[col]] == '105' ~ 3,
        # 102: Condos
        .data[[col]] == '102' ~ 1,
        .default = .data[[units_col]]
      )
    )
}

std_test_given_units <- function(df, col, luc_col, muni_id_col) {
  df |>
    dplyr::mutate(
      units_valid = dplyr::case_when(
        # 4-8 Unit
        .data[[muni_id_col]] != '035' & .data[[luc_col]] == '111' & dplyr::between(.data[[col]], 4, 8) ~
          TRUE,
        # 112: >8 Unit
        .data[[muni_id_col]] != '035' & .data[[luc_col]] == '112' & .data[[col]] > 8 ~
          TRUE,
        # BOSTON, 112: 4-6 Unit
        .data[[muni_id_col]] == '035' & .data[[luc_col]] == '111' & dplyr::between(.data[[col]], 4, 6) ~
          TRUE,
        # BOSTON, 112: 7-30 Unit
        .data[[muni_id_col]] == '035' & .data[[luc_col]] == '112' & dplyr::between(.data[[col]], 7, 30) ~
          TRUE,
        # BOSTON, 113: 31-99 Unit
        .data[[muni_id_col]] == '035' & .data[[luc_col]] == '113' & dplyr::between(.data[[col]], 7, 30) ~
          TRUE,
        # BOSTON, 114: 31-99 Unit
        .data[[muni_id_col]] == '035' & .data[[luc_col]] == '114' & .data[[col]] > 100 ~
          TRUE,
        .data[[muni_id_col]] == '035' & .data[[luc_col]] %in% c('025', '026', '027') ~
          TRUE,
        .data[[luc_col]] %in% c('101', '102', '104', '105') ~
          TRUE,
        .default = FALSE
      )
    )
}

std_estimate_units <- function(df) {
  df |>
    dplyr::mutate(
      units = dplyr::case_when(
        # 4-8 Unit
        # ===
        luc == '111' & dplyr::between(addr_count, 4, 8) ~
          addr_count,
        luc == '111' & (!dplyr::between(addr_count, 4, 8) | is.na(addr_count)) ~
          4,
        # 112: >8 Unit
        # ===
        luc == '112' & addr_count > 8 ~
          addr_count,
        luc == '112' & (addr_count <= 8 | is.na(addr_count)) ~
          9,
        # 109: Multi House, 0xx: Multi-Use, 103: Mobile Home
        # ===
        luc %in% c('109', '0xx', '103', '970', '908', '959') & units == 0 ~
          addr_count,
        luc %in% c('109', '0xx', '103', '970', '908', '959') & units != 0 & units > addr_count ~
          units,
        .default = units
      )
    )
}

# Address Refinement/Completion ====
std_leading_zero_vector <- function(x) {
  stringr::str_remove_all(x, "(?<=^|\\-)0+(?=[1-9]|[A-Z]|0$)")
}

std_addr2_floor <- function(df, cols) {
  std_addr2_parser(df, cols, "([0-9]+[A-Z]{2}|[0-9\\.]|TOP|GROUND)[ \\-]FLOOR|FLOOR[ \\-][0-9]+")
}

std_addr2_twr <- function(df, cols) {
  std_addr2_parser(df, cols, "[ \\-]TOWER[ \\-][0-9\\.]+")
}

std_addr2_bldg <- function(df, cols) {
  std_addr2_parser(df, cols, "[ \\-]BUILDING[ \\-][0-9\\.]+")
}

std_addr2_alpha_num <- function(df, cols) {
  std_addr2_parser(df, cols, " [0-9]*[A-Z]{1,2}[\\- ][0-9\\.]+( ([0-9\\.]+|A-Z]))?$")
}

std_addr2_num_alpha <- function(df, cols) {
  std_addr2_parser(df, cols, " [A-Z]{0,2}[0-9\\.]+[\\- ]?[A-Z]{1,2}( ([0-9\\.]+|A-Z]))?$")
}

std_addr2_all_num <- function(df, cols) {
  std_addr2_parser(df, cols, " ([0-9\\-\\. ]+)?[0-9]+$")
}

std_addr2_alpha <- function(df, cols) {
  std_addr2_parser(df, cols, "[ \\-]+[A-Z]$")
}

std_addr2_words <- function(df, cols) {
  std_addr2_parser(df, cols, "[ \\-]+(BASEMENT|PH|REAR|FRONT)$")
}

std_addr2_parser <- function(df, cols, regex) {
  if (!all(stringr::str_c(cols, "2", sep="") %in% names(df))) {
    df <- df |> 
      dplyr::mutate(
        dplyr::across(
          cols,
          list(
            "2" = ~ NA_character_
          ),
          .names = "{.col}{.fn}"
        )
      )
  }
  df |>
    dplyr::mutate(
      dplyr::across(
        cols,
        list(
          temp = ~ stringr::str_extract(.x, regex)
        ),
        .names = "{.col}_{.fn}"
      ),
      dplyr::across(
        cols,
        ~ dplyr::case_when(
          !is.na(get(paste0(dplyr::cur_column(), "_temp"))) ~ 
            stringr::str_remove(
              .x, 
              stringr::str_c(get(paste0(dplyr::cur_column(), "_temp")), ".*$")
            ),
          .default = .x
        )
      ),
      dplyr::across(
        stringr::str_c(cols, "_temp"),
        ~ dplyr::case_when(
          !is.na(.x) ~ std_leading_zero_vector(stringr::str_squish(.x)),
          .default = .x
        )
      ),
      dplyr::across(
        cols,
        list(
          "2" = ~ dplyr::case_when(
            !is.na(get(paste0(dplyr::cur_column(), "2"))) & 
              !is.na(get(paste0(dplyr::cur_column(), "_temp"))) 
            ~ stringr::str_c(
              get(paste0(dplyr::cur_column(), "_temp")),
              get(paste0(dplyr::cur_column(), "2")),
              sep=" "
            ),
            is.na(get(paste0(dplyr::cur_column(), "2"))) & 
              !is.na(get(paste0(dplyr::cur_column(), "_temp"))) 
            ~ get(paste0(dplyr::cur_column(), "_temp")),
            !is.na(get(paste0(dplyr::cur_column(), "2"))) & 
              is.na(get(paste0(dplyr::cur_column(), "_temp"))) 
            ~ get(paste0(dplyr::cur_column(), "2")),
            .default = NA_character_
          )
        ),
        .names = "{.col}{.fn}"
      )
    ) |>
    dplyr::select(-stringr::str_c(cols, "_temp")) |>
    std_squish(stringr::str_c(cols, "2"))
}

std_addr2_po_pmb <- function(df, cols) {
  terms <- c(
    "(P ?[0O] ?)+B[0X]?X",
    "((P ?)?[0O])+ ?BOX",
    "BX "
  )
  terms <- stringr::str_c(terms, "[\\,\\-](?= ?[0-9])") |>
    std_collapse_regex()
  
  
  df <- std_replace_generic(
    df, 
    cols,
    terms, 
    "PO BOX"
  )
  
  std_replace_generic(
    df, 
    cols,
    "^BO?X ", 
    "PO BOX"
  ) |>
    dplyr::mutate(
      dplyr::across(
        cols,
        list(
          po = ~ stringr::str_extract(., "(?<=PO BOX ?)[A-Z0-9\\-]+( [0-9]+)?"),
          pmb = ~ stringr::str_extract(., "(?<= PMB ?)[A-Z0-9\\-]+( [0-9]+)?")
        ),
        .names = "{.col}_{.fn}"
      ),
      dplyr::across(
        cols,
        ~ stringr::str_remove_all(., ",?(PO BOX|PMB) ?[A-Z0-9\\-]+( [0-9]+)?")
      )
    ) |>
    std_replace_blank(cols) |>
    std_squish(cols)
}

std_addr2_remove_keywords <- function(df, cols) {
  
  terms <- c(
    "UNITS?",
    "UN",
    "S(UI)?TE(S)?",
    "AP(AR)?T(MENT)?",
    "R(OO)?M",
    "PS( AREA)?"
  )
  terms <- stringr::str_c("[\\,\\-\\ ]", terms, "[\\,\\-\\ 0-9]") |>
    std_collapse_regex()
  df <- std_replace_generic(
    df, 
    cols,
    terms, 
    " "
  )
  
  terms <- c(
    "UNITS?",
    "SUITES?",
    "APARTMENT",
    "ROOM"
  )
  terms <- stringr::str_c("[\\,\\-\\ ]?", terms, "([\\,\\-\\ ]|[A-Z]{0,2})") |>
    std_collapse_regex()
  
  df <- std_replace_generic(
    df, 
    cols,
    terms, 
    " "
  )
  
  std_replace_generic(
    df, 
    cols,
    c(
      "(?<=[ \\-\\,])FLR?(?= |$)" = "FLOOR",
      "(?<=[ \\-\\,])BSMT(?= |$)" = "BASEMENT",
      "(?<=[ \\-\\,])BLDG(?= |$)" = "BUILDING",
      "(?<=[ \\-\\,])TWR(?= |$)" = "TOWER",
      "(?<=[A-Z]{4}[ \\-\\s])FLOOR$" = "",
      "PENT(HOUSE)?( |$|\\-)" = "PH",
      " A K A " = " ",
      "[\\, ]+NO[\\. ]+(?=[A-Z]{0,2}[0-9][1,8][A-Z]{0,2}$)" = " ",
      "[\\, ]+NO[\\. ]+(?=[A-Z]$)" = " "
    )
  )
}

std_simp_street <- function(df, cols) {
  street <-c(
    "STREET", "AVENUE", "LANE", "EXTENSION", "PARK", "DRIVE",
    "ROAD", "BOULEVARD", "PARKWAY", "TERRACE", "PLACE", "WAY",
    "CIRCLE", "ALLEY", "SQUARE", "HIGHWAY", "CENTER", "FREEWAY",
    "COURT", "PLAZA", "WHARF"
  ) |>
    std_collapse_regex()
  street <- stringr::str_c(" (", street, ")$", sep="")
  
  df |>
    dplyr::mutate(
      dplyr::across(
        dplyr::all_of(cols), 
        ~ stringr::str_replace(., street, ""),
        .names="{.col}_simp"
      )
    )
}

std_hyphenate_range <- function (df, cols) {
  df |>
    dplyr::mutate(
      dplyr::across(
        cols,
        ~ stringr::str_replace(
          ., 
          "(?<=^[0-9]{1,6}[A-Z]{0,2}) (?=[0-9]{1,6}[A-Z]{0,1} )", 
          "-"
          )
        ),
      
      dplyr::across(
        cols,
        ~ stringr::str_replace(
          ., 
          "(?<=^[0-9]{1,6}[A-Z]{1,2})(?=[0-9]{1,6}[A-Z]{0,1} )", 
          "-"
        )
      )
    )
}

std_frac_to_dec <- function(df, cols) {
  df |>
    dplyr::mutate(
      dplyr::across(
        cols,
        ~ stringr::str_replace_all(., "(?<=[0-9]) 1\\/2", ".5")
      ),
      dplyr::across(
        cols,
        ~ stringr::str_replace_all(., "\\/", " ")
      )
    )
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

# NEED TO IMPLEMENT fill_muni_by_zip ====
# Replace/fill unmatched muni names using unambiguous ZIP codes
# (i.e., ZIP codes that are fully within a municipality).

# std_fill_zip_by_muni <- function(df, col, muni_col, zips) {
#   
# }

# df_ma <- df_ma |>
#   dplyr::filter(!match, !is.na(postal)) |>
#   dplyr::left_join(
#     zips |>
#       dplyr::filter(ma) |>
#       dplyr::filter(!is.na(muni_unambig_to)) |>
#       dplyr::select(zip, pl_name = muni_unambig_to),
#     by = c("postal" = "zip")
#   ) |>
#   dplyr::mutate(
#     match = !is.na(pl_name),
#     muni = dplyr::case_when(
#       match ~ pl_name,
#       .default = muni
#     )
#   ) |>
#   dplyr::select(-c(pl_name)) |>
#   dplyr::bind_rows(
#     df_ma |>
#       dplyr::filter(match | is.na(postal))
#   )

std_fill_zip_by_muni <- function(df, col, muni_col, zips) {
  df <- df |>
    dplyr::filter(
      is.na(.data[[col]]) & !is.na(.data[[muni_col]])
      ) |>
    dplyr::left_join(
      zips |>
        sf::st_drop_geometry() |>
        dplyr::filter(!is.na(muni_unambig_from)) |>
        dplyr::select(muni_unambig_from, zip) |>
        dplyr::distinct(),
      by = dplyr::join_by(
        !!muni_col == muni_unambig_from
        )
    ) |>
    dplyr::mutate(
      !!col := dplyr::case_when(
        !is.na(zip) ~ zip,
        .default = .data[[col]]
      )
    ) |>
    dplyr::select(-zip) |>
    dplyr::bind_rows(
      df |> 
        dplyr::filter(
          !is.na(.data[[col]]) | is.na(.data[[muni_col]])
        )
    )
}

std_munis_by_places <- function(df, 
                                col, 
                                state_col, 
                                postal_col, 
                                places) {
  
  
  places <- dplyr::filter(places, muni != "BOLTON")
  
  # Direct matches to municipality names.
  state <- df |>
    dplyr::mutate(
      match = .data[[col]] %in% places$muni,
      temp_id = dplyr::row_number()
    )
  
  # Replace neighborhood/sub-municipality names
  # with standardized names. (I.e., Roxbury > Boston)
  state <- state |>
    dplyr::filter(!match & !is.na(.data[[col]])) |>
    dplyr::left_join(
      places |>
        dplyr::select(pl_name = name, replace = muni),
      by = dplyr::join_by(!!col == pl_name),
      na_matches = "never"
    ) |>
    dplyr::mutate(
      match = !is.na(replace),
      new_town = dplyr::case_when(
        match ~ replace,
        .default = NA_character_
      )
    ) |>
    dplyr::select(-c(replace)) |>
    dplyr::bind_rows(
      state |>
        dplyr::filter(match | is.na(.data[[col]]))
    )
  
  # Replace/fill muni names by fuzzy matching on places.
  # state <- state |>
  #   dplyr::filter(!match & !is.na(.data[[col]])) |>
  #   fuzzyjoin::regex_left_join(
  #     places |>
  #       dplyr::select(name_fuzzy, replace = muni),
  #     by = dplyr::join_by(!!col == name_fuzzy)
  #   ) |>
  #   dplyr::mutate(
  #     dist = stringdist::stringdist(.data[[col]], replace)
  #   ) |>
  #   dplyr::group_by(dplyr::across(-c(dist, replace, name_fuzzy))) |>
  #   dplyr::slice_min(dist, n=1) |>
  #   dplyr::ungroup() |>
  #   dplyr::mutate(
  #     match = !is.na(replace),
  #     !!col := dplyr::case_when(
  #       match ~ replace,
  #       .default = .data[[col]]
  #     )
  #   ) |>
  #   dplyr::select(-c(dist, replace, name_fuzzy)) |>
  #   dplyr::bind_rows(
  #     state |>
  #       dplyr::filter(match | is.na(.data[[col]]))
  #   )

  state <- state |>
    dplyr::filter(!match & !is.na(.data[[col]])) |>
    fuzzyjoin::stringdist_left_join(
      places |>
        dplyr::select(pl_name = name, replace = muni),
      by = dplyr::join_by(!!col == pl_name),
      max_dist=2,
      method="dl",
      distance_col="dist"
    ) |>
    dplyr::group_by(dplyr::across(-c(dist, replace, pl_name))) |>
    dplyr::slice_min(dist, n=1) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      match = !is.na(replace),
      new_town = dplyr::case_when(
        match ~ replace,
        .default = new_town
      )
    ) |>
    dplyr::select(-c(replace, pl_name, dist)) |>
    dplyr::bind_rows(
      state |>
        dplyr::filter(match | is.na(.data[[col]]))
    )

  state |>
    dplyr::group_by(temp_id) |>
    dplyr::mutate(
      count = dplyr::n()
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      !!col := dplyr::case_when(
        match & count > 1 ~ .data[[col]],
        match & count == 1 ~ new_town,
        .default = .data[[col]]
      )
    ) |>
    dplyr::select(-c(match, new_town, count, temp_id)) |>
    dplyr::distinct()
}

std_address_fill_downup <- function(df) {
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

# DBA/CO etc. Handling ====

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


# Type Flags ====

std_flag_hns <- function(df, col) {
  hns_munis <- c(
    "LYNN", "EVERETT", "CHELSEA", "BROCKTON", 
    "FALL RIVER", "NEW BEDFORD", "BOSTON")
  df |>
    dplyr::mutate(
      hns =  .data[[col]] %in% hns_munis
    )
}

std_flag_condos <- function(df, luc_col, id_col) {
  df |>
    dplyr::group_by(
      dplyr::across(
        dplyr::all_of(id_col)
        )
      ) |>
    dplyr::mutate(
      condo = dplyr::case_when(
        any(.data[[luc_col]] ==) ~ TRUE,
        .default = FALSE
      )
    ) |>
    dplyr::ungroup()
}

std_flag_inst <- function(df, col) {
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

std_flag_trust <- function(df, col) {
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

std_flag_residential <- function(df, col, muni_id_col, name = "res") {
  flags <- c(
    # Single-Family
    "101",
    # Condo
    "102",
    # Mobile Home
    "103",
    # Two-Family
    "104",
    # Three-Family
    "105",
    # Multiple Houses on One Parcel
    "109",
    # 4-8 Units
    "111",
    # 8+ Units
    "112",
    # Affordable Housing Units (> 50%)
    "114",
    # 121A Corporations
    # https://www.mass.gov/info-details/urban-redevelopment-corporations-urc
    "990",
    # Mixed Use with Residential
    "0xxR"
  )
  nonboston_flags <- c(
    # Housing Authority (MA)
    "970"
  )
  boston_flags <- c(
    "025",
    "026",
    "027",
    '120',
    # Housing Authority (Boston)
    "908"
  )
  df |>
    dplyr::mutate(
      !!name := dplyr::case_when(
        .data[[muni_id_col]] != '035' ~ .data[[col]] %in% c(nonboston_flags, flags),
        .data[[muni_id_col]] == '035' ~ .data[[col]] %in% c(boston_flags, flags),
        .default = FALSE
      )
    )
}

std_flag_estate <- function(df, col, estate_name = "estate") {
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

# Spatial Joiners ====

std_join_censusgeo_sp <- function(sdf, state = "MA", crs = 2249) {
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

std_fill_ma_zip_sp <- function(df, col, site_loc_id, site_muni_id, parcels, zips) {
  ma_zips <- zips |>
    dplyr::filter(state == "MA") |>
    dplyr::group_by(zip) |>
    dplyr::filter(dplyr::row_number() == 1)
  
  df <- df |> 
    dplyr::left_join(
      parcels |>
        sf::st_set_agr("constant") |>
        sf::st_point_on_surface(), 
      by=dplyr::join_by(
        !!site_loc_id == loc_id,
        !!site_muni_id == muni_id
        )
      ) |>
    sf::st_as_sf() |>
    sf::st_join(
      dplyr::select(ma_zips, zip)
      ) |>
    dplyr::mutate(
      !!col := zip
    ) |>
    dplyr::select(-zip) |>
    sf::st_drop_geometry()
}

std_calculate_overlap <- function(x, y, threshold = 0) {
  #' Calculate Overlap
  #' 
  #' Given two `sf` ajects, returns a table of cases where the overlap is
  #' greater than the threshold.
  #' 
  #' @param x An `sf` object.
  #' @param y An `sf` object.
  #' @param threshold Number between 0 and 1 that sets a threshold for ambiguity.
  #' 
  #' @return A data frame with columns 
  #' resulting from the intersection of `x` and `y`.
  #' @export
  
  if(!dplyr::between(threshold, 0, 1)) {
    stop("Threshold must be between 0 and 1.")
  }
  
  if(!("sf" %in% class(x))) {
    stop("X is not an sf dataframe.")
  }
  
  if(!("sf" %in% class(y))) {
    stop("Y is not an sf dataframe.")
  }
  
  if(sf::st_crs(x) != sf::st_crs(y)) {
    stop("CRS of X and Y must match.")
  }
  
  x |>
    dplyr::mutate(
      area = sf::st_area(geometry)
    ) |>
    sf::st_set_agr("constant") |>
    sf::st_intersection(sf::st_set_agr(y, "constant")) |>
    dplyr::mutate(
      overlap = units::drop_units(sf::st_area(geometry) / area)
    ) |>
    sf::st_drop_geometry() |>
    dplyr::filter(
      overlap > threshold
    )
}

# Name Handlers ====

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


# Workflows ====

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

# Deprecated ====

#' std_use_codes <- function(df, col) {
#'   df |>
#'     # Simplify by removing prefixes/suffixes.
#'     std_replace_generic(
#'       c("use_code"),
#'       pattern = c("[A-Z]|0(?=[1-9][0-9][1-9])|(?<=[1-9][0-9]{2})0|(?<=0[1-9][1-9])0|(?<=[1-9][0-9]{2})[1-9]" = "")
#'     ) |>
#'     dplyr::filter(
#'       stringr::str_starts(use_code, "1") |
#'         (stringr::str_detect(use_code, "^(01|0[2-9]1|959|9[79]0)") & muni != "BOSTON") |
#'         (stringr::str_detect(use_code, "^90[78]") & muni == "BOSTON") |
#'         (use_code != "199")
#'     ) |>
#'     # Appears to be some kind of condo quirk in Cambridge...|>
#'     dplyr::mutate(
#'       area = dplyr::case_when(
#'         stringr::str_starts(use_code, "0") | use_code %in% c('113', '114') ~ res_area,
#'         .default = pmax(res_area, bld_area, na.rm = TRUE)
#'       )
#'     ) |>
#'     dplyr::filter(area > 0) |>
#'     dplyr::mutate(
#'       units = dplyr::case_when(
#'         units == 0 ~ NA,
#'         .default = units
#'       ),
#'       missing_units = is.na(units),
#'       units = dplyr::case_when(
#'         # Single-family.
#'         use_code == '101' ~ 1,
#'         # Condo.
#'         use_code == '102' & (land_val == 0 | is.na(land_val)) ~ 1,
#'         # Two-family.
#'         use_code == '104' ~ 2,
#'         # Three-family.
#'         use_code == '105' ~ 3,
#'         # Multiple houses.
#'         use_code == '109' & is.na(units) ~
#'           as.integer(round(area * 0.7 / 1200)),
#'         use_code == '111' & (!dplyr::between(units, 4, 8) | missing_units) ~
#'           as.integer(round(pmax(4, pmin(8, area * 0.7 / 1000)))),
#'         # 8+ MA, 7-30 in Boston
#'         use_code == '112' & muni != "BOSTON" & (!(units >= 8) | missing_units) ~
#'           as.integer(round(pmax(8, area * 0.7 / 1000))),
#'         use_code == '112' & muni == "BOSTON" & (!(units >= 7) | missing_units) ~
#'           as.integer(round(pmax(7, pmin(30, area * 0.7 / 1000)))),
#'         # 113: Boston, 39-99
#'         use_code == '113' & muni == "BOSTON" & (!dplyr::between(units, 30, 99) | missing_units) ~
#'           as.integer(round(pmax(30, pmin(99, area * 0.7 / 1000)))),
#'         # 114: Boston, 100+
#'         use_code == '114' & muni == "BOSTON" & (!(units >= 100) | missing_units) ~
#'           as.integer(round(pmax(100, area * 0.7 / 1000))),
#'         # Unclear what these are outside of Boston.
#'         use_code %in% c('113', '114') & muni != "BOSTON" & missing_units ~
#'           as.integer(round(area * 0.7 / 1000)),
#'         # Boston Housing Authority
#'         use_code == '908' & muni == "BOSTON" & missing_units ~
#'           as.integer(round(area * 0.7 / 1000)),
#'         missing_units ~
#'           pmax(1, as.integer(round(area * 0.7 / 1000))),
#'         .default = units
#'       )
#'     )
#' }
#' 
#' std_assess_addr <- function(df, 
#'                                    zips,
#'                                    places,
#'                                    states = FALSE, 
#'                                    postal = FALSE,
#'                                    country = FALSE) {
#'   df <- df |>
#'     std_street_types("addr") |>
#'     std_directions(c("muni", "addr")) |>
#'     std_leading_zeros("addr", rmsingle = TRUE) |>
#'     std_massachusetts("addr", street_name = TRUE) |>
#'     std_postal_format(c("postal"), zips) |>
#'     std_muni_names(c("muni"))
#'   
#'   if (country) {
#'     df <- df |>
#'       dplyr::mutate(
#'         country = stringr::str_squish(
#'           stringr::str_remove_all(country, "[:digit:]|\\/|//-")
#'         ),
#'         country = countrycode::countrycode(
#'           country,
#'           'country.name.en.regex',
#'           'iso2c',
#'           warn = FALSE
#'         ),
#'         country = dplyr::case_when(
#'           is.na(country) & stringr::str_detect(muni, "SINGAPORE") ~ "SG",
#'           is.na(country) & stringr::str_detect(muni, "JERUSALEM") ~ "IL",
#'           is.na(country) & stringr::str_detect(muni, "BEIJING") ~ "CN",
#'           is.na(country) & stringr::str_detect(muni, "LONDON") ~ "GB",
#'           is.na(country) & stringr::str_detect(muni, "TOKYO") ~ "JP",
#'           is.na(country) & state %in% state.abb & postal %in% zips$all ~ "US",
#'           .default = country
#'         ),
#'         muni = dplyr::case_when(
#'           country == "SG" ~ "SINGAPORE",
#'           .default = muni
#'         )
#'       )
#'   }
#'   
#'   df <- df |>
#'     std_address_by_matching(zips = zips, places = places)
#'   
#'   df
#' }
#' 
#' 
#' 
#' std_hyphenated_numbers <- function(df, cols) {
#'   #' Strips away second half of hyphenated number
#'   #'
#'   #' @param cols Columns to be processed.
#'   #' @returns A dataframe.
#'   #' @export
#'   df |>
#'     # Remove "C / O" prefix.
#'     dplyr::mutate(
#'       dplyr::across(
#'         tidyselect::where(is.character) & tidyselect::all_of(cols),
#'         ~ stringr::str_replace_all(
#'           stringr::str_replace_all(., "(?<=[0-9]{1,4}[A-Z]?)-[0-9]+[A-Z]?", ""),
#'           "(?<=[0-9]{1,4}[A-Z]?)-(?=[A-Z]{1,2})",
#'           ""
#'         )
#'       )
#'     )
#' }
