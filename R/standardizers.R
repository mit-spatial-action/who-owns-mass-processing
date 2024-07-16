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

std_squish <- function(df, cols) {
  df |> 
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ stringr::str_squish(.)
      )
    )
}

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

std_remove_special <- function(df, cols) {
  #' Removes all special characters from columns, except slash
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  
  # Remove (almost all) special characters.
  std_replace_generic(
    df, 
    cols, 
    pattern = "[^[:alnum:][:space:]\\/\\-\\,\\.]|((?<![0-9]) ?\\. ?(?![0-9]))", 
    replace = " "
  ) |>
    # Remove special characters and spaces at front and back of string.
    std_replace_generic(
      cols, 
      pattern = "(^[^[:alnum:]]+)|([^[:alnum:]]+$)", 
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
    std_collapse_regex()
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
    "N / A",
    "UNKNOWN",
    " *"
    # "^[- ]*SAME( ADDRESS)?"
    # "ABOVE"
  ) |>
    std_collapse_regex(full_string = TRUE)
  std_replace_generic(
    df,
    cols, 
    pattern = blank,
    replace = NA_character_
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

std_remove_street_types <- function(df, cols, fill = FALSE) {
  #' Standardize street types.
  #'
  #' @param df A dataframe.
  #' @param cols Column or columns to be processed.
  #' @param fill Whether one should be used to fill the other.
  #' @returns A dataframe.
  #' @export
  street <-c(
    "STREET", "AVENUE", "LANE", "EXTENSION", "PARK", "DRIVE",
    "ROAD", "BOULEVARD", "PARKWAY", "TERRACE", "PLACE", "WAY",
    "CIRCLE", "ALLEY", "SQUARE", "HIGHWAY", "CENTER", "FREEWAY",
    "COURT", "PLAZA", "WHARF"
    ) |>
    std_collapse_regex()
  street <- stringr::str_c(" (", street, ")$", sep="")
  
  df <- df |>
    dplyr::mutate(
      across(
        dplyr::all_of(cols), 
        ~ stringr::str_replace(.x, street, ""),
        .names="{.col}_temp"
      )
    )
  if (length(cols) == 2 & fill) {
    df <- df |>
      dplyr::mutate(
        first_longer = nchar(.data[[cols[1]]]) > nchar(.data[[cols[2]]]),
        match = .data[[paste0(cols[1], "_temp")]] == .data[[paste0(cols[2], "_temp")]],
        dplyr::across(
          cols,
          ~ dplyr::case_when(
            !first_longer & match ~ .data[[cols[2]]],
            first_longer & match ~ .data[[cols[1]]],
            .default = .x,
          )
        )
      ) |> 
      dplyr::select(-c(first_longer, match))
  }
  df |>
    dplyr::select(-paste0(cols, "_temp"))
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
    "(?<= )EXT(?=$| |\\-)" = "EXTENSION",
    "(?<= )PR?K(?=$| |\\-)" = "PARK",
    "(?<= )DRV?(?=$| |\\-)" = "DRIVE",
    "(?<= )RD(?=$| |\\-)" = "ROAD",
    "(?<= )T[ER]+R+(CE)?(?=$| |\\-)" = "TERRACE",
    "(?<= )TE(?=$| |\\-)" = "TERRACE",
    "(?<= )PLC?E?(?=$| |\\-)" = "PLACE",
    "(?<= )WY(?=$| |\\-)" = "WAY",
    "(?<= )(CI?RC?)(?=$| |\\-)" = "CIRCLE",
    "(?<= )A[L]+E?Y(?=$| |\\-)" = "ALLEY",
    "(?<= )SQR?(?=$| |\\-)" = "SQUARE",
    "(?<= )HG?WY?(?=$| |\\-)" = "HIGHWAY",
    "(?<= )CNTR(?=$| |\\-)" = "CENTER",
    "(?<= )FR?WY(?=$| |\\-)" = "FREEWAY",
    "(?<= )CR?T(?=$| |\\-)" = "COURT",
    "(?<= )PL?Z(?=$| |\\-)" = "PLAZA",
    "(?<= )W[HR]+F(?=$| |\\-)" = "WHARF",
    "(?<= )DEPT(?=$| |\\-)" = "DEPARTMENT",
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

std_remove_counties <- function(df, cols, places, munis, state_col, state_val="MA") {
  counties <- tigris::counties(state=state_val, cb=TRUE, resolution="20m") |>
    dplyr::rename_with(stringr::str_to_lower) |>
    sf::st_drop_geometry() |>
    dplyr::select(name) |>
    std_uppercase(c("name")) |>
    dplyr::filter(!(name %in% places$name)) |>
    dplyr::filter(!(name %in% munis$muni)) |>
    dplyr::pull(name) |>
    std_collapse_regex() |>
    suppressMessages()
  
  df |>
    dplyr::filter(.data[[state_col]] == state_val) |>
    std_replace_generic(
      cols,
      stringr::str_c("(?<=[A-Z] )(", counties, ")$", collapse=""),
      ""
    ) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(.data[[state_col]] != state_val | is.na(.data[[state_col]]))
    )
}

std_muni_names <- function(df, cols, state_col, state_val="MA") {
  #' Replace Boston neighborhoods with Boston
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @param postal_col Columns to be processed.
  #' @returns A dataframe.
  #' @export
  #' 
  muni_corrections <- c(
    "BORO$" = "BOROUGH",
    "^ACT$" = "ACTON",
    "^GLOUSTER$" = "GLOUCESTER",
    "^NEW TOWN$" = "NEWTON",
    "^[A-Z]AMBRIDGE$" = "CAMBRIDGE",
    "^NEWBURY PORT$" = "NEWBURYPORT",
    "^DEVEN$" = "DEVENS",
    "^PRIDE CROSSING$" = "PRIDES CROSSING",
    "^MANCHESTER$" = "MANCHESTER-BY-THE-SEA",
    "^MANC[A-Z \\/]+SEA$" = "MANCHESTER-BY-THE-SEA"
  )
  ma <- df |>
    dplyr::filter(.data[[state_col]] == state_val)
  
  ma |>
    std_replace_generic(
      cols, 
      pattern = muni_corrections
    ) |>
    dplyr::bind_rows(
      df |> dplyr::filter(
        .data[[state_col]] != state_val | is.na(.data[[state_col]])
        )
    )
}

std_zip_format <- function(df, col, only_zip = FALSE) {
  #' Standardize and simplify (i.e., remove 4-digit suffix) US ZIP codes.
  #'
  #' @param df A dataframe.
  #' @param col Column containing the ZIP code to be simplified.
  #' @returns A dataframe.
  #' @export
  
  df <- df |>
    dplyr::mutate(
      # Replace  "O"s that should be 0s.
      !!col := stringr::str_replace(.data[[col]], "^O[0-9]", '0'),
      # Deal with suffixes.
      !!col := stringr::str_replace(.data[[col]], "(?<=^[0-9]{5})[ \\-\\/][0-9]{4}$", ''),
      # In cases where only ZIP codes are expected, set any ill-formatted
      # codes to NA.
      !!col := dplyr::case_when(
        only_zip & stringr::str_length(.data[[col]]) != 5 ~ NA_character_,
        .default = .data[[col]]
      )
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


std_luc <- function(df, path, file = "luc_crosswalk.csv"){
  
  # Read Land Use Codes
  lu <- readr::read_csv(file.path(path, file), show_col_types = FALSE) |>
    dplyr::rename_with(tolower) |>
    dplyr::select(c(use_code, luc_assign))
  
  nonboston <- df |> 
    dplyr::filter(muni_id != "035")
  
  boston <- df |> 
    dplyr::filter(muni_id == "035")
  
  nonboston <- nonboston |>
    dplyr::left_join(
      lu,
      by = c("use_code" = "use_code"),
      na_matches = "never"
    )
  
  unmatched <- nonboston |>
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
  
  boston <- boston |>
    dplyr::mutate(
      luc = dplyr::case_when(
        use_code %in% c('012', '013', '019', '031') ~ '0xxR',
        .default = use_code
      )
    )
  
  nonboston |>
    dplyr::filter(!is.na(luc_assign)) |>
    dplyr::bind_rows(unmatched) |>
    dplyr::rename(luc = luc_assign) |>
    dplyr::bind_rows(
      boston
    ) |>
    dplyr::mutate(
      luc = dplyr::case_when(
        is.na(luc) ~ use_code,
        .default = luc
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

std_units_from_luc <- function(df, col, muni_id_col, units_col="units") {
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
        .default = units
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
        .default = units
      )
    )
}

std_test_given_units <- function(df) {
  df |>
    dplyr::mutate(
      units_valid = dplyr::case_when(
        # 4-8 Unit
        # ===
        luc == '111' & dplyr::between(units, 4, 8) ~
          TRUE,
        # 112: >8 Unit
        # ===
        luc == '112' & units > 8 ~
          TRUE,
        # 109: Multi House, 0xx: Multi-Use, 103: Mobile Home
        # ===
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

std_fill_units_flow <- function(df, parcels, ma_munis, crs=CRS) {
  # Identify unit counts for unambiguous cases where there is one property
  # on a parcel and that property is single-, two-, or three-family.
  units <- df |>
    std_property_units()
  
  units <- units |>
    dplyr::filter(condo) |>
    dplyr::group_by(loc_id) |>
    dplyr::mutate(
      condo_count = sum(luc %in% c('102', '908', '970', '0xx')),
      units = dplyr::case_when(
        luc %in% c('908', '970', '0xx') ~ 1,
        .default = units
      ),
      condo_overcount = condo_count == count - 1,
      count = dplyr::case_when(
        condo_overcount ~ condo_count,
        .default = count
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(
      !(condo_overcount & !(luc %in% c('101', '102', '908', '970', '0xx')))) |>
    dplyr::select(-c(condo_overcount, condo_count)) |>
    dplyr::bind_rows(
      units |>
        dplyr::filter(!condo)
    ) |>
    dplyr::mutate(
      possible_ambiguous = !(luc %in% c('101', '104', '105', '102') | 
                               luc %in% c('908', '970', '0xx') & condo)
    ) |>
    std_test_given_units()
  
  # Here we need to make a decision: there are cases where there are
  # multiple non-condo properties on a parcel. 
  units_ambiguous <- units |>
    dplyr::filter(possible_ambiguous & !units_valid)
  
  units_resolved <- units |>
    dplyr::filter(!(possible_ambiguous & !units_valid))
  
  units_from_addresses <- parcels |>
    dplyr::semi_join(units_ambiguous, by = dplyr::join_by(loc_id)) |>
    sf::st_join(
      load_layer_flow(
        conn,
        "ma_addresses",
        load_address_points(ma_munis, crs=crs),
        refresh=refresh
      ), 
      join = sf::st_contains_properly
    ) |>
    sf::st_drop_geometry()
  
  units_one <- units_ambiguous |>
    dplyr::filter(count == 1)
  
  units_from_addresses_one <- units_from_addresses |>
    dplyr::semi_join(units_one, by = dplyr::join_by(loc_id)) |>
    dplyr::group_by(loc_id) |>
    dplyr::summarize(
      addr_count = sum(unit_count)
    ) |>
    dplyr::ungroup()
  
  units_one <- units_one |>
    dplyr::left_join(units_from_addresses_one, by = dplyr::join_by(loc_id)) |>
    std_estimate_units() |>
    dplyr::select(-addr_count)
  
  units_resolved <- units_resolved |>
    dplyr::bind_rows(units_one)
  
  units_multi <- units_ambiguous |>
    dplyr::filter(count != 1)
  
  units_multi <- units_multi |>
    dplyr::bind_rows(
      units_resolved |>
        dplyr::semi_join(units_multi, by = dplyr::join_by(loc_id)) |>
        dplyr::mutate(add = TRUE)
    )
  
  units_from_addresses_multi <- units_from_addresses |>
    dplyr::semi_join(units_multi, by = dplyr::join_by(loc_id)) |>
    dplyr::group_by(loc_id) |>
    dplyr::summarize(
      addr_count = sum(unit_count)
    ) |>
    dplyr::ungroup()
  
  units_multi <- units_multi |>
    dplyr::left_join(units_from_addresses_multi, by = dplyr::join_by(loc_id)) |> 
    dplyr::group_by(loc_id) |>
    dplyr::mutate(
      filled = sum(units != 0),
      units = dplyr::case_when(
        units == 0 ~ floor((addr_count - sum(units)) / (count - filled)),
        .default = units
      )
    ) |>
    dplyr::filter(is.na(add)) |>
    dplyr::select(-c(filled, add)) |>
    std_estimate_units() |>
    dplyr::select(-addr_count)
  
  units_resolved <- units_resolved |>
    dplyr::bind_rows(units_multi) |>
    dplyr::select(-c(condo, possible_ambiguous, distinct, units_valid))
}

# Address Refinement/Completion ====
std_leading_zero_vector <- function(x) {
  stringr::str_remove_all(x, "(?<=^|\\-)0+(?=[1-9]|[A-Z]|0$)")
}

std_addr2_floor <- function(df, cols) {
  std_addr2_parser(df, cols, " [0-9]+[A-Z]{2} FLOOR[ |$]|[^| ]FLOOR [0-9]+")
}

std_addr2_range <- function(df, cols) {
  std_addr2_parser(df, cols, " [0-9]+[A-Z]{0,2}?[\\- ][0-9]+[A-Z]{0,2}?$")
}

std_addr2_alpha_num <- function(df, cols) {
  std_addr2_parser(df, cols, " [A-Z]{1,2}[\\-\\ ]?[0-9]+$")
}

std_addr2_num_alpha <- function(df, cols) {
  std_addr2_parser(df, cols, " [0-9]+[\\-\\ ]?[A-Z]{1,2}$")
}

std_addr2_num <- function(df, cols) {
  std_addr2_parser(df, cols, " [A-Z]{0,2}?[0-9]+[A-Z]{0,2}?$")
}

std_addr2_alpha <- function(df, cols) {
  std_addr2_parser(df, cols, " [A-Z0-9]$")
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
            stringr::str_remove_all(
              .x, 
              get(paste0(dplyr::cur_column(), "_temp"))
            ),
          .default = .x
        )
      ),
      dplyr::across(
        stringr::str_c(cols, "_temp"),
        ~ dplyr::case_when(
          !is.na(.x) ~ stringr::str_replace_all(
            std_leading_zero_vector(stringr::str_squish(.x)), 
            " ", 
            "-"
          ),
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
    dplyr::select(-stringr::str_c(cols, "_temp"))
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
    "S(UI)?TE(S)?",
    "AP(AR)?T(MENT)?",
    "R(OO)?M",
    "PS( AREA)?"
  )
  terms <- stringr::str_c("[\\,\\-\\ ]", terms, "[\\,\\-\\ ]") |>
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
  terms <- stringr::str_c("[\\,\\-\\ ]?", terms, "([\\,\\-\\ ]|[A-Z]{1,2}|[0-9]|$)") |>
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
      "(?<= )FLR?(?= |$)" = "FLOOR",
      "PENT(HOUSE)?( |$|\\-)" = "PH",
      " A K A " = " ",
      "[\\, ]+NO (?=[A-Z]{0,2}[0-9][1,8][A-Z]{0,2}$)" = " ",
      "[\\, ]+NO (?=[A-Z]$)" = " "
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
        ~ stringr::str_replace_all(., "(?<=[0-9]) ?1\\/2", ".5")
      ),
      dplyr::across(
        cols,
        ~ stringr::str_replace_all(., "\\/", " ")
      )
    )
}

std_address_to_range <- function(df, cols) {
  df |>
    dplyr::mutate(
      dplyr::across(
        cols,
        list(
          range_ = ~ stringr::str_detect(.x, "^[0-9]+[A-Z]{0,2} *((1 \\/ 2)|\\.[0-9])?([ -]+[0-9]+[A-Z]{0,2} *((1 \\/ 2)|\\.[0-9])?)+ +(?=[A-Z0-9])"),
          num_init_ = ~ stringr::str_extract(.x, "^[0-9]+[A-Z]{0,2} *((1 \\/ 2)|\\.[0-9])?([ -]+(([0-9]+[A-Z]{0,2} *((1 \\/ 2)|\\.[0-9])?)*|[A-Z]))? +(?=[A-Z0-9])")
        )
      ),
      dplyr::across(
        cols,
        list(
          body = ~ stringr::str_remove(.x, get(paste0(dplyr::cur_column(), "_num_init_")))
        )
      ),
      dplyr::across(
        cols,
        list(
          start = ~ as.numeric(
            stringr::str_extract(
              stringr::str_replace(
                .x, 
                " 1 \\/ 2", 
                "\\.5"
              ),
              "^[0-9\\.]+")
            )
        )
      ),
      dplyr::across(
        cols,
        list(
          end_init_ = ~ dplyr::case_when(
            get(paste0(dplyr::cur_column(), "_range_")) ~ as.numeric(
                stringr::str_extract(
                  stringr::str_replace(
                    .x, 
                    " 1 \\/ 2", 
                    "\\.5"
                  ),
                  "(?<=[- ])[0-9\\.]+"
                )
              ),
              .default = get(paste0(dplyr::cur_column(), "_start"))
            )
        )
      ),
      dplyr::across(
        cols,
        list(
          end = ~ dplyr::case_when(
            (get(paste0(dplyr::cur_column(), "_end_init_")) < get(paste0(dplyr::cur_column(), "_start"))) | is.na(get(paste0(dplyr::cur_column(), "_end_init_")))
              ~ get(paste0(dplyr::cur_column(), "_start")),
            .default = get(paste0(dplyr::cur_column(), "_end_init_"))
          )
        )
      ),
      dplyr::across(
        cols,
        list(
          even = ~ dplyr::case_when(
            floor(get(paste0(dplyr::cur_column(), "_start"))) %% 2 == 0 ~ TRUE,
            floor(get(paste0(dplyr::cur_column(), "_end"))) %% 2 == 1 ~ FALSE,
            .default = FALSE
          )
        )
      ),
      dplyr::across(
        cols,
        list(
          num_ = ~ dplyr::case_when(
            (get(paste0(dplyr::cur_column(), "_start")) == get(paste0(dplyr::cur_column(), "_end")))
              & get(paste0(dplyr::cur_column(), "_range_"))
              ~ stringr::str_c(get(paste0(dplyr::cur_column(), "_start"))),
            (get(paste0(dplyr::cur_column(), "_start")) != get(paste0(dplyr::cur_column(), "_end")))
              & get(paste0(dplyr::cur_column(), "_range_"))
              ~ stringr::str_c(get(paste0(dplyr::cur_column(), "_start")), get(paste0(dplyr::cur_column(), "_end")), sep="-"),
            .default = get(paste0(dplyr::cur_column(), "_num_init_"))
          ),
          parsed_ = ~ !is.na(get(paste0(dplyr::cur_column(), "_start"))) & !is.na(get(paste0(dplyr::cur_column(), "_end"))) & !is.na(get(paste0(dplyr::cur_column(), "_body")))
        )
      ),
      dplyr::across(
        cols,
        ~ dplyr::case_when(
          get(paste0(dplyr::cur_column(), "_parsed_")) ~ stringr::str_squish(stringr::str_c(get(paste0(dplyr::cur_column(), "_num_")), get(paste0(dplyr::cur_column(), "_body")), sep = " ")),
          .default = .x
        )
      )
    ) |>
    dplyr::select(-dplyr::ends_with("_"))
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

std_fill_zip_by_muni <- function(df, zips) {
  df <- df |>
    dplyr::filter(is.na(postal), !is.na(muni)) |>
    dplyr::left_join(
      zips |>
        sf::st_drop_geometry() |>
        dplyr::filter(!is.na(muni_unambig_from)) |>
        dplyr::select(muni_unambig_from, zip),
      by = c("muni" = "muni_unambig_from")
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

std_munis_by_places <- function(df, places, muni_col, state_col, state_val="MA") {
  places <- dplyr::filter(places, muni != "BOLTON")
  # Direct matches to municipality names.
  df_ma <- df |>
    dplyr::filter(.data[[state_col]] == state_val) |>
    dplyr::mutate(
      match = .data[[muni_col]] %in% places$muni
    )
  
  # Replace neighborhood/sub-municipality names
  # with standardized names. (I.e., Roxbury > Boston)
  df_ma <- df_ma |>
    dplyr::filter(!match & !is.na(.data[[muni_col]])) |>
    dplyr::left_join(
      places |>
        dplyr::select(name, replace = muni),
      by = dplyr::join_by(!!muni_col == name),
      na_matches = "never"
    ) |>
    dplyr::mutate(
      match = !is.na(replace),
      !!muni_col := dplyr::case_when(
        match ~ replace,
        .default = .data[[muni_col]]
      )
    ) |>
    dplyr::select(-c(replace)) |>
    dplyr::bind_rows(
      df_ma |>
        dplyr::filter(match | is.na(.data[[muni_col]]))
    )
  
  # Replace/fill muni names by fuzzy matching on places.
  df_ma <- df_ma |>
    dplyr::filter(!match & !is.na(.data[[muni_col]])) |>
    fuzzyjoin::regex_left_join(
      places |>
        dplyr::select(name_fuzzy, replace = muni),
      by = dplyr::join_by(!!muni_col == name_fuzzy)
    ) |>
    dplyr::mutate(
      dist = stringdist::stringdist(.data[[muni_col]], replace)
    ) |>
    dplyr::group_by(dplyr::across(-c(dist, replace, name_fuzzy))) |>
    dplyr::slice_min(dist, n=1) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      match = !is.na(replace),
      !!muni_col := dplyr::case_when(
        match ~ replace,
        .default = .data[[muni_col]]
      )
    ) |>
    dplyr::select(-c(dist, replace, name_fuzzy)) |>
    dplyr::bind_rows(
      df_ma |>
        dplyr::filter(match | is.na(.data[[muni_col]]))
    )

  # Replace/fill muni names by fuzzy matching on places using simplified
  # municipality names.
  df_ma <- df_ma |>
    dplyr::filter(!match, !is.na(.data[[muni_col]])) |>
    dplyr::mutate(
      simp = stringr::str_remove(.data[[muni_col]], "^(SOUTH(WEST|EAST)?|NORTH(WEST|EAST)?|EAST|WEST) ")
    ) |>
    fuzzyjoin::regex_left_join(
      places |>
        dplyr::select(name_fuzzy, replace = muni),
      by = dplyr::join_by(simp == name_fuzzy)
    ) |>
    dplyr::mutate(
      dist = stringdist::stringdist(simp, replace)
    ) |>
    dplyr::group_by(dplyr::across(-c(dist, replace, name_fuzzy))) |>
    dplyr::slice_min(dist, n=1) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      match = !is.na(replace),
      !!muni_col := dplyr::case_when(
        match ~ replace,
        .default =.data[[muni_col]]
      )
    ) |>
    dplyr::select(-c(dist, replace, name_fuzzy, simp)) |>
    dplyr::bind_rows(
      df_ma |>
        dplyr::filter(match | is.na(.data[[muni_col]]))
    )

  df_ma <- df_ma |>
    dplyr::filter(!match & !is.na(.data[[muni_col]])) |>
    fuzzyjoin::stringdist_left_join(
      places |>
        dplyr::select(name, replace = muni),
      by = dplyr::join_by(!!muni_col == name),
      max_dist=1,
      method="dl",
      distance_col="dist"
    ) |>
    dplyr::group_by(dplyr::across(-c(dist, replace, name))) |>
    dplyr::slice_min(dist, n=1) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      match = !is.na(replace),
      !!muni_col := dplyr::case_when(
        match ~ replace,
        .default = .data[[muni_col]]
      )
    ) |>
    dplyr::select(-c(replace, name, dist)) |>
    dplyr::bind_rows(
      df_ma |>
        dplyr::filter(match | is.na(.data[[muni_col]]))
    )

  df_ma |>
    dplyr::group_by(prop_id) |>
    dplyr::mutate(
      count = dplyr::n()
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      !!muni_col := dplyr::case_when(
        match & count > 1 ~ NA_character_,
        .default = .data[[muni_col]]
      )
    ) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(.data[[state_col]] != state_val | is.na(.data[[state_col]]))
    ) |>
    dplyr::select(-match) |>
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

std_flag_condos <- function(df) {
  df |>
    dplyr::group_by(loc_id) |>
    dplyr::mutate(
      condo = dplyr::case_when(
        ('102' %in% luc) ~ TRUE,
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

std_flag_residential <- function(df, col, name = "res") {
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
        muni_id != '035' ~ .data[[col]] %in% c(nonboston_flags, flags),
        muni_id == '035' ~ .data[[col]] %in% c(boston_flags, flags),
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

std_fill_zip_sp <- function(df, parcels, zips) {
  ma_zips <- zips |>
    dplyr::filter(ma)
  
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

std_calculate_overlap <- function(x, y, threshold = 0) {
  #' Calculate Overlap
  #' 
  #' Given two `sf` objects, returns a table of cases where the overlap is
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

# Workflows ====

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
