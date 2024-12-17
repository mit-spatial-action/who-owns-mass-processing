SEARCH <- tibble::lst(
  estate = c(
    "ESTATE OF", "(A )?LIFE ESTATE", "FOR LIFE", "LE"
  ),
  street_types = c(
    "STREET", "AVENUE", "LANE", "EXTENSION", "PARK", "DRIVE",
    "ROAD", "BOULEVARD", "PARKWAY", "TERRACE", "PLACE", "WAY",
    "CIRCLE", "ALLEY", "SQUARE", "HIGHWAY", "CENTER", "FREEWAY",
    "COURT", "PLAZA", "WHARF", "SQUARE"
  ),
  inst = c(
    "CORPORATION", " INC( |$)", "LLC", "LTD", "COMPANY",
    "LP", "PROPERT(IES|Y)", "GROUP", "MANAGEMENT", "PARTNERS", 
    "REALTY", "DEVELOPMENT", "EQUITIES", "HOLDING", "INSTITUTE", 
    "DIOCESE", "PARISH", "CITY", "HOUSING", "AUTHORITY", "SERVICE(S|R)?", 
    "LEGAL", "SERVICES", "LLP", "UNIVERSITY", "COLLEGE", "ASSOCIATION",
    "CONDOMINIUM", "HEALTH", "HOSPITAL", "SYSTEM", "ACCOUNTS?", "PAYABLE",
    "ASSOCIATES", "ATTORNEY", "AT LAW", "DEPARTMENT", "REGISTERED", "AGENTS",
    "MORTGAGE", "COMMUNITY", "BANK", "LOANS?"
  ),
  trust = "(?= \\bTRUST(EES?)?)",
  trustees = "\\bTRUST(EES?)( OF)?\\b",
  trust_definite = c(
    "(IR)?REVOCABLE", "NOMINEE", "INCOME ONLY", "FOR LIFE", "UNDER DECLARATION OF", "LIVING", "FAMILY"
  ),
  trust_types = c(
    trust_definite,
    "REALTY", "REAL ESTATE", 
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
    "ESQ(UIRE)?", "MD", "JD", "PHD", "PC", "MR", "MS", "MRS",
    # Generations and numbers
    "JR", "SR", "I+", "I*[VX]I*",
    "(AND )?ET( - | )?ALL?"
  )
)

# Standardizer Helpers ====

std_collapse_regex <- function(c, full_string = FALSE) {
  #' Collapse vector elements into a single string suitable for use as a regular expression.
  #' 
  #' @param c A character vector. 
  #' @param full_string Logical. If TRUE, the regex pattern will match the entire string by adding `^` at the
  #' start and `$` at the end. Default is `FALSE`. The regex will match the input strings anywhere within a text.
  #'
  #' @return A single character string representing the collapsed regular expression.
  #' @export
  str <- stringr::str_c(c, collapse="|")
  if (full_string) {
    str <- stringr::str_c("^(", str, ")$")
  }
  str
}

std_replace_generic <- function(df, cols, pattern, replace) {
  #' for specified columns of a data frame, replace a pattern with a replacement string 
  #'
  #' @param df A data frame to be changed
  #' @param cols A vector of column names or a logical selector to operate replacement
  #' @param pattern A string represents the regex pattern to match.
  #' @param replace A string used as the replacement for matched patterns. 
  #'   If not provided, matches will be replaced with an empty string.
  #'
  #' @return A data frame with the specified columns that operated the 
  #'   pattern replacements.
  #' @export
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
  #' calculates the total row count and the count of distinct values in a specified column, 
  #' grouped by one or more columns in a data frame.
  #'
  #' @param df A data frame to analyze.
  #' @param cols A vector of the column names to group by.
  #' @param col_distinct A single string of the column name where the distinct count will be calculated.
  #'
  #' @return A data frame with two new columns count(total number of rows in each group)
  #'   and distinct(number of distinct values in col_distinct for each group).
  #' @export
  df |>
    dplyr::group_by(dplyr::across(dplyr::all_of(cols))) |>
    dplyr::mutate(
      count = dplyr::n(), 
      distinct := dplyr::n_distinct(get(col_distinct))
    ) |>
    dplyr::ungroup()
}

std_pad_muni_ids <- function(ids) {
  #' standard municipality ids with a fixed width of three characters.
  #'
  #' @param ids A vector or numeric vector of municipality IDs to be standardized 
  #' @return A vector where each ID is padded to have a total width of three characters, 
  #'   with leading zeros added where necessary.
  #'   @export
  ids |>
    stringr::str_pad(3, side="left", pad="0")
}

std_col_prefixes <- function (df, prefixes=c(), parsed_cols=c()) {
  if (length(prefixes) != 0) {
    prefixes <- stringr::str_c(
      "(?<=(", 
      stringr::str_c(prefixes, collapse="|"), 
      ")_)"
    )
  } 
  
  df |>
    dplyr::rename_with(
      ~ stringr::str_replace(
        .x,
        stringr::str_c(
          prefixes,
          "[a-z\\_]+(?=(", 
          stringr::str_c(parsed_cols, collapse="|"), 
          ")$)"
        ),
        ""
      )
    )
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
        "(?<![0-9]) *\\. *(?!5)" = " ",
        "(\\b[0-9]+O\\b)|(\\b[0-9]+O\\b)|(\\b[0-9]+O[0-9]+\\b)" = "0"
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
    "U ?NKNOWN",
    "N N",
    "LLC|LLP|INC|LTD",
    "DOING BUSINESS (AS|BY)",
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
        dplyr::all_of(cols),
        ~ dplyr::case_when(
          stringr::str_detect(., "^(SAME|NONE|UNKNOWN)(?=\\s|\\,|$)") ~ NA_character_,
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

std_fix_concatenated_ranges <- function(df, cols) {
  #' Fix e.g., 234234A TEST ST
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @returns A dataframe.
  #' @export
  std_replace_generic(
    df,
    cols, 
    pattern = c(
      "^([0-9]{3,4})[A-Z]?(?=(\\1){1})" = "",
      "(?<=^([0-9]{2,3})[0-9][A-Z]?)(?=(\\1){1}[0-9])" = "-"
      )
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
    "(?<=[02-9] )ST(?= [A-Z]{3})" = "SAINT",
    "^ST(?= [A-Z]{3})" = "SAINT",
    "(?<= 1) (?=ST )" = "",
    "(?<= 2) (?=ND )" = "",
    "(?<= 3) (?=RD )" = "",
    "(?<= [1-9]?[04-9]) (?=TH )" = "",
    "\\b(ST|ST[RET]{3,5}|STREE)\\b" = "STREET",
    "\\bAVE?\\b" = "AVENUE",
    "\\bLA?N\\b" = "LANE",
    "\\bBLV?R?D?\\b" = "BOULEVARD",
    "\\bP(A?R?KWA?)?Y\\b" = "PARKWAY",
    "\\bPW\\b" = "PARKWAY",
    "\\bEXT\\b" = "EXTENSION",
    "\\bPR?K\\b" = "PARK",
    "\\bDRV?\\b" = "DRIVE",
    "\\bRD\\b" = "ROAD",
    "\\bRO\\b" = "ROW",
    "\\bT[ER]+R+(CE)?\\b" = "TERRACE",
    "\\bTE\\b" = "TERRACE",
    "\\bPLC?E?\\b" = "PLACE",
    "\\bWY\\b" = "WAY",
    "\\b(CI?RC?|CI)\\b" = "CIRCLE",
    "\\bA[L]+E?Y\\b" = "ALLEY",
    "\\bSQR?\\b" = "SQUARE",
    "\\bHG?WY?\\b" = "HIGHWAY",
    "\\bCNTR\\b" = "CENTER",
    "\\bFR?WY\\b" = "FREEWAY",
    "\\bMSGR\\b" = "MONSIGNOR",
    "\\bO ?BRI\\b" = "OBRI",
    "\\b(MONSIGNOR)?( P ?J)? LYDON\\b" = " MONSIGNOR PJ LYDON",
    "\\b(MONSIGNOR)? OBRI[AE]N(?= HIGH\\b)" = " MONSIGNOR OBRIEN",
    "\\bWM F MCCLELLAN\\b" = "WILLIAM F MCCLELLAN",
    "\\bCR?T\\b" = "COURT",
    "\\bPL?Z\\b" = "PLAZA",
    "\\bW[HR]+F\\b" = "WHARF",
    "\\bDEPT\\b" = "DEPARTMENT",
    "\\bP ?O SQUARE\\b" = "POST OFFICE SQUARE",
    "\\bPRUDENTIAL TOWER\\b" = "",
    "\\bP ?O( ?BO?X)?[ \\-]*(?=[A-Z]{0,1}[0-9])" = "PO BOX ",
    "\\b(?<!PO )BO?X[ \\-](?=[A-Z]{0,1}[0-9])" = "PO BOX "
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
    "^ZERO(?= )" = "0",
    "(^ONE(?= ))|((?<= )I(?= |$))" = "1",
    "(^ONE(?= ))|((?<= )II(?= |$))" = "2",
    "(^THREE(?= ))|((?<= )III(?= |$))" = "3",
    "(^FOUR(?= ))|((?<= )IV(?= |$))" = "4",
    "(^FIVE(?= ))|((?<= )V(?= |$))" = "5",
    "(^SIX(?= ))|((?<= )VI(?= |$))" = "6",
    "(^SEVEN(?= ))|((?<= )VII(?= |$))" = "7",
    "(^EIGHT(?= ))|((?<= )VIII(?= |$))" = "8",
    "(^NINE(?= ))|((?<= )IX(?= |$))" = "9",
    "(^TEN(?= ))|((?<= )X(?= |$))" = "10",
    "(^TWENTY(?= ))|((?<= )XX(?= |$))" = "20"
  )
  std_replace_generic(
    df,
    cols, 
    pattern = sm_num
  ) |>
    std_replace_generic(
      cols, 
      pattern = c("^([1-9]) (?=[0-9]{2,4}[-\\ ])" = "\\1")
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
      temp = stringr::str_extract(.data[[col]], "[0-9]{5}([ \\-]*[0-9]{4})?(?=[A-Z\\s]*$)"),
      !!postal_col := dplyr::case_when(
        !is.na(temp) & 
          (is.na(.data[[postal_col]]) | 
             !stringr::str_detect(.data[[postal_col]], "[0-9]{5}([ \\-]*[0-9]{4})?[A-Z\\s]*$") ) ~ temp,
        .default = .data[[postal_col]]
      ),
      !!col := stringr::str_squish(
        stringr::str_remove(.data[[col]], paste0(temp, ""))
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

std_zip_format <- function(df, col, state_col, zips, state_constraint = "") {
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
    dplyr::select(zip, new_state = unambig_state) |> 
    dplyr::distinct()
  
  if (state_constraint != "") {
    zips <- zips |>
      dplyr::filter(state == state_constraint)
  } else {
    state_constraint <- c(state.abb, "DC")
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
      in_zips = temp %in% zips,
      # In cases where only ZIP codes are expected, set any ill-formatted
      # codes to NA.
      !!col := dplyr::case_when(
        in_zips & .data[[state_col]] %in% state_constraint ~ temp,
        !in_zips & .data[[state_col]] %in% state_constraint ~ NA_character_,
        .default = .data[[col]]
      )
    )
  
  df |>
    dplyr::select(-c(temp, in_zips))
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

std_mass_corp <- function(df, col) {
  df |> 
    dplyr::mutate(
      !!col := stringr::str_remove_all(
        .data[[col]],
        "(?<= (CO|INC|CORP|LLC|L?LPS?|LTD)) (A )?(MASS|MASSACHUSETTS)( (CO|INC|CORP|LLC|L?LPS?|LTD))?$"
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
    "LPS?" = "LLP",
    "JU?ST[ \\-]*A[ \\-]*START" = "JUST A START",
    "GENERAL PARTNERS(HIP)?" = "GP",
    "AUTH[ORITY]{0,6}" = "AUTHORITY",
    "(ASS[N]?|ASSOC)" = "ASSOCATION",
    "DEPT" = "DEPARTMENT",
    "((G?ST|[0-9]{0,4}) )?(TRUST|TRU?ST|TR|TRT|TRUS|TRU|TRYST|T[RUS]{3}T)( (OF )?[0-9\\s\\-]+)?" = "TRUST",
    "(C ?O-?)?(TRS|TRU?ST[ES]{1,4}|TRSTS|T[RUSTEE]{6}S|TS|BE)" = "TRUSTEES",
    "([A-Z]+)TRUST" = "\\1 TRUST",
    "([A-Z]+)TRUSTEES" = "\\1 TRUSTEES",
    "([A-Z]+)LLC" = "\\1 LLC",
    "LLC( A)? MASSACHUSETTS LLC" = "LLC",
    "LLP( A)? MASSACHUSETTS (LLP|LPS)" = "LLP",
    "(?:(INC|COMPANY)( A)?) MASSACHUSETTS CORPORATION" = "",
    "([A-Z]+)LLP" = "\\1 LLP"
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

# Use Code/Unit Estimation ====

std_luc <- function(
    df, 
    col,
    muni_id_col, 
    path=DATA_PATH,
    name="luc",
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
        !!col == use_code
      ),
      na_matches = "never"
    )
  
  unmatched <- nonboston |>
    dplyr::filter(is.na(luc_assign)) |>
    dplyr::select(-luc_assign) |>
    dplyr::mutate(
      !!stringr::str_c(col, "_simp") := 
        substr(.data[[col]], 1, 3)
    ) |>
    dplyr::left_join(
      lu,
      by = dplyr::join_by(
        !!stringr::str_c(col, "_simp") == use_code
      ),
      na_matches = "never"
    ) |>
    dplyr::select(-dplyr::ends_with("_simp"))
  
  boston <- boston |>
    dplyr::mutate(
      !!name := dplyr::case_when(
        .data[[col]] %in% c('012', '013', '019', '031') ~ '0xxR',
        .default = .data[[col]]
      )
    )
  
  nonboston |>
    dplyr::filter(!is.na(luc_assign)) |>
    dplyr::bind_rows(unmatched) |>
    dplyr::rename(!!name := luc_assign) |>
    dplyr::bind_rows(
      boston
    ) |>
    dplyr::mutate(
      !!name := dplyr::case_when(
        is.na(.data[[name]]) ~ .data[[col]],
        .default = .data[[name]]
      )
    )
}

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
        # Single-family. 102: Condos
        .data[[col]] %in% c('101', '102') ~ 1,
        # Two-family.
        .data[[col]] == '104' ~ 2,
        # Three-family.
        .data[[col]] == '105' ~ 3,
        .default = .data[[units_col]]
      )
    )
}

std_test_units <- function(df, col, luc_col, muni_id_col) {
  boston <- df |>
    dplyr::filter(.data[[muni_id_col]] == '035') |>
    dplyr::mutate(
      units_valid = dplyr::case_when(
        # BOSTON, 112: 4-6 Unit
        .data[[luc_col]] == '111' & !dplyr::between(.data[[col]], 4, 6) ~
          FALSE,
        # BOSTON, 112: 7-30 Unit
        .data[[luc_col]] == '112' & !dplyr::between(.data[[col]], 7, 30) ~
          FALSE,
        # BOSTON, 113: 31-99 Unit
        .data[[luc_col]] == '113' & !dplyr::between(.data[[col]], 7, 30) ~
          FALSE,
        # BOSTON, 114: 31-99 Unit
        .data[[luc_col]] == '114' & .data[[col]] < 100 ~
          FALSE
      )
    )
  
  nonboston <- df |>
    dplyr::filter(.data[[muni_id_col]] != '035') |>
    dplyr::mutate(
      units_valid = dplyr::case_when(
        # 4-8 Unit
        .data[[luc_col]] == '111' & !dplyr::between(.data[[col]], 4, 8) ~
          FALSE,
        # 112: >8 Unit
        .data[[luc_col]] == '112' & .data[[col]] <= 8 ~
          FALSE
      )
    )
  
  nonboston |>
    dplyr::bind_rows(boston) |>
    dplyr::mutate(
      units_valid = dplyr::case_when(
        condo & .data[[col]] != 1 & res ~
          FALSE,
        res & .data[[col]] == 0 ~
          FALSE,
        .default = TRUE
      )
    )
}

std_estimate_units <- function(df, col, luc_col, muni_id_col, count_col, addresses, est_size=900) {
  if ("sf" %in% class(addresses)) {
    addresses <- addresses |>
      sf::st_drop_geometry()
  }
  
  addresses <- addresses |>
    dplyr::group_by(loc_id) |>
    dplyr::mutate(
      all_addr = sum(.data[[count_col]], na.rm=TRUE)
    ) |>
    dplyr::ungroup()
  
  df <- df |>
    dplyr::left_join(
      addresses |>
        dplyr::select(c(loc_id, body, even, muni, postal, dplyr::all_of(count_col))),
      by = dplyr::join_by(loc_id, body, even, muni, postal)
    ) |>
    dplyr::left_join(
      addresses |>
        dplyr::select(c(loc_id, all_addr)),
      by = dplyr::join_by(loc_id),
      multiple = "any"
    ) |>
    dplyr::mutate(
      units_by_area = ceiling(res_area / est_size)
      )
  
  boston <- df |>
    dplyr::filter(.data[[muni_id_col]] == '035') |>
    dplyr::mutate(
      !!col := dplyr::case_when(
        .data[[luc_col]] == '111' & dplyr::between(.data[[count_col]] - 1, 4, 6) ~
          .data[[count_col]] - 1,
        .data[[luc_col]] == '111' & dplyr::between(units_by_area, 4, 6) ~
          units_by_area,
        .data[[luc_col]] == '111' ~
          4,
        .data[[luc_col]] == '112' & dplyr::between(.data[[count_col]] - 1, 7, 30) ~
          .data[[count_col]] - 1,
        .data[[luc_col]] == '112' & dplyr::between(units_by_area, 7, 30) ~
          units_by_area,
        .data[[luc_col]] == '112' ~
          7,
        .data[[luc_col]] == '113' & dplyr::between(.data[[count_col]] - 1, 31, 99) ~
          .data[[count_col]] - 1,
        .data[[luc_col]] == '113' & dplyr::between(units_by_area, 31, 99)  ~
          units_by_area,
        .data[[luc_col]] == '113' ~
          31,
        .data[[luc_col]] == '114' & .data[[count_col]] - 1 >= 100 ~
          .data[[count_col]] - 1,
        .data[[luc_col]] == '114' & units_by_area >= 100 ~
          units_by_area,
        .data[[luc_col]] == '114' ~
          100,
        .default = .data[[col]]
      )
    )
  
  nonboston <- df |>
    dplyr::filter(.data[[muni_id_col]] != '035') |>
    dplyr::mutate(
      !!col := dplyr::case_when(
        # 4-8 Unit
        # ===
        .data[[luc_col]] == '111' & dplyr::between(.data[[count_col]], 4, 8) ~
          .data[[count_col]] - 1,
        .data[[luc_col]] == '111' & dplyr::between(units_by_area, 4, 8) ~
          units_by_area,
        .data[[luc_col]] == '111' ~
          4,
        # 112: >8 Unit
        # ===
        .data[[luc_col]] == '112' & .data[[count_col]] > 8 ~
          .data[[count_col]] - 1,
        .data[[luc_col]] == '112' & units_by_area > 8 ~
          units_by_area,
        .data[[luc_col]] == '112' ~
          9,
        .default = .data[[col]]
      ),
    )
  
  nonboston |>
    dplyr::bind_rows(boston) |>
    dplyr::group_by(loc_id) |>
    dplyr::mutate(
      total_units = sum(.data[[col]], na.rm=TRUE),
      total_missing = sum(.data[[col]] == 0, na.rm=TRUE)
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      !!col := dplyr::case_when(
        res & .data[[col]] == 0  & !is.na(.data[[count_col]]) ~
          .data[[count_col]],
        res & .data[[col]] == 0 & !is.na(all_addr) & (((all_addr - total_units - 1) / total_missing) >= 1) ~
          (all_addr - total_units - 1) / total_missing,
        res & .data[[col]] == 0 & units_by_area > 0 ~
          units_by_area,
        .default = .data[[col]]
      )
    ) |>
    dplyr::select(-c(units_by_area, addr_count, all_addr, total_units, total_missing))
}

# Address Refinement/Completion ====

std_extract_address_vector <- function(vector, start = FALSE) {
  string <- stringr::str_c(
    "(ZERO|ONE|TWO|THREE|FOUR|FIVE|([0-9]+[A-Z]{0,1}([ -][0-9]+[A-Z]{0,1})?)) (([A-Z]{2,}|[0-9]+(TH|ST|RD|ND))\\s){1,3}",
    "((",
    stringr::str_c(SEARCH$street_types, collapse="|"),
    ")( |$))+"
  )
  if (start) {
    string <- stringr::str_c("^", string, "(?=[ -]|$)")
  } else {
    string <- stringr::str_c("(?<=^|[ -])", string, "$")
  }
  stringr::str_extract(
    vector, 
    string
  )
}

std_extract_address <- function(df, col, target_col) {
  new_col <- paste0(col, "_addr")
  df |>
    dplyr::mutate(
      !!new_col := std_extract_address_vector(.data[[col]]),
      !!target_col := dplyr::case_when(
        !is.na(.data[[new_col]]) ~ .data[[new_col]],
        .default = .data[[target_col]]
      ),
      !!col := dplyr::case_when(
        .data[[new_col]] == .data[[col]] ~ NA_character_,
        !is.na(.data[[new_col]]) ~ stringr::str_remove(.data[[col]], paste0(.data[[new_col]], "$")),
        .default = .data[[col]]
      )
    ) |>
    dplyr::select(-dplyr::all_of(new_col))
}

std_assemble_addr <- function(df, range = TRUE) {
  if (!("po" %in% names(df))) {
    df <- dplyr::mutate(df, po = NA_character_)
  }
  if (!("pmb" %in% names(df))) {
    df <- dplyr::mutate(df, pmb = NA_character_)
  }
  if (range) {
    df |>
      dplyr::mutate(
        addr = dplyr::case_when(
          !is.na(body) & start == end ~ stringr::str_c(start, " ", body),
          !is.na(body) & start < end ~ stringr::str_c(start, "-", end, " ", body),
          is.na(body) & !is.na(po) ~ stringr::str_c("PO BOX ", po),
          is.na(body) & is.na(po) & !is.na(pmb) ~ stringr::str_c("PMB ", pmb),
          .default = NA_character_
        )
      )
  } else {
    df |>
      dplyr::mutate(
        addr = dplyr::case_when(
          (!is.na(addr) | addr == "") & !is.na(po) ~ stringr::str_c("PO BOX ", po),
          (!is.na(addr) | addr == "") & is.na(po) & !is.na(pmb) ~ stringr::str_c("PMB ", pmb),
          .default = addr
        )
      )
  }
}

std_leading_zero_vector <- function(x) {
  stringr::str_remove_all(x, "(?<=^|\\-)0+(?=[1-9]|[A-Z]|0$)")
}

std_addr2_floor <- function(df, cols) {
  std_addr2_parser(df, cols, "[ \\-]([0-9]+[A-Z]{2}|[0-9\\.]|TOP|GROUND)[ \\-]FLOOR|FLOOR[ \\-][0-9]+[ \\-]?", from_end=FALSE)
}

std_addr2_twr <- function(df, cols) {
  std_addr2_parser(df, cols, "[ \\-]TOWER[ \\-][0-9\\.]+[ \\-]?", from_end=FALSE)
}

std_addr2_bldg <- function(df, cols) {
  std_addr2_parser(df, cols, "[ \\-]BUILDING[ \\-][0-9\\.]+[ \\-]?", from_end=FALSE)
}

std_addr2_and <- function(df, cols) {
  std_addr2_parser(df, cols,"[\\- ][0-9\\.]+[A-Z]? AND [0-9\\.]+[A-Z]?$")
}

std_addr2_alpha_num <- function(df, cols) {
  std_addr2_parser(df, cols, "[\\- ][0-9]*[A-Z]{1,2}[\\- ]?[0-9\\.]+([\\- ]?([0-9\\.]|[A-Z]))?$")
}

std_addr2_num_alpha <- function(df, cols) {
  std_addr2_parser(df, cols, "[\\- ][A-Z]{0,2}[0-9\\.]+[\\- ]?([A-Z]{1,2}|PH|ABC)(([\\- ]?[0-9\\.]|[\\- ][A-Z]))?$")
}

std_addr2_all_num <- function(df, cols) {
  std_addr2_parser(df, cols, "[\\- ]([0-9\\-\\. ]+)?[0-9]+$")
}

std_addr2_alpha <- function(df, cols) {
  std_addr2_parser(df, cols, "[ \\-]((PH)?[A-Z])$")
}

std_addr2_words <- function(df, cols) {
  std_addr2_parser(df, cols, "[ \\-](BASEMENT|PH|REAR|FRONT)$")
}

std_addr2_parser <- function(df, cols, regex, from_end = TRUE) {
  if(from_end) {
    end_string <- "[ \\-]?$"
  } else {
    end_string <- ""
  }
  if (!all(stringr::str_c(cols, "addr2", sep="_") %in% names(df))) {
    df <- df |> 
      dplyr::mutate(
        dplyr::across(
          dplyr::all_of(cols),
          list(
            addr2 = ~ NA_character_
          ),
          .names = "{.col}_{.fn}"
        )
      )
  }
  df |>
    dplyr::mutate(
      dplyr::across(
        dplyr::all_of(cols),
        list(
          temp = ~ stringr::str_extract(.x, regex)
        ),
        .names = "{.col}_{.fn}"
      ),
      dplyr::across(
        dplyr::all_of(cols),
        ~ dplyr::case_when(
          !is.na(get(paste0(dplyr::cur_column(), "_temp"))) ~ 
            stringr::str_remove(
              .x, 
              stringr::str_c(get(paste0(dplyr::cur_column(), "_temp")), end_string)
            ),
          .default = .x
        )
      ),
      dplyr::across(
        dplyr::all_of(stringr::str_c(cols, "_temp")),
        ~ dplyr::case_when(
          !is.na(.x) ~ std_leading_zero_vector(stringr::str_squish(.x)),
          .default = .x
        )
      ),
      dplyr::across(
        dplyr::all_of(stringr::str_c(cols, "_addr2")),
        ~ dplyr::case_when(
          !is.na(.x) & 
            !is.na(get(stringr::str_replace(dplyr::cur_column(), "_addr2", "_temp"))) 
          ~ stringr::str_c(get(stringr::str_replace(dplyr::cur_column(), "_addr2", "_temp")),
            .x,
            sep=" "
          ),
          is.na(.x) & 
            !is.na(get(stringr::str_replace(dplyr::cur_column(), "_addr2", "_temp"))) 
          ~ get(stringr::str_replace(dplyr::cur_column(), "_addr2", "_temp")),
          !is.na(.x) & 
            is.na(get(stringr::str_replace(dplyr::cur_column(), "_addr2", "_temp"))) 
          ~ .x,
          .default = NA_character_
        )
      )
    )  |>
    std_squish(stringr::str_c(cols, "_addr2")) |>
    dplyr::select(-stringr::str_c(cols, "_temp")) |>
    std_replace_blank(cols)
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
        dplyr::all_of(cols),
        list(
          po = ~ stringr::str_extract(., "(?<=PO BOX ?)[A-Z0-9\\-]+( [0-9]+)?"),
          pmb = ~ stringr::str_extract(., "(?<= PMB ?)[A-Z0-9\\-]+( [0-9]+)?")
        ),
        .names = "{.col}_{.fn}"
      ),
      dplyr::across(
        dplyr::all_of(cols),
        ~ stringr::str_remove_all(., ",?(PO BOX|PMB) ?[A-Z0-9\\-]+( [0-9]+)?")
      )
    ) |>
    std_replace_blank(cols) |>
    std_squish(cols)
}

std_addr2_remove_keywords <- function(df, cols, start = FALSE) {
  if (start) {
    start_string <- "^ ?"
  } else {
    start_string <- "[\\,\\-\\ ]" 
  }
  
  terms <- c(
    "UNITS?",
    "UN",
    "S(UI)?TE(S)?",
    "AP(AR)?T(MENT)?",
    "R(OO)?M",
    "PS( AREA)?"
  )
  terms <- stringr::str_c(start_string, terms, "[\\,\\-\\ 0-9]") |>
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
  street <- SEARCH$street_types |>
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
        dplyr::all_of(cols),
        ~ stringr::str_replace(
          ., 
          "(?<=^[0-9]{1,6}[A-Z]{0,2}) (?=[0-9]{1,6}[A-Z]{0,1} )", 
          "-"
          )
        ),
      
      dplyr::across(
        dplyr::all_of(cols),
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
        dplyr::all_of(cols),
        ~ stringr::str_replace_all(., "(?<=[0-9]) 1\\/2", ".5")
      ),
      dplyr::across(
        dplyr::all_of(cols),
        ~ stringr::str_replace_all(., "\\/", " ")
      )
    )
}

std_fill_state_by_zip <- function(df, col, postal_col, zips) {
  df |>
    dplyr::filter(
      is.na(.data[[col]]) & !is.na(.data[[postal_col]])
    ) |>
    dplyr::left_join(
      zips |>
        sf::st_drop_geometry() |>
        dplyr::filter(!is.na(unambig_state)) |>
        dplyr::select(zip, zip_state = unambig_state)|>
        dplyr::distinct(), 
      by = dplyr::join_by(
        !!postal_col == zip
      )
    )|>
    dplyr::mutate(
      !!col := dplyr::case_when(
        !is.na(zip_state) ~ zip_state,
        .default = .data[[col]]
      )
    ) |>
    dplyr::select(-c(zip_state)) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(
          !(is.na(.data[[col]]) & !is.na(.data[[postal_col]]))
        )
    )
}

std_fill_muni_by_zip <- function(df, col, postal_col, state_col, zips) {
  df <- df |>
    dplyr::filter(
      is.na(.data[[col]]) & !is.na(.data[[postal_col]])
    ) |>
    dplyr::left_join(
      zips |>
        sf::st_drop_geometry() |>
        dplyr::filter(!is.na(muni_unambig_from) & (unambig_state == "MA")) |>
        dplyr::select(muni_unambig_from, zip, state) |>
        dplyr::distinct(),
      by = dplyr::join_by(
        !!postal_col == zip,
        !!state_col == state
      )
    ) |>
    dplyr::mutate(
      !!col := dplyr::case_when(
        !is.na(muni_unambig_from) ~ muni_unambig_from,
        .default = .data[[col]]
      )
    ) |>
    dplyr::select(-muni_unambig_from) |>
    dplyr::bind_rows(
      df |> 
        dplyr::filter(
          !(is.na(.data[[col]]) & !is.na(.data[[postal_col]]))
        )
    )
}

std_fill_zip_by_muni <- function(df, col, muni_col, zips) {
  df <- df |>
    dplyr::filter(
      is.na(.data[[col]]) & !is.na(.data[[muni_col]])
      ) |>
    dplyr::left_join(
      zips |>
        sf::st_drop_geometry() |>
        dplyr::filter(!is.na(muni_unambig_to)) |>
        dplyr::select(muni_unambig_to, zip) |>
        dplyr::distinct(),
      by = dplyr::join_by(
        !!muni_col == muni_unambig_to
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
          !(is.na(.data[[col]]) & !is.na(.data[[muni_col]]))
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
        dplyr::select(pl_name = name, zip, replace = muni),
      by = dplyr::join_by(!!col == pl_name, !!postal_col == zip),
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
  
  places <- places |>
    dplyr::group_by(name) |>
    dplyr::filter(dplyr::n() == 1) |>
    dplyr::ungroup()
  
  state <- state |>
    dplyr::filter(!match & !is.na(.data[[col]])) |>
    dplyr::left_join(
      places |>
        dplyr::select(pl_name = name, zip, replace = muni),
      by = dplyr::join_by(!!col == pl_name, !!postal_col == zip),
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

std_multiname <- function(df, col) {
  df <- df |>
    dplyr::mutate(
      name_and = dplyr::case_when(
        stringr::str_detect(.data[[col]], " AND ") ~ TRUE,
        .default = FALSE
      )
    ) |>
    tibble::rowid_to_column("temp_id")
  
  df |>
    dplyr::filter(
      name_and & !trust & !inst
    ) |>
    std_separate_and_label(
      col = col,
      target_col = col,
      regex = " AND ",
      label="and"
    ) |>
    std_replace_blank(col) |>
    dplyr::mutate(
      last = stringr::str_extract(.data[[col]], "^[A-Z]{2,}(?= [A-Z]{2,20} [A-Z]$)")
    ) |>
    dplyr::group_by(temp_id) |>
    tidyr::fill(last, .direction="updown") |>
    dplyr::ungroup() |>
    std_remove_middle_initial(col, restrictive = FALSE) |>
    dplyr::mutate(
      !!col := dplyr::case_when(
        stringr::str_count(.data[[col]], "([A-Z]{2,}\\s)") == 0 & !is.na(last) ~ stringr::str_c(.data[[col]], last, sep=" "),
        .default = .data[[col]]
      )
    ) |>
    dplyr::filter(
      stringr::str_count(.data[[col]], "([A-Z]{2,}\\s)") > 0) |>
    dplyr::select(-last) |>
    dplyr::bind_rows(
    df |>
      dplyr::filter(!name_and | trust | inst)
    ) |>
    dplyr::select(-c(name_and, temp_id))
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

std_separate_and_label <- function(df, 
                                   col, 
                                   regex, 
                                   label = "",
                                   target_col = "name",
                                   clear_cols = c(),
                                   retain = TRUE) {
  regex = stringr::regex(regex)
  df <- df |>
    tibble::rowid_to_column("t_id") |>
    dplyr::mutate(
      flag = dplyr::case_when(
        stringr::str_detect(.data[[col]], regex) ~ TRUE,
        .default = FALSE
      )
    )
  
  match <- df |>
    dplyr::filter(flag) |>
    tidyr::separate_longer_delim(
      tidyselect::all_of(col),
      regex
    ) |>
    dplyr::mutate(
      !!col := dplyr::na_if(.data[[col]], "")
    ) |>
    dplyr::group_by(dplyr::across(-dplyr::all_of(col))) |>
    dplyr::mutate(
      row = dplyr::row_number(),
      count = dplyr::n()
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      type = dplyr::case_when(
        nchar(label) == 0 ~ type,
        row > 1 | row == count ~ label,
        .default = type
      )
    )

  if(target_col != col) {
    match <- match |>
      dplyr::mutate(
        !!target_col := dplyr::case_when(
          type == label ~ .data[[col]],
          .default = .data[[target_col]]
        ),
        !!col := dplyr::case_when(
          type == label ~ NA_character_,
          .default = .data[[col]]
        )
      )
  } else {
    match <- match |>
      dplyr::group_by(t_id) |>
      dplyr::arrange(.data[[col]]) |>
      dplyr::mutate(
        rm_test = sum(!is.na(.data[[col]])) == 1,
        type = dplyr::first(type)
      ) |>
      dplyr::ungroup() |>
      dplyr::filter(!(rm_test & is.na(.data[[col]]))) |>
      dplyr::select(-rm_test)
  }
  
  match <- match |> 
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(clear_cols) & tidyselect::where(is.character),
        ~ dplyr::case_when(
          type != label ~
            NA_character_,
          .default = .x
        )
      )
    ) 
  
  if (!retain) {
    match <- match |>
      dplyr::filter(type != label)
  }
  
  match |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(!flag)
    ) |>
    dplyr::select(-c(row, count, flag, t_id))
    
}


# Type Flags ====

std_flag_generic <- function(df, col, string, flag_col) {
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

std_flag_agent <- function(df, col, position_col) {
  df |>
    dplyr::mutate(
      agent = dplyr::case_when(
        stringr::str_detect(.data[[col]], "(^C ?T ?CORP)|( REGISTERED A)|( AGENTS?)|(CORPORAT(E|ION) SERVICE)|(INC(ORP(ORATING)?)? SERVICES)|(BUSINESS FILL?INGS)|(CORPORATION COMPANY)|(PRENTICE[-\\s]?HALL CORP)|(COGENCY GLOB)") ~ TRUE,
        stringr::str_detect(.data[[position_col]], "AGENT|SIGNATORY") ~ TRUE,
        stringr::str_detect(.data[[col]], "\\b(LAW|ATTORNEY|LLP|ESQ(UIRE)?)\\b") ~ TRUE,
        .default = FALSE
      )
    )
}

std_flag_manager <- function(df, col) {
  df |>
    dplyr::mutate(
      manager = dplyr::case_when(
        stringr::str_detect(.data[[col]], "MANAGE(R|MENT)") ~ TRUE,
        stringr::str_detect(.data[[col]], "PROPERT(Y|IES)") ~ TRUE,
        .default = FALSE
      )
    )
}

std_flag_condos <- function(df, luc_col, id_cols) {
  condo <- df |>
    dplyr::filter(.data[[luc_col]] == '102')
  
  df |>
    dplyr::mutate(
      condo = dplyr::case_when(
        id %in% condo$id ~ TRUE,
        .default = NA
      )
    ) |>
    dplyr::group_by(
      dplyr::across(
        dplyr::all_of(id_cols)
      )
    ) |>
    tidyr::fill(condo, .direction = "downup") |>
    dplyr::ungroup() |>
    tidyr::replace_na(list(condo = FALSE))
}


std_flag_inst <- function(df, col) {
  #' Flags institutional names in provided column using terms defined in global
  #' SEARCH list.
  #' @param df A dataframe.
  #' @param col The column to be flagged.
  #' @returns A dataframe with added columns 'trust_{col}' and 'trustee_{col}'.
  #' @export
  df |>
    dplyr::mutate(
      inst := dplyr::case_when(
        stringr::str_detect(
        .data[[col]],
        stringr::str_c(
          "\\b(",
          stringr::str_c(SEARCH$inst, collapse = "|"),
          ")\\b",
          sep = "")
        ) ~ TRUE,
        .default = FALSE
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
  
  df |>
    dplyr::mutate(
      trust = dplyr::case_when(
        stringr::str_detect(.data[[col]], "TRUST(?!EES)") ~ TRUE,
        stringr::str_detect(.data[[col]], "^TRUSTEES OF ")
          & !stringr::str_detect(.data[[col]], "UNIVERSITY|COLLEGE|INSTITUTE") ~ TRUE,
        stringr::str_detect(.data[[col]], 
          stringr::str_c(
            "\\b(",
            stringr::str_c(SEARCH$trust_definite, collapse = "|"),
            ")\\b",
            sep = "")
        ) ~ TRUE,
        .default = FALSE
      ),
      trustees = dplyr::case_when(
        stringr::str_detect(.data[[col]], "TRUSTEES") 
        & !stringr::str_detect(.data[[col]], "UNIVERSITY|COLLEGE|INSTITUTE") & !trust ~ TRUE,
        .default = FALSE
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

std_flag_lawyers <- function(df, cols) {
  #' Flags likely law offices and lawyers.
  #'
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::mutate(
      lawyer = dplyr::case_when(
        dplyr::if_any(
          tidyselect::any_of(cols),
          ~stringr::str_detect(
            .,
            "( (PC)|([A-Z]+ AND [A-Z]+ LLP)|(ESQ(UIRE)?$)|(LAW (OFFICES?|LLC|LLP|GROUP))|(ATTORNEY))"
          )
        ) ~ TRUE,
        TRUE ~ FALSE
      )
    )
}

# Spatial Joiners ====

std_fill_ma_zip_sp <- function(df, col, site_loc_id, site_muni_id, parcels_point, zips) {
  ma_zips <- zips |>
    dplyr::filter(state == "MA") |>
    dplyr::group_by(zip) |>
    dplyr::filter(dplyr::row_number() == 1) |>
    dplyr::ungroup()
  
  df <- df |> 
    dplyr::left_join(
      parcels_point,
      by=dplyr::join_by(
        !!site_loc_id == loc_id,
        !!site_muni_id == muni_id
        ),
      na_matches="never",
      multiple="any"
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

std_calculate_overlap <- function(x, y, thresh = 0) {
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

std_alphabetize_name <- function(df, col) {
  new_col <- stringr::str_c(col, "_alpha")
  df |>
    dplyr::mutate(
      !!new_col := .data[[col]]
    ) |>
    tibble::rowid_to_column("row_id") |>
    tidyr::separate_longer_delim(
      .data[[new_col]],
      delim = " "
    ) |>
    dplyr::group_by(row_id) |>
    dplyr::arrange(.data[[new_col]], .by_group = TRUE) |>
    dplyr::mutate(!!new_col := paste(.data[[new_col]], collapse = " ")) |>
    dplyr::slice(1) |>
    dplyr::ungroup() |>
    dplyr::select(-row_id)
}

std_remove_middle_initial <- function(df, cols, restrictive=TRUE) {
  #' Replace middle initial when formatted like "ERIC R HUNTLEY"
  #' 
  #' @param df A dataframe.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  if (restrictive) {
    string <- "(?<=[A-Z] )[A-Z] (?=[A-Z])"
  } else {
    string <- "(?<= |^)[A-Z](?= |$)"
  }
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ stringr::str_squish(stringr::str_replace_all(., string, ""))
      )
    )
}

std_match_address_to_address <- function(a1, a2, fill_cols, ...) {
  a1 |>
    dplyr::left_join(
      a2 |>
        dplyr::mutate(
          dplyr::across(
            dplyr::all_of(fill_cols),
            list(
              replace = ~ .
            )
          )
        ) |> 
        dplyr::select(
          c(
            dplyr::all_of(stringr::str_c(fill_cols, "_replace")),
            start_y = start,
            end_y = end,
            ...
          )
        ),
      by = dplyr::join_by(
        ...,
        dplyr::within(start, end, start_y, end_y)
      ),
      na_matches = "never",
      multiple = "first"
    ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::all_of(fill_cols),
        ~ dplyr::case_when(
          !is.na(get(paste0(dplyr::cur_column(), "_replace"))) ~ get(paste0(dplyr::cur_column(), "_replace")),
          .default = .x
        )
      )
    ) |>
    dplyr::select(-dplyr::all_of(stringr::str_c(fill_cols, "_replace")), -c(start_y, end_y))
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


