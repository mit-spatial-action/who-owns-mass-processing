source("R/globals.R")

log_message <- function(status) {
  #' Print message to `logr` logs.
  #'
  #' @param status Status to print.
  #' @returns Nothing.
  #' @export
  time <- format(Sys.time(), "%a %b %d %X %Y")
  message <- stringr::str_c(time, status, sep = ": ")
  logr::log_print(message)
}

subset_town_ids <- function(subset) {
  if (subset == "hns") {
    MA_MUNIS %>%
      dplyr::filter(HNS == 1) %>%
      dplyr::pull(town_id) 
    # %>%
    #   stringr::str_c(collapse = ", ")
  } else if (subset == "test") {
    TEST_MUNIS
  } else if (subset == "all") {
    FALSE
  } else {
    stop("Invalid subset.")
  }
}

write_multi <- function(df, name, formats = c("csv", "rds", "pg")){
  if ("csv" %in% formats) {
      # Write pipe-delimited text file of edges.
      readr::write_delim(
        df,
        file.path(RESULTS_DIR, stringr::str_c(name, "csv", sep = ".")),
        delim = "|", 
        quote = "needed"
      )
  }
  if ("rds" %in% formats) {
    saveRDS(
      df, 
      file = file.path(RESULTS_DIR, stringr::str_c(name, "rds", sep = "."))
    )
  }
  df
}


std_format_name <- function(df, col) {
  #' Standardizes names from LASTNAME, FIRSTNAME to FIRSTNAME LASTNAME
  #' Does not standardize the names of corporate owners (if name includes LP, LLP, LLC, see list below)
  #' Removes DISM(ISSALS) in parentheses and brackets
  df |>
    dplyr::rowwise() |>
    dplyr::mutate(
      temp=list(ifelse(
        stringr::str_detect(get(col)," LP| LLP| LLC| LTD| INC| CORP", negate = TRUE), stringr::str_split(get(col), ", "), get(col)))
    ) |>
    dplyr::mutate(
      !! col := stringr::str_squish(
        stringr::str_replace_all(
          stringr::str_flatten(base::rev(unlist(temp)), collapse = " "),
          "(\\(|\\[|\\s)DISM[ISED]{0,5}(\\s[0-9///]{6,10})?(\\)|\\]|\\s)?`?",
          " "
        )
      )
    ) |> 
    std_remove_special(col) |>
    dplyr::select(-c(temp))
}

std_format_attorney_name <- function(df, col) {
  #' Removes Esq.s 
  #' Standardizes names from LASTNAME, FIRSTNAME to FIRSTNAME LASTNAME
  df |>
    dplyr::rowwise() |>
    dplyr::mutate(
      temp = gsub(" Esq.[,]?", "", get(col))
    ) |>
    dplyr::mutate(
      temp=list(stringr::str_split(temp, ", "))
    ) |>
    dplyr::mutate(
      !! col := stringr::str_squish(
        stringr::str_flatten(base::rev(unlist(temp)), collapse = " ")
      )
    ) |> 
    std_remove_special(col) |>
    dplyr::select(-c(temp))
}

std_remove_special <- function(df, cols) {
  #' Removes all special characters from columns, except slash
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ stringr::str_replace_all(., c(
          "[^[:alnum:][:space:]/-]" = ""
        )
        )
      )
    )
}


std_jdg_type <- function(df, col) {
  df |>
    dplyr::mutate(
      !! col := dplyr::case_when(
        base::get(col) == "By Clerk JdmtSP10" ~ "Default",
        base::get(col) == "By Judge Jdmt55b2" ~ "Default",
        TRUE ~ base::get(col)
      )
    )
}

std_jdg_method <- function(df, col) {
  df |>
    dplyr::mutate(
      !! col := dplyr::case_when(
        base::get(col) == "On Finding" ~ "Finding",
        base::get(col) == "On Verdict" ~ "Verdict",
        TRUE ~ base::get(col)
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
        tidyselect::where(is.character),
        stringr::str_to_upper
      ),
    )
}

std_corp_types <- function(df, cols) {
  #' Standardize street types.
  #'
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ stringr::str_replace_all(., c(
          "LIMITED PART[A-Z]{5,8}" = "LP",
          "LIMITED LIABILITY PART[A-Z]{5,8}" = "LLP",
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


std_the <- function(df, cols) {
  #' Strips away leading or trailing the
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character),
        ~ stringr::str_replace_all(
          .,
          c(
            " THE$" = "",
            "^THE " = "")
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
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character),
        ~ stringr::str_replace_all(., c(
          "[^[:alnum:][:space:]/-]" = ""
        )
        )
      )
    )
}

std_case_type <- function(df, cols = 'case_type') {
  #' Replace Boston neighborhood names with 'Boston''.
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  no_cause <- c("SP Transfer- No Cause")
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ dplyr::case_when(
          . %in% no_cause ~ "No Cause",
          TRUE ~ .
        )
      )
    )
}

std_boston <- function(df, cols = 'city') {
  #' Replace Boston neighborhood names with 'Boston''.
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  neighs <- load_utility_file("bos_neigh.csv") |> 
    std_uppercase() |>
    dplyr::pull(name)
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ dplyr::case_when(
          . %in% neighs ~ "BOSTON",
          TRUE ~ .
        )
      )
    )
}

parse_jdg_number <- function(col, string, ignore_case = FALSE){
  readr::parse_number(
    stringr::str_extract(
      col, 
      stringr::regex(
        stringr::str_c("(?<=", string, "\\s)\\d{0,3}(?:,?\\d{3})*\\.\\d{2,8}"),
        ignore_case = ignore_case
      )
    )
  )
}

parse_jdg_str <- function(col, string, ignore_case = FALSE, end_pipe = TRUE, end_word = FALSE){
  regex <- stringr::str_c("(?<=", string, "\\s)[^\\|\\s]+(?:\\s[^\\|\\s]+)*(?:\\s\\|\\s[^\\|\\s]+(?:\\s[^\\|\\s]+)*)*")
  if (!isFALSE(end_word)) {
    regex <- stringr::str_c(regex, "(?=\\s", end_word, ")")
    end_pipe = FALSE
  }
  if (end_pipe) {
    # Many strings of interest end in two pipes, separated by spaces.
    regex <- stringr::str_c(regex, "(?=\\s\\|\\s\\|)")
  }
  stringr::str_extract(
    col,
    stringr::regex(regex, ignore_case = ignore_case)
  )
}

parse_jdg_list <- function(col, string, ignore_case = FALSE){
  # E.g., "...Llc |  |  Judgment Against: Ruth Savitzky |  Devin Smith |  | Terms..."
  stringr::str_split(
    parse_jdg_str(col, string, ignore_case = ignore_case),
    pattern = "\\s\\|\\s"
  )
}

parse_jdg_date <- function(col, string, ignore_case = FALSE){
  long_date <- stringr::str_c(
    "(?:",
    stringr::str_c(
      c("Jan(?:uary)?", "Feb(?:uary)?", "Mar(?:ch)?", "Apr(?:il)?",
        "May", "June?", "July?", "Aug(?:ust)?",
        "Sep(?:t(?:ember)?)?", "Oct(?:ober)?", "Nov(?:ember)?", "Dec(?:ember)?"),
      collapse = '|'
    ),
    ")\\s[0-9]{1,2},\\s2[0-9]{3}"
  )
  regex <- stringr::regex(
    stringr::str_c(
      "(?<=", 
      string, 
      "\\s)(?:(?:", 
      long_date, 
      ")|(?:\\d{1,2}[\\.\\/\\-]\\d{1,2}[\\.\\/\\-]\\d{2,4}))(?=\\.?)"
    ),
    ignore_case = ignore_case
  )
  stringr::str_extract(
    col, 
    regex
  ) |>
    as.Date('%m/%d/%Y')
}


flag_text <- function(col, string, ignore_case = FALSE) {
  stringr::str_detect(
    col, 
    stringr::regex(string, ignore_case = ignore_case)
  )
}

parse_jdg_replacetabs <- function(col){
  stringr::str_replace(
    col,
    pattern = "\\s{4}",
    replacement = " | "
  )
}


std_case_type <- function(df, cols = 'case_type') {
  #' Replace Boston neighborhood names with 'Boston''.
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  no_cause <- c("SP Transfer- No Cause")
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ dplyr::case_when(
          . %in% no_cause ~ "No Cause",
          TRUE ~ .
        )
      )
    )
}

std_boston <- function(df, cols = 'city') {
  #' Replace Boston neighborhood names with 'Boston''.
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  neighs <- load_utility_file("bos_neigh.csv") |> 
    std_uppercase() |>
    dplyr::pull(name)
  df |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character) & tidyselect::all_of(cols),
        ~ dplyr::case_when(
          . %in% neighs ~ "BOSTON",
          TRUE ~ .
        )
      )
    )
}
