filings <- load_filings()
dockets <- load_dockets()
ptfs <- load_ptfs()

vacated_judgments <- dockets |>
  dplyr::filter(
    stringr::str_detect(
      text, 
      stringr::regex("vacated", ignore_case = TRUE)
    )
  )

extended_dockets <- dockets |>
  # extend dockets with row number and total case lengths
  dplyr::arrange(desc(date)) |>
  dplyr::group_by(docket_id) |>
  dplyr::mutate(
    row = dplyr::row_number(), 
    case_length = as.integer(
      difftime(
        dplyr::first(date), 
        dplyr::last(date), 
        units="days")
    )
  )

closed_filings <- filings |> 
  std_uppercase() |>
  sf::st_drop_geometry() |>
  dplyr::filter(case_status == "CLOSED",
                !(docket_id %in% vacated_judgments$docket_id)
  ) |> 
  dplyr::select(-c("geocoder", "file_date", "close_date", "street")) |>
  dplyr::left_join(extended_dockets, by="docket_id")


tagged_closed_filings <- closed_filings |>
  std_uppercase() |>
  dplyr::mutate(
    row = dplyr::row_number(),
    dismissal = stringr::str_detect(text, "CASE DISMISSED|VOLUNTARY DISMISSAL|RESULT: DISMISSED|STIPULATION OF DISMISSAL|STIPULATION WITH DISMISSAL|JUDGMENT OF DISMISSAL"),
    default_judgment = stringr::str_detect(text, "JUDGMENT IN SP BY DEFAULT|JUDGMENT BY DEFAULT|JUDGMENT ISSUED: DEFAULT"),
    judgment = stringr::str_detect(text, "JUDGMENT ISSUED: FINAL"),
    agreement = stringr::str_detect(text, "AGREEMENT FOR JUDGMENT"),
    execution = stringr::str_detect(text, "EXECUTION ISSUED|EXECUTION FOR POSSESSION|Constable or Sheriff's 48hr Notice Filed|Physical Eviction Inventory filed by Sheriff/Constable"),
    execution_returned = stringr::str_detect(text, "RETURN OF EXECUTION")
  ) |> dplyr::filter(dismissal | default_judgment | judgment | agreement | execution | execution_returned) |>
  dplyr::mutate(
    dismissal = ifelse(dismissal, row, 0),
    default_judgment = ifelse(default_judgment, row, 0),
    judgment = ifelse(judgment, row, 0),
    agreement = ifelse(agreement, row, 0),
    execution = ifelse(execution, row, 0),
    execution_returned = ifelse(execution_returned, row, 0)
  ) |>
  dplyr::mutate(likely_result = "") |>
  dplyr::group_by(docket_id) |>
  dplyr::arrange(desc(date)) |>
  dplyr::select(-c(row))


dockets_with_results <- tagged_closed_filings |> 
  #' complicated set of logic for figuring out what result to count
  #' there's probably a much simpler way of doing this.
  dplyr::mutate(
    likely_result = dplyr::case_when(
      
      judgment == 1 ~ "judged", 
      default_judgment == 1 ~ "judged_default",
      agreement == 1 ~ "agreement",
      
      # if only judgment, or default_judgment, or agreement_row, or dismissed_row then you know what to do
      judgment & !default_judgment & !dismissal & !agreement ~ "judged",
      default_judgment & !judgment & !dismissal & !agreement ~ "judged_default",
      agreement &  !judgment & !dismissal & !default_judgment ~ "agreement",
      dismissal & !default_judgment & !judgment & !agreement ~ "dismissed",
      
      # if agreement happens last, then agreement
      !default_judgment & !dismissal & agreement < judgment ~ "agreement",
      !judgment & !dismissal & agreement < default_judgment ~ "agreement",
      
      # if agreement and judgment rows are the same, then agreement
      agreement == judgment & agreement > 0 & !dismissal & !execution & !execution_returned ~ "agreement",
      
      # if judgment is latest following an agreement, then judgment
      judgment > 0 & judgment < agreement & !default_judgment & !dismissal & !execution ~ "judged",
      default_judgment > 0 & default_judgment < agreement & !judgment & !dismissal & !execution ~ "judged_default",
      
      
      # if judgments or agreements happened most recently, not dismissed -- judged or agreed
      # either value (e.g. dismissal) exists and judgment/agreement is still more recent, or value doesn't exist
      judgment > 0 & 
        (judgment < min(na.omit(dplyr::na_if(c(default_judgment, agreement, dismissal), 0)))) ~ "judged",
      default_judgment > 0 & 
        (default_judgment < min(na.omit(dplyr::na_if(c(judgment, agreement, dismissal), 0)))) ~ "judged_default",
      agreement > 0 & 
        (agreement < min(na.omit(dplyr::na_if(c(default_judgment, judgment, dismissal), 0)))) ~ "agreement",

      # if nothing but execution, consider that a judgment
      execution > 0 & !dismissal & !default_judgment & !judgment & !agreement ~ "judged", 
      # if execution happens MORE RECENTLY THAN dismissal, while everything else is empty, still judged (rare, 1 or 2 cases)
      execution > 0 & execution < dismissal & !default_judgment & !judgment & !agreement ~ "judged",
      
      agreement > 0 & agreement < judgment & judgment < default_judgment & !dismissal ~ "agreement",
      agreement > 0 & agreement < default_judgment & default_judgment < judgment & !dismissal ~ "agreement",
      
      # if dismissed in the end, but first judged and executed, that's a judgment
      dismissal > 0 & min(execution, judgment) > dismissal &
        (judgment < min(na.omit(dplyr::na_if(c(agreement, default_judgment), 0))) | (!agreement & !default_judgment)) ~ "judged",
      
      # same thing for default judgment rows
      default_judgment > 0 & min(dismissal, execution) > default_judgment &
        (default_judgment < min(na.omit(dplyr::na_if(c(agreement, judgment), 0))) | (!agreement & !judgment)) ~ "judged_default",
      
      # same thing for agreement
      agreement > 0 & min(dismissal, execution) > agreement &
        (agreement < min(na.omit(dplyr::na_if(c(default_judgment, judgment), 0))) | (!default_judgment & !judgment)) ~ "agreement"
    )
  ) 

find_winner <- function(df) {
  #' Identify winner in judgments.
  #' @returns A `tibble()`.
  #' @export
  # ptfs <- load_ptfs()
  # defs <- load_defs()
  
  df |>
    dplyr::left_join(
      dplyr::rename(
        ptfs, 
        ptf_name = name,
        ptf_dismissed = dismissed
      ), 
      by = 'docket_id', 
      relationship = "many-to-many"
    ) |>
    dplyr::left_join(
      dplyr::rename(
        defs,
        def_name = name,
        def_dismissed = dismissed
      ), 
      by = 'docket_id', 
      relationship = "many-to-many"
    ) |>
    dplyr::rowwise() |>
    dplyr::mutate(
      for_def = (def_name %in% j_for) | (ptf_name %in% j_against),
      for_ptf = (ptf_name %in% j_for) | (def_name %in% j_against),
      winner = dplyr::case_when(
        (for_def & !for_ptf) ~ "def",
        (!for_def & for_ptf) ~ "ptf"
      )
    )
}

std_winners <- function(df) {
  # get one winner (if there is consensus) per docket
  df |>
    dplyr::group_by(docket_id) |>
    dplyr::summarise(winner = if (length(unique(list(na.omit(winner)))) == 1) first(na.omit(winner)) else list(na.omit(winner)))
}

process_dismissals <- function(df) {
  df |>
    std_uppercase() |>
    dplyr::group_by(docket_id) |> 
    dplyr::arrange(date) |>
    slice(tail(dplyr::row_number(), 1)) |>
    dplyr::mutate(
      case_dismissed = grepl("CASE DISMISSED|VOLUNTARY DISMISSAL|RESULT: DISMISSED", text, ignore.case=TRUE)
    ) 
}

process_judgments <- function(df) {
  #' THIS TAKES A VERY LONG TIME TO RUN (~15 minutes)
  df |>
    std_uppercase() |>
    # Flag judgments.
    dplyr::mutate(
      judgment = flag_text(text, 'JUDGMENT ISSUED')
    ) |>
    dplyr::mutate(
      text = parse_jdg_replacetabs(text),
      text = stringr::str_squish(text),
      date = parse_jdg_date(text, "JDGMNT DATE:"),
      damage = parse_jdg_number(text, "DAMAGE AMT:"),
      filing_fees = parse_jdg_number(text, "FILING FEES:"),
      court_costs = parse_jdg_number(text, "COSTS PD TO COURT:"),
      other_costs = parse_jdg_number(text, "OTHER COSTS:"),
      punitive = parse_jdg_number(text, "PUNITIVE DAMAGES:"),
      atty_fee = parse_jdg_number(text, "CRT ORD ATTY FEE:"),
      total = parse_jdg_number(text, "JUDGMENT TOTAL:"),
      int_start = parse_jdg_date(text, "INTEREST BEGINS:"),
      int_rate = parse_jdg_number(text, "INTEREST RATE:"),
      int_rate_d = parse_jdg_number(text, "DAILY INTEREST RATE:"),
      presiding = parse_jdg_str(text, "PRESIDING:"),
      j_for = parse_jdg_list(text, "JUDGMENT FOR:"),
      j_against = parse_jdg_list(text, "JUDGMENT AGAINST:"),
      further_orders = parse_jdg_str(text, "FURTHER ORDERS:", end_pipe = FALSE),
      judg_info = parse_jdg_str(
        text, 
        "JUDGMENT ISSUED:",
        end_word = "PRESIDING:"
      )
    ) |>
    dplyr::mutate(date=as.Date(date, '%m/%d/%Y')) |>
    tidyr::separate_wider_delim(
      judg_info,
      delim = stringr::regex("\\s\\|\\s"),
      names = c("type", "method"),
      cols_remove = TRUE,
      too_many = "drop",
      too_few = "align_start"
    ) |>
    std_jdg_type('type') |>
    std_jdg_method('method') |>
    dplyr::mutate(
      npt = flag_text(further_orders, "[NUNC]{3,4} PRO [TUNC]{3,4}"),
      error = flag_text(further_orders, '(ISSUED\\sIN\\s)?ERROR'),
      amend = flag_text(further_orders, '(AMENDED|CORRECTED) ([A-Z]+ )?JU?DGE?ME?NT'),
      amend = dplyr::case_when(
        (npt | amend) ~ TRUE,
        TRUE ~ FALSE
      ),
      npt_date =  parse_jdg_date(
        further_orders,
        "[NUNC]{3,4} PRO [TUNC]{3,4}\\)?\\s?(?:(?:TO(?: AGREEMENT)?|(?:A|E)FFECTIVE))(\\s|\\()?"
      ),
      stay = flag_text(further_orders, '(STAY)|(SHALL ISSUE ON)'),
      stay_date =  parse_jdg_date(further_orders, "STAYED\\s(?:(?:UNTIL)|(?:TO)|(?:THROUGH))")
    )
}

get_judgments_by_parties <- function(df) {
  #' Getting judgments we can be sure about
  #' Grouped by unique parties. 
  #' Removing any judgments where the number of defendants or plaintiffs 
  #' is inconsistent. This is because it's difficult to draw conclusions about 
  #' the final outcome for inconsistent parties. 
  #' For the judgments that are left, we return the last judgment.
  #' This also means that we return all judgments for a docket where there are
  #' equal number of but different parties. 
  #' For instance, for docket_X, both judgments will be return: 
  #' (judgment_A against def_A, def_B; judgment_B against def_C, def_D)
  df |>
    dplyr::arrange(docket_id, dplyr::desc(date)) |>
    dplyr::group_by(docket_id, j_for, j_against) |>
    dplyr::mutate(
      against_count = sum(dplyr::n_distinct(unlist(j_against))),
      for_count = sum(dplyr::n_distinct(unlist(j_for))),
      total = ifelse(is.na(total), 0, total)
    ) |>
    dplyr::group_by(docket_id) |> 
    dplyr::filter(dplyr::n_distinct(against_count) == 1 & dplyr::n_distinct(for_count) == 1) |>
    dplyr::group_by(docket_id, j_for, j_against) |>
    dplyr::filter(dplyr::row_number() == 1)
}

find_party_outliers <- function(df) {
  df |> 
    dplyr::group_by(docket_id) |> 
    dplyr::summarise(
      distinct = dplyr::n_distinct(unlist(j_against)),
      total_party_count = sum(length(unlist(j_against))),
      row_count = dplyr::n()
    ) |> 
    dplyr::filter(
      distinct != total_party_count & 
        row_count > 1 & 
        distinct > 1 &
        (total_party_count / row_count) != 1
    )
}

docket_dismissals <- process_dismissals(dockets)
judgments <- process_judgments(dockets)
judgments_by_party <- get_judgments_by_parties(judgments)
party_outliers <- find_party_outliers(judgments_by_party)

judgments_by_party <- judgments_by_party |>
  dplyr::filter(!docket_id %in% party_outliers$docket_id)

get_judgments_by_docket <- function(df) {
  #' Getting winning party per docket
  #' Returns DF with average arrears per party (`average_per_party`)
  #' ASSUMPTION: to get average arrears per party, 
  #' dividing total by ALL parties, so if there are two winner LLs
  #' total will be divided by two, even if one LL is potentially a PM or a party
  #' that does not receive the money
  
  winners <- df |> 
    find_winner() |> 
    std_winners()
  
  df |>
    dplyr::left_join(winners, by='docket_id') |>
    dplyr::mutate(
      average_per_party = total / (for_count*against_count)
    ) |>
    dplyr::group_by(docket_id) |>
    dplyr::summarise(
      party_count = n(),
      average_per_party = sum(average_per_party)/party_count,
      total = sum(total),
      docket_id = docket_id,
      winner = winner
    ) |>
    dplyr::filter(dplyr::row_number() == 1)
}

docket_judgments <- get_judgments_by_docket(judgments_by_party) |>
  dplyr::filter(!docket_id %in% party_outliers$docket_id) |>
  # adding filings data to judgments
  dplyr::left_join(filings |> sf::st_drop_geometry(), by = 'docket_id') 


dockets_results_and_judgments <- 
  dockets_with_results |> 
    dplyr::left_join(
      docket_judgments |> dplyr::select(docket_id, winner), 
      by="docket_id"
    )

