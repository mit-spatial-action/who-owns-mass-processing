source("R/utilities.R")

dedupe_unique_addresses <- function(df, sites, addresses) {
  df <- df |>
    dplyr::select(id, addr, start, end, body, even, muni, postal, state, loc_id)  |>
    flow_address_to_address_seq(sites, addresses) |>
    dplyr::filter(!is.na(addr)) |>
    dplyr::group_by(addr, start, end, body, even, muni, postal, loc_id) |>
    tidyr::fill(state, .direction = "updown") |>
    dplyr::mutate(
      addr_id = stringr::str_c("address-", dplyr::cur_group_id())
    ) |>
    dplyr::ungroup()
  
  list(
    through_table = df |> dplyr::select(id, addr_id) |> dplyr::distinct(),
    addresses = df |> dplyr::select(-id) |> dplyr::rename(id = addr_id) |> dplyr::distinct()
  )
}

dedupe_address_to_id <- function(df, df2) {
  df |>
    dplyr::select(-dplyr::any_of(c("addr", "start", "end", "body", "even", "muni", "postal", "state", "loc_id"))) |>
    dplyr::left_join(
      df2,
      dplyr::join_by(id == id)
    )
}

dedupe_naive <- function(df, str_field, prefix="naive") {
  #' Naive deduplication function, based on duplicate names and addresses.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param str_field The column used for deduplication.
  #' @returns A dataframe.
  #' @export
  distinct <- df |>
    dplyr::group_by(
      dplyr::across({{ str_field }})
      ) |>
    dplyr::mutate(
      group_naive = stringr::str_c(prefix, "-", dplyr::cur_group_id())
      ) |>
    dplyr::ungroup()
}

dedupe_community <- function(df, prefix, name, membership, nodes = NULL) {
  #' Identifies communities using igraph community detection.
  #'
  #' @param df A dataframe containing edges.
  #' @param df A dataframe containing nodes.
  #' @param prefix Prefix for group ids.
  #' @param name id column.
  #' @param membership Name for community column.
  #' @returns A dataframe.
  #' @export
  g <- df |>
    # Create igraph from data frame.
    # This is how we identify groups of owners.
    igraph::graph_from_data_frame(
      vertices = nodes,
      directed = FALSE
    ) |>
    # Finds a community using modularity score.
    # (Implements fast greedy modularity optimization algorithm.)
    igraph::cluster_fast_greedy(
      merges = FALSE,
      modularity = FALSE
    )
  tibble::tibble(
    # Membership is a vector of groups.
    !!membership := g$membership,
    # Names is a vector of the strings deduplicated.
    !!name := g$names
    ) |>
    dplyr::mutate(
      # Prepend a prefix onto the group id to ensure
      # uniqueness from other deduplication groups.
      !!membership := stringr::str_c(prefix, "-", get(membership))
    )
}

dedupe_text_mode <- function(df, group_col, cols) {
  #' Identify most common string appearing in columns by group.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param group_col Column containing group id.
  #' @param cols Columns with strings.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::filter(!is.na(get({{ group_col }}))) |>
    dplyr::group_by(across(c({{ group_col }}, {{ cols }}))) |>
    dplyr::summarize(
      count = dplyr::n()
    ) |>
    dplyr::arrange(get({{ group_col }}), dplyr::desc(count)) |>
    dplyr::group_by(across({{ group_col }})) |>
    dplyr::summarize(
      dplyr::across({{ cols }}, ~dplyr::first(.))
    )
}

dedupe_cosine <- function(df,
                          str_field,
                          thresh,
                          group = "group_cosine"
                          ) {
  #' Cosine similarity-based deduplication.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param str_field The column used for deduplication.
  #' @param group Name for group column.
  #' @param thresh The minimum similarity threshold.
  #' @returns A dataframe.
  #' @export
  community <- df |>
    # Reduce to only unique string fields.
    dplyr::distinct(dplyr::across({{ str_field }})) |>
    # Create a quanteda corpus.
    quanteda::corpus(docid_field = str_field, text_field = str_field) |>
    # Construct character-basedtokens object from corpus.
    quanteda::tokens(what = "character") |>
    # Create character 3-grams from tokenized text.
    quanteda::tokens_ngrams(n = 3, concatenator = "") |>
    # Construct document-feature matrix from tokens.
    quanteda::dfm(remove_padding = TRUE) |>
    # Weight document-feature matrix by term frequency-
    # inverse document frequency.
    quanteda::dfm_tfidf() |>
    # Compute similarity matrix using cosine-similarity.
    quanteda.textstats::textstat_simil(
      method = "cosine",
      margin = "document",
      min_simil = thresh
      ) |>
    # Coerce to data frame.
    as.data.frame() |>
    # Coerce factors to characters.
    # (Necessary to read as igraph.)
    dplyr::mutate(
      document1 = as.character(document1),
      document2 = as.character(document2)
    )
  # Join to original dataframe.
  df |>
    dplyr::left_join(
      community |>
        dedupe_community(
          prefix = "cosine",
          name = str_field,
          membership = group
        ),
      by = str_field
    ) |>
    dplyr::arrange( .data[[group]] )
}

dedupe_cosine_bounded <- function(df, id, field1, field2, thresh) {
  df <- df |>
    dplyr::filter(!is.na(.data[[field1]]) & !is.na(.data[[field2]])) |>
    dedupe_naive(field1) |>
    dedupe_cosine(field1, thresh=thresh) |>
    dplyr::mutate(
      group_cosine = dplyr::case_when(
        !is.na(group_cosine) ~ group_cosine,
        .default = group_naive
      )
    ) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(field2)), group_cosine) |>
    dplyr::mutate(
      cosine_bounded = stringr::str_c("cosine-bounded-", dplyr::cur_group_id())
    ) |>
    dplyr::ungroup()
  
  df |>
    dplyr::left_join(
      df |>
        dplyr::select(cosine_bounded, group_naive) |>
        dplyr::distinct() |>
        dedupe_community(
          "grouped",
          name="cosine_bounded",
          membership="group_id"
        ),
      dplyr::join_by(cosine_bounded)
    ) |>
    dplyr::select(-c(cosine_bounded, group_cosine, group_naive))
}

dedupe_fill_group <- function(df, group, fill_col) {
  #' Fill group rows with no corporate matches with in-group corporate matches.
  #'
  #' @param df A dataframe.
  #' @param group Column identifying group.
  #' @param fill_col Column containing data to be filled.
  #' @returns A dataframe.
  #' @export
  df |>
    dplyr::group_by(
      dplyr::across({{ group }})
      ) |>
    tidyr::fill({{ fill_col }}, .direction = "updown") |>
    dplyr::ungroup()
}

dedupe_all <- function() {
  own_dedupe <- OWNERS |>
    dedupe_cosine_bounded(
      id="id", field1="name", field2="addr_id", thresh=0.8
    )
  
  OWNERS_ <<- own_dedupe |>
    dplyr::select(site_id, group_id, name, addr_id, inst, trust, trustees)
  
  matched_companies <- OWNERS_ |>
    dplyr::left_join(
      COMPANIES_PROC |> dplyr::select(company_id, name),
      by=dplyr::join_by(name)
    )
  
  OFFICERS_PROC_ <<- OFFICERS_PROC |>
    dedupe_cosine_bounded(
      id="id", field1="name", field2="addr_id", thresh=0.95
    ) |>
    dplyr::left_join(
      matched_companies |> 
        dplyr::filter(!is.na(company_id)) |> 
        dplyr::select(company_id) |>
        dplyr::mutate(match = TRUE) |>
        dplyr::distinct(),
      by=dplyr::join_by(company_id)
    ) |>
    dplyr::group_by(group_id) |>
    tidyr::fill(match) 
  
  OFFICERS_MATCHED_ <<- OFFICERS_PROC_ |>
    dplyr::filter(match) |>
    dplyr::select(-match) |>
    dplyr::select(company_id, group_id)
  
  
  OWNERS_ <<- matched_companies |>
    dplyr::left_join(
      OFFICERS_MATCHED_ |>
        dplyr::distinct() |>
        dedupe_community(
          "network",
          name="company_id",
          membership="network_id"
        ),
      by=dplyr::join_by(company_id)
    ) |>
    dplyr::mutate(
      group_id = dplyr::case_when(
        !is.na(network_id) ~ network_id,
        .default = group_id
      )
    )
  
  METACORPS_ <<- OWNERS_ |>
    dplyr::group_by(group_id, name) |>
    dplyr::summarize(
      count = dplyr::n()
    ) |>
    dplyr::arrange(dplyr::desc(count)) |>
    dplyr::slice(1) |>
    dplyr::ungroup() |>
    dplyr::select(-count)
}