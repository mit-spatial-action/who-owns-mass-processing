source("R/utilities.R")

dedupe_address_to_id <- function(df, df2) {
  df |>
    dplyr::select(-dplyr::any_of(c("addr", "start", "end", "body", "even", "muni", "postal", "state", "loc_id"))) |>
    dplyr::left_join(
      df2,
      dplyr::join_by(id == id)
    )
}

dedupe_unique_addresses <- function(owners, officers, companies, sites, addresses) {
  if ("sf" %in% class(addresses)) {
    addresses <- addresses |>
      sf::st_drop_geometry()
  }
  
  addresses <- addresses |>
    dplyr::mutate(
      id=stringr::str_c("address-", dplyr::row_number())
    ) |>
    std_assemble_addr() |>
    dplyr::select(-c(po, pmb))
  
  df <- owners |>
    dplyr::bind_rows(
      officers, 
      companies
    )
  
  df <- df |>
    dplyr::select(id, addr, start, end, body, even, muni, postal, state, loc_id)  |>
    flow_address_to_address_seq(sites, addresses) |>
    dplyr::bind_rows(
      addresses |>
        dplyr::select(id, addr, start, end, body, even, muni, postal, state, loc_id),
      sites |>
        dplyr::select(id, addr, start, end, body, even, muni, postal, state, loc_id)
    ) |>
    dplyr::filter(!is.na(addr)) |>
    dplyr::group_by(addr, start, end, body, even, muni, postal, loc_id) |>
    tidyr::fill(state, .direction = "updown") |>
    dplyr::mutate(
      addr_id = stringr::str_c("address-", dplyr::cur_group_id())
    ) |>
    dplyr::ungroup()
  
  relation <- df |> dplyr::select(id, addr_id) |> dplyr::distinct()
  addresses <- df |> dplyr::select(-id) |> dplyr::rename(id = addr_id) |> dplyr::distinct()
  
  results <- purrr::map(
    list(
      owners = owners, 
      sites = sites |> dplyr::filter(res),
      companies = companies, 
      officers = officers
    ), 
    ~ dedupe_address_to_id(.x, relation)
  )
  
  results[['unique_addresses']] <- addresses
  results
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
    dplyr::ungroup() |>
    dplyr::group_by(across({{ group_col }})) |>
    dplyr::summarize(
      dplyr::across({{ cols }}, ~dplyr::first(.))
    ) |>
    dplyr::ungroup()
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

dedupe_cosine_bounded <- function(df, field1, field2, thresh, fill_by_naive = TRUE) {
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
    dplyr::ungroup() |>
    dplyr::select(-c(group_cosine))
  
  if (fill_by_naive) {
    df <- df |>
      dplyr::left_join(
        df |>
          dplyr::select(group_naive, .data[[field2]]) |>
          dplyr::distinct() |>
          dedupe_community(
            "group-naive",
            name="group_naive",
            membership="naive_bounded"
          ),
        dplyr::join_by(group_naive)
      ) |>
      dplyr::select(-group_naive)
    
    df <- df |>
      dplyr::left_join(
        df |>
          dplyr::select(cosine_bounded, naive_bounded) |>
          dplyr::distinct() |>
          dedupe_community(
            "cosine-bounded",
            name="cosine_bounded",
            membership="group_cosine"
          ),
        dplyr::join_by(cosine_bounded)
      ) |>
      dplyr::select(-c(cosine_bounded)) |>
      dplyr::rename(
        group_naive = naive_bounded
      )
  }
  df
}

dedupe_all <- function(
    owners,
    officers,
    companies,
    sites,
    addresses) {
  
  dedupe_unique_addresses(
    owners = owners, 
    officers = officers,
    companies = companies, 
    sites = sites, 
    addresses = addresses
  ) |>
    wrapr::unpack(
      owners <- owners,
      officers <- officers,
      companies <- companies,
      sites <- sites,
      unique_addresses <- unique_addresses
    )
  
  owners <- owners |>
    dplyr::left_join(
      sites |>
        dplyr::select(id, condo),
      by=dplyr::join_by(site_id == id),
      multiple="any"
    )
  
  owners <- owners |>
    dplyr::filter(!condo) |>
    dedupe_cosine_bounded(
      field1="name", 
      field2="addr_id", 
      thresh=0.8
    ) |>
    dplyr::left_join(
      companies |> dplyr::select(company_id, name),
      by=dplyr::join_by(name),
      na_matches="never",
      multiple="any"
    ) |>
    dplyr::bind_rows(
      owners |>
        dplyr::filter(condo)
    )
  
  officers <- officers |>
    dedupe_cosine_bounded(
      field1="name",
      field2="addr_id",
      thresh=0.95
    ) |>
    dplyr::left_join(
      owners |>
        dplyr::filter(!is.na(company_id)) |>
        dplyr::select(company_id) |>
        dplyr::mutate(match = TRUE) |>
        dplyr::distinct(),
      by=dplyr::join_by(company_id),
      na_matches="never",
      multiple="any"
    ) |>
    dplyr::group_by(group_cosine) |>
    tidyr::fill(match, .direction="downup") |>
    dplyr::ungroup()
  
  owners <- owners |>
    dplyr::left_join(
      officers |>
        dplyr::filter(match) |>
        dplyr::select(-match) |>
        dplyr::select(company_id, group_cosine) |>
        dplyr::distinct() |>
        dedupe_community(
          "network",
          name="company_id",
          membership="network_id"
        ),
      by=dplyr::join_by(company_id),
      na_matches="never",
      multiple="any"
    ) |>
    dplyr::group_by(group_cosine) |>
    tidyr::fill(network_id, .direction="downup") |>
    dplyr::ungroup() |>
    dplyr::mutate(
      group_network = dplyr::case_when(
        !is.na(network_id) ~ network_id,
        .default = group_cosine
      )
    ) |>
    dplyr::select(-network_id)

  metacorps <- owners |>
    dedupe_text_mode(
      group_col="group_network",
      cols = "name"
    )
  
  list(
    owners = owners,
    officers = officers,
    metacorps = metacorps,
    sites = sites,
    unique_addresses = unique_addresses
  )
}
