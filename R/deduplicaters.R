source("R/globals.R")
source("R/standardizers.R")
source("R/loaders.R")
source("R/run_utils.R")

flag_lawyers <- function(df, cols) {
  #' Flags likely law offices and lawyers.
  #'
  #' @param df A dataframe.
  #' @param cols Columns to be processed.
  #' @returns A dataframe.
  #' @export
  df %>%
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

dedupe_naive <- function(df, str_field) {
  #' Naive deduplication function, based on duplicate names and addresses.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param str_field The column used for deduplication.
  #' @returns A dataframe.
  #' @export
  distinct <- df %>%
    dplyr::group_by(
      dplyr::across({{ str_field }})
      ) %>%
    dplyr::mutate(
      group_naive = stringr::str_c("naive", dplyr::cur_group_id(), sep = "_")
      ) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(
      dplyr::across({{ str_field }}), group_naive) %>%
    dplyr::summarize()
  df %>%
    dplyr::left_join(distinct, by = str_field)
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
  g <- df %>%
    # Create igraph from data frame.
    # This is how we identify groups of owners.
    igraph::graph_from_data_frame(
      vertices = nodes,
      directed = FALSE
    ) %>%
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
    ) %>%
    dplyr::mutate(
      # Prepend a prefix onto the group id to ensure
      # uniqueness from other deduplication groups.
      !!membership := stringr::str_c(prefix, get(membership), sep = "_")
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
  df %>%
    dplyr::filter(!is.na(get({{ group_col }}))) %>%
    dplyr::group_by(across(c({{ group_col }}, {{ cols }}))) %>%
    dplyr::summarize(
      count = dplyr::n()
    ) %>%
    dplyr::arrange(get({{ group_col }}), dplyr::desc(count)) %>%
    dplyr::group_by(across({{ group_col }})) %>%
    dplyr::summarize(
      dplyr::across({{ cols }}, ~dplyr::first(.))
    )
}

dedupe_cosine <- function(df,
                          str_field,
                          group = "group_cosine",
                          thresh = 0.75) {
  #' Cosine similarity-based deduplication.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param str_field The column used for deduplication.
  #' @param group Name for group column.
  #' @param thresh The minimum similarity threshold.
  #' @returns A dataframe.
  #' @export
  community <- df %>%
    # Reduce to only unique string fields.
    dplyr::distinct(across({{ str_field }})) %>%
    # Create a quanteda corpus.
    quanteda::corpus(docid_field = str_field, text_field = str_field) %>%
    # Construct character-basedtokens object from corpus.
    quanteda::tokens(what = "character") %>%
    # Create character 3-grams from tokenized text.
    quanteda::tokens_ngrams(n = 3, concatenator = "") %>%
    # Construct document-feature matrix from tokens.
    quanteda::dfm(remove_padding = TRUE) %>%
    # Weight document-feature matrix by term frequency-
    # inverse document frequency.
    quanteda::dfm_tfidf() %>%
    # Compute similarity matrix using cosine-similarity.
    quanteda.textstats::textstat_simil(
      method = "cosine",
      margin = "document",
      min_simil = thresh
      ) %>%
    # Coerce to data frame.
    as.data.frame() %>%
    # Coerce factors to characters.
    # (Necessary to read as igraph.)
    dplyr::mutate(
      document1 = as.character(document1),
      document2 = as.character(document2)
    )
  # Join to original dataframe.
  df %>%
    dplyr::left_join(
      community %>%
        dedupe_community(
          prefix = "cosine",
          name = str_field,
          membership = group
        ),
      by = str_field
    )
}

dedupe_fill_group <- function(df, group, fill_col) {
  #' Fill group rows with no corporate matches with in-group corporate matches.
  #'
  #' @param df A dataframe.
  #' @param group Column identifying group.
  #' @param fill_col Column containing data to be filled.
  #' @returns A dataframe.
  #' @export
  df %>%
    dplyr::group_by(
      dplyr::across({{ group }})
      ) %>%
    tidyr::fill({{ fill_col }}, .direction = "updown") %>%
    dplyr::ungroup()
}

process_assess <- function(df) {
  log_message("Processing assessors records and 
              flagging owner-occupancy...")
  df <- df %>%
    std_flow_strings(c("owner1", "city", "own_city", "own_addr", "site_addr", "own_zip")) %>%
    std_zip(c("zip", "own_zip")) %>% 
    std_flow_addresses(c("own_addr", "site_addr")) %>%
    std_flow_cities(c("city", "own_city")) %>%
    std_flow_names(c("owner1", "own_addr")) %>%
    # Extract 'care of' entities to co and remove from own_addr.
    dplyr::mutate(
      co = dplyr::case_when(
        stringr::str_detect(
          own_addr,
          "C / O"
        )
        ~ stringr::str_extract(own_addr, "(?<=C / O ).*$")
      ),
      own_addr = dplyr::case_when(
        stringr::str_detect(
          own_addr,
          "C / O"
        )
        ~ dplyr::na_if(
            stringr::str_extract(own_addr, ".*(?= ?C / O )"), 
            ""
          ),
        TRUE ~ own_addr
      )
    ) %>%
    # Flag owner-occupied properties on the basis of standardized addresses.
    dplyr::mutate(
      ooc = dplyr::case_when(
        own_addr == site_addr ~ TRUE,
        TRUE ~ FALSE
      )
    )
  
  # For rows that are missing a ZIP code, pull it from `tigris`.
  # Described in GitHub issue #19
  df_no_zip <- assess %>%
    dplyr::filter(is.na(zip))
  
  if (nrow(df_no_zip) > 0) {
    df_zip <- assess %>%
      dplyr::filter(!is.na(zip))
    
    
    parcels <- parcels %>%
      dplyr::filter(loc_id %in% dplyr::pull(df_no_zip, loc_id)) %>%
      st_get_zips("zip") %>%
      sf::st_drop_geometry()
    
    df <- df_no_zip %>%
      dplyr::select(-c(zip)) %>%
      dplyr::left_join(
        dplyr::select(parcels, c(loc_id, zip)),
        by = c("loc_id" = "loc_id"),
        na_matches = "never"
      ) %>%
      dplyr::distinct(loc_id, zip, .keep_all = TRUE) %>%
      dplyr::bind_rows(df_zip)
  }
  df
}

process_owners <- function(df) {
  df %>%
    # Concatenate name_address
    dplyr::mutate(
      name_address = dplyr::case_when(
        !is.na(owner1) & is.na(own_addr) ~ owner1,
        is.na(owner1) & !is.na(own_addr) ~ own_addr,
        is.na(owner1) & is.na(own_addr) ~ NA_character_,
        TRUE ~ stringr::str_c(owner1, own_addr, sep = " ")
      )
    ) %>%
    dplyr::select(-c(site_addr, ooc)) %>%
    # Naive deduplication on prepared, concatenated name and address.
    dedupe_naive(str_field = "name_address") %>%
    # Cosine-similarity-based deduplication.
    dedupe_cosine(
      str_field = "name_address",
      group = "group_cosine",
      thresh = 0.75
    ) %>%
    dplyr::mutate(
      group = dplyr::case_when(
        !is.na(group_cosine) ~ group_cosine,
        TRUE ~ group_naive
      )
    ) %>%
    dplyr::select(-c(group_cosine, group_naive))
}

process_corps <- function(df){
  df %>%
    std_flow_strings(c("entityname", "agentname", "agentaddr1", "agentaddr2")) %>%
    std_zip(c("agentpostalcode")) %>% 
    std_flow_addresses(c("agentaddr1", "agentaddr2")) %>%
    std_flow_cities(c("agentcity")) %>%
    std_flow_names(c("entityname", "agentname", "agentaddr1", "agentaddr2")) %>%
    tidyr::drop_na("entityname")
}

process_deduplication <- function(town_ids = c(274), return_results = TRUE) {
  #' Run complete owner deduplication process.
  #'
  #' @param town_ids list of town ids
  #' @param return_results If `TRUE`, return results
  #'  in a named list. If `FALSE`, return nothing.
  #'  In either case, results are output to delimited
  #'  text and `*.RData` files
  #' @returns If `store_results` is `TRUE`, a named
  #'  list of dataframes. Else, nothing
  #' @export
  log_message("Reading assessors table from GDB...")
  assess <- load_assess(path = file.path(DATA_DIR, ASSESS_GDB), town_ids = town_ids)
  log_message("Processing assessors records...")
  assess <- process_assess(assess) %>%
    # Write pipe-delimited text file of edges.
    readr::write_delim(
      file.path(RESULTS_DIR, stringr::str_c(ASSESS_OUT_NAME, "csv", sep = ".")),
      delim = "|", 
      quote = "needed"
    )
  # Write it out for later use in filing linker.
  saveRDS(
    assess, 
    file = file.path(RESULTS_DIR, stringr::str_c(ASSESS_OUT_NAME, "rds", sep = "."))
    )
  # Separate owners from assessors records.
  log_message("Extracting owners from assessors table...")
  owners <- process_owners(assess)
  
  
  rm(assess)
  
  log_message("Reading corporations from delimited text...")
  corps <- load_corps(file.path(DATA_DIR, CORPS))
  
  log_message("Processing corporation records...")
  corps <- process_corps(corps)
  
  log_message("Matching owners in assessors records to corps table, distinguishing between lawyers and non-lawyers...")
  corps_active <- corps %>%
    dplyr::filter(activeflag == "Y") %>%
    dplyr::select(c(id_corp, entityname))
  
  owners <- owners %>%
    # Flag lawyers.
    flag_lawyers(
      c("owner1")
    ) %>%
    dplyr::rename(
      own_lawyer = lawyer
    ) %>%
    dplyr::left_join(
      corps_active %>%
        dplyr::rename(id_corp_own = id_corp),
      by = c("owner1" = "entityname")
    ) %>%
    # # Flag lawyers.
    flag_lawyers(
      c("own_addr")
    ) %>%
    dplyr::rename(
      addr_lawyer = lawyer
    ) %>%
    dplyr::left_join(
      corps_active %>%
        dplyr::rename(id_corp_addr = id_corp),
      by = c("own_addr" = "entityname")
    ) %>%
    dplyr::mutate(
      id_corp = dplyr::case_when(
        !is.na(id_corp_addr) & 
          !addr_lawyer & 
          is.na(id_corp_own) ~ id_corp_addr,
        is.na(id_corp_addr) & 
          !is.na(id_corp_own) & 
          !own_lawyer ~ id_corp_own,
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::select(-c(id_corp_addr, id_corp_own, own_lawyer, addr_lawyer)) %>%
    dedupe_fill_group(
      group = "group",
      fill_col = "id_corp"
    )
  
  # Create list of MA companies identified in assessors records.
  corps_list <- owners %>%
    tidyr::drop_na(id_corp) %>%
    dplyr::pull(id_corp) %>%
    unique()
  
  log_message("Identifying individual agents and matching 
              company agents to corps table, distinguishing between 
              lawyers and non-lawyers...")
  agents <- load_agents(
      corps,
      cols = c("id_corp", "agentname", "agentaddr1", "agentaddr2"),
      drop_na_col = "agentname"
    ) %>%
    dplyr::filter(id_corp %in% corps_list) %>%
    # Flag lawyers.
    flag_lawyers(
      c("agentname", "agentaddr1", "agentaddr2")
    ) %>%
    # Remove 'C / O' (care of) prefix.
    std_remove_co(
      c("agentname", "agentaddr1", "agentaddr2")
    ) %>%
    std_trailingwords(c("agentname", "agentaddr1", "agentaddr2")) %>%
    dplyr::left_join(
      corps_active %>%
        dplyr::rename(id_agentname = id_corp),
      by = c("agentname" = "entityname")
    ) %>%
    dplyr::left_join(
      corps_active %>%
        dplyr::rename(id_agentaddr1 = id_corp),
      by = c("agentaddr1" = "entityname")
    ) %>%
    dplyr::left_join(
      corps_active %>%
        dplyr::rename(id_agentaddr2 = id_corp),
      by = c("agentaddr2" = "entityname")
    ) %>%
    dplyr::mutate(
      id_link = dplyr::case_when(
        !is.na(id_agentname) ~ id_agentname,
        !is.na(id_agentaddr1) ~ id_agentaddr1,
        !is.na(id_agentaddr2) ~ id_agentaddr2,
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::select(-c(id_agentname, id_agentaddr1, id_agentaddr2))
  
  rm(corps_active)
  
  # Initiate individual deduplication.
  corps_from_agents <- agents %>%
    dplyr::filter(
      !is.na(id_link) & !lawyer
    ) %>%
    dplyr::select(c(id_corp, id_link)) %>%
    dplyr::distinct()
  
  corps_list <- c(
      dplyr::pull(corps_from_agents, id_corp),
      dplyr::pull(corps_from_agents, id_link),
      corps_list
    ) %>%
    unique()
  
  inds_from_agents <- agents %>%
    dplyr::filter(
      is.na(id_link) & !lawyer
    ) %>%
    dplyr::select(-c(id_link, lawyer)) %>%
    dplyr::mutate(
      address = dplyr::case_when(
        !is.na(agentaddr1) & 
          !is.na(agentaddr2)
          ~ stringr::str_c(agentaddr1, agentaddr2, sep = " "),
        !is.na(agentaddr1) & 
          is.na(agentaddr2) ~ agentaddr1,
        is.na(agentaddr1) & 
          !is.na(agentaddr2) ~ agentaddr2,
        TRUE ~ NA_character_
      ),
      name_address = dplyr::case_when(
        !is.na(agentname) & 
          !is.na(address)
          ~ stringr::str_c(agentname, address, sep = " "),
        !is.na(agentname) & 
          is.na(address) ~ agentname,
        is.na(agentname) & 
          !is.na(address) ~ address,
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::rename(
      fullname = agentname
    ) %>%
    dplyr::select(c(id_corp, fullname, address, name_address)) %>%
    dplyr::distinct()
  
  # law_from_agents <- agents %>%
  #   filter(lawyer) %>%
  #   select(-c(lawyer))
  
  rm(agents)
  
  log_message("Deduplicating individuals and isolating companies.")
  # Initiate individual deduplication.
  inds <- load_inds(file.path(DATA_DIR, INDS))
  
  inds <- inds %>%
    dplyr::filter(id_corp %in% corps_list) %>%
    std_flow_strings(c("firstname", "lastname", "busaddr1", "resaddr1")) %>%
    std_flow_addresses(c("busaddr1", "resaddr1")) %>%
    std_flow_names(c("firstname", "lastname")) %>%
    dplyr::filter(
      dplyr::if_any(
        c(firstname, lastname, busaddr1, resaddr1), ~ !is.na(.)
      )
    ) %>%
    # Remove 'C / O' (care of) prefix.
    std_remove_co(
      c("resaddr1", "busaddr1")
    ) %>%
    std_trailingwords(c("resaddr1", "busaddr1")) %>%
    dplyr::mutate(
      # Concatenate fullname.
      fullname = dplyr::case_when(
        !is.na(firstname) &
          is.na(lastname) ~ firstname,
        is.na(firstname) & 
          !is.na(lastname) ~ lastname,
        is.na(firstname) & 
          is.na(lastname) ~ NA_character_,
        TRUE ~ stringr::str_c(firstname, lastname, sep = " "),
      ),
      # Choose address.
      address = dplyr::case_when(
        !is.na(busaddr1) & 
          is.na(resaddr1) ~ busaddr1,
        is.na(busaddr1) & 
          !is.na(resaddr1) ~ resaddr1,
        is.na(busaddr1) & 
          is.na(resaddr1) ~ NA_character_,
        TRUE ~ busaddr1
      ),
      co = dplyr::case_when(
        stringr::str_detect(address, "LLC[ $]")
          ~ stringr::str_extract(address, "^.*LLC")
      ),
      address = dplyr::case_when(
        stringr::str_detect(address, "LLC[ $]")
          ~ dplyr::na_if(
              stringr::str_extract(address, "(?<=LLC ).*$"), ""
            ),
        TRUE ~ address
      ),
      name_address = dplyr::case_when(
        !is.na(fullname) & 
          !is.na(address) 
          ~ stringr::str_c(fullname, address, sep = " "),
        !is.na(fullname) & 
          is.na(address) 
          ~ fullname,
        is.na(fullname) & 
          !is.na(address) 
          ~ address,
        TRUE ~ NA_character_
      )
    ) %>%
    flag_lawyers(
      c("name_address")
    ) %>%
    dplyr::left_join(
      corps %>%
        dplyr::filter(activeflag == "Y") %>%
        dplyr::select(c(id_corp, entityname)) %>%
        dplyr::rename(id_fullname = id_corp),
      by = c("fullname" = "entityname")
    ) %>%
    dplyr::left_join(
      corps %>%
        dplyr::filter(activeflag == "Y") %>%
        dplyr::select(c(id_corp, entityname)) %>%
        dplyr::rename(id_address = id_corp),
      by = c("address" = "entityname")
    ) %>%
    dplyr::left_join(
      corps %>%
        dplyr::filter(activeflag == "Y") %>%
        dplyr::select(c(id_corp, entityname)) %>%
        dplyr::rename(id_co = id_corp),
      by = c("co" = "entityname")
    )
  
  corps_from_inds <- inds %>%
    dplyr::filter(
      (!is.na(id_fullname) | !is.na(id_address) | !is.na(id_co)) 
      & !lawyer
    ) %>%
    tidyr::pivot_longer(
      cols = c(id_fullname, id_address, id_co),
      names_to = NULL,
      values_to = "id_link",
      values_drop_na = TRUE
    ) %>%
    dplyr::select(c(id_corp, id_link)) %>%
    dplyr::distinct()
  
  corps_list <- c(
      dplyr::pull(corps_from_inds, id_corp),
      dplyr::pull(corps_from_inds, id_link),
      corps_list
    ) %>%
    unique()
  
  inds_from_inds <- inds %>%
    dplyr::filter(
      (is.na(id_fullname) & is.na(id_address) & is.na(id_co)) 
      & !lawyer
    ) %>%
    dplyr::select(c(id_corp, fullname, address)) %>%
    dplyr::mutate(
      name_address = dplyr::case_when(
        !is.na(fullname) & 
          !is.na(address) 
          ~ stringr::str_c(fullname, address, sep = " "),
        !is.na(fullname) &
          is.na(address)
          ~ fullname,
        is.na(fullname) & 
          !is.na(address) 
          ~ address,
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::select(c(id_corp, fullname, address, name_address)) %>%
    dplyr::distinct()
  
  log_message("Parsing data into nodes and vertices...")
  
  inds_links <- inds_from_inds %>%
    dplyr::bind_rows(inds_from_agents) %>%
    dplyr::distinct() %>%
    # Cosine-similarity based deduplication on the same.
    dedupe_naive(str_field = "name_address") %>%
    dedupe_cosine(
      str_field = "name_address",
      group = "group_cosine",
      thresh = 0.85
    ) %>%
    dplyr::mutate(
      id_link = dplyr::case_when(
        !is.na(group_cosine) ~ group_cosine,
        TRUE ~ group_naive
      )
    ) %>%
    dplyr::select(-c(group_cosine, group_naive)) %>%
    # distinct(id_corp, group) %>%
    dplyr::left_join(
      dedupe_text_mode(
        ., 
        "id_link", 
        c("fullname", "address", "name_address")
      ) %>%
        dplyr::rename(
          fullname_simp = fullname,
          address_simp = address,
          name_address_simp = name_address
        ),
      by = "id_link"
    )
  
  corps_links <- corps_from_inds %>%
    dplyr::bind_rows(corps_from_agents) %>%
    dplyr::distinct() %>%
    dplyr::mutate(relation = "LINKED_TO")
  
  inds_nodes <- inds_links %>%
    dplyr::rename(
      id = id_link,
    ) %>%
    dplyr::select(id, fullname_simp, address_simp, name_address_simp) %>%
    dplyr::distinct() %>%
    dplyr::mutate(
      label = "inds"
    )
  
  corps_nodes <- corps %>%
    dplyr::rename(
      id = id_corp
    ) %>%
    dplyr::filter(id %in% corps_list) %>%
    dplyr::select(c(id, entityname)) %>%
    dplyr::mutate(
      label = "corps"
    )
  
  edges <- inds_links %>%
    dplyr::select(id_link, id_corp) %>%
    dplyr::distinct() %>%
    dplyr::mutate(
      relation = "AGENT_OF"
    ) %>%
    dplyr::bind_rows(corps_links) %>%
    # Write pipe-delimited text file of edges.
    readr::write_delim(
      file.path(
        RESULTS_DIR, 
        stringr::str_c(EDGES_OUT_NAME, "csv", sep = ".")
      ),
      delim = "|", 
      quote = "needed"
    )
  
  nodes <- inds_nodes %>%
    dplyr::bind_rows(corps_nodes) %>%
    # Write pipe-delimited text file of corporations.
    readr::write_delim(
      file.path(
        RESULTS_DIR, 
        stringr::str_c(NODES_OUT_NAME, "csv", sep = ".")
      ),
      delim = "|", 
      quote = "needed"
    )
  
  log_message("Identifying community and writing to delimited file...")
  community <- edges %>%
    dedupe_community(
      nodes = nodes,
      prefix = "network",
      name = "id",
      membership = "group_network"
    )
  
  log_message("Writing corporate nodes to delimited file...")
  corps_nodes <- corps_nodes %>%
    dplyr::left_join(
      community,
      by = c("id" = "id")
    ) %>%
    # Write pipe-delimited text file of corporations.
    readr::write_delim(
      file.path(
        RESULTS_DIR, 
        stringr::str_c(CORPS_OUT_NAME, "csv", sep = ".")
      ),
      delim = "|", 
      quote = "needed"
    )
  
  log_message("Writing indiviudals nodes to delimited file...")
  inds_nodes <- inds_nodes %>%
    dplyr::left_join(
      community,
      by = c("id" = "id")
    ) %>%
    # Write pipe-delimited text file of individuals.
    readr::write_delim(
      file.path(
        RESULTS_DIR, 
        stringr::str_c(INDS_OUT_NAME, "csv", sep = ".")
      ),
      delim = "|", 
      quote = "needed"
    )
  
  log_message("Writing deduplicated ownership to delimited file...")
  owners <- owners %>%
    # Join
    dplyr::left_join(
      community,
      by = c("id_corp" = "id")
    ) %>%
    # Assign id based on priority.
    dplyr::mutate(
      group = dplyr::case_when(
        !is.na(group_network) ~ group_network,
        !is.na(group) ~ group,
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::select(-c(group_network)) %>%
    dplyr::group_by(group) %>%
    dplyr::mutate(
      count = dplyr::n()
    ) %>%
    dplyr::ungroup() %>%
    # Write pipe-delimited text file of ownership.
    readr::write_delim(
      file.path(
        RESULTS_DIR, 
        stringr::str_c(OWNERS_OUT_NAME, "csv", sep = ".")
      ),
      delim = "|", 
      quote = "needed"
    )
  
  log_message("Finishing up.")
  # Save RData image.
  save.image(
    file.path(RESULTS_DIR, stringr::str_c(RDATA_OUT_NAME, "RData", sep = "."))
  )
  # If directed to store results, return them in a named list.
  if (return_results == TRUE) {
    list(
      "owners" = owners, 
      "inds" = inds_nodes, 
      "corps" = corps_nodes, 
      "community" = community
    )
  } else {
    NULL
  }
}