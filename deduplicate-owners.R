source("log.R")
source("std_helpers.R")
source("assess_helpers.R")

# For Cosine Similarity statistics.
library(quanteda)
library(quanteda.textstats)

# For network-based community detection.
library(igraph)

flag_lawyers <- function(df, cols) {
  df %>%
    mutate(
      lawyer = case_when(
        if_any(
          cols,
          ~str_detect(
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
    group_by(across({{ str_field }})) %>%
    mutate(group_naive = paste("naive", cur_group_id(), sep = "_")) %>%
    ungroup() %>%
    group_by(across({{ str_field}} ), group_naive) %>%
    summarize()
  df %>%
    left_join(distinct, by = str_field)
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
    graph_from_data_frame(
      vertices = nodes,
      directed = FALSE
    ) %>%
    # Finds a community using modularity score.
    # (Implements fast greedy modularity optimization algorithm.)
    cluster_fast_greedy(
      merges = FALSE,
      modularity = FALSE
    )
  tibble(
    # Membership is a vector of groups.
    !!membership := g$membership,
    # Names is a vector of the strings deduplicated.
    !!name := g$names
    ) %>%
    mutate(
      # Prepend a prefix onto the group id to ensure
      # uniqueness from other deduplication groups.
      !!membership := paste(prefix, get(membership), sep = "_")
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
    filter(!is.na(get({{ group_col }}))) %>%
    group_by(across(c({{ group_col }}, {{ cols }}))) %>%
    summarize(
      count = n()
    ) %>%
    arrange(get({{ group_col }}), desc(count)) %>%
    group_by(across({{ group_col }})) %>%
    summarize(
      across({{ cols }}, ~first(.))
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
    distinct(across({{ str_field }})) %>%
    # Create a quanteda corpus.
    corpus(docid_field = str_field, text_field = str_field) %>%
    # Construct character-basedtokens object from corpus.
    tokens(what = "character") %>%
    # Create character 3-grams from tokenized text.
    tokens_ngrams(n = 3, concatenator = "") %>%
    # Construct document-feature matrix from tokens.
    dfm(remove_padding = TRUE) %>%
    # Weight document-feature matrix by term frequency-
    # inverse document frequency.
    dfm_tfidf() %>%
    # Compute similarity matrix using cosine-similarity.
    textstat_simil(
      method = "cosine",
      margin = "document",
      min_simil = thresh
      ) %>%
    # Coerce to data frame.
    as.data.frame() %>%
    # Coerce factors to characters.
    # (Necessary to read as igraph.)
    mutate(
      document1 = as.character(document1),
      document2 = as.character(document2)
    )
  # Join to original dataframe.
  df %>%
    left_join(
      community %>%
        dedupe_community(
          prefix = "cosine",
          name = str_field,
          membership = group
        ),
      by = str_field
    )
}


load_corps <- function(path) {
  #' Load corporations, sourced from the MA Secretary of the Commonwealth.
  #'
  #' @param path Path to delimited text Corporations file
  #' @returns A dataframe.
  #' @export
  read_delim(
      path,
      delim = "|",
      col_select = c(
        DataID, EntityName,
        AgentName, AgentAddr1, AgentAddr2, AgentCity,
        AgentState, AgentPostalCode, ActiveFlag)
    ) %>%
    rename(
      id_corp = DataID
    ) %>%
    rename_with(str_to_lower)
}

load_agents <- function(df, cols, drop_na_col) {
  #' Load agents, which are listed alongside corporations.
  #'
  #' @param df Dataframe created by `load_corps`
  #' @param cols Columns containing fields describing agents.
  #' @param drop_na_col Column for which NA rows should be dropped.
  #' @returns A dataframe of corporate agents.
  #' @export
  df %>%
    select(all_of(cols)) %>%
    filter(!is.na(get({{ drop_na_col }})))
}



load_parcels <- function(path, town_ids=FALSE) {
  #' Load assessing table from MassGIS geodatabase.
  #' https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/gdbs/l3parcels/MassGIS_L3_Parcels_gdb.zip
  #' 
  #' @param path Path to MassGIS Parcels GDB.
  #' @param test Whether to only load a sample subset of rows.
  #' @export
  parcel_query <- "SELECT * FROM L3_TAXPAR_POLY"
  if (!isFALSE(town_ids)) {
    parcel_query <- paste(parcel_query, "WHERE TOWN_ID IN (", paste(town_ids, collapse=", "), ")")
  }
  st_read(
    path,
    query = parcel_query
  ) %>%
    rename_with(str_to_lower) %>% 
    # Correct weird naming conventions of GDB.
    st_set_geometry("shape") %>%
    st_set_geometry("geometry") %>%
    # Select only unique id.
    select(c(loc_id)) %>%
    # Reproject to specified CRS.
    st_transform(crs) %>%
    # Cast from MULTISURFACE to MULTIPOLYGON.
    mutate(
      geometry = st_cast(geometry, "MULTIPOLYGON")
    )
 
}

load_inds <- function(path) {
  #' Load individuals from corporate db, 
  #' sourced from the MA Secretary of the Commonwealth.
  #'
  #' @param path Path to delimited text Corporations file
  #' @returns A dataframe.
  #' @export
  read_delim(
    path,
    delim = "|",
    col_select = c(
      DataID, FirstName, LastName, BusAddr1,
      ResAddr1
    )
  ) %>%
    rename(
      id_corp = DataID
    ) %>%
    rename_with(str_to_lower)
}

fill_group <- function(df, group, fill_col) {
  #' Fill group rows with no corporate matches with in-group corporate matches.
  #'
  #' @param df A dataframe.
  #' @param group Column identifying group.
  #' @param fill_col Column containing data to be filled.
  #' @returns A dataframe.
  #' @export
  df %>%
    group_by(across({{ group }})) %>%
    fill({{ fill_col }}, .direction = "updown") %>%
    ungroup()
}

run_deduplicate <- function(town_ids = c(274), return_results = TRUE) {
  #' Run complete owner deduplication process.
  #'
  #' @param town_ids list of town ids
  #' @param store_results If `TRUE`, return results
  #'  in a named list. If `FALSE`, return nothing.
  #'  In either case, results are output to delimited
  #'  text and `*.RData` files
  #' @returns If `store_results` is `TRUE`, a named
  #'  list of dataframes. Else, nothing
  #' @export
  # Load assessors table.
  # DATA_DIR and ASSESS_GDB set globally above.
  log_message("Reading assessors table from GDB...")
  assess <- load_assess(path = file.path(DATA_DIR, ASSESS_GDB), town_ids = town_ids)
  log_message("Processing assessors records and 
              flagging owner-occupancy...")
  assess <- assess %>%
    # Run string standardization procedures.
    std_flow_strings(c("owner1", "own_addr", "site_addr", "own_zip")) %>%
    std_zip("own_zip") %>% 
    std_flow_addresses("own_addr", "site_addr") %>%
    std_cities("own_city") %>%
    std_flow_names("owner1", "own_addr") %>%
    # Extract 'care of' entities to co and remove from own_addr.
    mutate(
      co = case_when(
        str_detect(
          own_addr,
          "C / O"
        )
        ~ str_extract(own_addr, "(?<=C / O ).*$")
      ),
      own_addr = case_when(
        str_detect(
          own_addr,
          "C / O"
        )
        ~ na_if(str_extract(own_addr, ".*(?= ?C / O )"), ""),
        TRUE ~ own_addr
      )
    ) %>%
    # Flag owner-occupied properties on the basis of standardized addresses.
    mutate(
      ooc = case_when(
        own_addr == site_addr ~ TRUE,
        TRUE ~ FALSE
      )
    ) %>%
    # Write pipe-delimited text file of edges.
    write_delim(
      file.path(RESULTS_DIR, paste(ASSESS_OUT_NAME, "csv", sep = ".")),
      delim = "|", quote = "needed"
    )
  # Separate owners from assessors records.
  owners <- assess %>%
    # Concatenate name_address
    mutate(
      name_address = case_when(
        !is.na(owner1) & is.na(own_addr) ~ owner1,
        is.na(owner1) & !is.na(own_addr) ~ own_addr,
        is.na(owner1) & is.na(own_addr) ~ NA_character_,
        TRUE ~ str_c(owner1, own_addr, sep = " ")
      )
    ) %>%
    select(-c(site_addr, ooc))
  
  rm(assess)
  
  log_message("Deduplicating ownership based on listed name and address...")
  owners <- owners %>%
    # Naive deduplication on prepared, concatenated name and address.
    dedupe_naive(str_field = "name_address") %>%
    # Cosine-similarity-based deduplication.
    dedupe_cosine(
      str_field = "name_address",
      group = "group_cosine",
      thresh = 0.75
    ) %>%
    mutate(
      group = case_when(
        !is.na(group_cosine) ~ group_cosine,
        TRUE ~ group_naive
      )
    ) %>%
    select(-c(group_cosine, group_naive))
  
  log_message("Reading corporations from delimited text...")
  corps <- load_corps(file.path(DATA_DIR, CORPS))
  
  log_message("Processing corporation records...")
  corps <- corps %>%
    std_flow_strings(c("entityname", "agentname", "agentaddr1", "agentaddr2")) %>%
    std_zip("agentpostalcode") %>% 
    std_flow_addresses("agentaddr1", "agentaddr2") %>%
    std_cities("agentcity") %>%
    std_flow_names("entityname", "agentname", "agentaddr1", "agentaddr2") %>%
    drop_na("entityname")
  
  log_message("Matching owners in assessors records to corps table, distinguishing between lawyers and non-lawyers...")
  corps_active <- corps %>%
    filter(activeflag == "Y") %>%
    select(c(id_corp, entityname))
  
  owners <- owners %>%
    # Flag lawyers.
    flag_lawyers(
      c("owner1")
    ) %>%
    rename(
      own_lawyer = lawyer
    ) %>%
    left_join(
      corps_active %>%
        rename(id_corp_own = id_corp),
      by = c("owner1" = "entityname")
    ) %>%
    # # Flag lawyers.
    flag_lawyers(
      c("own_addr")
    ) %>%
    rename(
      addr_lawyer = lawyer
    ) %>%
    left_join(
      corps_active %>%
        rename(id_corp_addr = id_corp),
      by = c("own_addr" = "entityname")
    ) %>%
    mutate(
      id_corp = case_when(
        !is.na(id_corp_addr) & !addr_lawyer & is.na(id_corp_own) ~ id_corp_addr,
        is.na(id_corp_addr) & !is.na(id_corp_own) & !own_lawyer ~ id_corp_own,
        TRUE ~ NA_character_
      )
    ) %>%
    select(-c(id_corp_addr, id_corp_own, own_lawyer, addr_lawyer)) %>%
    fill_group(
      group = "group",
      fill_col = "id_corp"
    )
  
  # Create list of MA companies identified in assessors records.
  corps_list <- owners %>%
    drop_na(id_corp) %>%
    pull(id_corp) %>%
    unique()
  
  log_message("Identifying individual agents and matching 
              company agents to corps table, distinguishing between 
              lawyers and non-lawyers...")
  agents <- load_agents(
    corps,
    cols = c("id_corp", "agentname", "agentaddr1", "agentaddr2"),
    drop_na_col = "agentname"
  ) %>%
    filter(id_corp %in% corps_list) %>%
    # Flag lawyers.
    flag_lawyers(
      c("agentname", "agentaddr1", "agentaddr2")
    ) %>%
    # Remove 'C / O' (care of) prefix.
    std_remove_co(
      c("agentname", "agentaddr1", "agentaddr2")
    ) %>%
    std_trailingwords(c("agentname", "agentaddr1", "agentaddr2")) %>%
    left_join(
      corps_active %>%
        rename(id_agentname = id_corp),
      by = c("agentname" = "entityname")
    ) %>%
    left_join(
      corps_active %>%
        rename(id_agentaddr1 = id_corp),
      by = c("agentaddr1" = "entityname")
    ) %>%
    left_join(
      corps_active %>%
        rename(id_agentaddr2 = id_corp),
      by = c("agentaddr2" = "entityname")
    ) %>%
    mutate(
      id_link = case_when(
        !is.na(id_agentname) ~ id_agentname,
        !is.na(id_agentaddr1) ~ id_agentaddr1,
        !is.na(id_agentaddr2) ~ id_agentaddr2,
        TRUE ~ NA_character_
      )
    ) %>%
    select(-c(id_agentname, id_agentaddr1, id_agentaddr2))
  
  rm(corps_active)
  
  # Initiate individual deduplication.
  corps_from_agents <- agents %>%
    filter(
      !is.na(id_link) & !lawyer
    ) %>%
    select(c(id_corp, id_link)) %>%
    distinct()
  
  corps_list <- c(
    corps_from_agents %>%
      pull(id_corp) %>%
      unique(),
    corps_from_agents %>%
      pull(id_link) %>%
      unique(),
    corps_list) %>%
    unique()
  
  inds_from_agents <- agents %>%
    filter(
      is.na(id_link) & !lawyer
    ) %>%
    select(-c(id_link, lawyer)) %>%
    mutate(
      address = case_when(
        !is.na(agentaddr1) & !is.na(agentaddr2)
        ~ str_c(agentaddr1, agentaddr2, sep = " "),
        !is.na(agentaddr1) & is.na(agentaddr2) ~ agentaddr1,
        is.na(agentaddr1) & !is.na(agentaddr2) ~ agentaddr2,
        TRUE ~ NA_character_
      ),
      name_address = case_when(
        !is.na(agentname) & !is.na(address)
        ~ str_c(agentname, address, sep = " "),
        !is.na(agentname) & is.na(address) ~ agentname,
        is.na(agentname) & !is.na(address) ~ address,
        TRUE ~ NA_character_
      )
    ) %>%
    rename(
      fullname = agentname
    ) %>%
    select(c(id_corp, fullname, address, name_address)) %>%
    distinct()
  
  # law_from_agents <- agents %>%
  #   filter(lawyer) %>%
  #   select(-c(lawyer))
  
  rm(agents)
  
  log_message("Deduplicating individuals and isolating companies.")
  # Initiate individual deduplication.
  inds <- load_inds(file.path(DATA_DIR, INDS))
  
  inds <- inds %>%
    filter(id_corp %in% corps_list) %>%
    std_flow_strings(c("firstname", "lastname", "busaddr1", "resaddr1")) %>%
    std_zip("agentpostalcode") %>% 
    std_flow_addresses("busaddr1", "resaddr1") %>%
    std_cities("agentcity") %>%
    std_flow_names("firstname", "lastname") %>%
    filter(
      if_any(
        c(firstname, lastname, busaddr1, resaddr1), ~ !is.na(.)
      )
    ) %>%
    # Remove 'C / O' (care of) prefix.
    std_remove_co(
      c("resaddr1", "busaddr1")
    ) %>%
    std_trailingwords(c("resaddr1", "busaddr1")) %>%
    mutate(
      # Concatenate fullname.
      fullname = case_when(
        !is.na(firstname) & is.na(lastname) ~ firstname,
        is.na(firstname) & !is.na(lastname) ~ lastname,
        is.na(firstname) & is.na(lastname) ~ NA_character_,
        TRUE ~ str_c(firstname, lastname, sep = " "),
      ),
      # Choose address.
      address = case_when(
        !is.na(busaddr1) & is.na(resaddr1)
        ~ busaddr1,
        is.na(busaddr1) & !is.na(resaddr1)
        ~ resaddr1,
        is.na(busaddr1) & is.na(resaddr1)
        ~ NA_character_,
        TRUE ~ busaddr1
      ),
      co = case_when(
        str_detect(address, "LLC[ $]")
        ~ str_extract(address, "^.*LLC")
      ),
      address = case_when(
        str_detect(address, "LLC[ $]")
        ~ na_if(str_extract(address, "(?<=LLC ).*$"), ""),
        TRUE ~ address
      ),
      name_address = case_when(
        !is.na(fullname) & !is.na(address) ~
          str_c(fullname, address, sep = " "),
        !is.na(fullname) & is.na(address) ~
          fullname,
        is.na(fullname) & !is.na(address) ~
          address,
        TRUE ~ NA_character_
      )
    ) %>%
    flag_lawyers(
      c("name_address")
    ) %>%
    left_join(
      corps %>%
        filter(activeflag == "Y") %>%
        select(c(id_corp, entityname)) %>%
        rename(id_fullname = id_corp),
      by = c("fullname" = "entityname")
    ) %>%
    left_join(
      corps %>%
        filter(activeflag == "Y") %>%
        select(c(id_corp, entityname)) %>%
        rename(id_address = id_corp),
      by = c("address" = "entityname")
    ) %>%
    left_join(
      corps %>%
        filter(activeflag == "Y") %>%
        select(c(id_corp, entityname)) %>%
        rename(id_co = id_corp),
      by = c("co" = "entityname")
    )
  
  corps_from_inds <- inds %>%
    filter(
      (!is.na(id_fullname) | !is.na(id_address) | !is.na(id_co)) 
      & !lawyer
    ) %>%
    pivot_longer(
      cols = c(id_fullname, id_address, id_co),
      names_to = NULL,
      values_to = "id_link",
      values_drop_na = TRUE
    ) %>%
    select(c(id_corp, id_link)) %>%
    distinct()
  
  corps_list <- c(
    corps_from_inds %>%
      pull(id_corp) %>%
      unique(),
    corps_from_inds %>%
      pull(id_link) %>%
      unique(),
    corps_list) %>%
    unique()
  
  inds_from_inds <- inds %>%
    filter(
      (is.na(id_fullname) & is.na(id_address) & is.na(id_co)) 
      & !lawyer
    ) %>%
    select(c(id_corp, fullname, address)) %>%
    mutate(
      name_address = case_when(
        !is.na(fullname) & !is.na(address) 
        ~ str_c(fullname, address, sep = " "),
        !is.na(fullname) & is.na(address) ~ fullname,
        is.na(fullname) & !is.na(address) ~ address,
        TRUE ~ NA_character_
      )
    ) %>%
    select(c(id_corp, fullname, address, name_address)) %>%
    distinct()
  
  log_message("Parsing data into nodes and vertices...")
  
  inds_links <- inds_from_inds %>%
    bind_rows(inds_from_agents) %>%
    distinct() %>%
    # Cosine-similarity based deduplication on the same.
    dedupe_naive(str_field = "name_address") %>%
    dedupe_cosine(
      str_field = "name_address",
      group = "group_cosine",
      thresh = 0.85
    ) %>%
    mutate(
      id_link = case_when(
        !is.na(group_cosine) ~ group_cosine,
        TRUE ~ group_naive
      )
    ) %>%
    select(-c(group_cosine, group_naive)) %>%
    # distinct(id_corp, group) %>%
    left_join(
      dedupe_text_mode(
        ., 
        "id_link", 
        c("fullname", "address", "name_address")
      ) %>%
        rename(
          fullname_simp = fullname,
          address_simp = address,
          name_address_simp = name_address
        ),
      by = "id_link"
    )
  
  corps_links <- corps_from_inds %>%
    rbind(corps_from_agents) %>%
    distinct() %>%
    mutate(relation = "LINKED_TO")
  
  inds_nodes <- inds_links %>%
    rename(
      id = id_link,
    ) %>%
    select(id, fullname_simp, address_simp, name_address_simp) %>%
    distinct() %>%
    mutate(
      label = "inds"
    )
  
  corps_nodes <- corps %>%
    rename(
      id = id_corp
    ) %>%
    filter(id %in% corps_list) %>%
    select(c(id, entityname)) %>%
    mutate(
      label = "corps"
    )
  
  edges <- inds_links %>%
    select(id_link, id_corp) %>%
    distinct() %>%
    mutate(
      relation = "AGENT_OF"
    ) %>%
    bind_rows(corps_links) %>%
    # Write pipe-delimited text file of edges.
    write_delim(
      file.path(
        RESULTS_DIR, 
        paste(EDGES_OUT_NAME, "csv", sep = ".")
      ),
      delim = "|", quote = "needed"
    )
  
  nodes <- bind_rows(inds_nodes, corps_nodes) %>%
    # Write pipe-delimited text file of corporations.
    write_delim(
      file.path(
        RESULTS_DIR, 
        paste(NODES_OUT_NAME, "csv", sep = ".")
      ),
      delim = "|", quote = "needed"
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
    left_join(
      community,
      by = c("id" = "id")
    ) %>%
    # Write pipe-delimited text file of corporations.
    write_delim(
      file.path(
        RESULTS_DIR, 
        paste(CORPS_OUT_NAME, "csv", sep = ".")
      ),
      delim = "|", quote = "needed"
    )
  
  log_message("Writing indiviudals nodes to delimited file...")
  inds_nodes <- inds_nodes %>%
    left_join(
      community,
      by = c("id" = "id")
    ) %>%
    # Write pipe-delimited text file of individuals.
    write_delim(
      file.path(
        RESULTS_DIR, 
        paste(INDS_OUT_NAME, "csv", sep = ".")
      ),
      delim = "|", quote = "needed"
    )
  
  log_message("Writing deduplicated ownership to delimited file...")
  owners <- owners %>%
    # Join
    left_join(
      community,
      by = c("id_corp" = "id")
    ) %>%
    # Assign id based on priority.
    mutate(
      group = case_when(
        !is.na(group_network) ~ group_network,
        !is.na(group) ~ group,
        TRUE ~ NA_character_
      )
    ) %>%
    select(-c(group_network)) %>%
    group_by(group) %>%
    mutate(
      count = n()
    ) %>%
    ungroup() %>%
    # Write pipe-delimited text file of ownership.
    write_delim(
      file.path(
        RESULTS_DIR, 
        paste(OWNERS_OUT_NAME, "csv", sep = ".")
      ),
      delim = "|", quote = "needed"
    )
  
  log_message("Finishing up.")
  # Save RData image.
  save.image(
    file.path(RESULTS_DIR, paste(RDATA_OUT_NAME, "RData", sep = "."))
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