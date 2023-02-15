source("deduplicate-owners.R")

# Name of directory in which input data is stored.
DATA_DIR <- "data"
# Name of directory in which results are written.
RESULTS_DIR <- "results"
# Filename of delimited text file containing individuals.
INDS <- "CSC_CorporationsIndividualExport_VB.txt"
# Filename of delimited text file containing corporate entities.
CORPS <- "CSC_CorpDataExports_VB.txt"
# Filename of MassGIS Parcels geodatabase.
ASSESS_GDB <- "MassGIS_L3_Parcels.gdb"
# Name of CSV containing limited collection of HNS municipalities
MUNI_CSV <- "hns_munis"
# Name of delimited text output files.
OWNERS_OUT_NAME <- "owners"
CORPS_OUT_NAME <- "corps"
INDS_OUT_NAME <- "inds"
COMMUNITY_OUT_NAME <- "community"
# Name of RData image.
RDATA_OUT_NAME <- "results"

run <- function(subset = "test", return_results = TRUE){
  #' Run complete owner deduplication process.
  #' 
  #' @param subset If value is `"test"`, processes only Somerville and Cambridge. If value is `"hns"`, processes only HNS municipalities. If value is `"all"`, runs entire state. Otherwise, stops and generates an error.
  #' @param store_results If `TRUE`, return results in a named list. If `FALSE`, return nothing. In either case, results are output to delimited text and `*.RData` files.
  #' @returns If `store_results` is `TRUE`, a named list of dataframes. Else, nothing.
  #' @export
  
  # Create and open log file with timestamp name.
  lf <- log_open(file.path("logs", format(Sys.time(), "%Y-%m-%d_%H%M%S")))
  
  log_message("Reading assessors table from GDB...")
  # Load assessors table.
  # DATA_DIR and ASSESS_GDB set globally above.
  if (subset == "hns") {
    town_ids <- read_csv(file.path(DATA_DIR, paste(MUNI_CSV, "csv", sep = "."))) %>%
      pull(town_id) %>%
      paste(collapse = ", ")
  } else if (subset == "test") {
    town_ids <- c(274, 49)
  } else if (subset == "all") {
    town_ids <- FALSE
  } else {
    stop("Invalid subset.")
  }
  
  log_message("Reading assessors table from GDB...")
  assess <- load_assess(file.path(DATA_DIR, ASSESS_GDB), town_ids = town_ids) 
  
  log_message("Processing assessors records and flagging owner-occupancy...")
  assess <- assess %>%
    # Run string standardization procedures.
    process_records(
      cols = c("owner1", "own_addr", "site_addr"),
      keep_cols = c("prop_id", "loc_id", "fy", "town_id", "own_state"), 
      zip_cols = c("own_zip"),
      city_cols = c("own_city"),
      name_cols = c("owner1", "own_addr"),
      addr_cols = c("own_addr", "site_addr")
    ) %>%
    # Extract 'care of' entities to co and remove from own_addr.
    mutate(
      co = case_when(
        str_detect(own_addr, "C / O") ~ str_extract(own_addr, "(?<=C / O ).*$")
      ),
      own_addr = case_when(
        str_detect(own_addr, "C / O") ~ na_if(str_extract(own_addr, ".*(?= ?C / O )"), ""),
        TRUE ~ own_addr
      )
    ) %>%
    # Flag owner-occupied properties on the basis of standardized addresses.
    mutate(
      ooc = case_when(
        own_addr == site_addr ~ TRUE,
        TRUE ~ FALSE
      )
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
    process_records(
      cols = c("entityname", "agentname", "agentaddr1", "agentaddr2"),
      zip_cols = c("agentpostalcode"),
      city_cols = c("agentcity"),
      addr_cols = c("agentaddr1", "agentaddr2"),
      # This is inclusive because agents are a mess.
      name_cols = c("entityname", "agentname", "agentaddr1", "agentaddr2"),
      keep_cols = c("id_corp", "activeflag")
    ) %>%
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
  
  log_message("Identifying individual agents and matching company agents to corps table, distinguishing between lawyers and non-lawyers...")
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
        !is.na(agentaddr1) & !is.na(agentaddr2) ~ str_c(agentaddr1, agentaddr2, sep = " "),
        !is.na(agentaddr1) & is.na(agentaddr2) ~ agentaddr1,
        is.na(agentaddr1) & !is.na(agentaddr2) ~ agentaddr2,
        TRUE ~ NA_character_
      ),
      name_address = case_when(
        !is.na(agentname) & !is.na(address) ~ str_c(agentname, address, sep = " "),
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
    process_records(
      cols = c("firstname", "lastname", "busaddr1", "resaddr1"),
      addr_cols = c("busaddr1", "resaddr1"),
      name_cols = c("firstname", "lastname"),
      keep_cols = c("id_corp")
    ) %>%
    filter(if_any(c(firstname, lastname, busaddr1, resaddr1), ~ !is.na(.))) %>%
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
        !is.na(busaddr1) & is.na(resaddr1) ~ busaddr1,
        is.na(busaddr1) & !is.na(resaddr1) ~ resaddr1,
        is.na(busaddr1) & is.na(resaddr1) ~ NA_character_,
        TRUE ~ busaddr1
      ),
      co = case_when(
        str_detect(address, "LLC[ $]") ~ str_extract(address, "^.*LLC")
      ),
      address = case_when(
        str_detect(address, "LLC[ $]") ~ na_if(str_extract(address, "(?<=LLC ).*$"), ""),
        TRUE ~ address
      ),
      name_address = case_when(
        !is.na(fullname) & !is.na(address) ~ str_c(fullname, address, sep = " "),
        !is.na(fullname) & is.na(address) ~ fullname,
        is.na(fullname) & !is.na(address) ~ address,
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
    filter((!is.na(id_fullname) | !is.na(id_address) | !is.na(id_co)) & !lawyer) %>%
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
    filter((is.na(id_fullname) & is.na(id_address) & is.na(id_co)) & !lawyer) %>%
    select(c(id_corp, fullname, address)) %>%
    mutate(
      name_address = case_when(
        !is.na(fullname) & !is.na(address) ~ str_c(fullname, address, sep = " "),
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
      dedupe_text_mode(., "id_link", c("fullname", "address", "name_address")) %>%
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
    bind_rows(corps_links)
  
  log_message("Identifying community and writing to delimited file...")
  community <- edges %>%
    dedupe_community(
      nodes = bind_rows(inds_nodes, corps_nodes),
      prefix = "network", 
      name = "id", 
      membership = "group_network"
    ) %>%
    # Write pipe-delimited text file of edges.
    write_delim(
      file.path(RESULTS_DIR, paste(COMMUNITY_OUT_NAME, "csv", sep = ".")),
      delim = "|", quote = "needed"
    )
  
  log_message("Writing corporate nodes to delimited file...")
  corps_nodes <- corps_nodes %>%
    left_join(
      community,
      by = c("id" = "id")
    ) %>%
    # Write pipe-delimited text file of corporations.
    write_delim(
      file.path(RESULTS_DIR, paste(CORPS_OUT_NAME, "csv", sep = ".")),
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
      file.path(RESULTS_DIR, paste(INDS_OUT_NAME, "csv", sep = ".")),
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
      file.path(RESULTS_DIR, paste(OWNERS_OUT_NAME, "csv", sep = ".")),
      delim = "|", quote = "needed"
    )
  
  log_message("Finishing up.")
  # Save RData image.
  save.image(file.path(RESULTS_DIR, paste(RDATA_OUT_NAME, "RData", sep = ".")))
  # Close logs.
  log_close()
  # If directed to store results, return them in a named list.
  if (return_results == TRUE) {
    list("owners" = owners, "inds" = inds_nodes, "corps" = corps_nodes, "community" = community)
  }
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  run(subset = "all", return_results = FALSE)
}
