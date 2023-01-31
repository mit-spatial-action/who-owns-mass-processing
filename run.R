source("parcel_process.R")

RESULTS_DIR <- "results"
DATA_DIR <- "data"
INDS <- "CSC_CorporationsIndividualExport_VB.txt"
CORPS <- "CSC_CorpDataExports_VB.txt"
PARCELS_GDB <- "MassGIS_L3_Parcels.gdb"
ASSESS_OUT_NAME <- "assess"
CORPS_OUT_NAME <- "corps"
INDS_OUT_NAME <- "inds"
RDATA_OUT_NAME <- "results"

run <- function(test = TRUE, store_results = TRUE){
  lf <- log_open(file.path("logs", format(Sys.time(), "%Y-%m-%d_%H%M%S")))
  
  log_message("Reading and processing corporations from delimited text.")
  corps <- file.path(DATA_DIR, CORPS) %>%
    load_corps() %>%
    process_corps(id = "id_corp", name = "entityname")
  
  log_message("Reading and processing parcels and assessors table from GDB.")
  parc <- load_assess_parc(file.path(DATA_DIR, PARCELS_GDB), test = test) %>%
    process_parc(census = FALSE, test = test)
  
  log_message("Deduplicating assessor's ownership information")
  parc_dedupe <- parc %>%
    select(c(loc_id, owner1, own_addr, name_address)) %>%
    dedupe_naive(str_field = "name_address") %>%
    dedupe_cosine(
      str_field = "name_address", 
      group = "group_cosine", 
      thresh = 0.75
    ) %>%
    merge_parcel_corp(
      select(corps, c(id_corp, entityname)), 
      by = c("owner1" = "entityname"), 
      group = "group_cosine", 
      id_c = "id_corp"
    )
  
  log_message("Deduplicating individuals associated with companies.")
  inds <- process_inds(
    i_df = load_inds(file.path(DATA_DIR, INDS)),
    # Here, we load agents, which appear in the corporations table.
    a_df = load_agents(
      corps, 
      cols = c("id_corp", "agentname", "agentaddr1", "agentaddr2"), 
      drop_na_col = "agentname"
    ), 
    owners = parc_dedupe %>%
      select(id_corp) %>%
      drop_na() %>%
      distinct()
    ) %>%
    dedupe_naive(str_field = "name_address") %>%
    dedupe_cosine(
      str_field = "name_address",
      group = "group_cosine",
      thresh = 0.85
    ) %>%
    mutate(
      id = case_when(
        !is.na(group_cosine) ~ group_cosine,
        TRUE ~ group_naive
      )
    ) %>%
    filter(!is.na(name_address)) %>%
    distinct(id_corp, name_address, id) %>%
    left_join(
      dedupe_text_mode(., "id", c("name_address")) %>%
        rename(name_address_simp = name_address),
      by = "id"
    ) %>%
    select(-c(count)) %>%
    write_delim(file.path(RESULTS_DIR, paste(INDS_OUT_NAME, "txt", sep = ".")), delim = "|", quote = "needed")
  
  log_message("Identifying ownership network.")
  parc_network <- parc_dedupe %>%
    left_join(
      inds %>% 
        distinct(id_corp, id) %>%
        dedupe_community(
          prefix = "network", 
          name = "id_corp", 
          membership = "group_network"
        ), 
      by = c("id_corp" = "id_corp")
    ) %>%
    mutate(
      id = case_when(
        !is.na(group_network) ~ group_network,
        !is.na(group_cosine) ~ group_cosine,
        TRUE ~ group_naive
      )
    )
  
  log_message("Merging network-identified companies with assessors table.")
  corps_simp <- parc_network %>%
    dedupe_text_mode("id", c("owner1", "own_addr")) %>%
    rename(owner1_simp = owner1, own_addr_simp = own_addr) %>%
    write_delim(file.path(RESULTS_DIR, paste(CORPS_OUT_NAME, "txt", sep = ".")), delim = "|", quote = "needed")
  
  assess <- parc_network %>%
    left_join(
      corps_simp,
      by = "id"
    ) %>%
    write_delim(file.path(RESULTS_DIR, paste(ASSESS_OUT_NAME, "txt", sep = ".")), delim = "|", quote = "needed")
  
  log_message("Finishing up.")
  save.image(file.path(RESULTS_DIR, paste(RDATA_OUT_NAME, "RData", sep = ".")))
  log_close()
  if (store_results == TRUE) {
    list("assess" = assess, "inds" = inds, "corps" = corps_simp)
  }
}

if (!interactive()) {
  run(test = FALSE, store_results = FALSE)
}
