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
ASSESS_OUT_NAME <- "assess"
CORPS_OUT_NAME <- "corps"
INDS_OUT_NAME <- "inds"
# Name of RData image.
RDATA_OUT_NAME <- "results"
# CSV containing Boston Neighborhoods
BOSTON_NEIGHBORHOODS <- "bos_neigh.csv"

run <- function(subset = "test", return_results = TRUE){
  #' Run complete owner deduplication process.
  #' 
  #' @param subset If value is `"test"`, processes only Somerville. If value is `"hns"`, processes only HNS municipalities. If value is `"all"`, runs entire state. Otherwise, stops and generates an error.
  #' @param store_results If `TRUE`, return results in a named list. If `FALSE`, return nothing. In either case, results are output to delimited text and `*.RData` files.
  #' @returns If `store_results` is `TRUE`, a named list of dataframes. Else, nothing.
  #' @export
  log_message("Reading and processing parcels and assessors table from GDB.")
  # Load assessors table.
  # DATA_DIR and ASSESS_GDB set globally above.
  if (subset == "hns") {
    town_ids <- read_csv(file.path(DATA_DIR, paste(MUNI_CSV, "csv", sep = "."))) %>%
      pull(town_id) %>%
      paste(collapse = ", ")
  } else if (subset == "test") {
    town_ids <- 274
  } else if (subset == "all") {
    town_ids <- NA
  } else {
    stop("Invalid subset.")
  }
  
  assess <- load_assess(file.path(DATA_DIR, ASSESS_GDB), town_ids = town_ids) %>%
    # Run string standardization procedures.
    # If census = TRUE, will join to parcels and link to census geographies.
    # Note that ^ this ^ adds a somewhat costly load-merge procedure.
    process_assess(census = FALSE, town_ids = town_ids)
  # Create and open log file with timestamp name.
  lf <- log_open(file.path("logs", format(Sys.time(), "%Y-%m-%d_%H%M%S")))
  
  log_message("Reading and processing corporations from delimited text.")
  # Load corporations from delimited text.
  # DATA_DIR and CORPS set globally above.
  corps <- load_corps(file.path(DATA_DIR, CORPS)) %>%
    # Run string standardization procedures on entity name.
    process_corps(id = "id_corp", name = "entityname")
  
  log_message("Deduplicating assessor's ownership information")
  # Initiate assessing deduplication.
  assess_dedupe <- assess %>%
    select(c(
      prop_id, loc_id, town_id, fy, site_addr, city, owner1, 
      own_addr, own_city, own_state, own_zip, 
      name_address)
      ) %>%
    # Naive deduplication on prepared, concatenated name and address.
    dedupe_naive(str_field = "name_address") %>%
    # Cosine-similarity-based deduplication.
    dedupe_cosine(
      str_field = "name_address", 
      group = "group_cosine", 
      thresh = 0.75
    ) %>%
    # Match prepared name to prepared name in corps file.
    # ====
    # Note: This actually yields more matches than strict matching
    # because all cosine-similarity identified groups are assumed to
    # link to the same company.
    # ===
    # TODO: Explore replacement with fuzzy matching.
    merge_assess_corp(
      select(corps, c(id_corp, entityname)), 
      by = c("owner1" = "entityname"), 
      group = "group_cosine", 
      id_c = "id_corp"
    )
  
  log_message("Deduplicating individuals associated with companies.")
  # Initiate individual deduplication.
  inds <- load_inds(file.path(DATA_DIR, INDS)) %>%
    process_inds(
    # Here, we load agents, which appear in the corporations table,
    # and bind them to the individuals table.
    a_df = load_agents(
      corps, 
      cols = c("id_corp", "agentname", "agentaddr1", "agentaddr2"), 
      drop_na_col = "agentname"
    ), 
    # This filters individuals to those who belong to corporations
    # matched to assessors records.
    owners = assess_dedupe %>%
      select(id_corp) %>%
      drop_na() %>%
      distinct()
    ) %>%
    # Filter rows where name_address is blank.
    filter(!is.na(name_address)) %>%
    # Naive deduplication on basis of prepared concatenated name/address.
    dedupe_naive(str_field = "name_address") %>%
    # Cosine-similarity based deduplication on the same.
    dedupe_cosine(
      str_field = "name_address",
      group = "group_cosine",
      thresh = 0.85
    ) %>%
    # Assign ID---where cosine match, use this id as id.
    mutate(
      id = case_when(
        !is.na(group_cosine) ~ group_cosine,
        TRUE ~ group_naive
      )
    ) %>%
    # Join list of individuals to simplified name.
    # Name is based on the "modal text", i.e., text
    # that appears most frequently in a given group.
    left_join(
      dedupe_text_mode(., "id", c("name_address")) %>%
        rename(name_address_simp = name_address),
      by = "id"
    ) %>%
    select(c(id_corp, id, name_address_simp)) %>%
    # Write pipe-delimited text file of individuals.
    write_delim(
      file.path(RESULTS_DIR, paste(INDS_OUT_NAME, "csv", sep = ".")), 
      delim = "|", quote = "needed"
      )
  
  log_message("Identifying ownership network.")
  # Initiate network-based community detection.
  assess_network <- assess_dedupe %>%
    # Join 
    left_join(
      inds %>% 
        distinct(id_corp, id) %>%
        # Identify network communities of corporations
        # linked by individuals.
        dedupe_community(
          prefix = "network", 
          name = "id_corp", 
          membership = "group_network"
        ), 
      by = c("id_corp" = "id_corp")
    ) %>%
    # Assign id based on priority.
    mutate(
      id = case_when(
        !is.na(group_network) ~ group_network,
        !is.na(group_cosine) ~ group_cosine,
        TRUE ~ group_naive
      )
    )
  
  log_message("Merging network-identified companies with assessors table.")
  # Simplify names of corporate entities using modal text approach
  # described above.
  corps_simp <- assess_network %>%
    dedupe_text_mode("id", c("owner1", "own_addr", "own_city", "own_state", "own_zip")) %>%
    rename(
      owner1_simp = owner1, 
      own_addr_simp = own_addr, 
      own_city_simp = own_city, 
      own_state_simp = own_state, 
      own_zip_simp = own_zip
      ) %>%
    # Write pipe-delimited text file of corps.
    write_delim(
      file.path(RESULTS_DIR, paste(CORPS_OUT_NAME, "csv", sep = ".")), 
      delim = "|", 
      quote = "needed"
      )
  
  # Join assessors records to simplified corporate names.
  assess <- assess_network %>%
    left_join(
      corps_simp,
      by = "id"
    ) %>%
    select(-c(group_naive, group_cosine, id_corp, group_network)) %>%
    group_by(id) %>%
    mutate(
      count = n()
    ) %>%
    ungroup() %>%
    # Write pipe-delimited text file of assessors records.
    write_delim(
      file.path(RESULTS_DIR, paste(ASSESS_OUT_NAME, "csv", sep = ".")), 
      delim = "|", 
      quote = "needed"
      )
  
  log_message("Finishing up.")
  # Save RData image.
  save.image(file.path(RESULTS_DIR, paste(RDATA_OUT_NAME, "RData", sep = ".")))
  # Close logs.
  log_close()
  # If directed to store results, return them in a named list.
  if (return_results == TRUE) {
    list("assess" = assess, "inds" = inds, "corps" = corps_simp)
  }
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  run(subset = "all", return_results = FALSE)
}
