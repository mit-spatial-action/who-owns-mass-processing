source("std_helpers.R")
# Tidyverse components.
library(dplyr)
library(tidyr)
library(readr)
library(purrr)
library(stringr)
library(logr)
library(tidyselect)
# Spatial support.
library(sf)

# For Cosine Similarity statistics.
library(quanteda)
library(quanteda.textstats)

# For network-based community detection.
library(igraph)

# Name of directory in which input data is stored.
DATA_DIR <- "data"
# CSV containing Boston Neighborhoods
BOS_NBHD <- "bos_neigh.csv"


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


process_records <- function(df,
                            cols,
                            zip_cols = FALSE,
                            city_cols = FALSE,
                            addr_cols = FALSE,
                            name_cols = FALSE,
                            keep_cols = FALSE) {
  #' Run a series of string standardizing functions.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param cols Column or columns to be processed by
  #'  `std_directions`, `std_andslash`, `std_remove_special`,
  #'  and `std_the`.
  #' @param zip_cols Column or columns to be processed by
  #'  `std_directions`, `std_andslash`, `std_remove_special`,
  #'  and `std_the`.
  #' @returns A dataframe.
  #' @export
  all_cols <- cols
  if (!isFALSE(keep_cols)) {
    all_cols <- c(keep_cols, all_cols)
  } else {
    keep_cols <- c()
  }
  if (!isFALSE(zip_cols)) {
    all_cols <- c(all_cols, zip_cols)
  }
  if (!isFALSE(city_cols)) {
    all_cols <- c(all_cols, city_cols)
  }
  if (!isFALSE(addr_cols)) {
    all_cols <- c(all_cols, addr_cols)
  }
  if (!isFALSE(name_cols)) {
    all_cols <- c(all_cols, name_cols)
  }
  all_cols <- unique(all_cols)
  df <- df %>%
    select(
      all_of(all_cols)
    ) %>%
    std_uppercase_all(keep_cols) %>%
    std_andslash(cols) %>%
    std_remove_special(cols) %>%
    std_replace_blank(keep_cols) %>%
    std_the(cols) %>%
    std_massachusetts(cols) %>%
    std_small_numbers(cols) %>%
    std_trailingwords(cols)
  if (!isFALSE(zip_cols)) {
    df <- df %>%
      std_simplify_zip(zip_cols)
  }
  if (!isFALSE(city_cols)) {
    df <- df %>%
      std_cities(city_cols)
  }
  if (!isFALSE(addr_cols)) {
    df <- df %>%
      std_street_types(addr_cols) %>%
      std_simplify_address(addr_cols) %>%
      std_directions(addr_cols) %>%
      std_hyphenated_numbers(addr_cols) %>%
      std_onewordaddress(addr_cols)
  }
  if (!isFALSE(name_cols)) {
    df <- df %>%
      std_corp_types(name_cols) %>%
      std_corp_rm_sys(name_cols)
  }
  df
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


assess_res_filter <- function(df, col) {
  #' Filter assessors records by MA residential use codes.
  #' Massachusetts Codebook
  #' https://www.mass.gov/files/documents/2016/08/wr/classificationcodebook.pdf
  #' Boston Codebook
  #' https://www.cityofboston.gov/Images_Documents/MA_OCCcodes_tcm3-16189.pdf
  #' @param df A dataframe.
  #' @param cols The columns containing the use codes.
  #' @returns A dataframe.
  #' @export
  df %>%
    filter(
      str_detect(
        get({{ col }}), paste(c(
          # Residential use codes.
          "^0?10[13-59][0-9A-Z]?$",
          # Apartments.
          "^0?11[1-5][0-9A-Z]?$",
          # Subsidized Housing.
          "^0?12[5-7]",
          # Mixed use codes.
          "^0(1[0-9]|[1-9]1)[A-Z]?$",
          # Boston Housing Authority.
          "^908",
          # Housing authority outside Boston.
          "^0?970",
          # Section 121-A Property...
          # (Tax-exempt 'blight' redevelopment.)
          # in Boston
          "^0?907",
          # outside Boston.
          "^990",
          # 'Other' Housing.
          "^959",
          "^000"
        ), collapse = "|")
        )
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

load_assess <- function(path = ".", town_ids = FALSE, write=TRUE) {
  #' Load assessing table from MassGIS geodatabase.
  #' https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/gdbs/l3parcels/MassGIS_L3_Parcels_gdb.zip
  #'
  #' @param path Path to MassGIS Parcels GDB.
  #' @param town_ids list of town IDs
  #' @param write write a new file or use the written file in RESULTS_DIR
  #' @export
  if (file.exists(file.path(RESULTS_DIR, paste(ASSESS_OUT_NAME, "csv", sep = "."))) & write == FALSE) {
    read_delim(
      file.path(RESULTS_DIR, paste(ASSESS_OUT_NAME, "csv", sep = ".")),
      delim = "|", quote = "needed"
    )
  } else {
    assess_query <- "SELECT * FROM L3_ASSESS"
    if (!isFALSE(town_ids)) {
      assess_query <- paste(
        assess_query,
        "WHERE TOWN_ID IN (",
        paste(town_ids, collapse = ", "),
        ")"
        )
    }
    st_read(
        path,
        query = assess_query
      ) %>%
      rename_with(str_to_lower) %>%
      assess_res_filter("use_code")
  }
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

log_message <- function(status) {
    #' Print message to `logr` logs.
    #'
    #' @param status Status to print.
    #' @returns Nothing.
    #' @export
    time <- format(Sys.time(), "%a %b %d %X %Y")
    message <- paste(time, status, sep = ": ")
    log_print(message)
}
