# Tidyverse components.
library(dplyr)
library(tidyr)
library(readr)
library(purrr)
library(stringr)
library(logr)
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
BOSTON_NEIGHBORHOODS <- "bos_neigh.csv"
BOSTON_NEIGHS <- std_uppercase_all(read.csv(file.path(DATA_DIR, BOSTON_NEIGHBORHOODS)))

std_uppercase_all <- function(df, except){
  #' Uppercase all strings
  #' 
  #' @param df A dataframe containing only string datatypes.
  #' @param except Columns to remain untouched.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(-c({{except}}), str_to_upper),
    )
}

std_directions <- function(df, except) {
  #' Standardizes abbreviated cardinal directions.
  #' 
  #' @param df A dataframe containing only string datatypes.
  #' @param except Columns to remain untouched.
  #' @returns A dataframe.
  #' @export
  replace <- c(
    # Directions
    "(^| )N.? " = " NORTH ", 
    "(^| )N.?W.? " = " NORTHWEST ",
    "(^| )N.?E.? " = " NORTHEAST ",
    "(^| )S.? " = " SOUTH ", 
    "(^| )S.?W.? " = " SOUTHWEST ",
    "(^| )S.?E.? " = " SOUTHEAST ",
    "(^| )E.? " = " EAST", 
    "(^| )W.? " = " WEST "
  )
  df %>%
    mutate(
      across(
        -c({{except}}), 
        ~str_trim(str_replace_all(., replace))
      )
    )
}

std_andslash <- function(df, except) {
  #' Standardizes slashes to have a space on either side and
  #' replaces all instances of an ampersand with the word "AND"
  #' 
  #' @param df A dataframe containing only string datatypes.
  #' @param except Columns to remain untouched.
  #' @returns A dataframe.
  #' @export
  replace <- c(
    # Put space around slashes
    " ?/ ?" =" / ", 
    # Replace & with AND
    " ?& ?" = " AND "
  )
  df %>%
    mutate(
      across(
        -c({{except}}), 
        ~str_replace_all(., replace)
      )
    )
}

std_remove_special <- function(df, except){
  #' Removes all special characters from columns, except slash
  #' 
  #' @param df A dataframe containing only string datatypes.
  #' @param except Columns to remain untouched.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(-c({{except}}), ~gsub("[^[:alnum:][:space:]/]", "", .))
    )
}

std_remove_middle_initial <- function(df, name_col) {
  #' Replace middle inital when formated like "ERIC R HUNTLEY"
  #' 
  #' @param df A dataframe.
  #' @param except Column from which middle initials should be replaced.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(
        c({{name_col}}), 
        ~str_replace(., "(?<=[A-Z] )[A-Z] (?=[A-Z])", "")
      )
    )
}

std_replace_blank <- function(df, except) {
  #' Replace blank string with NA.
  #' 
  #' @param df A dataframe containing only string datatypes.
  #' @param except Columns to remain untouched.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(-c({{except}}), ~case_when(
        str_detect(., "^X+$|^NONE$|^UNKNOWN$|^N$") ~ NA_character_,
        TRUE ~ str_squish(.)
      )
      )
    )
}

std_the <- function(df, except){
  #' Takes appended ", THE" and places it at the front.
  #' 
  #' @param df A dataframe containing only string datatypes.
  #' @param except Columns to remain untouched.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across(-c({{except}}), ~case_when(
        substr(., nchar(.) - 3, nchar(.)) == " THE" ~ paste("THE", substr(., 1, nchar(.) - 3)),
        TRUE ~ .
      )
      )
    )
}

std_char <- function(df, id, except){
  #' Run a series of string standardizing functions.
  #' 
  #' @param df A dataframe containing only string datatypes.
  #' @param id The column containing the id which will remain untouched by all functions.
  #' @param except Columns that should be left alone by more descructive functions.
  #' @returns A dataframe.
  #' @export
  df %>%
    std_uppercase_all({{id}}) %>%
    std_directions(c({{id}}, {{except}})) %>%
    std_andslash({{id}}) %>%
    std_remove_special({{id}}) %>%
    std_replace_blank({{id}}) %>%
    std_the({{id}})
}

std_small_numbers <- function(df, cols) {
  #' Convert alphanumeric small numbers to numbers.
  #' 
  #' @param df A dataframe.
  #' @param cols The columns to be standardized.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across({{cols}}, ~ str_replace_all(
        ., 
        c(
          "(?<=^| )ZERO(?= |$)" = "0",
          "(?<=^| )ONE(?= |$)" = "1",
          "(?<=^| )TWO(?= |$)" = "2",
          "(?<=^| )THREE(?= |$)" = "3",
          "(?<=^| )FOUR(?= |$)" = "4",
          "(?<=^| )FIVE(?= |$)" = "5",
          "(?<=^| )SIX(?= |$)" = "6", 
          "(?<=^| )SEVEN(?= |$)" = "7",
          "(?<=^| )EIGHT(?= |$)" = "8",
          "(?<=^| )NINE(?= |$)" = "9",
          "(?<=^| )TEN(?= |$)" = "10"
        )
      )
      )
    )
}

std_street_types <- function(df, addr_col) {
  #' Standardize street types.
  #' 
  #' @param df A dataframe.
  #' @param addr_col The column containing the address to be standardized.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across({{addr_col}}, ~ str_replace_all(
        ., 
        c(
          "(?<= )ST(?=$|\\s|\\.)" = "STREET", 
          "(?<= )AVE?(?=$|\\s|\\.)" = "AVENUE", 
          "(?<= )LA?N(?=$|\\s|\\.)" = "LANE",
          "(?<= )BLV?R?D?(?=$|\\s|\\.)" = "BOULEVARD",
          "(?<= )PR?KWA?Y(?=$|\\s|\\.)" = "PARKWAY",
          "(?<= )DRV?(?=$|\\s|\\.)" = "DRIVE",
          "(?<= )RD(?=$|\\s|\\.)" = "ROAD",
          "(?<= )TE?[R]+C?E?(?=$|\\s|\\.)" = "TERRACE",
          "(?<= )PLC?E?(?=$|\\s|\\.)" = "PLACE",
          "(?<= )(CI?RC?)(?=$|\\s|\\.)" = "CIRCLE",
          "(?<= )A[L]+E?Y(?=$|\\s|\\.)" = "ALLEY",
          "(?<= )SQR?(?=$|\\s|\\.)" = "SQUARE",
          "(?<= )HG?WY(?=$|\\s|\\.)" = "HIGHWAY",
          "(?<= )FR?WY(?=$|\\s|\\.)" = "FREEWAY",
          "(?<= )CR?T(?=$|\\s|\\.)" = "COURT",
          "(?<= )PLZ?(?=$|\\s|\\.)" = "PLAZA",
          "(?<= )W[HR]+F(?=$|\\s|\\.)" = "WHARF",
          "(?<= |^)P.? ?O.?[ ]+BO?X(?=$|\\s|\\.)" = "PO BOX"
        )
      )
      )
    )
}

std_simplify_zip <- function(df, zip_col) {
  #' Standardize and simplify (i.e., remove 4-digit suffix) US Postal codes.
  #' 
  #' @param df A dataframe.
  #' @param zip_col The column containing the ZIP code to be simplified.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across({{zip_col}}, ~ case_when(
        str_detect(., "[0-9] [0-9]") ~ str_extract(
          ., 
          ".*(?=\\ )"
        ),
        str_detect(., "-") ~ str_extract(
          ., 
          ".*(?=\\-)"
        ),
        str_detect(., "^0+$") ~ NA_character_,
        TRUE ~ .
        )
      )
    )
}

std_split_addresses <- function(df, addr_col, unit_col = "unit") {
  #' Split 'second-line' address components into separate field.
  #' 
  #' @param df A dataframe.
  #' @param addr_col Address column to standardize.
  #' @param unit_col Name of column used to store unit.
  #' @returns A dataframe.
  #' @export
  bldg_loc_words <- paste(
    "BLDG", "UN?I?T", "SUITE", "APT", "NO", "P ?O BOX", "FLOOR", "R(?:OO)?M", "PMB", "NO",
    sep = "|"
  )
  df <- df %>%
    mutate(
      pobox = case_when(
        str_detect(get({{addr_col}}), "^PO BOX") ~ TRUE,
        TRUE ~ FALSE
      )
    )
  
  po_box <- filter(df, pobox)
  filter(df, !pobox) %>%
    extract(
      {{addr_col}}, 
      c("addr", unit_col), 
      paste0(
        "([[:alnum:] ]*)[ ]+(?:",
        bldg_loc_words,
        ")[ ]+(#?[A-Z]?[0-9]*(?:[A-Z]|(?:ABC)?)$)"
        ),
      remove = FALSE
    ) %>% st_set_geometry("geometry") %>%
    st_drop_geometry("geometry") %>%
    mutate(
      across({{addr_col}}, ~case_when(
        addr != "" ~ addr,
        TRUE ~ .
      )
      )
    ) %>%
    select(-c(addr)) %>%
    extract(
      {{addr_col}},
      c("addr", unit_col),
      "([[:alnum:] ]*)[ ]+([A-Z]?[0-9]+(?:[A-Z]|(?:ABC)?)$)",
      remove = FALSE
    ) %>%
    mutate(
      across({{addr_col}}, ~case_when(
        addr != "" ~ addr,
        TRUE ~ .
      )
      )
    ) %>%
    select(-c(addr)) %>%
    extract(
      {{addr_col}},
      c("addr", unit_col),
      "([[:alnum:] ]*)[ ]+([0-9]+(?:(?:TH|ND|RD|ST)?)+(?:[[:space:]])?(?:FLO?O?R?)?)$",
      remove = FALSE
    ) %>%
    mutate(
      across({{addr_col}}, ~case_when(
        addr != "" ~ addr,
        TRUE ~ .
      )
      )
    ) %>%
    select(-c(addr)) %>%
    bind_rows(po_box) %>%
    select(-c(pobox))
}

std_select_address <- function(df, addr_col1, addr_col2, output_col = "address") {
  #' Choose address column on simple criteria.
  #' 
  #' @param df A dataframe.
  #' @param addr_col1 First address column to be compared.
  #' @param addr_col2 Second address column to be compared.
  #' @param output_col Name of column that stores selected address.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      !!output_col := case_when(
        str_detect(get({{addr_col2}}), "^[0-9]") & !str_detect(get({{addr_col1}}), "^[0-9]") ~ get({{addr_col2}}),
        str_detect(get({{addr_col2}}), "^[0-9]") & str_detect(get({{addr_col1}}), "LLC") ~ get({{addr_col2}}),
        TRUE ~ get({{addr_col1}})
      )
    )
}

std_massachusetts <- function(df, cols) {
  #' Replace "MASS" with "MASSACHUSETTS"
  #' 
  #' @param df A dataframe.
  #' @param cols Columns in which to replace "MASS"
  #' @returns A dataframe.
  #' @export
  
  replace <- c(
    # Unabbreviate mass ave.
    "MASS " = "MASSACHUSETTS "
  )
  df %>%
    mutate(
      across(
        c({{cols}}), 
        ~str_replace_all(., replace)
      )
    )
}

st_get_zips <- function(sdf, zip_col, state = "MA", crs = 2249) {
  #' Get ZIP codes based on actual parcel location.
  #' (These are frequently misreported.)
  #' 
  #' @param sdf A sf dataframe.
  #' @param zip_col Name the column to which you want postal codes written.
  #' @param state State of your study. (TODO: multiple states?)
  #' @param crs EPSG code of appropriate coordinate reference system.
  #' @returns A dataframe.
  #' @export
  options(tigris_use_cache = TRUE)
  sdf %>% 
    mutate(
      point = st_point_on_surface(geometry)
    ) %>%
    st_set_geometry("point") %>%
    st_join(
      zctas(year = 2010, state = state, zip_col) %>%
        st_transform(crs) %>%
        select(ZCTA5CE10)
    ) %>%
    st_set_geometry("geometry") %>%
    select(
      -c(point)
    ) %>%
    mutate(
      !!zip_col := ZCTA5CE10
    ) %>%
    select(-c(ZCTA5CE10))
}

st_get_censusgeo <- function(sdf, state = "MA", crs = 2249) {
  #' Bind census geography IDs to geometries of interest.
  #' 
  #' @param sdf A sf dataframe.
  #' @param state State of your study. (TODO: multiple states?)
  #' @param crs EPSG code of appropriate coordinate reference system.
  #' @returns A dataframe.
  #' @export
  options(tigris_use_cache = TRUE)
  censusgeo <- block_groups(state = state) %>%
    st_transform(crs) %>%
    rename(
      geoid_bg = GEOID
    ) %>%
    select(geoid_bg)
  
  sdf %>% 
    mutate(
      point = st_point_on_surface(geometry)
    ) %>%
    st_set_geometry("point") %>%
    st_join(
      censusgeo
    ) %>%
    st_set_geometry("geometry") %>%
    mutate(
      geoid_t = str_sub(geoid_bg, start = 1L, end = 11L)
    ) %>%
    select(
      -c(point)
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
    group_by(across({{str_field}})) %>%
    mutate(group_naive = paste("naive", cur_group_id(), sep = "_")) %>%
    ungroup() %>%
    group_by(across({{str_field}}), group_naive) %>%
    summarize()
  df %>%
    left_join(distinct, by = str_field)
}

dedupe_community <- function(df, prefix, name, membership) {
  #' Identifies communities using igraph community detection.
  #' 
  #' @param df A dataframe containing only string datatypes.
  #' @param prefix Prefix for group ids.
  #' @param name id column.
  #' @param membership Name for community column.
  #' @returns A dataframe.
  #' @export
  g <- df %>%
    drop_na() %>%
    # Create igraph from data frame.
    # This is how we identify groups of owners.
    graph_from_data_frame(
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
    filter(!is.na(get({{group_col}}))) %>%
    # count(
    #   get({{group_col}}), get({{cols}}), sort = TRUE)
    group_by_at(c({{group_col}}, {{cols}})) %>%
    summarize(
      count = n()
    ) %>%
    arrange(get({{group_col}}), desc(count)) %>%
    group_by_at({{group_col}}) %>%
    summarize(
      across({{cols}}, ~first(.))
    )
}

dedupe_cosine <- function(df, str_field, group = "group_cosine", thresh = 0.75) {
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
    distinct(across({{str_field}})) %>%
    # Create a quanteda corpus.
    corpus(docid_field = str_field, text_field = str_field) %>%
    # Construct character-basedtokens object from corpus.
    tokens(what = "character") %>% 
    # Create character 3-grams from tokenized text.
    tokens_ngrams(n = 3, concatenator="") %>%
    # Construct document-feature matrix from tokens.
    dfm(remove_padding = TRUE) %>%
    # Weight document-feature matrix by term frequency-
    # inverse document frequency.
    dfm_tfidf() %>%
    # Compute similarity matrix using cosine-similarity.
    textstat_simil(method = "cosine", margin = "document", min_simil = thresh) %>%
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

corp_rm_corp_sys <- function(df, cols) {
  #' Replace variations on "CORPORATION SYSTEMS" with NA in corp addresses.
  #' 
  #' @param df A dataframe.
  #' @param cols The columns containing the addresses to be standardized.
  #' @returns A dataframe.
  #' @export
  df %>%
    mutate(
      across({{cols}}, ~ case_when(
        str_detect(., "CORP(ORATION)? (SYS)|(SER)") ~ NA_character_,
        TRUE ~ .
      )
      )
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
        get({{col}}), paste(c(
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
        ), collapse="|")
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
      col_select = 
        c(DataID, EntityName,
          AgentName, AgentAddr1, AgentAddr2, AgentCity, 
          AgentState, AgentPostalCode)
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
    select({{cols}}) %>%
    filter(!is.na(get({{drop_na_col}})))
}

load_assess <- function(path, town_ids = NA) {
  #' Load assessing table from MassGIS geodatabase.
  #' https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/gdbs/l3parcels/MassGIS_L3_Parcels_gdb.zip
  #' 
  #' @param path Path to MassGIS Parcels GDB.
  #' @param test Whether to only load a sample subset of rows.
  #' @export
  assess_query <- "SELECT * FROM L3_ASSESS"
  if (!is.na(town_ids)) {
    assess_query <- paste(assess_query, "WHERE TOWN_ID IN (", paste(town_ids, collapse = ","), ")")
  }
  st_read(
      path,
      query = assess_query
    ) %>%
    rename_with(str_to_lower) %>%
    assess_res_filter("use_code")
}

load_inds <- function(path) {
  #' Load individuals from corporate db, sourced from the MA Secretary of the Commonwealth.
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

process_inds <- function(i_df, a_df, owners) {
  #' Process Individuals, merging individuals with agents.
  #' 
  #' @param i_df Individuals dataframe. (Created by e.g., `load_inds`)
  #' @param a_df Agents dataframe. (Created by e.g., `load_agents`)
  #' @param owners dataframe containing a vector of unique owner ids used to filter inds.
  #' @returns A dataframe.
  #' @export
  i_df %>%
    filter(
      id_corp %in% discard(pull(owners, id_corp), is.na)
    ) %>%
    filter(
      # Remove cases of missing names and addresses.
      busaddr1 != "SAME"
      & firstname != "NONE"
      # Filter out corportions not present in data.
      # Remove null firstname and addresses.
      & !is.na(firstname)
      & (!is.na(busaddr1) | !is.na(resaddr1))
    ) %>%
    # Standardize strings, excluding ID corp.
    std_char(c(id_corp)) %>%
    # Standardize street types (e.g., ST > STREET)
    std_street_types(c(busaddr1, resaddr1)) %>%
    mutate(
      # Concatenate fullname.
      fullname = paste(firstname, lastname),
      # Choose address.
      address = case_when(
        !is.na(busaddr1) & is.na(resaddr1) ~ busaddr1,
        is.na(busaddr1) & !is.na(resaddr1) ~ resaddr1,
        is.na(busaddr1) & is.na(resaddr1) ~ NA_character_,
        TRUE ~ busaddr1
      )
    ) %>%
    select(c(id_corp, fullname, address)) %>%
    # Bind processed agents, following similar logic.
    bind_rows(
      a_df %>%
        rename(
          fullname = agentname,
          address = agentaddr1
        ) %>%
        filter(id_corp %in% discard(pull(owners, id_corp), is.na)) %>%
        filter(
          !is.na(fullname) 
          & fullname != "NONE"
        ) %>%
        std_char(c(id_corp)) %>%
        std_street_types(c(address, agentaddr2)) %>%
        std_select_address(
          "address", "agentaddr2"
        ) %>%
        # remove middle initials
        std_remove_middle_initial("fullname") %>%
        select(
          c(id_corp, fullname, address)
        )
    ) %>%
    # Remove "Corporation Sys/Services" and variants.
    corp_rm_corp_sys(fullname) %>%
    # Split address line 2s.
    std_split_addresses("address", "unit") %>%
    # Remove unit.
    select(-c(unit)) %>%
    # Standardize instances of "MASS"
    std_massachusetts(c(fullname, address)) %>%
    # Concatenate name_address
    mutate(
      name_address = str_c(fullname, address, sep = " ")
    ) %>%
    # Keep only distinct pairs of names and corporate ids.
    distinct(id_corp, name_address)
}

process_corps <- function(df, id, name) {
  #' Process corps, primarily cleaning names.
  #' 
  #' @param df Corps dataframe.
  #' @param id ID field to be excluded from data preparation.
  #' @param name Name field to be prepared.
  #' @returns A dataframe.
  #' @export
  df %>%
    std_char(c({{id}})) %>%
    std_massachusetts(c({{name}})) %>%
    std_street_types(c({{name}})) %>%
    corp_rm_corp_sys(c({{name}}))
}

process_assess <- function(df, crs = NA, census = FALSE, gdb_path = NA, town_ids = NA) {
  #' Process assessors records, optionally downloading and imputing census ids.
  #' 
  #' @param sdf Spatial dataframe.
  #' @param crs Integer representing coordinate reference system EPSG code.
  #' @param census Whether to download and impute census (e.g., tract, block group) ids.
  #' @param gdb_path String representing path to geodatabase. (Required if `census = TRUE`).
  #' @param test If `TRUE`, import only a subset of assessors records.
  #' @returns A dataframe.
  #' @export
  if (census) {
    library(tigris)
    parcel_query <- "SELECT * FROM L3_TAXPAR_POLY"
    if (!is.na(town_ids)) {
      parcel_query <- paste(parcel_query, "WHERE TOWN_ID IN (", town_ids, ")")
    }
    df <- st_read(
        gdb_path,
        query = parcel_query
      ) %>%
      # Rename all columns to lowercase.
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
      ) %>%
      # Join to assessing table.
      left_join(
        df,
        by = c("loc_id" = "loc_id")
        )
    if (is.na(crs)) {
      crs <- st_crs(df)
    }
    df <- df %>%
      st_get_zips("zip", crs = crs) %>%
      st_get_censusgeo(crs = crs) %>%
      select(
        c(
          prop_id, loc_id, town_id, fy, site_addr, location, 
          city, zip, owner1, own_addr, 
          own_city, own_zip, own_state, own_co, 
          geoid_t, geoid_bg)
      )  %>% 
      st_drop_geometry()
  } else {
    df <- df %>%
      select(
        c(
          prop_id, loc_id, town_id, fy, site_addr, location, 
          city, owner1, own_addr, 
          own_city, own_zip, own_state, own_co)
      )
  }
  df %>%
    rename(
      unit = location
    ) %>%
    std_simplify_zip(c(own_zip)) %>%
    std_char(c(prop_id, loc_id), owner1) %>%
    std_massachusetts(c(owner1, site_addr, own_addr)) %>%
    std_street_types(c(owner1, site_addr, own_addr)) %>%
    std_split_addresses("own_addr", "own_unit") %>%
    mutate(
      name_address = str_c(owner1, own_addr, sep = " ")
    ) %>%
    filter(!is.na(name_address))
}

merge_assess_corp <- function(a_df, c_df, by, group, id_c) {
  #' Merge assessors records with corporations on the basis of name.
  #' 
  #' @param a_df Assessors records dataframe.
  #' @param c_df Corportions dataframe.
  #' @param by A character vector of variables to join by.
  #' @param group String column identifying groups identified in previous processing.
  #' @param id_c Corporation id.
  #' @returns A dataframe.
  #' @export
  joined <- a_df %>%
    left_join(
      c_df, 
      by = by, 
      na_matches = "never"
    )
  no_group <- joined %>%
    filter(is.na(get({{group}})))
  joined %>%
    filter(!is.na(get({{group}}))) %>%
    group_by_at(group) %>%
    fill({{id_c}}, .direction = "updown") %>%
    ungroup() %>%
    bind_rows(no_group)
}
  
std_cities <- function(df) {
  #' Move Boston neighborhoods to Boston
  #' Update some other common neighborhoods
  df %>% mutate(df, city_cleaned = case_when(
    (city %in% c(BOSTON_NEIGHS$Name, c("ROXBURY CROSSING" , "DORCHESTER CENTER"))) ~ "BOSTON",
    (city == "NORTHWEST BEDFORD") ~ "BEDFORD",
    !(city %in% BOSTON_NEIGHS$Name) ~ city
    ))
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
