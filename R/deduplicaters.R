dedupe_address_to_id <- function(df, df2) {
  df |>
    dplyr::select(-dplyr::any_of(c("addr", "start", "end", "body", "even", "muni", "postal", "state", "loc_id"))) |>
    dplyr::left_join(
      df2,
      dplyr::join_by(id == id, table == table)
    )
}

dedupe_unique_addresses <- function(owners, officers, companies, sites, addresses, quiet=FALSE) {
  if(!quiet) {
    util_log_message(glue::glue("DEDUPLICATING: Creating table of unique addresses."))
  }
  if ("sf" %in% class(addresses)) {
    addresses <- addresses |>
      sf::st_drop_geometry()
  }
  
  sites <- sites |>
    dplyr::rename(old_site_id = id) |>
    dplyr::mutate(table="sites") |>
    tibble::rowid_to_column("id")
  
  companies <- companies |>
    dplyr::rename(old_company_id = company_id) |>
    dplyr::mutate(table="companies") |>
    tibble::rowid_to_column("id")
  
  owners <- owners |>
    dplyr::mutate(table="owners") |>
    dplyr::rename(old_site_id = site_id) |>
    dplyr::left_join(
      sites |> 
        dplyr::select(old_site_id, site_id = id, muni_id) |>
        dplyr::distinct(),
      dplyr::join_by(old_site_id == old_site_id, site_muni_id == muni_id),
      na_matches="never"
    ) |>
    dplyr::select(-old_site_id) |>
    tibble::rowid_to_column("id")
  
  sites <- sites |>
    dplyr::select(-old_site_id)
  
  officers <- officers |>
    dplyr::mutate(table="officers") |>
    dplyr::rename(old_company_id = company_id) |>
    dplyr::left_join(
      companies |> 
        dplyr::select(old_company_id, company_id = id) |>
        dplyr::distinct(),
      dplyr::join_by(old_company_id == old_company_id),
      multiple="any",
      na_matches="never"
    ) |>
    tibble::rowid_to_column("id")
  
  companies <- companies |>
    dplyr::select(-old_company_id)
  
  addresses <- addresses |>
    dplyr::mutate(table="addresses") |>
    std_assemble_addr() |>
    dplyr::select(-c(po, pmb))
  
  df <- owners |>
    dplyr::bind_rows(
      officers, 
      companies
    )
  
  df <- df  |>
    flow_address_to_address_seq(
      sites, 
      addresses
      ) |>
    dplyr::bind_rows(
      addresses,
      sites
    ) |>
    dplyr::select(id, addr, start, end, body, even, muni, postal, state, loc_id, table) |>
    dplyr::filter(!is.na(addr)) |>
    dplyr::group_by(addr, start, end, body, even, muni, postal, state, loc_id) |>
    dplyr::mutate(
      addr_id =  dplyr::cur_group_id()
    ) |>
    dplyr::ungroup()
  
  relation <- df |> dplyr::select(id, table, addr_id) |> dplyr::distinct()
  addresses <- df |> 
    dplyr::select(-c(id, table)) |> 
    dplyr::rename(id = addr_id) |> 
    dplyr::distinct(id, addr, start, end, body, even, muni, postal, state, loc_id)
  
  results <- purrr::map(
    list(
      owners = owners, 
      sites = sites |> dplyr::filter(res),
      companies = companies, 
      officers = officers
    ), 
    ~ dplyr::select(dedupe_address_to_id(.x, relation), -table)
  )
  
  results[['addresses']] <- addresses
  results[['officers']] <- results[['officers']] |>
    dplyr::group_by(name, old_company_id) |>
    tidyr::fill(addr_id) |>
    dplyr::ungroup() |>
    dplyr::select(-old_company_id)
  results
}

dedupe_naive <- function(df, field, group_name="naive") {
  #' Naive deduplication function, based on duplicate names and addresses.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param field The column used for deduplication.
  #' @returns A dataframe.
  #' @export
  distinct <- df |>
    dplyr::group_by(
      dplyr::across(dplyr::all_of({{ field }}))
      ) |>
    dplyr::mutate(
      !!group_name := dplyr::cur_group_id()
      ) |>
    dplyr::ungroup()
}

dedupe_igraph <- function(df, group_name, col_name, nodes=NULL) {
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
    !!group_name := g$membership,
    # Names is a vector of the strings deduplicated.
    !!col_name := g$names
  )
}

dedupe_network <- function(df, cols, group_name, nodes = NULL) {
  #' Identifies communities using igraph community detection.
  #'
  #' @param df A dataframe containing edges.
  #' @param prefix Prefix for group ids.
  #' @param name id column.
  #' @param membership Name for community column.
  #' @returns A dataframe.
  #' @export
  df <- df |>
    dplyr::mutate(
      dplyr::across(
        dplyr::all_of(cols),
        list(
          char = ~ dplyr::case_when(
            !is.na(get(dplyr::cur_column())) ~ stringr::str_c(dplyr::cur_column(), get(dplyr::cur_column()), sep="-"),
            .default = NA_character_
          )
        )
      )
    )
  
  selection <- df |>
    dplyr::select(dplyr::ends_with("_char")) |>
    dplyr::distinct() |>
    tidyr::drop_na()
  
  network <- dedupe_igraph(
    selection,
    group_name=group_name,
    col_name=cols[1],
    nodes=nodes
  )
  
  df |>
    dplyr::left_join(
      network,
      by=dplyr::join_by(!!stringr::str_c(cols[1], "_char") == !!cols[1])
    ) |>
    dplyr::select(-dplyr::ends_with("_char"))
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
    dplyr::group_by(dplyr::across(c({{ group_col }}, {{ cols }}))) |>
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
                          field,
                          thresh,
                          group_name = "cosine",
                          nodes=NULL
                          ) {
  #' Cosine similarity-based deduplication.
  #'
  #' @param df A dataframe containing only string datatypes.
  #' @param field The column used for deduplication.
  #' @param group Name for group column.
  #' @param thresh The minimum similarity threshold.
  #' @returns A dataframe.
  #' @export
  network <- df |>
    # Reduce to only unique string fields.
    dplyr::distinct(dplyr::across(dplyr::all_of( field ))) |>
    # Create a quanteda corpus.
    quanteda::corpus(docid_field = field, text_field = field) |>
    # Construct character-basedtokens object from corpus.
    quanteda::tokens(what = "character") |>
    # Create character 3-grams from tokenized text.
    quanteda::tokens_ngrams(n = 3, concatenator = "") |>
    # Construct document-feature matrix from tokens.
    quanteda::dfm(remove_padding = TRUE) |>
    # Weight document-feature matrix by term frequency-
    # inverse document frequency.
    quanteda::dfm_tfidf() |>
    # Compute similarity matrix using cosine similarity.
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
    ) |>
    dedupe_igraph(
      group_name=group_name,
      col_name=field,
      nodes=nodes
    )
  
  df <- df |>
    dplyr::left_join(
      network,
      by=dplyr::join_by(!!field == !!field)
    )
  
  df |>
    dplyr::filter(is.na(.data[[group_name]])) |>
    dedupe_naive(field, group_name="naive_temp") |>
    dplyr::mutate(
      !!group_name := naive_temp + max(df[[group_name]], na.rm=TRUE)
    ) |>
    dplyr::select(-naive_temp) |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(!is.na(.data[[group_name]]))
    )
}

dedupe_cosine_join <- function(owners, companies) {
  cosine_relations <- owners |>
    dplyr::filter(is.na(company_id) & (inst | trust)) |>
    dplyr::mutate(
      table = "owners"
    ) |>
    dplyr::bind_rows(
      companies |>
        dplyr::mutate(
          table = "companies"
        ) |>
        dplyr::rename(company_id = id)
    ) |>
    dplyr::filter(!is.na(cosine_bound)) |>
    dplyr::group_by(init_group = cosine_bound, table) |>
    dplyr::slice_head() |>
    dplyr::ungroup() |>
    dplyr::select(name, company_id, addr_id, init_group, table) |>
    dplyr::distinct() |>
    dedupe_cosine_bounded(
      "name",
      bounding_field="addr_id",
      thresh=0.85,
      table_col="table"
    ) |>
    dplyr::group_by(cosine_bound) |>
    tidyr::fill(company_id, .direction="updown") |>
    dplyr::ungroup() |>
    dplyr::filter(table == "owners" & !is.na(company_id)) |>
    dplyr::select(init_group, company_id)
  
  owners |>
    dplyr::filter(is.na(company_id)) |>
    dplyr::select(-company_id) |>
    dplyr::left_join(
      cosine_relations,
      by=dplyr::join_by(
        cosine_bound == init_group
      )
    ) |>
    dplyr::group_by(cosine_bound) |>
    tidyr::fill(company_id) |>
    dplyr::ungroup() |>
    dplyr::group_by(cosine_bound) |>
    tidyr::fill(naive) |>
    dplyr::ungroup() |>
    dplyr::bind_rows(
      owners |>
        dplyr::filter(!is.na(company_id))
    )
}

dedupe_cosine_bounded <- function(df, field, bounding_field, thresh, table_col, inds_thresh = 0.95) {
  naive <- df |>
    dplyr::filter(!is.na(.data[[field]])) |>
    dedupe_naive(field) |>
    dplyr::group_by(naive) |>
    tidyr::fill(
      dplyr::all_of(bounding_field),
      .direction="updown"
    )
  
  naive_addr <- naive |>
    dplyr::filter(!is.na(.data[[bounding_field]])) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(bounding_field)), naive) |>
    dplyr::mutate(
      naive_bound = dplyr::cur_group_id()
    ) |>
    dplyr::ungroup()
  
  if (all(c("inst", "trust") %in% names(df))) {
    cosine_addr <- naive_addr |>
      dplyr::filter(
        (inst | trust)
      )
  } else {
    cosine_addr <- naive_addr
  }
  
  cosine_addr <- cosine_addr |>
    dedupe_cosine(field, thresh=thresh)
  
  if(!missing(table_col)) {
    cosine_addr <- cosine_addr |>
      dplyr::group_by(cosine) |>
      dplyr::filter(dplyr::n_distinct(.data[[table_col]]) > 1) |>
      dplyr::ungroup()
  }
  
  cosine_addr <- cosine_addr |>
    dplyr::group_by(dplyr::across(dplyr::all_of(bounding_field)), cosine) |>
    dplyr::mutate(
      cosine_bound = dplyr::cur_group_id()
    ) |>
    dplyr::ungroup() |>
    dedupe_network(
      cols=c("cosine_bound", "naive"),
      group_name="cosine_filled"
    ) 
  
  if (all(c("inst", "trust") %in% names(df))) {
    inds <- naive_addr |>
      dplyr::filter(
        !(inst | trust)
      )
    
    if (nrow(inds) > 0) {
      inds <- inds |>
        dedupe_cosine(field, thresh=inds_thresh) |>
        dplyr::group_by(dplyr::across(dplyr::all_of(bounding_field)), cosine) |>
        dplyr::mutate(
          cosine_bound = dplyr::cur_group_id()
        )
    }
    
    cosine_addr <- cosine_addr |>
      dplyr::bind_rows(
        inds
      )
  }
  
  cosine_addr <- cosine_addr |> 
    dplyr::bind_rows(
      naive |> 
        dplyr::filter(is.na(.data[[bounding_field]])),
      df |>
        dplyr::filter(is.na(.data[[field]]))
    ) |>
    dplyr::mutate(
      group = dplyr::case_when(
        !is.na(cosine_filled) ~ stringr::str_c("cosine", "filled", cosine_filled, sep="-"),
        !is.na(cosine_bound) ~ stringr::str_c("cosine", "bound", cosine_bound, sep="-"),
        !is.na(naive_bound) ~ stringr::str_c("naive", "bound", cosine_bound, sep="-")
      )
    )
  if (all(c("inst", "trust") %in% names(df))) {
    cosine_addr <- cosine_addr |>
      dplyr::mutate(
        group = dplyr::case_when(
          !is.na(naive) & (trust | inst) & is.na(group) ~ stringr::str_c("naive", naive, sep="-"),
          .default = group
        )
      )
  }
  cosine_addr
}

dedupe_owner_to_company <- function(df, companies, quiet=FALSE) {
  if(!quiet) {
    util_log_message(glue::glue("DEDUPLICATING: Linking assessors table owners to companies."))
  }
  df |>
    dplyr::left_join(
      companies |>
        dplyr::select(name, company_id = id, replace=addr_id) |>
        dplyr::distinct(),
      by=dplyr::join_by(name),
      multiple="any",
      na_matches="never"
    ) |>
    dplyr::mutate(
      addr_id = dplyr::case_when(
        is.na(addr_id) & !is.na(replace) ~ replace,
        .default = addr_id
      )
    ) |>
    dplyr::select(-replace)
}

dedupe_all <- function(
    owners,
    officers,
    companies,
    sites,
    addresses,
    thresh,
    inds_thresh,
    refresh=FALSE,
    quiet=FALSE) {
  
  if(!quiet) {
    util_log_message("BEGINNING DEDUPLICATION SEQUENCE", header=TRUE)
  }
  
  conn <- load_conn()
  on.exit(DBI::dbDisconnect(conn))
  
  tables <- list(
    owners = "dedupe_owners",
    companies = "dedupe_companies",
    officers = "dedupe_officers",
    metacorps = "dedupe_metacorps",
    sites = "dedupe_sites",
    addresses = "dedupe_addresses"
  )
  
  tables_exist <- load_check_for_tables(
    conn, 
    unlist(unname(tables))
    )
  
  if(!tables_exist) {
    if(!quiet) {
      util_log_message("INPUT/OUTPUT: Deduplicaton results not found in database. Running process.")
    }
  } else if (tables_exist & refresh) {
    if(!quiet) {
      util_log_message("INPUT/OUTPUT: Deduplicaton results found in database, but user specified refresh. Running process.")
    }
  } else if (tables_exist & !refresh) {
    if(!quiet) {
      util_log_message("INPUT/OUTPUT: Deduplicaton results found in database.")
    }
    for(t in names(tables)) {
      assign(t, load_postgis_read(conn, tables[[t]]))
    }
  }
  
  if (!tables_exist | refresh) {
    dedupe_unique_addresses(
        owners = owners,
        officers = officers,
        companies = companies,
        sites = sites,
        addresses = addresses,
        quiet=quiet
      ) |>
        wrapr::unpack(
          owners <- owners,
          officers <- officers,
          companies <- companies,
          sites <- sites,
          addresses <- addresses
        )

      owners <- owners |>
        dplyr::filter(type != "co") |>
        dedupe_owner_to_company(
          companies,
          quiet=quiet
          ) |>
        dplyr::bind_rows(
          owners |>
            dplyr::filter(type == "co")
        )

      if(!quiet) {
        util_log_message(glue::glue("DEDUPLICATING: Naive- and cosine similarity-deduplicating owners."))
      }

      owners <- owners |>
        dplyr::filter(type != "co") |>
        dedupe_cosine_bounded(
          "name",
          bounding_field="addr_id",
          thresh=thresh,
          inds_thresh=inds_thresh
        ) |>
        dplyr::bind_rows(
          owners |>
            dplyr::filter(type == "co")
        )

      owners <- owners |>
        dplyr::filter(is.na(company_id) & type != "co") |>
        dplyr::select(-company_id) |>
        dedupe_owner_to_company(
          companies,
          quiet=quiet
        ) |>
        dplyr::bind_rows(
          owners |>
            dplyr::filter(!(is.na(company_id) & type != "co"))
        ) |>
        dplyr::group_by(group) |>
        tidyr::fill(company_id, .direction="downup") |>
        dplyr::ungroup()
      

      if(!quiet) {
        util_log_message(glue::glue("DEDUPLICATING: Naive- and cosine similarity-deduplicating companies."))
      }

      companies <- companies |>
        dedupe_cosine_bounded(
          "name",
          bounding_field="addr_id",
          thresh=thresh
        )

      if(!quiet) {
        util_log_message(glue::glue("DEDUPLICATING: Joining owners to table by cosine matching."))
      }

      owners <- owners |>
        dplyr::filter(type != "co") |>
        dedupe_cosine_join(companies) |>
        dplyr::bind_rows(
          owners |>
            dplyr::filter(type == "co")
        )
      
      co <- owners |>
        dplyr::group_by(site_id) |>
        tidyr::fill(company_id) |>
        dplyr::ungroup() |>
        dplyr::mutate(
          type = "officer",
          position = "CO"
          ) |>
        std_flag_agent("name", position_col ="position") |>
        dplyr::filter(type=="co" & !is.na(company_id)) |>
        dplyr::select(name, addr_id, company_id, inst, trust, trustees)
      
      owners <- owners |>
        dplyr::filter(type != "co") |>
        dplyr::mutate(type="owner")

      if(!quiet) {
        util_log_message(glue::glue("DEDUPLICATING: Naive- and cosine similarity-deduplicating officers."))
      }

      officers <- officers |>
        dplyr::bind_rows(
          co
        ) |>
        dplyr::select(-id) |>
        tibble::rowid_to_column("id")
      
      rm(co)
      
      officers <- officers |>
        std_flag_agent("name", position_col ="position") |>
        dplyr::filter(!agent) |>
        dplyr::left_join(
          companies |>
            dplyr::select(id, company_group=group),
          by=dplyr::join_by(company_id==id),
          multiple="any",
          na_matches="never"
        ) |>
        dplyr::group_by(company_group, name) |>
        tidyr::fill(addr_id) |>
        dplyr::ungroup()
      
      officers <- officers |>
        dplyr::filter(!is.na(addr_id)) |>
        dedupe_cosine_bounded(
          "name",
          bounding_field="addr_id",
          thresh=thresh
        ) |>
        dplyr::group_by(group) |>
        tidyr::fill(company_group, .direction="downup") |>
        dplyr::ungroup() |>
        dplyr::bind_rows(
          officers |>
            dplyr::filter(is.na(addr_id))
        )

      if(!quiet) {
        util_log_message(glue::glue("DEDUPLICATING: Corporate community detection."))
      }

      officers <- officers |>
        dedupe_network(
          cols=c("company_group", "group"),
          group_name="network"
        ) |>
        dplyr::mutate(
          network =  stringr::str_c("network", network, sep="-")
        )

      if(!quiet) {
        util_log_message(glue::glue("DEDUPLICATING: Attaching network ids to owners."))
      }

      owners <- owners |>
        dplyr::left_join(
          officers |>
            dplyr::select(company_id, network),
          by=dplyr::join_by(company_id),
          na_matches="never",
          multiple="any"
        )

      owners <- owners |>
        dplyr::filter(!is.na(group)) |>
        dplyr::group_by(group) |>
        tidyr::fill(network, .direction="downup") |>
        dplyr::ungroup() |>
        dplyr::mutate(
          group = dplyr::case_when(
            !is.na(network) ~ network,
            .default = group
          )
        ) |>
        dplyr::bind_rows(
          owners |>
            dplyr::filter(is.na(group))
        ) |>
        dplyr::select(-network)


      if(!quiet) {
        util_log_message(glue::glue("DEDUPLICATING: Identifying provisional metacorps."))
      }

      metacorps <- owners |>
        dedupe_text_mode(
          group_col="group",
          cols = "name"
        )
      for(t in names(tables)) {
        load_write(
          get(t),
          conn=conn,
          table_name=tables[[t]],
          other_formats=c("csv", "r"),
          overwrite=TRUE,
          quiet=quiet
          )
      }
  }
  
  list(
    owners = owners,
    companies = companies,
    officers = officers,
    metacorps = metacorps,
    sites = sites,
    addresses = addresses
  )
}
