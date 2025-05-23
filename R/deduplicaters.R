dedupe_address_to_id <- function(df, df2) {
  df |>
    dplyr::select(-dplyr::any_of(c("addr", "start", "end", "body", "even", "muni", "postal", "state", "loc_id"))) |>
    dplyr::left_join(
      df2,
      dplyr::join_by(id == id, table == table)
    )
}

dedupe_address_to_address_seq <- function(a1, addresses) {
  if ("sf" %in% class(addresses)) {
    addresses <- addresses |>
      sf::st_drop_geometry()
  }
  
  if (!("loc_id" %in% names(a1))) {
    a1 <- a1 |>
      dplyr::mutate(
        loc_id = NA_character_
      )
  }
  
  a1 |>
    std_match_address_to_address(
      addresses,
      fill_col="loc_id",
      body, muni, postal
    )  |>
    std_match_address_to_address(
      addresses,
      fill_col="loc_id",
      body, muni, postal
    ) |>
    std_match_address_to_address(
      addresses |> dplyr::filter(unique_in_muni),
      fill_col=c("loc_id", "postal"),
      body, muni
    ) |>
    std_match_address_to_address(
      addresses |> dplyr::filter(unique_in_postal),
      fill_col=c("loc_id", "muni"),
      body, postal
    ) |>
    std_simp_street("body") |>
    std_match_address_to_address(
      addresses |> dplyr::filter(unique_in_muni_simp),
      fill_col=c("loc_id", "postal", "body"),
      body_simp, muni
    ) |>
    std_match_address_to_address(
      addresses |> dplyr::filter(unique_in_postal_simp),
      fill_col=c("loc_id", "postal", "body"),
      body_simp, postal
    ) |>
    dplyr::select(-body_simp)
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
  
  owners <- owners |>
    dplyr::mutate(table="owners") |>
    dplyr::rename(old_site_id = site_id) |>
    dplyr::left_join(
      sites |> 
        dplyr::select(old_site_id, site_id = id, muni_id) |>
        dplyr::distinct(),
      dplyr::join_by(old_site_id == old_site_id, site_muni_id == muni_id),
      na_matches="never",
      multiple="any"
    ) |>
    dplyr::select(-old_site_id)
  
  sites <- sites |>
    dplyr::select(-old_site_id)
  
  addresses <- addresses |>
    dplyr::mutate(table="addresses") |>
    std_assemble_addr() |>
    dplyr::select(-c(po, pmb))
  
  if (!is.null(companies) & !is.null(officers)) {
    companies <- companies |>
      dplyr::rename(old_company_id = company_id) |>
      dplyr::mutate(table="companies")
    
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
      )
    
    companies <- companies |>
      dplyr::select(-old_company_id)
    
    df <- owners |>
      dplyr::bind_rows(
        officers, 
        companies
      )
  } else {
    df <- owners
  }
  
  df <- df  |>
    dedupe_address_to_address_seq(
      addresses
      ) |>
    dplyr::bind_rows(
      addresses,
      sites
    ) |>
    dplyr::select(id, addr, start, end, body, even, muni, muni_id, postal, state, loc_id, table) |>
    dplyr::filter(!is.na(addr))
  
  df <- df |>
    dplyr::filter(!is.na(state)) |>
    dplyr::group_by(muni, state) |>
    tidyr::fill(muni_id, .direction="downup") |>
    dplyr::ungroup() |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(is.na(state))
    )
  
  df <- df |>
    dplyr::filter(!is.na(postal)) |>
    dplyr::group_by(muni, postal) |>
    tidyr::fill(muni_id, .direction="downup") |>
    dplyr::ungroup()  |>
    dplyr::bind_rows(
      df |>
        dplyr::filter(is.na(postal))
    )
  
  df <- df |>
    dplyr::group_by(addr, start, end, body, even, muni, muni_id, postal, state, loc_id) |>
    dplyr::mutate(
      addr_id =  dplyr::cur_group_id()
    ) |>
    dplyr::ungroup()
  
  relation <- df |> dplyr::select(id, table, addr_id) |> dplyr::distinct()
  addresses <- df |> 
    dplyr::select(-c(id, table)) |> 
    dplyr::rename(id = addr_id) |> 
    dplyr::distinct(id, addr, start, end, body, even, muni, muni_id, postal, state, loc_id)
  
  if (!is.null(companies) & !is.null(officers)) {
    l = list(
      owners = owners, 
      sites = sites |> dplyr::filter(res),
      companies = companies, 
      officers = officers
    )
  } else {
    l = list(
      owners = owners, 
      sites = sites |> dplyr::filter(res)
    )
  }
  results <- purrr::map(
    l, 
    ~ dplyr::select(dedupe_address_to_id(.x, relation), -table)
  )
  rm(l)
  
  if (!is.null(companies) & !is.null(officers)) {
    addr_ids <- c(results$owners$addr_id, 
                  results$sites$addr_id, 
                  results$companies$addr_id,
                  results$officers$addr_id) |> unique()
    results[['officers']] <- results[['officers']] |>
      dplyr::group_by(name, old_company_id) |>
      tidyr::fill(addr_id, .direction="downup") |>
      dplyr::ungroup() |>
      dplyr::select(-old_company_id)
  } else {
    addr_ids <- c(results$owners$addr_id, 
                  results$sites$addr_id) |> unique()
    results <- append(results, list(companies = NULL, officers = NULL))
  }
  
  results[['addresses']] <- addresses |>
    dplyr::filter(id %in% addr_ids)
  
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
  owners <- owners |>
    dplyr::mutate(
      table = "owners"
    )
  
  companies <- companies |>
    dplyr::mutate(
      table = "companies"
    )
    
  
  cosine_relations <- owners |>
    dplyr::filter(is.na(company_id) & (inst | trust)) |>
    dplyr::bind_rows(
      companies
    ) |>
    dplyr::filter(!is.na(group)) |>
    dplyr::group_by(group, table) |>
    dplyr::slice_head() |>
    dplyr::ungroup() |>
    dplyr::select(name, company_id, addr_id, init_group = group, table) |>
    dplyr::distinct() |>
    dedupe_cosine_bounded(
      "name",
      bounding_field="addr_id",
      thresh=0.85,
      table_col="table",
      prefix="joined"
    ) |>
    dplyr::filter(!is.na(group)) |>
    dplyr::group_by(group) |>
    tidyr::fill(company_id, .direction="updown") |>
    dplyr::ungroup() |>
    dplyr::filter(table == "owners" & !is.na(company_id)) |>
    dplyr::select(init_group, company_id)
  
  save(cosine_relations, file="relations.Rda")
  
  unmatched <- owners |>
    dplyr::filter(is.na(company_id)) |>
    dplyr::select(-company_id) |>
    dplyr::left_join(
      cosine_relations,
      by=dplyr::join_by(
        group == init_group
      ),
      na_matches="never"
    )

  df <- unmatched |>
    dplyr::filter(!is.na(group)) |>
    dplyr::group_by(group) |>
    tidyr::fill(company_id, .direction="downup") |>
    dplyr::ungroup() |>
    dplyr::group_by(group) |>
    tidyr::fill(naive, .direction="downup") |>
    dplyr::ungroup() |>
    dplyr::bind_rows(
      unmatched |>
        dplyr::filter(is.na(group)),
      owners |>
        dplyr::filter(!is.na(company_id))
    )
}

dedupe_cosine_bounded <- function(df, field, bounding_field, thresh, table_col, prefix, inds_thresh = 0.95) {
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
        !is.na(cosine_filled) ~ stringr::str_c(prefix, "cosine", "filled", cosine_filled, sep="-"),
        !is.na(cosine_bound) ~ stringr::str_c(prefix, "cosine", "bound", cosine_bound, sep="-"),
        !is.na(naive_bound) ~ stringr::str_c(prefix, "naive", "bound", cosine_bound, sep="-")
      )
    )
  if (all(c("inst", "trust") %in% names(df))) {
    cosine_addr <- cosine_addr |>
      dplyr::mutate(
        group = dplyr::case_when(
          !is.na(naive) & (trust | inst) & is.na(group) ~ stringr::str_c(prefix, "naive", naive, sep="-"),
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
    tables,
    formats=c("csv", "r"),
    push_db="",
    refresh=FALSE,
    quiet=FALSE) {
  
  out <- list(
    addresses = NULL,
    owners = NULL,
    sites_to_owners = NULL,
    sites = NULL,
    officers = NULL,
    companies = NULL,
    metacorps_cosine = NULL,
    metacorps_network = NULL
  )
  
  if (is.null(tables)) {
    if (!quiet) {
      util_log_message("NO DEDUPLICATED TABLES REQUESTED. SKIPPING SUBROUTINE.", header=TRUE)
    }
    all_tables_exist <- TRUE
  } else {
    if (!quiet) {
      util_log_message("BEGINNING DEDUPLICATION SEQUENCE", header=TRUE)
    }
    conn <- util_conn(push_db)
    all_tables_exist <- util_check_for_tables(conn, tables)
    if(!all_tables_exist) {
      if(!quiet) {
        util_log_message("INPUT/OUTPUT: Deduplicaton results not found in database. Running process.")
      }
    } else if (all_tables_exist & refresh) {
      if(!quiet) {
        util_log_message("INPUT/OUTPUT: Deduplicaton results found in database, but user specified refresh. Running process.")
      }
    } else if (all_tables_exist & !refresh) {
      if(!quiet) {
        util_log_message("INPUT/OUTPUT: Deduplicaton results found in database.")
      }
      for(t in tables) {
        out[[t]] <- load_postgis_read(conn, t)
      }
    }
    DBI::dbDisconnect(conn)
  }
  
  if (!all_tables_exist | refresh) {
    dedupe_unique_addresses(
        owners = owners,
        officers = officers,
        companies = companies,
        sites = sites,
        addresses = addresses,
        quiet=quiet
      ) |>
        wrapr::unpack(
          owners,
          officers,
          companies,
          sites,
          addresses
        )
    
    conn <- util_conn(push_db)
    sites |>
      dplyr::select(
        id,
        fy,
        muni_id,
        ls_date,
        ls_price,
        bld_area,
        res_area,
        units,
        bld_val = bldg_val,
        lnd_val = land_val,
        use_code,
        luc,
        ooc,
        condo,
        addr_id
      ) |>
      load_write(
        conn=conn,
        table_name="sites",
        id_col="id",
        other_formats=formats,
        overwrite=TRUE,
        quiet=quiet
      )
    DBI::dbDisconnect(conn)
    if (interactive()) {
      out[['sites']] <- sites
    }
    rm(sites)
    
    conn <- util_conn(push_db)
    addresses |>
      load_write(
        conn=conn,
        table_name="addresses",
        id_col="id",
        other_formats=formats,
        overwrite=TRUE,
        quiet=quiet
      )
    DBI::dbDisconnect(conn)
    if (interactive()) {
      out[['addresses']] <- addresses
    }
    rm(addresses)
    
    sites_to_owners <- owners |>
      dplyr::group_by(name, type, inst, trust, trustees, addr_id) |>
      dplyr::mutate(
        owner_id = dplyr::cur_group_id()
      ) |>
      dplyr::ungroup()
    
    
    owners <- sites_to_owners |> 
      dplyr::group_by(id = owner_id, name, type, inst, trust, trustees, addr_id) |>
      dplyr::summarize() |>
      dplyr::ungroup()
    
    conn <- util_conn(push_db)
    sites_to_owners <- sites_to_owners |>
      dplyr::select(site_id, owner_id) |>
      tibble::rowid_to_column("id") |>
      load_write(
        conn=conn,
        table_name="sites_to_owners",
        id_col="id",
        other_formats=formats,
        overwrite=TRUE,
        quiet=quiet
      )
    DBI::dbDisconnect(conn)
    out[['sites_to_owners']] <- sites_to_owners
    
    if (!is.null(companies) & !is.null(officers)) {
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
    }

    if(!quiet) {
      util_log_message(glue::glue("DEDUPLICATING: Naive- and cosine-similarity-deduplicating owners."))
    }

    owners <- owners |>
      dplyr::filter(type != "co") |>
      dedupe_cosine_bounded(
        "name",
        bounding_field="addr_id",
        thresh=thresh,
        inds_thresh=inds_thresh,
        prefix="owner"
      ) |>
      dplyr::bind_rows(
        owners |>
          dplyr::filter(type == "co")
      )
    
    if (!is.null(companies) & !is.null(officers)) {
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
        ) 
    
      owners <- owners |>
        dplyr::filter(!is.na(group)) |>
        dplyr::group_by(group) |>
        tidyr::fill(company_id, .direction="downup") |>
        dplyr::ungroup() |>
        dplyr::bind_rows(
          owners |>
            dplyr::filter(is.na(group))
        )
    

      if(!quiet) {
        util_log_message(glue::glue("DEDUPLICATING: Naive- and cosine-similarity-deduplicating companies."))
      }

      companies <- companies |>
        dedupe_cosine_bounded(
          "name",
          bounding_field="addr_id",
          thresh=thresh,
          prefix="company"
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
      
      if(!quiet) {
        util_log_message(glue::glue("DEDUPLICATING: Pulling C/Os out from owners table."))
      }
      
      co <- sites_to_owners |>
        dplyr::left_join(
          owners,
          by = dplyr::join_by(owner_id==id)
        ) |>
        dplyr::group_by(site_id) |>
        tidyr::fill(company_id, .direction="downup") |>
        dplyr::ungroup() |>
        dplyr::mutate(
          type = "officer",
          position = "CO"
        ) |>
        std_flag_agent("name", position_col ="position") |>
        dplyr::filter(type=="co" & !is.na(company_id)) |>
        dplyr::select(name, addr_id, company_id, inst, trust, trustees)
      
      rm(sites_to_owners)
      
    }
    
    owners <- owners |>
      dplyr::filter(type != "co") |>
      dplyr::mutate(type="owner")

    if(!quiet) {
      util_log_message(glue::glue("DEDUPLICATING: Naive- and cosine-similarity-deduplicating officers."))
    }
    
    if (!is.null(companies) & !is.null(officers)) {
      officers <- officers |>
        dplyr::bind_rows(
          co
        ) |>
        dplyr::select(-id) |>
        tibble::rowid_to_column("id")
      
      rm(co)
      
      officers <- officers |>
        std_flag_agent("name", position_col ="position") |>
        std_flag_manager("name") |>
        dplyr::filter(!agent & !manager) |>
        dplyr::group_by(
          name,
          inst,
          trust,
          trustees,
          company_id,
          addr_id
        ) |>
        dplyr::summarize(
          positions = stringr::str_c(unique(position), collapse=",")
        ) |>
        dplyr::left_join(
          companies |>
            dplyr::select(id, company_group=group),
          by=dplyr::join_by(company_id==id),
          multiple="any",
          na_matches="never"
        ) |>
        dplyr::group_by(company_group, name) |>
        tidyr::fill(addr_id, .direction="downup") |>
        dplyr::ungroup()
        
      
      officers <- officers |>
        dedupe_cosine_bounded(
          "name",
          bounding_field="addr_id",
          thresh=thresh,
          prefix="officers"
        ) |>
        dplyr::group_by(company_group, naive) |>
        dplyr::mutate(
          group = dplyr::case_when(
            is.na(group) & !is.na(company_group) ~ stringr::str_c("officers-naive-", dplyr::cur_group_id()),
            .default = group
          )
        ) |>
        dplyr::group_by(group) |>
        tidyr::fill(company_group, .direction="downup") |>
        dplyr::ungroup()
  
      if(!quiet) {
        util_log_message(glue::glue("DEDUPLICATING: Corporate community detection."))
      }
  
      officers <- officers |>
        dplyr::filter(!is.na(group) & !is.na(company_group)) |>
        dedupe_network(
          cols=c("company_group", "group"),
          group_name="network"
        ) |>
        dplyr::mutate(
          network =  stringr::str_c("network", network, sep="-")
        ) |>
        dplyr::bind_rows(
          officers |>
            dplyr::filter(is.na(group) | is.na(company_group))
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
          network_group = dplyr::case_when(
            !is.na(network) ~ network,
            .default = group
          )
        ) |>
        dplyr::bind_rows(
          owners |>
            dplyr::filter(is.na(group))
        ) |>
        dplyr::rename(
          cosine_group = group
        ) |>
        dplyr::select(
          c(id, name, inst, trust, trustees, 
            addr_id, company_id, cosine_group, network_group)
        )
    } else {
      owners <- owners |>
        dplyr::rename(
          cosine_group = group
        ) |>
        dplyr::select(
          c(id, name, inst, trust, trustees, 
            addr_id, cosine_group)
        )
    }
    
    conn <- util_conn(push_db)
    owners |>
      load_write(
        conn=conn,
        table_name="owners",
        id_col="id",
        other_formats=formats,
        overwrite=TRUE,
        quiet=quiet
      )
    DBI::dbDisconnect(conn)
    if (interactive()) {
      out[['owners']] <- owners
    }
    if (!is.null(companies) & !is.null(officers)) {
      if(!quiet) {
        util_log_message(glue::glue("DEDUPLICATING: Removing all companies and officers whose networks don't meet property records."))
      }
      
      conn <- util_conn(push_db)
      officers <- officers |> 
        dplyr::mutate(
          match = company_id %in% (owners |> 
            dplyr::filter(!is.na(company_id)) |>
            dplyr::pull(company_id) |>
            unique())
        ) |>
        dplyr::rename(network_id = network) |>
        dplyr::group_by(network_id) |>
        tidyr::fill(match, .direction="downup") |>
        dplyr::ungroup() |>
        dplyr::filter(match) |>
        dplyr::select(
          c(name, inst, positions, company_id, addr_id, network_id)
          ) |>
        tibble::rowid_to_column("id") |>
        load_write(
          conn=conn,
          table_name="officers",
          id_col="id",
          other_formats=formats,
          overwrite=TRUE,
          quiet=quiet
        )
      DBI::dbDisconnect(conn)
      
      if(interactive()) {
        out[['officers']] <- officers
      }
      
      companies <- companies |> 
        dplyr::mutate(
          match = id %in% (officers |> 
            dplyr::filter(!is.na(company_id)) |>
            dplyr::pull(company_id) |>
            unique())
        ) 
      
      conn <- util_conn(push_db)
      companies <- companies |>
        dplyr::group_by(group) |>
        tidyr::fill(match, .direction="downup") |>
        dplyr::ungroup() |>
        dplyr::filter(match) |>
        dplyr::left_join(
          officers |>
            dplyr::select(company_id, network_id),
          by=dplyr::join_by(id == company_id),
          multiple = "any"
        )  |>
        dplyr::select(c(id, name, company_type, addr_id, network_id)) |>
        load_write(
          conn=conn,
          table_name="companies",
          id_col="id",
          other_formats=formats,
          overwrite=TRUE,
          quiet=quiet
        )
      DBI::dbDisconnect(conn)
      
      if(interactive()) {
        out[['companies']] <- companies
      }
      rm(companies, officers)
      
      if(!quiet) {
        util_log_message(glue::glue("DEDUPLICATING: Identifying provisional metacorps."))
      }
      
      conn <- util_conn(push_db)
      metacorps_network <- owners |>
        dedupe_text_mode(
          group_col="network_group",
          cols = "name"
        ) |>
        dplyr::rename(id = network_group) |>
        load_write(
          conn=conn,
          table_name="metacorps_network",
          id_col="id",
          other_formats=formats,
          overwrite=TRUE,
          quiet=quiet
        )
      DBI::dbDisconnect(conn)
      
      if(interactive()) {
        out[['metacorps_network']] <- metacorps_network
      }
      rm(metacorps_network)
    }
    
    conn <- util_conn(push_db)
    metacorps_cosine <- owners |>
      dedupe_text_mode(
        group_col="cosine_group",
        cols = "name"
      ) |>
      dplyr::rename(id = cosine_group) |>
      load_write(
        conn=conn,
        table_name="metacorps_cosine",
        id_col="id",
        other_formats=formats,
        overwrite=TRUE,
        quiet=quiet
        )
    DBI::dbDisconnect(conn)
    
    if(interactive()) {
      out[['metacorps_cosine']] <- metacorps_cosine
    }
    rm(metacorps_cosine)
  }
  out
}
