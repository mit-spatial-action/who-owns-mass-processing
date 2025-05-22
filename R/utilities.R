util_get_config <- function(file = "config.yaml") {
    config::get(file = file)  # auto-detects R_CONFIG_ACTIVE
}

util_log_message <- function(status, header=FALSE, timestamp=TRUE) {
  #' Print message to `logr` logs, or to message if log is not opened.
  #'
  #' @param status Status to print.
  #' @returns Nothing.
  #' @export
  if (timestamp) {
    status <- stringr::str_c(format(Sys.time()), ": ", status)
  }
  if (header) {
    status <- stringr::str_c(
      "\n",
      status,
      "\n",
      strrep("=", nchar(status)),
      "\n",
      sep=""
    )
  }
  if(logr::log_status() == "open") {
    logr::log_print(status, hide_notes = TRUE, console=FALSE)
    message(status)
  } else {
    message(status)
  }
}

# Validation ====

utils_validate_config <- function(config) {
  # Validate CRS
  if(suppressWarnings(is.na(sf::st_crs(config$crs)$input))) {
    stop("You provided an invalid CRS. Check `config.yaml`.")
  }
  # Validate Thresholds
  threshes <- c(config$cosine_thresh, config$inds_thresh, config$zip_int_thresh)
  if (!all(dplyr::between(threshes, 0, 1)) & length(threshes) == 3) {
    stop("`cosine_thresh`, `inds_thresh`, and `zip_int_thresh` must be between 1 and 0.
         Check `config.yaml`.")
  }
  # Validate Municipality IDs
  util_validate_muni_ids(config$muni_ids)
  # Validate State
  if (!(config$state %in% state.abb)) {
    stop("You provided an invalid `state`. Check `config.yaml`.")
  }
  # Validate Data Directory
  if (!dir.exists(config$data_dir)) {
    stop("You provided an invalid `data_dir`. Check `config.yaml`.")
  }
  # Validate Remote Parcels
  if(httr::HEAD(config$parcels_remote)$all_headers[[1]]$status != 200) {
    stop("Your `parcels_remote` does not exist. Check `config.yaml`.")
  }
  # Validate OpenCorporates Files
  if (!file.exists(config$companies_path)) {
    stop("You provided an invalid `companies_path`. Check `config.yaml`.")
  }
  if (!file.exists(config$officers_path)) {
    stop("You provided an invalid `officers_path`. Check `config.yaml`.")
  }
  if (!file.exists(config$alt_names_path)) {
    stop("You provided an invalid `alt_names_path`. Check `config.yaml`.")
  }
  util_conn(test=TRUE)
  config
}

util_validate_muni_ids <- function(muni_ids) {
  #' Test Validity of Muni IDs and Pad
  #' 
  #' Tests whether provided municipality ids are valid (`stop()` if they are 
  #' not) and pads them out to three characters with zeroes to the left.
  #'
  #' If need to create file...
  #' MUNIS |> 
  #'     sf::st_drop_geometry() |> 
  #'     dplyr::select(muni_id, muni) |>
  #'     readr::write_csv("data/munis.csv")
  #'
  #' @param muni_ids Vector of municipality IDs.
  #' 
  #' @return A transformed vector of municipality IDs.
  #' 
  #' @export
  muni_table <- load_muni_helper() 
  ids <- muni_table |>
    dplyr::pull(id)
  if (is.null(muni_ids)) {
    muni_ids <- stringr::str_pad(ids, 3, side="left", pad="0") |>
      unique()
  } else if (all(muni_ids == "hns")) {
    muni_ids <- muni_table |>
      dplyr::filter(hns) |> 
      dplyr::pull(muni_id) |>
      unique()
  } else if (all(muni_ids == "mapc")) {
    muni_ids <- muni_table |>
      dplyr::filter(mapc) |> 
      dplyr::pull(muni_id) |>
      unique()
  } else {
    muni_ids <- stringr::str_pad(muni_ids, 3, side="left", pad="0")
  }
  if(!all(muni_ids %in% ids)) {
    stop("VALIDATION: Invalid municipality IDs provided.")
  }
}

#' Test DBMS Connection
#' 
#' Creates connection to remote or local PostGIS connection. Requires
#' variables "DB_NAME", "DB_HOST", "DB_PORT", "DB_USER", "DB_PASS", "DB_SSL" 
#' be set in `.Renviron`
#' 
#' @return Nothing
util_conn <- function(test = FALSE) {
  
  dbname <- Sys.getenv("DB_NAME")
  host <- Sys.getenv("DB_HOST")
  port <- Sys.getenv("DB_PORT")
  user <- Sys.getenv("DB_USER")
  pass <- Sys.getenv("DB_PASS")
  ssl <- Sys.getenv("DB_SSL")
  
  if (any(c(dbname, host, port, user, pass, ssl) == "")) {
    stop(glue::glue("Can't find credentials in .Renviron!"))
  } 
  conn <- DBI::dbCanConnect(
      RPostgres::Postgres(),
      dbname = dbname,
      host = host,
      port = port,
      user = user,
      password = pass,
      sslmode = ssl
    )
  
  if(!conn) {
    stop(glue::glue("Couldn't make connection to database `{dbname}`."))
  }
  if (test) {
    DBI::dbDisconnect(conn)
    return(invisible(NULL))
  } else {
    return(conn)
  }
}

util_check_for_tables <- function(conn, table_names) {
  #' Check Whether Database Table Exists
  #' 
  #' Checks whether a specified table exists in a PostGIS database.
  #'
  #' @param conn A `DBIConnection`.
  #' @param table_name Name of table to check for existence of.
  #' 
  #' @return `TRUE` if table exists, `FALSE` if it does not.
  #' 
  #' @export
  
  all(table_names %in% DBI::dbListTables(conn))
}

util_run_tables_exist <- function(tables, push_dbs) {
  l <- util_conn(push_dbs$load)
  p <- util_conn(push_dbs$proc)
  d <- util_conn(push_dbs$dedupe)
  tables_exist <- list(
    load = util_check_for_tables(
      l,
      tables$load
    ),
    proc = util_check_for_tables(
      p,
      tables$proc
    ),
    dedupe = util_check_for_tables(
      d,
      tables$dedupe
    )
  )
  DBI::dbDisconnect(l)
  DBI::dbDisconnect(p)
  DBI::dbDisconnect(d)
  tables_exist
}

util_run_which_tables <- function(routines, push_dbs, oc_path) {
  load_tables <- c()
  proc_tables <- c()
  dedupe_tables <- c()
  oc_path <- !is.null(oc_path)
  if (routines$proc) {
    load_tables <- c(
      load_tables, 
      c("zips", "places", "init_assess", "parcels",
        "init_addresses")
    )
    proc_tables <- c(
      proc_tables, 
      c("proc_assess", "proc_sites", "proc_owners", 
        "parcels_point")
    )
    if(oc_path) {
      load_tables <- c(
        load_tables,
        c("init_companies", "init_officers")
      )
      proc_tables <- c(
        proc_tables,
        c("proc_companies", "proc_officers")
      )
    }
  }
  if (routines$load) {
    load_tables <- c(
      load_tables,
      c("munis", "zips", "places", "init_assess", "init_addresses",
      "init_companies", "init_officers", "parcels", "block_groups", "tracts")
    )
    if(oc_path) {
      load_tables <- c(
        load_tables,
        c("init_companies", "init_officers")
      )
    }
  }
  if (routines$dedupe) {
    load_tables <- c(
      load_tables, 
      c("init_addresses")
    )
    if (push_dbs$load != push_dbs$dedupe) {
      load_tables  <- c(
        load_tables, 
        c("munis", "zips", "block_groups", "tracts")
      )
    }
    proc_tables <- c(
      proc_tables,
      c("proc_sites", "proc_owners", 
        "parcels_point")
    )
    dedupe_tables <- c(
      dedupe_tables,
      c("sites_to_owners", 'owners',
        "sites", 'metacorps_cosine',
        'addresses')
    )
    if(oc_path) {
      proc_tables <- c(
        proc_tables,
        c("proc_companies", "proc_officers")
      )
      dedupe_tables <- c(
        dedupe_tables,
        c('companies', 'officers', 'metacorps_network')
      )
      
    }
  }
  list(
    load = unique(load_tables),
    proc = unique(proc_tables),
    dedupe = unique(dedupe_tables)
  )
}

util_prompt_check <- function(prompt) {
  util_log_message(prompt)
  if (interactive()) {
    r <- readline()
  } else {
    r <- readLines("stdin",n=1);
  }
  if (r %in% c("Y", "y", "N", "n")) {
    check <- TRUE
  } else {
    util_log_message(
      glue::glue("VALIDATION: Response '{r}' is invalid. Must be Y or N.")
      )
    check <- FALSE
  }
  if (!check) {
    util_prompt_check(prompt)
  } else {
    if (r %in% c("Y", "y")) {
      util_log_message(
        glue::glue("VALIDATION: You answered '{r}'! Moving right along.")
      )
      return(TRUE)
    } else if (r %in% c("N", "n")) {
      util_log_message(
        glue::glue("VALIDATION: You answered '{r}'! Stopping.")
      )
      return(FALSE)
    }
  }
}

util_prompts <- function(refresh, muni_ids, company_count_gt0) {
  if (refresh) {
    continue <- util_prompt_check(
      "VALIDATION: Refresh set. Any explictly identified subroutines will rerun. Continue? (Y/N) "
    )
    if (!continue) {
      return(continue)
    }
  }
  
  if (is.null(muni_ids)) {
    continue <- util_prompt_check(
      "VALIDATION: No municipalities specified. This will run the process for the whole state, which will take a long time. Continue? (Y/N) "
    ) 
    if (!continue) {
      return(continue)
    }
  }
  
  if (!company_count_gt0) {
    continue <- util_prompt_check(
      "VALIDATION: No company count passed. This will run the process for all companies, which will take a long time. Continue? (Y/N) "
    ) 
    if (!continue) {
      return(continue)
    }
  }
  TRUE
}

# util_what_should_run <- function(routines, tables_exist, refresh) {
#   load_init <- routines$load
#   if (((routines$proc & tables_exist$proc) | (routines$dedupe & tables_exist$dedupe)) & !routines$load & !refresh) {
#     routines$load <- FALSE
#   } else if (routines$proc | routines$dedupe) {
#     routines$load <- TRUE
#   }
#   proc_init <- routines$proc
#   if (routines$dedupe & tables_exist$dedupe & !routines$proc & !refresh) {
#     routines$proc <- FALSE
#   } else if (routines$dedupe) {
#     routines$proc <- TRUE
#   }
#   list(
#     routines = routines,
#     load_init = load_init,
#     proc_init = proc_init
#   )
# }

util_table_list <- function() {
  list(
    munis = NULL,
    zips = NULL,
    places = NULL,
    block_groups = NULL,
    tracts = NULL,
    hydro = NULL,
    parcels = NULL,
    parcels_point = NULL,
    init_assess = NULL,
    init_addresses = NULL,
    init_companies = NULL,
    init_officers = NULL,
    proc_assess = NULL,
    proc_sites = NULL,
    proc_owners = NULL,
    proc_companies = NULL,
    proc_officers = NULL,
    dedupe_owners = NULL,
    dedupe_companies = NULL,
    dedupe_officers = NULL,
    dedupe_sites = NULL,
    dedupe_sites_to_owners = NULL,
    dedupe_addresses = NULL,
    dedupe_metacorps = NULL
  )
}
