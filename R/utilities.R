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

util_print_splash <- function() {
  body <- c(
    "",
    "WHO OWNS MASSACHUSETTS?",
    "====================",
    "Assessor's database enrichment and property owner deduplication workflow ",
    "https://github.com/mit-spatial-action/who-owns-mass-processing",
    "",
    "A project of the...",
    "====================",
    "MIT DUSP Spatial Action & Analysis Research Group",
    "MIT DUSP Healthy Neighborhoods Study",
    "",
    "Primary Contributors",
    "====================",
    "Eric Robsky Huntley, PhD (ehuntley@mit.edu)",
    "Asya Aizman (aizman@mit.edu)",
    "",
    "(c) 2024 Eric Robsky Huntley. Made available under an MIT License",
    ""
  )
  
  length <- ceiling(max(nchar(body)) / 2) * 2
  body <- stringr::str_pad(body, width=length, side="both")
  
  util_log_message(
    stringr::str_c(
      stringr::str_c(
        stringr::str_c(
          "# ",
          c(
            strrep("# ", length / 2), 
            body, 
            strrep("# ", length / 2)
          ),
          "#"
        ),
        collapse = "\n"
      ),
      "\n\n",
      sep=""
    ),
    timestamp = FALSE
  )
}

util_test_muni_ids <- function(muni_ids, path, quiet=FALSE) {
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
  #' @param path Path to data directory.
  #' @param file CSV file containing municipality IDs.
  #' 
  #' @return A transformed vector of municipality IDs.
  #' 
  #' @export
  muni_table <- load_muni_helper(path) 
  ids <- muni_table |>
    dplyr::pull(id)
  if (is.null(muni_ids)) {
    muni_ids <- std_pad_muni_ids(ids) |>
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
    muni_ids <- std_pad_muni_ids(muni_ids)
  }
  if(all(muni_ids %in% ids)) {
    if(!quiet) {
      util_log_message("VALIDATION: Municipality IDs are valid. 🚀🚀🚀")
    }
  } else {
    if(!quiet) {
      stop("VALIDATION: Invalid municipality IDs provided.")
    }
  }
  muni_ids
}

util_test_conn <- function(prefix="") {
  #' Load DBMS Connection
  #' 
  #' Creates connection to remote or local PostGIS connection. Requires a
  #' variables to be set in `.Renviron`.
  #'
  #' @param remote If `TRUE`, creates connection to remote db. If `FALSE`,
  #'    creates connection to local PostGIS instance.
  #' 
  #' @return dbConnect() returns an S4 object that inherits from DBIConnection.
  #'    This object is used to communicate with the database engine.
  #' 
  #' @export
  if (prefix=="") {
    prefix <- "DB"
  } else {
    prefix <- stringr::str_c(stringr::str_to_upper(prefix), "DB", sep="_")
  }
  dbname <- Sys.getenv(stringr::str_c(prefix, "NAME", sep="_"))
  host <- Sys.getenv(stringr::str_c(prefix, "HOST", sep="_"))
  port <- Sys.getenv(stringr::str_c(prefix, "PORT", sep="_"))
  user <- Sys.getenv(stringr::str_c(prefix, "USER", sep="_"))
  pass <- Sys.getenv(stringr::str_c(prefix, "PASS", sep="_"))
  ssl <- Sys.getenv(stringr::str_c(prefix, "SSL", sep="_"))
  if (any(c(dbname, host, port, user) == "")) {
    stop(glue::glue("VALIDATION: Can't find '{prefix}' credentials in .Renviron!"))
  } 
  test_connect <- DBI::dbCanConnect(
      RPostgres::Postgres(),
      dbname = dbname,
      host = host,
      port = port,
      user = user,
      password = pass,
      sslmode = ssl
    )
  if(!test_connect) {
    stop(glue::glue("VALIDATION: Couldn't make connection to {prefix} database."))
  } else {
    util_log_message(
      glue::glue("VALIDATION: Successfully tested connection to '{dbname}' on '{host}'.")
      )
  }
}

util_conn <- function(prefix="") {
  #' Load DBMS Connection
  #' 
  #' Creates connection to remote or local PostGIS connection. Requires a
  #' variables to be set in `.Renviron`.
  #'
  #' @param remote If `TRUE`, creates connection to remote db. If `FALSE`,
  #'    creates connection to local PostGIS instance.
  #' 
  #' @return dbConnect() returns an S4 object that inherits from DBIConnection.
  #'    This object is used to communicate with the database engine.
  #' 
  #' @export
  if (prefix=="") {
    prefix <- "DB"
  } else {
    prefix <- stringr::str_c(stringr::str_to_upper(prefix), "DB", sep="_")
  }
  dbname <- Sys.getenv(stringr::str_c(prefix, "NAME", sep="_"))
  host <- Sys.getenv(stringr::str_c(prefix, "HOST", sep="_"))
  port <- Sys.getenv(stringr::str_c(prefix, "PORT", sep="_"))
  user <- Sys.getenv(stringr::str_c(prefix, "USER", sep="_"))
  pass <- Sys.getenv(stringr::str_c(prefix, "PASS", sep="_"))
  ssl <- Sys.getenv(stringr::str_c(prefix, "SSL", sep="_"))
  DBI::dbConnect(
    RPostgres::Postgres(),
    dbname = dbname,
    host = host,
    port = port,
    user = user,
    password = pass,
    sslmode = ssl
  )
}

util_check_for_results <- function() {
  results_dfs <- c("addresses", "block_groups", "companies", "metacorps_network", 
                   "munis", "officers", "owners", "parcels_point", "sites",
                   "sites_to_owners", "tracts", "zips")
  all(sapply(results_dfs, exists))
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
