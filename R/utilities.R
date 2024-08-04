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

util_muni_table <- function(path, file="muni_ids.csv") { 
  readr::read_csv(
    file.path(path, file), 
    progress=TRUE,
    show_col_types = FALSE)
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
  #'     readr::write_csv("data/muni_ids.csv")
  #'
  #' @param muni_ids Vector of municipality IDs.
  #' @param path Path to data directory.
  #' @param file CSV file containing municipality IDs.
  #' 
  #' @return A transformed vector of municipality IDs.
  #' 
  #' @export
  ids <- util_muni_table(path)  |>
    dplyr::pull(muni_id)
  if (is.null(muni_ids)) {
    muni_ids <- std_pad_muni_ids(ids)
  } else if (all(muni_ids == "hns")) {
    muni_ids <- c("163", "057", "044", "095", "035", "201", "274", "049")
  } else {
    if(!all(std_pad_muni_ids(muni_ids) %in% ids)) {
      util_log_message("VALIDATION: Got it! Check your config.R.")
      tryCatch(stop(), error = function(e) {})
    } else {
      if(!quiet) {
        util_log_message("VALIDATION: Municipality IDs are valid. 🚀🚀🚀")
      }
    }
    muni_ids <- std_pad_muni_ids(muni_ids)
  }
  muni_ids
}

util_conn <- function(remote=FALSE) {
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
  
  if (remote) {
    dbname <- "REMOTE_DB_NAME"
    host <- "REMOTE_DB_HOST"
    port <- "REMOTE_DB_PORT"
    user <- "REMOTE_DB_USER"
    password <- "REMOTE_DB_PASS"
  } else {
    dbname <- "DB_NAME"
    host <- "DB_HOST"
    port <- "DB_PORT"
    user <- "DB_USER"
    password <- "DB_PASS"
  }
  DBI::dbConnect(
    RPostgres::Postgres(),
    dbname = Sys.getenv(dbname),
    host = Sys.getenv(host),
    port = Sys.getenv(port),
    user = Sys.getenv(user),
    password = Sys.getenv(password),
    sslmode = "allow"
  )
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

util_run_tables_exist <- function(tables, push_remote) {
  l <- util_conn(push_remote$load)
  p <- util_conn(push_remote$proc)
  d <- util_conn(push_remote$dedupe)
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
      p,
      tables$proc
    )
  )
  DBI::dbDisconnect(l)
  DBI::dbDisconnect(p)
  DBI::dbDisconnect(d)
  tables_exist
}

util_run_which_tables <- function(routines, push_remote) {
  load_tables <- c()
  proc_tables <- c()
  if (routines$proc) {
    load_tables <- c(
      load_tables, 
      c("zips", "places", "init_assess", "parcels",
        "init_addresses", "init_companies", "init_officers")
    )
    proc_tables <- c(
      proc_tables, 
      c("proc_assess", "proc_sites", "proc_owners", 
        "proc_companies", "proc_officers", "parcels_point")
    )
  }
  if (routines$load) {
    load_tables <- tables <- c(
      load_tables,
      "munis", "zips", "places", "init_assess", "init_addresses",
      "init_companies", "init_officers", "parcels", "block_groups", "tracts", "hydro"
    )
  }
  if (routines$dedupe) {
    load_tables <- c(
      load_tables, 
      c("init_addresses")
    )
    proc_tables <- tables <- c(
      proc_tables,
      c("proc_sites", "proc_owners", 
        "proc_companies", "proc_officers", "parcels_point")
    )
  }
  list(
    load = unique(load_tables),
    proc = unique(proc_tables)
  )
}

util_prompt_check <- function(prompt) {
  util_log_message(prompt)
  r <- readline()
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

util_prompts <- function(refresh, muni_ids, company_test) {
  if (refresh) {
    continue <- util_prompt_check(
      "VALIDATION: REFRESH is TRUE. Any explictly identified subroutines will rerun. Continue? (Y/N) "
    )
    return(continue)
  }
  
  if (is.null(muni_ids)) {
    continue <- util_prompt_check(
      "VALIDATION: MUNI_IDS is set to NULL. This will run the process for the whole state, which will take a long time. Continue? (Y/N) "
    ) 
    return(continue)
  }
  
  if (!company_test) {
    continue <- util_prompt_check(
      "VALIDATION: COMPANY_TEST is FALSE. This will run the process for all companies, which will take a long time. Continue? (Y/N) "
    ) 
    return(continue)
  }
  return(TRUE)
}

util_what_should_run <- function(routines, tables_exist, refresh) {
  load_init <- routines$load
  if (((routines$proc & tables_exist$proc) | (routines$dedupe & tables_exist$dedupe)) & !routines$load & !refresh) {
    routines$load <- FALSE
  } else if (routines$proc | routines$dedupe) {
    routines$load <- TRUE
  }
  proc_init <- routines$proc
  if (routines$dedupe & tables_exist$dedupe & !routines$proc & !refresh) {
    routines$proc <- FALSE
  } else if (routines$dedupe) {
    routines$proc <- TRUE
  }
  list(
    # load = routines$load,
    # proc = routines$proc,
    routines = routines,
    load_init = load_init,
    proc_init = proc_init
  )
}

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
