run <- function(data_path,
                muni_ids,
                refresh,
                crs,
                gdb_path,
                oc_path,
                thresh,
                inds_thresh,
                zip_int_thresh,
                routines,
                company_test,
                company_test_count,
                push_dbs,
                return_intermediate,
                quiet) {
  
  if (!company_test) {
    company_test_count <- NULL
  }
  
  # Check for Existence of Tables Needed for Each Subroutine
  # ===
  tables <- util_run_which_tables(
    routines, 
    push_dbs, 
    oc_path
    )
  tables_exist <- util_run_tables_exist(tables, push_dbs)
  
  if (interactive()) {
    out <- list()
  } else {
    out <- NULL
  }
  
  # Ingest or Load Data
  # ===
  if (routines$load | 
      ((routines$proc & refresh) | (routines$proc & !tables_exist$proc)) | 
      ((routines$dedupe & refresh) | (routines$dedupe & !tables_exist$dedupe))) {
    load_read_write_all(
      data_path=data_path,
      muni_ids=muni_ids,
      crs=crs,
      gdb_path=gdb_path,
      oc_path=oc_path,
      zip_int_thresh=zip_int_thresh,
      tables=tables$load,
      quiet=quiet,
      company_test_count=company_test_count,
      # Don't refresh if load tables exist and user has specified a subroutine.
      refresh=refresh & routines$load,
      push_db=push_dbs$load
    ) |>
      wrapr::unpack(
        munis,
        zips,
        block_groups,
        tracts,
        places,
        parcels,
        assess,
        addresses,
        companies,
        officers
      )
    
    if ((return_intermediate | (routines$load & (!routines$dedupe & !routines$proc))) & interactive()) {
      out[['init_assess']] <- assess
      out[['init_addresses']] <- addresses
      out[['init_companies']] <- companies
      out[['init_officers']] <- officers
      out[['places']] <- places
    }
    
    if (interactive()) {
      out[['parcels']] <- parcels
      out[['munis']] <- munis
      out[['zips']] <- zips
      out[['block_groups']] <- block_groups
      out[['tracts']] <- tracts
    }
    
    if (push_dbs$load != push_dbs$dedupe & routines$dedupe) {
      util_log_message(
        "Deduplication database different than load. Writing select results"
      )
      conn <- util_conn(push_dbs$dedupe)
      
      if (!util_check_for_tables(conn, c("munis", "zips", "tracts", "block_groups")) | refresh) {
        util_log_message(
          "Writing load tables to prod.",
          header = TRUE
        )
        munis |>
          load_write(conn, "munis", id_col="muni_id", overwrite=TRUE)
        zips |>
          dplyr::filter(state == "MA") |>
          load_write(conn, "zips", id_col=c("zip", "state"), overwrite=TRUE)
        block_groups |>
          load_write(conn, "block_groups", id_col="id", overwrite=TRUE)
        tracts |>
          load_write(conn, "tracts", id_col="id", overwrite=TRUE)
      } else {
        util_log_message(
          "Load tables already exist on prod."
        )
      }
      DBI::dbDisconnect(conn)
    }
    
    rm(block_groups, tracts, munis) |> suppressWarnings()
    invisible(gc())
  }
  
  # Process All Input Tables
  # ===
  if (routines$proc | 
      ((routines$dedupe & refresh) | (routines$dedupe & !tables_exist$dedupe))) {
    proc_all(
      assess=assess,
      companies=companies,
      officers=officers,
      addresses=addresses,
      zips=zips,
      parcels=parcels,
      places=places,
      crs=crs,
      tables=tables$proc,
      quiet=quiet,
      # Don't refresh if tables exist and user has specified a subroutine.
      refresh=refresh & routines$proc,
      push_db=push_dbs$proc
    ) |>
      wrapr::unpack(
        parcels_point,
        assess,
        sites,
        owners,
        companies,
        officers
      )
    
    rm(parcels, zips, places) |> suppressWarnings()
    
    if (interactive()) {
      out[['parcels_point']] <- parcels_point
    }
    
    if (push_dbs$proc != push_dbs$dedupe & routines$dedupe) {
      conn <- util_conn(push_dbs$dedupe)
      if (!util_check_for_tables(conn, "parcels_point") | refresh) {
        util_log_message(
          "Deduplication database different than processing. Writing select results.",
        )
        parcels_point |>
          load_write(conn, "parcels_point", id_col="loc_id", overwrite=TRUE)
      } else {
        util_log_message(
          "Processing tables already exist on prod."
        )
      }
      DBI::dbDisconnect(conn)
    }
    
    rm(parcels_point) |> suppressWarnings()
    
    if ((return_intermediate | (routines$proc & !routines$dedupe)) & interactive()) {
      out[['proc_assess']] <- assess
      out[['proc_sites']] <- sites
      out[['proc_owners']] <- owners
      out[['proc_companies']] <- companies
      out[['proc_officers']] <- officers
    }
    
    rm(assess) |> suppressWarnings()
    invisible(gc())
  }
  
  # De-duplicate!
  # ===
  if (routines$dedupe) {
    dedupe_all(
      owners=owners,
      companies=companies,
      officers=officers,
      sites=sites,
      addresses=addresses,
      thresh=thresh,
      inds_thresh=inds_thresh,
      tables=tables$dedupe,
      quiet=quiet,
      refresh=refresh & routines$dedupe,
      push_db=push_dbs$dedupe
    ) |>
      wrapr::unpack(
        sites_to_owners,
        owners,
        companies,
        officers,
        sites,
        metacorps_network,
        metacorps_cosine,
        addresses
      )
    
    if (interactive()) {
      out[['sites_to_owners']] <- sites_to_owners
      out[['owners']] <- owners
      out[['companies']] <- companies
      out[['officers']] <- officers
      out[['sites']] <- sites
      out[['metacorps_network']] <- metacorps_network
      out[['metacorps_cosine']] <- metacorps_cosine
      out[['addresses']] <- addresses
    }
  }
  
  util_log_message(
    "PROCESS COMPLETE!",
    header = TRUE
  )
  
  invisible(gc())
  return(out)
}