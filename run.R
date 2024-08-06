source("R/loaders.R")
source('R/standardizers.R')
source('R/deduplicaters.R')
source("R/processors.R")
source("R/utilities.R")
source("R/runner.R")
source("config.R")

if (COMPLETE_RUN) {
  REFRESH <- TRUE
  COMPANY_TEST <- FALSE
  MUNI_IDS <- NULL
  ROUTINES <- list(
    load = TRUE,
    proc = TRUE,
    dedupe = TRUE
  )
}

# This is like if __name__ == "__main__" in python.
if (!interactive()) {
  results <- run(
    data_path=DATA_PATH,
    muni_ids=MUNI_IDS,
    refresh=REFRESH,
    crs=CRS,
    gdb_path=GDB_PATH,
    oc_path=OC_PATH,
    thresh=COSINE_THRESH,
    inds_thresh=INDS_THRESH,
    zip_int_thresh=ZIP_INT_THRESH,
    routines=ROUTINES,
    company_test=COMPANY_TEST,
    company_test_count=COMPANY_TEST_COUNT,
    push_dbs=PUSH_DBS,
    return_intermediate=RETURN_INTERMEDIATE,
    quiet=QUIET
  )
  invisible(results)
  util_log_message("CLOSING R SESSION.", header=TRUE)
} else {
  results <- run(
    data_path=DATA_PATH,
    muni_ids=MUNI_IDS,
    refresh=REFRESH,
    crs=CRS,
    gdb_path=GDB_PATH,
    oc_path=OC_PATH,
    thresh=COSINE_THRESH,
    inds_thresh=INDS_THRESH,
    zip_int_thresh=ZIP_INT_THRESH,
    routines=ROUTINES,
    company_test=COMPANY_TEST,
    company_test_count=COMPANY_TEST_COUNT,
    push_dbs=PUSH_DBS,
    return_intermediate=RETURN_INTERMEDIATE,
    quiet=QUIET
  )
  util_log_message("ASSIGNING R OBJECTS.", header=TRUE)
  for(i in 1:length(results)) assign(names(results)[i], results[[i]])
  rm(results)
}