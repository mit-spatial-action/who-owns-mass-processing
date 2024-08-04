# Use cached tigris geographies.
options(tigris_use_cache = TRUE)

# Prevent annoying "`summarise()` has grouped output by..." error.
options(dplyr.summarise.inform = FALSE)

# DO NOT CHANGE. This simply overrides other variables if COMPLETE_RUN is set.
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