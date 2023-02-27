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

