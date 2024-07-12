source("R/globals.R")
source("R/standardizers.R")
source("R/loaders.R")

# Read Municipalities ====
MUNIS <- load_layer_flow(
  load_conn(), 
  "munis", 
  loader=load_munis(
    crs=CRS
    ), 
  refresh=REFRESH_DATA
)

load_test_muni_subset(
  test_munis=TEST_MUNIS, 
  munis=MUNIS
)

# Read ZIPs ====
ZIPS <- load_layer_flow(
  load_conn(),
  "zips",
  load_zips(
    munis=MUNIS,
    crs=CRS
    ),
  refresh=REFRESH_DATA
)

# Read Placenames ====
PLACES <- load_layer_flow(
  load_conn(),
  "places",
  load_places(
    munis=MUNIS,
    crs=CRS
    ),
  refresh=REFRESH_DATA
)

# Read Assessors Tables ====
ASSESS <- load_layer_flow(
  load_conn(),
    "assess",
    load_assess_all_vintages(
      path=file.path(DATA_DIR, ASSESS_GDB_FOLDER),
      muni_ids=load_muni_subset(TEST_MUNIS, munis = MUNIS),
      munis=MUNIS
      ),
    refresh=REFRESH_DATA
  )

# Read Parcels ====
PARCELS <- load_layer_flow(
    load_conn(),
    "parcels",
    loader=load_parcels_all_vintages(
      path=file.path(DATA_DIR, ASSESS_GDB_FOLDER),
      muni_ids=load_muni_subset(TEST_MUNIS, munis = MUNIS),
      crs=CRS
      ),
    refresh=REFRESH_DATA
  )

# Read Master Address File ====
ADDRESSES <- load_layer_flow(
    load_conn(),
    "addresses",
    load_address_points(
      muni_ids=load_muni_subset(TEST_MUNIS, munis = MUNIS),
      munis=MUNIS, 
      parcels=PARCELS, 
      crs=CRS
      ),
    refresh=REFRESH_DATA
  )

# Read OpenCorpoates Companies ====
COMPANIES <- load_layer_flow(
  load_conn(),
  "companies",
  load_companies(
    path=file.path(DATA_DIR, OC_FOLDER),
    gdb_path=file.path(DATA_DIR, ASSESS_GDB_FOLDER),
    filename=OC_COMPANIES
  ),
  refresh=REFRESH_DATA
)

# Read OpenCorporates Officers ====
OFFICERS <- load_layer_flow(
  load_conn(),
  "officers",
  load_officers(
    path=file.path(DATA_DIR, OC_FOLDER),
    filename=OC_OFFICERS,
    companies=COMPANIES
  ),
  refresh=REFRESH_DATA
)

