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
      muni_ids=load_muni_subset(TEST),
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
      muni_ids=load_muni_subset(TEST),
      crs=CRS
      ),
    refresh=REFRESH_DATA
  )

# Read Master Address File ====
ADDRESSES <- load_layer_flow(
    load_conn(),
    "addresses",
    load_address_points(
      muni_ids=load_muni_subset(TEST),
      munis=MUNIS, 
      parcels=PARCELS, 
      crs=CRS
      ),
    refresh=REFRESH_DATA
  )