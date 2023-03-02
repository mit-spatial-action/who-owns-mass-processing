# Required to use piping syntax using otherwise-explicit
# namespacing.
require(magrittr)

# Use cached tigris geographies
options(tigris_use_cache = TRUE)

DATA_DIR <- "data"
# Name of directory in which results are written.
RESULTS_DIR <- "results"
# Filename of delimited text file containing individuals.
INDS <- "CSC_CorporationsIndividualExport_VB.txt"
# Filename of delimited text file containing corporate entities.
CORPS <- "CSC_CorpDataExports_VB.txt"
# Filename of MassGIS Parcels geodatabase.
ASSESS_GDB <- "MassGIS_L3_Parcels.gdb"
# Name of CSV containing limited collection of HNS municipalities
MUNI_CSV <- "hns_munis"
# Name of CSV containing Boston neighborhood names.
BOS_NBHD <- "bos_neigh"
# Name of delimited text output files.
ASSESS_OUT_NAME <- "assess"
OWNERS_OUT_NAME <- "owners"
CORPS_OUT_NAME <- "corps"
INDS_OUT_NAME <- "inds"
NODES_OUT_NAME <- "nodes"
EDGES_OUT_NAME <- "edges"
EVICTORS_OUT_NAME <- "evictors"
# Name of RData image.
RDATA_OUT_NAME <- "results"