# Data Folder

This folder should contain input data. Standard data files include...

+ `CSC_CorpDataExports_VB.txt`: Describes corporate entities in the state of Massachusetts. (Secretary of the Commonwealth.)
+ `CSC_CorporationsIndividualExport_VB.txt`: Describes individual managers of corporations. (Secretary of the Commonwealth.)
+ `MassGIS_L3_Parcels.gdb`: Geodatabase containing parcels and assessors tables in Massachusetts. (MassGIS)

Included in this repo are the following utility files...

+ `hns_munis.csv`: A table of Healthy Neighborhoods Study municipalities, at this point only used to filter data to be processed.
+ `bos_neigh.csv`: Not yet required, but we anticipate needing it for Boston neighborhood standardization (e.g., "DORCHESTER" -> "BOSTON").