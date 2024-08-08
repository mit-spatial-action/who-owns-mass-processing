# Who Owns Massachusetts Processing and Deduplication

This repository deduplicates property owners in Massachusetts using the [MassGIS standardized assessors' parcel dataset](https://www.mass.gov/info-details/massgis-data-property-tax-parcels) and legal entity data sourced from [OpenCorporates](https://opencorporates.com/) under their ['public-benefic project' program](https://opencorporates.com/plug-in-our-data/). The process builds on [Hangen and O'Brien's methods (2024)](https://www.tandfonline.com/doi/full/10.1080/02673037.2024.2325508), which are themselves similar (though not identical) to methods used by [Henry Gomory (2021)](https://doi.org/10.1093/sf/soab063) and the Anti-Eviction Mapping Project's [Evictorbook](https://evictorbook.com/) (see e.g., [McElroy and Amir-Ghassemi 2021](https://logicmag.io/commons/evictor-structures-erin-mcelroy-and-azad-amir-ghassemi-on-fighting/)). It also builds on Eric's experience leading development of a tool called [TenantPower](https://tenantpower.org/) with Mutual Aid Medford and Somerville in 2020, which used the [`dedupe` Python package](https://github.com/dedupeio/dedupe) in a manner similar to [Immergluck et al. (2020)](https://www.tandfonline.com/doi/full/10.1080/02673037.2019.1639635).

While we share large parts of their approach (i.e., relying on community detection on company-officer relationships, following cosine-similarity deduplication of names), we believe that our results are more robust for several reasons. Inspired, in part, by [Preis (2024)](https://doi.org/10.1080/24694452.2023.2277810), we expend a great deal of effort on address standardization so that we can use addresses themselves as network entities (prior approaches, with the exception of Preis, have just concatenated addresses and names prior to deduplication). This is a substantial change: "similar" addresses, by whatever measure, can still be very different addresses. By relying on standardized unique addresses, we believe that we are substantially reducing our false positive rate.

Community detection---based on both network analysis and cosine similarity---is accomplished using the `igraph` implementation of the fast greedy modularity optimization algorithm.

While the full process requires that you source OpenCorporates data, you can run the cosine-similarity-based deduplication process using only the assessors tables. (See the documentation for the `OC_PATH` configuration variable.)

## Getting Started

### renv

This library's dependencies are managed using [`renv`](https://rstudio.github.io/renv/articles/renv.html). To install necessary dependencies, simply install `renv` and run `renv::restore()`. If you are using Windows, you'll probably have to [install the `Rtools` bundle appropriate for your version of R](https://cran.r-project.org/bin/windows/Rtools/).

### PostgreSQL/PostGIS

The respository uses an instance of PostgreSQL with the PostGIS extension as its primary data store. You'll need to set up a PostGIS instance on either localhost or a server.

#### Setting up .Renviron

The scripts expect to find your PostgreSQL credentials, host, port, etc. in an `.Renviron` file with the following environment variables defined:

``` r
DB_HOST="yourhost"
DB_USER="yourusername"
DB_PASS="yourpassword"
# Or whatever your port
DB_PORT=5432
DB_NAME="yourdbname"
```

Optionally, you can use the `PUSH_DBS` configuration parameter to specify a different database you'd like to point subroutine results to, allowing you to separate, for example, a development environment from a production environment. If you'd like to make of this parameter, you'll need to pass a string value to the appropriate named elements in `PUSH_DBS` (see section 'Configuration (`config.R`)' below) and define...

``` r
YOURSTRING_DB_HOST="yourhost"
YOURSTRING_DB_USER="yourusername"
YOURSTRING_DB_PASS="yourpassword"
# Or whatever your port
YOURSTRING_DB_PORT=5432
YOURSTRING_DB_NAME="yourdbname"
```

If you modify your `.Renviron` mid-RStudio session, you can simply run `readRenviron('.Renviron')` to reload.

`.Renviron` is in `.gitignore` to ensure that you don't commit your credentials.

## Loading Results (`load_results.R`)

If you want to simply read the results without worrying about triggering the deduplication process, you can simply begin a new RScript, source `load_results.R`, and run a one-liner like so...

``` r
source('load_results.R')
load_results("your_db_prefix", load_boundaries=TRUE)
```

This will load `companies`, `munis`, `officers`, `owners`, `sites`, `sites_to_owners`, `parcels_point`, `metacorps_cosine` and `metacorps_network` into your R environment. If `load_boundaries` is true, it will also return `munis`, `zips`, `tracts`, and `block_groups`. **This requires that you have `.Renviron` set up with appropriate prefixes (see 'Setting up `.Renviron`', above).**

Note that for statewide results, these are very large tables and therefore it might take 5-10 minutes depending on your network connection/whether you're reading from a local or remote database.

## Running the Process (`run.R`)

We provide an onmibus `manage_run()` function in `run.R`. It does preflight testing and triggers three sequences: a data ingestion sequence (`load_read_write_all()`, see `R/loaders.R`), a data processing sequence (`proc_all()`, see `R/processors.R`) and a deduplication sequence (`dedupe_all()`, see `R/deduplicators.R`).

We recommend running from the terminal using...

``` bash
Rscript run.R
```

This is because when the process is run interactively (i.e., in an RStudio environment), intermediate results are stored in an output object, which has memory costs. You can then read the results with `load_results.R`, as described above.

If the process is run interactively, it automatically outputs results to objects in your environment (including intermediate results if `RETURN_INTERMEDIATE` is `TRUE` in `config.R`. It also writes results to `.csv` and `.Rda` files in `/results`, but doesn't ever try to read these---the PostgreSQL database is the only output location from which our scripts read data.

### Configuration (`config.R`)

We expose a large number of configuration variables in `config.R`, which is sourced in `run.R`. In order...

| Variable              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|-----------|-------------------------------------------------------------|
| `COMPLETE_RUN`        | Default: `FALSE`A little helper that overrides values such that `ROUTINES=list(load = TRUE, proc = TRUE, dedupe = TRUE)`, `REFRESH=TRUE`, `MUNI_IDS=NULL`,and `COMPANY_TEST=FALSE`. This ensures a fresh, statewide run on complete datasets, not subsets.                                                                                                                                                                                                                                                                                                                                                                                                    |
| `REFRESH`             | Default: `TRUE`If `TRUE`, datasets will be reingested regardless of whether results already exist in the database.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `PUSH_DBS`            | Default: `list(load = "", proc = "", dedupe = "")` Named list with string values. If `""`, looks for `.Renviron` database connection parameters of the format `"DB_NAME"`. If string passed, looks for parameters of the format `"YOURSTRING_DB_NAME"` where `YOURSTRING` can be passed upper or lower case, though parameters must be all uppercase. **Note that whatever `dedupe` is set to is treated as "production", meaning that select intermediate tables from previous subroutines are pushed there as well. Requires that you set `.Renviron` parameters (see section 'Setting Up `.Renviron`' above).**                                            |
| `ROUTINES`            | Default: `list(load = TRUE, proc = TRUE, dedupe = TRUE)` Allows the user to run individual subroutines (i.e., load, process, deduplicate). The subroutines are not totally indepdent, but each will run in a simplified manner when it is set to `FALSE` here, returning only results needed by subsequent subroutines.                                                                                                                                                                                                                                                                                                                                       |
| `MUNI_IDS`            | Default: `c(274, 49, 35)`If `NULL`, runs process for all municipalities in Massachusetts. If `"hns"`, runs process for Healthy Neighborhoods Study Municipalities (minus Everett because they don't make owner names consistently available). If `"mapc"`, runs process for all municipalities in the MAPC region. Otherwise, a vector of numbers or strings, but must match municipality IDs used by the state. (Consult `muni_ids.csv` for these.) If numbers, they will be 0-padded.                                                                                                                                                                       |
| `MOST_RECENT`         | Default: `FALSE` If `TRUE` (and the complete vintages MassGIS collection is being used), reads the most recent vintage for each municipality. If `FALSE`, attempts to determine which vintage has the largest number of municipalities reporting, selecting that year where possible (and selecting the most recent where a given municipality did not report in that year).                                                                                                                                                                                                                                                                                  |
| `COMPANY_TEST_COUNT`  | Default: `50000`The OpenCorporates datasets are big. For that reason, during development it's useful to read in test subsets. This is the number of companies to read in when `COMPANY_TEST` is `TRUE`.                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `COMPANY_TEST`        | Default: `TRUE`If `TRUE`, reads in only `COMPANY_TEST_COUNT` companies and any officers associated with those companies. (Usually on the order of 4x the number of companies.)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `RETURN_INTERMEDIATE` | Default: `TRUE`If `TRUE`, `run()` returns intermediate tables. Otherwise, loads only the tables yielded by the last subroutine requested into the R environment while writing all tables to the appropriate databases. (I.e., if `ROUTES` is `list(load=TRUE, proc=TRUE, dedupe=FALSE)` and `RETURN_INTERMEDIATE` is `TRUE`, it will load tables by `proc_all()` into the R environment).                                                                                                                                                                                                                                                                     |
| `COSINE_THRESH`       | Default: `0.85`The minimum cosine similarity treated as a match. Lower numbers yield matches on less closely related strings.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `INDS_THRESH`         | Default: `0.95`The minimum cosine similarity treated as a match for non-institutional owners. Lower numbers yield matches on less closely related strings. This should generally be higher than `COSINE_THRESH` because there are so many more duplicative names. Note that this is address-bounded, so even close matches will not appear as the same unless there is a shared address.                                                                                                                                                                                                                                                                      |
| `ZIP_INT_THRESH`      | Default: `1`One of our address-parsing tricks is to use ZIP codes that fall entirely within a single MA municipality to fill missing cities, and MA municipalities that fall entirely within a ZIP code to fill missing ZIP codes. This adjusts how close to 'entirely' these need to be - note that a value of `1` introduces substantial computational efficiencies because we can simply use a spatial join with a `sf::st_contains_properly` predicate rather than the much more expensive intersection. (It also means, unfortunately, that there are none of the second case – no municipalities fall entirely within ZIP codes without some fuzziness. |
| `QUIET`               | Default: `FALSE`If `TRUE`, suppresses log messages. Logs are written to a datetime-stamped file in `/logs`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `CRS`                 | Default: `2249`EPSG code for coordinate reference system of spatial outputs and *almost* any spatial analysis in the workflow. `2249` is NAD83 / Massachusetts Mainland in US feet. (The *almost* is because ZIPS are processed nationwide using NAD 83 / Conus Albers, AKA EPSG `5070`. We don't expose this.)                                                                                                                                                                                                                                                                                                                                               |
| `DATA_PATH`           | Default: `"data"` This is the folder where input datasets (i.e., OpenCorporates data and MassGIS parcel databases) are located. **Do not change unless you also plan on moving `luc_crosswalk.csv` and `muni_ids.csv`.**                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `RESULTS_PATH`        | Default: `"results"` This is the folder where resulting `.csv` and `.Rda` files will be written. Note that tables will always be written to the PostGIS database, so this is for backup/uncredentialed result transfer only.                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `OC_PATH`             | Default: `"2024-04-12"` Either the name of the folder (within `/data`) that contains the OpenCorporates bulk data or `NULL`. Scripts depend on `companies.csv` and `officers.csv`. If `NULL`, a simplified cosine-similarity deduplication routine will run, returning a simpler set of tables.                                                                                                                                                                                                                                                                                                                                                               |
| `GDB_PATH`            | Default: `"L3_AGGREGATE_FGDB_20240703"`This is either a folder (within `/data`) containing all the vintages of the MassGIS parcel data *or* a single most recent vintage geodatabase (in `/data`).                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

## Data

### Required External Data

Successful execution of all features of this software requires that you source the following datasets:

-   MassGIS. "Property Tax Parcels." <https://www.mass.gov/info-details/massgis-data-property-tax-parcels>.

    -   You should be able to use either the 'all vintages' data product (which is packaged as many geodatabases) or the 'most recent' geodatabase (which is a single GDB). If the former, pass the name of the folder to the `GDB_PATH` config parameter. If the latter, pass the geodatabase filename.

-   OpenCorporates. [Bulk Data Product](https://opencorporates.com/plug-in-our-data/). Massachusetts extract.

    -   Unfortunately, we can't provide a copy of this due to our licensing agreement, but OpenCorporates has a 'public-benefit project' program that might be worth looking into. Also, you can run a simpler cosine similarity deduplication process if you set `OC_PATH` to `NULL` in `config.R`.

### Additional Data Sources

In addition, the script pulls in data from a range of sources to enrich our datasets. All of these are ingested from API and web sources by the script, so there is no need to source them independently.

-   MassGIS. 2023. [Geographic Placenames](https://www.mass.gov/info-details/massgis-data-geographic-place-names). October.

    -   This is used to standardize municipality names. Placenames are tied to places using a spatial join. (I.e., Roxbury \> Boston). A simplified and transformed version of this is written to the database and is an intermediate output from `loaders.R`.

-   MassGIS. 2024. [Municipalities](https://www.mass.gov/info-details/massgis-data-municipalities).

    -   This is used to standardize municipality names (i.e., Roxbury \> Boston). Placenames are tied to places using a spatial join. This is written to the database and is an intermediate output from `loaders.R`.

-   US Census Bureau TIGER/Line. ZIP Code Tabulation Areas. 2022. Fetched using [Tidycensus](https://walker-data.com/tidycensus/index.html).

    -   These are used to both attach ZIP codes to parcels whose site locations are missing them and to perform a range of address standardizations (for example, many ZIP codes lie within one municipality, meaning that we can assign a municipality when one is missing assuming that it has an unambiguous ZIP code).

-   US Census Bureau TIGER/Line. States and Equivalent Entities. 2022. Fetched using [Tidycensus](https://walker-data.com/tidycensus/index.html).

    -   Used to tie ZIP codes to states. (This is useful because many, though not all ZIP codes lie within a single state, so a ZIP code can be used to assign a state when one is missing in many cases.)

-   US Census Bureau TIGER/Line. Census Tracts and Block Groups. 2022. Fetched using [Tidycensus](https://walker-data.com/tidycensus/index.html).

    -   Used to locate parcels for subsequent analysis.

-   MassGIS. 2024. [Master Address Data](https://www.mass.gov/info-details/massgis-data-master-address-data).

-   City of Boston. 2024. [Boston Live Street Address Management System (SAM) Addresses](https://bostonopendata-boston.opendata.arcgis.com/datasets/b6bffcace320448d96bb84eabb8a075f/explore).

    -   We use these geolocated addresses for three primary purposes: linking MA owner addresses to locations, identifying unique addresses which are treated as network entities, and, estimating unit counts for properties missing them (following, largely, a method provided by the [Metropolitan Area Planning Council](https://www.mapc.org/)).

## Acknowledgements

This work received grant support from the Conservation Law Foundation and was developed under the auspices of the [Healthy Neighborhoods Study](https://hns.mit.edu/) in the [Department of Urban Studies and Planning](https://dusp.mit.edu/) at MIT with input from the [Metropolitan Area Planning Council](https://www.mapc.org/). OpenCorporates has also been a supportive data partner.

## References

-   Henry Gomory. 2022. "The Social and Institutional Contexts Underlying Landlords’ Eviction Practices." *Social Forces* 100 (4): 1774-805. <https://doi.org/10.1093/sf/soab063>.
-   Forrest Hangen and Daniel T. O’Brien. 2024 (Online First). "Linking Landlords to Uncover Ownership Obscurity." *Housing Studies*. 1–26. <https://doi.org/10.1080/02673037.2024.2325508>.
-   Dan Immergluck, Jeff Ernsthausen, Stephanie Earl, and Allison Powell. 2020. "Evictions, Large Owners, and Serial Filings: Findings from Atlanta." *Housing Studies* 35 (5): 903–24. <https://doi.org/10.1080/02673037.2019.1639635>.
-   Erin McElroy and Azad Amir-Ghassemi. 2020. “Evictor Structures: Erin McElroy and Azad Amir-Ghassemi on Fighting Displacement.” *Logic Magazine*, 2020. <https://logicmag.io/commons/evictor-structures-erin-mcelroy-and-azad-amir-ghassemi-on-fighting/>.
-   Benjamin Preis. 2024 (Online First). “Where the Landlords Are: A Network Approach to Landlord-Rental Locations.” *Annals of the American Association of Geographers*. 1–12.<https://doi.org/10.1080/24694452.2023.2277810>.
