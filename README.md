# Who Owns Massachusetts Processing and Deduplication

This repository deduplicates property owners in Massachusetts using the [MassGIS standardized assessors' parcel dataset](https://www.mass.gov/info-details/massgis-data-property-tax-parcels) and legal entity data sourced from [OpenCorporates](https://opencorporates.com/) under their ['public-benefic project' program](https://opencorporates.com/plug-in-our-data/). The process builds on [Hangen and O'Brien's methods (2022, in preprint)](https://osf.io/preprints/socarxiv/anvke/), which are themselves similar (though not identical) to methods used by Henry Gomory (2021) and the Anti-Eviction Mapping Project's Evictorbook (see e.g., McElroy and Amir-Ghassemi 2021).

While we share large parts of their approach (i.e., relying on community detection on company-officer relationships, following cosine-similarity deduplication of names), we believe that our results are more robust for several reasons. Primarily, we expend a great deal of effort on address standardization so that we can use addresses themselves as network entities (prior approaches, to our knowledge, have just concatenated addresses and names prior to deduplication). This is a substantial change: "similar" addresses, by whatever measure, can still be very different addresses. By relying on standardized unique addresses, we believe that we are substantially reducing our false positive rate.

Community detection---based on both network analysis and cosine similarity---is accomplished using the `igraph` implementation of the fast greedy modularity optimization algorithm.

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
# Or whatever your port name
DB_PORT=5432
DB_NAME="yourdbname"
```

`.Renviron` is in `.gitignore` to ensure that you don't commit your credentials.

### Configuration (`config.R`)

We expose a large number of configuration variables in `config.R`, which is sourced in `run.R`. In order...

| Variable              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|-----------|-------------------------------------------------------------|
| `COMPLETE_RUN`        | Default: `FALSE`A little helper that overrides values such that `REFRESH=TRUE`, `MUNI_IDS=NULL`,and `COMPANY_TEST=FALSE`. This ensures a fresh, statewide run on complete datasets, not subsets.                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `REFRESH`             | Default: `TRUE`If `TRUE`, datasets will be reingested regardless of whether results already exist in the database.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `MUNI_IDS`            | Default: `c(274, 49, 35)`If `NULL`, runs process for all municipalities in Massachusetts. If `"hns"`, runs process for Healthy Neighborhoods Study Municipalities (minus Everett because they don't make owner names consistently available). Otherwise, a vector of numbers or strings, but must match municipality IDs used by the state. (Consult `muni_ids.csv` for these.) If numbers, they will be 0-padded.                                                                                                                                                                                                                                            |
| `COMPANY_TEST_COUNT`  | Default: `50000`The OpenCorporates datasets are big. For that reason, during development it's useful to read in test subsets. This is the number of companies to read in when `COMPANY_TEST` is `TRUE`.                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `COMPANY_TEST`        | Default: `TRUE`If `TRUE`, reads in only `COMPANY_TEST_COUNT` companies and any officers associated with those companies. (Usually on the order of 4x the number of companies.)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `RETURN_INTERMEDIATE` | Default: `TRUE`If `TRUE`, `run()` returns intermediate tables. Otherwise, yields only the deduplication results.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `COSINE_THRESH`       | Default: `0.85`The minimum cosine similarity treated as a match.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `INDS_THRESH`         | Default: `0.95`The minimum cosine similarity treated as a match for non-institutional owners. This is higher because there are so many more duplicative names. Note that this is address-bounded, so even close matches will not appear as the same unless there is a shared address.                                                                                                                                                                                                                                                                                                                                                                         |
| `ZIP_INT_THRESH`      | Default: `1`One of our address-parsing tricks is to use ZIP codes that fall entirely within a single MA municipality to fill missing cities, and MA municipalities that fall entirely within a ZIP code to fill missing ZIP codes. This adjusts how close to 'entirely' these need to be - note that a value of `1` introduces substantial computational efficiencies because we can simply use a spatial join with a `sf::st_contains_properly` predicate rather than the much more expensive intersection. (It also means, unfortunately, that there are none of the second case – no municipalities fall entirely within ZIP codes without some fuzziness. |
| `QUIET`               | Default: `FALSE`If `TRUE`, suppresses log messages. Logs are written to a datetime-stamped file in `/logs`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `CRS`                 | Default: `2249`EPSG code for coordinate reference system of spatial outputs and *almost* any spatial analysis in the workflow. `2249` is NAD83 / Massachusetts Mainland in US feet. (The *almost* is because ZIPS are processed nationwide using NAD 83 / Conus Albers, AKA EPSG `5070`. We don't expose this.)                                                                                                                                                                                                                                                                                                                                               |
| `OC_PATH`             | Default: `"2024-04-12"`This is the name of the folder (within `/data`) that contains the OpenCorporates bulk data. Scripts depend on `companies.csv` and `officers.csv`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `GDB_PATH`            | Default: `"L3_AGGREGATE_FGDB_20240703"`This is either a folder (within `/data`) containing all the vintages of the MassGIS parcel data *or* a single most recent vintage geodatabase (in `/data`).                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

### Running the Script

We provide an onmibus `run()` function in `run.R`. It triggers three sequences: a data ingestion sequence (`load_read_write_all()`), a data processing sequence (`flow_process_all()`) and a deduplication sequence (`dedupe_all()`).

This function automatically outputs results to objects that will be visible in an RStudio environment using `wrapr::unpack()`. It also writes final results to `.csv` and `.Rda` files in `/results`.

## Data

The two databases necessary for this analysis are...

-   MassGIS. "Property Tax Parcels." <https://www.mass.gov/info-details/massgis-data-property-tax-parcels>.

-   OpenCorporates. [Bulk Data Product](https://opencorporates.com/plug-in-our-data/).

    -   Unfortunately, we can't provide a copy of this due to our licensing agreement.

## Acknowledgements

This work received grant support from the Conservation Law Foundation and was developed under the auspices of the [Healthy Neighborhoods Study](https://hns.mit.edu/) in the [Department of Urban Studies and Planning](https://dusp.mit.edu/) at MIT with input from the [Metropolitan Area Planning Council](https://www.mapc.org/).

## References

-   Henry Gomory. 2021. “The Social and Institutional Contexts Underlying Landlords’ Eviction Practices.” *Social Forces*. <https://doi.org/10.1093/sf/soab063>.
-   Forrest Hangen and Daniel T. O’Brien. 2022. “Linking Landlords to Uncover Ownership Obscurity.” SocArXiv. <https://doi.org/10.31235/osf.io/anvke>.
-   Erin McElroy and Azad Amir-Ghassemi. 2020. “Evictor Structures: Erin McElroy and Azad Amir-Ghassemi on Fighting Displacement.” *Logic Magazine*, 2020. <https://logicmag.io/commons/evictor-structures-erin-mcelroy-and-azad-amir-ghassemi-on-fighting/>.
