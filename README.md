# Deduplicate Owners

This repository deduplicates property owners in Massachusetts using the [MassGIS standardized assessors' parcel dataset](https://www.mass.gov/info-details/massgis-data-property-tax-parcels) and the [Secretary of the Commonwealth's Corporate Database](https://corp.sec.state.ma.us/corpweb/CorpSearch/CorpSearch.aspx). The process is very  similar to that documented by [Hangen and O'Brien (2022, in preprint)](https://osf.io/preprints/socarxiv/anvke/), which itself is similar (though not identical) to methods used by Henry Gomory 2021 and the Anti-Eviction Mapping Project's Evictorbook (see e.g., McElroy and Amir-Ghassemi 2021). In outline...

1. Prepare data using a large number of string-standardizing functions, some of which are place-based. (In other words, when adapting to non-Massachusetts locations, you'll want to consider how to adapt our codebase to your locale.)
2. Perform naive deduplication on assessors' tables using concatenated name and address.
3. Perform cosine-similarity-based deduplication on assessors' tables using concatenated name and address.
4. Join parcels to companies using simple string matching. Note that here, when an owner fails to match within a cosine-similarity group that contains successful matches (see step 3), the owners that fail to match are assigned to the company id of one of the successful matches.

    + TODO: replace this step with efficient fuzzy-matching (we're probably losing a fair number of matches here).
  
5. Identify agents of companies that are companies themselves (distinguishing between law firms and other companies) and agents of companies that are individuals.
6. Deduplicate individuals (including individual agents) associated with companies that match parcel owners using both naive and cosine similarity methods.
7. Identify communities within corporate-individual networks. (This is done using the `igraph` implementation of the fast greedy modularity optimization algorithm.)

## Getting Started

This library's dependencies are managed using `renv`. To install necessary dependencies, simply install `renv` and run `renv::restore()`. If you are using Windows, you'll probably have to [install the `Rtools` bundle appropriate for your version of R](https://cran.r-project.org/bin/windows/Rtools/).

We provide an onmibus `run()` function in `run.R`. It takes two parameters:

1. `subset`: If value is `"test"` (default), processes only Somerville. If value is `"hns"`, processes only HNS municipalities. If value is `"all"`, runs entire state. Otherwise, it stops and generates an error.
2. `return_results`:   If `TRUE` (default), return results in a named list. If `FALSE`, return nothing. In either case, results are output to delimited text and `*.RData` files.

In other words...

```r
# Runs on Somerville.
run(subset = "test")
# Runs on Healthy Neighborhoods municipalities.
run(subset = "hns")
# Runs on entire state.
run(subset = "all")
```

If `run.R` is executed from a non-interactive environment (i.e., a terminal), it will run on the entire state. (In other words: don't do this unless you want to wait 8 hours for results.)

This function automatically saves its results to... 

+ a simplified table of owners (by default, `owners.csv`, set using the `OWNERS_OUT_NAME` global variable at the top of `run.R`),
+ a table of matched companies (by default, `corps.csv`, set using the `CORPS_OUT_NAME` global variable at the top of `run.R`),
+ a table of individuals (by default, `inds.csv`, set using the `INDS_OUT_NAME` global variable at the top of `run.R`),
+ + a table of assessors records, supplemened by owner-occupancy flag (by default, `assess.csv`, set using the `ASSESS_OUT_NAME` global variable at the top of `run.R`),
+ a simplified `igraph` `community` object (by default, `community.csv`, set using the `COMMUNITY_OUT_NAME` global variable at the top of `run.R`),

## Data

The two databases necessary for this analysis are...

+ MassGIS. "Property Tax Parcels." https://www.mass.gov/info-details/massgis-data-property-tax-parcels. 

  + We assume that you're using the statewide geodatabase, which we encourage you to request from the state. (It's a very, very simple request form and the turnaround is essentially immediate.)
  
+ Massachusetts Secretary of the Commonwealth. "Corporate Database." [Public access to the database is provided by this interface](https://corp.sec.state.ma.us/corpweb/CorpSearch/CorpSearch.aspx).

  + We can provide our copy of the Secretary of the Commonwealth database on request. Please get in touch with Eric Robsky Huntley ([ehuntley@mit.edu](mailto:ehuntley@.mit.edu)) and Asya Aizman ([aizman@mit.edu](mailto:aizman@mit.edu)). We make no guarantees as to its currency.

## Acknowledgements

This work received grant support from the Conservation Law Foundation and was developed under the auspices of the [Healthy Neighborhoods Study](https://hns.mit.edu/) in the [Department of Urban Studies and Planning](https://dusp.mit.edu/) at MIT.

## References

+ Henry Gomory. 2021. “The Social and Institutional Contexts Underlying Landlords’ Eviction Practices.” _Social Forces_. https://doi.org/10.1093/sf/soab063.
+ Forrest Hangen and Daniel T. O’Brien. 2022. “Linking Landlords to Uncover Ownership Obscurity.” SocArXiv. https://doi.org/10.31235/osf.io/anvke.
+ Erin McElroy and Azad Amir-Ghassemi. 2020. “Evictor Structures: Erin McElroy and Azad Amir-Ghassemi on Fighting Displacement.” _Logic Magazine_, 2020. https://logicmag.io/commons/evictor-structures-erin-mcelroy-and-azad-amir-ghassemi-on-fighting/.
