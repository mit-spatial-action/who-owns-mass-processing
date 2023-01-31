# Deduplicate Owners

This repository deduplicates property owners in Massachusetts using the [MassGIS standardized assessors' parcel dataset](https://www.mass.gov/info-details/massgis-data-property-tax-parcels) and the [Secretary of the Commonwealth's Corporate Database](https://corp.sec.state.ma.us/corpweb/CorpSearch/CorpSearch.aspx). The process is very  similar to that documented by [Hangen and O'Brien (2022, in preprint)](https://osf.io/preprints/socarxiv/anvke/), which itself is similar (though not identical) to methods used by Henry Gomory 2021 and the Anti-Eviction Mapping Project's Evictorbook (see e.g., McElroy and Amir-Ghassemi 2021). In outline...

1. Prepare data using a large number of string-standardizing functions, some of which are place-based. (In other words, when adapting to non-Massachusetts locations, you'll want to consider how to adapt our codebase to your locale.)
2. Perform naive deduplication on assessors' tables using concatenated name and address.
3. Perform cosine-similarity-based deduplication on assessors' tables using concatenated name and address.
4. Join parcels to corporations using simple string matching. Note that here, when an owner fails to match within a cosine-similarity group that contains successful matches (see step 3), the owners that fail to match are assigned to the corporation id of one of the successful matches.
5. Deduplicate individuals associated with corporations that match parcel owners using both naive and cosine similarity methods.
6. Identify communities within corporate-individual networks. (This is done using the `igraph` implementation of the fast greedy modularity optimization algorithm.)
7. Assign each community a name based on the most common name of its members.

## Getting Started

This library's dependencies are managed using `renv`. To install necessary dependencies, simply install `renv` and run `renv::restore()`. If you are using Windows, you'll probably have to [install the `Rtools` bundle appropriate for your version of R](https://cran.r-project.org/bin/windows/Rtools/).

Given the datasets we're currently working with, simply run `run.R` and execute `run()`. Note that it defaults to `test = TRUE` which will perform analysis on a small subset of assessors records in Massachusetts---namely, the records for Cambridge, Somerville, and Medford. To run for the entire state, execute `run(test = FALSE)`. This function automatically saves a simplified assessors table (`assess.txt`) with a column `id_corp` that links with the `id` column of `corps.txt`, and an individuals file (`inds.txt`) that contains a link to corporations (`id_corp`).

## Data

The two databases necessary for this analysis are...

+ MassGIS. "Property Tax Parcels." https://www.mass.gov/info-details/massgis-data-property-tax-parcels. 

  + We assume that you're using the statewide geodatabase.
  
+ Massachusetts Secretary of the Commonwealth. "Corporate Database." [Public access to the database is provided by this interface](https://corp.sec.state.ma.us/corpweb/CorpSearch/CorpSearch.aspx).

  + We can provide our copy of the Secretary of the Commonwealth database on request. Please get in touch with Eric Robsky Huntley ([ehuntley@mit.edu](mailto:ehuntley@.mit.edu)) and Asya Aizman ([aizman@mit.edu](mailto:aizman@mit.edu)). We make no guarantees as to its currency.

## Acknowledgements

This work received grant support from the Conservation Law Foundation and was developed under the auspices of the [Healthy Neighborhoods Study](https://hns.mit.edu/) in the [Department of Urban Studies and Planning](https://dusp.mit.edu/) at MIT.

## References

+ Henry Gomory. 2021. “The Social and Institutional Contexts Underlying Landlords’ Eviction Practices.” _Social Forces_. https://doi.org/10.1093/sf/soab063.
+ Forrest Hangen and Daniel T. O’Brien. 2022. “Linking Landlords to Uncover Ownership Obscurity.” SocArXiv. https://doi.org/10.31235/osf.io/anvke.
+ Erin McElroy and Azad Amir-Ghassemi. 2020. “Evictor Structures: Erin McElroy and Azad Amir-Ghassemi on Fighting Displacement.” _Logic Magazine_, 2020. https://logicmag.io/commons/evictor-structures-erin-mcelroy-and-azad-amir-ghassemi-on-fighting/.
