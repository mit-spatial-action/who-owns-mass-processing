# Owner Deduplication

This repository deduplicates property owners in Massachusetts using the [MassGIS standardized assessors' parcel dataset](https://www.mass.gov/info-details/massgis-data-property-tax-parcels) and the [Secretary of the Commonwealth's Corporate Database](https://corp.sec.state.ma.us/corpweb/CorpSearch/CorpSearch.aspx). The process is very  similar to that documented by [Hangen and O'Brien (2022, in preprint)](https://osf.io/preprints/socarxiv/anvke/). In outline...

1. Prepare data using a large number of string-standardizing functions, some of which are place-based. (In other words, when adapting to non-Massachusetts locations, you'll want to consider how to adapt our codebase to your locale.)
2. Perform naive deduplication on assessors' tables using concatenated name and address.
3. Perform cosine-similarity-based deduplication on assessors' tables using concatenated name and address.
4. Join parcels to corporations using simple string matching. Note that here, when an owner fails to match within a cosine-similarity group that contains successful matches (see step 3), the owners that fail to match are assigned to the corporation id of one of the successful matches.
5. Deduplicate individuals associated with corporations that match parcel owners using both naive and cosine similarity methods.
6. Identify communities within corporate-individual networks. (This is done using the `igraph` implementation of the fast greedy modularity optimization algorithm.)
7. Assign each community a name based on the most common name of its members.

Given the datasets we're currently working with, simply run `run.R` and assign the output of `run()` to a variable. This will write the results to (by default) `results.RData` and a pipe-delimited text file called `results.txt`.

## Data

We can provide data on request. Please get in touch with [ehuntley@mit.edu](mailto:ehuntley@.mit.edu) and [aizman@mit.edu](mailto:aizman@mit.edu).
