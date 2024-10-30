# Data Dictionary

| **Author**               | Last Updated   |
|--------------------------|----------------|
| Eric Robsky Huntley, PhD | August 8, 2024 |

## Coordinate Reference System

For any spatial tables listed below (indicated using 🌐), data is stored in [NAD83 / Massachusetts Mainland (ftUS), EPSG:2249](https://epsg.io/2249-1696).

## Output Tables

### `sites`

Residential properties Each row represents a property in the assessors table.

| Field                         | Type      | Description                                                                                                                                                                                   |
|-----------|-----------|--------------------------------------------------|
| `id` (PK)                     | Integer   | Unique identifier.                                                                                                                                                                            |
| `fy`                          | Integer   | Fiscal year of assessor's database.                                                                                                                                                           |
| `muni_id` (FK to `munis`)     | String    | Identifier of property municipality.                                                                                                                                                          |
| `ls_date`                     | Date      | Last sale date. Unmodified from MassGIS, [see their documentation](https://www.mass.gov/info-details/massgis-data-property-tax-parcels).                                                      |
| `ls_price`                    | Integer   | Last sale price. Unmodified from MassGIS, [see their documentation](https://www.mass.gov/info-details/massgis-data-property-tax-parcels).                                                     |
| `bld_area`                    | Integer   | Building area. Unmodified from MassGIS, [see their documentation](https://www.mass.gov/info-details/massgis-data-property-tax-parcels).                                                       |
| `res_area`                    | Integer   | Residential area. Unmodified from MassGIS, [see their documentation](https://www.mass.gov/info-details/massgis-data-property-tax-parcels).                                                    |
| `units`                       | Integer   | Estimated unit count. For use codes that are tied to specific unit counts, assigns these counts. For others, estimates based on 1) number of addresses on the parcel and 2) residential area, |
| `lnd_val`                     | Integer   | Land value. Unmodified from MassGIS, [see their documentation](https://www.mass.gov/info-details/massgis-data-property-tax-parcels).                                                          |
| `bld_val`                     | Integer   | Building value. Unmodified from MassGIS, [see their documentation](https://www.mass.gov/info-details/massgis-data-property-tax-parcels).                                                      |
| `use_code`                    | Character | Use code. Unmodified from MassGIS, [see their documentation](https://www.mass.gov/info-details/massgis-data-property-tax-parcels).                                                            |
| `luc`                         | Character | Land use code. Assigned based on our modified version of a crosswalk supplied by MAPC.                                                                                                        |
| `ooc`                         | Boolean   | Owner occupied. If `TRUE`, listed owner address matches listed property address.                                                                                                              |
| `condo`                       | Boolean   | Condo. If `TRUE`, there are properties with a condo land use code on the parcel (which leads us to treat the whole thing as a 'condo', i.e., a parcel with multiple associated properties).   |
| `addr_id` (FK to `addresses`) | Integer   | Identifier of property's listed address.                                                                                                                                                      |

### `owners`

Each row represents either a unique owner name-address pair.

| Field                                       | Type    | Description                                                                                                                                                             |
|-------------|-------------|-----------------------------------------------|
| `id` (PK)                                   | Integer | Unique identifier.                                                                                                                                                      |
| `name`                                      | String  | Name of un-deduplicated owner.                                                                                                                                          |
| `inst`                                      | Boolean | Institutional owner. If `TRUE`, we flagged the owner as institutional using keywords unlikely to be identified with individuals.                                        |
| `trust`                                     | Boolean | Trust. If `TRUE`, we flagged the owner as a trust using keywords.                                                                                                       |
| `trustees`                                  | Boolean | Trustees. If `TRUE`, we flagged the owner as trustees of a trust using keywords.                                                                                        |
| `addr_id` (FK to `addresses`)               | Integer | Identifier of owner address.                                                                                                                                            |
| `company_id` (FK to `companies`)            | Integer | Identifier of company, if we were able to match the owner to a company in the OpenCorporates table.                                                                     |
| `cosine_group` (FK to `metacorps_cosine`)   | String  | Identifier of cosine-deduplicated metacorp. Group assigned by cosine deduplication process.                                                                             |
| `network_group` (FK to `metacorps_network`) | String  | Identifier of network-deduplicated metacorp. Group assigned by either network deduplication process or cosine deduplication process, when there are no network matches. |

### `sites_to_owners`

Represents the many-to-many relationship between `owners` and `sites`. All many-to-many relations are induced by splitting non-institutional owners on instances of the word "and" to identify multiple individual owners of a site.

| Field                                       | Type    | Description                                                                                                                                                             |
|-------------|-------------|-----------------------------------------------|
| `id` (PK)                                   | Integer | Unique identifier.                                                                                                                                                      |
| `site_id` (FK to ``` site``) ```            | Integer | Identifier of property.                                                                                                                                                 |
| `owner_id` (FK to `owners`)                 | Integer | Identifier of owner.                                                                                                                                                    |
| `cosine_group` (FK to `metacorps_cosine`)   | String  | Identifier of cosine-deduplicated metacorp. Group assigned by cosine deduplication process.                                                                             |
| `network_group` (FK to `metacorps_network`) | String  | Identifier of network-deduplicated metacorp. Group assigned by either network deduplication process or cosine deduplication process, when there are no network matches. |

### `companies`

Companies from OpenCorporates matched to at least one row in the assessors table, or present in the networks of those companies.

| Field                                    | Type    | Description                                  |
|---------------------------|---------------|------------------------------|
| `id` (PK)                                | Integer | Unique identifier.                           |
| `name`                                   | String  | Name of company.                             |
| `company_type`                           | String  | Type of company, given by OpenCorporates.    |
| `addr_id` (FK to `addresses`)            | String  | Identifier of address.                       |
| `network_id` (FK to `metacorps_network`) | String  | Identifier of network-deduplicated metacorp. |

### `officers`

Each row represents a unique name-company relationship.

| Field                                    | Type    | Description                                                   |
|-----------------------|---------------|-----------------------------------|
| `id` (PK)                                | Integer | Unique identifier.                                            |
| `name`                                   | String  | Name of officer.                                              |
| `positions`                              | String  | Comma-separated list of positions held by officer in company. |
| `company_id` (FK to `companies`)         | String  | Identifier of company.                                        |
| `addr_id` (FK to `addresses`)            | String  | Identifier of address.                                        |
| `network_id` (FK to `metacorps_network`) | String  | Identifier of network-deduplicated metacorp.                  |

#### Summary Fields

Currently included only when `load_results()` is run with `summarize=TRUE`.

| Field                     | Type    | Description                                                                           |
|-----------------------|---------------|-----------------------------------|
| `innetwork_company_count` | Integer | Number of companies the given officer is an officer of *within* the network metacorp. |

### `metacorps_network`

Each row represents a network-identified 'metacorp', or group of companies that we've identified as related.

| Field     | Type    | Description                               |
|-----------|---------|-------------------------------------------|
| `id` (PK) | Integer | Unique identifier.                        |
| `name`    | String  | Most common company name within metacorp. |

#### Summary Fields

Currently included only when `load_results()` is run with `summarize=TRUE`.

| Field            | Type    | Description                                                                                                                |
|---------------------|-----------------|----------------------------------|
| `prop_count`     | Integer | Number of properties (i.e., `sites` rows) linked to a given metacorp.                                                      |
| `unit_count`     | Numeric | Estimated number of units linked to a given metacorp.                                                                      |
| `area`           | Integer | Summed building area held by a particular metacorp (where 'building area' means the larger of `res_area` and `bld_area`).  |
| `val`            | Integer | Summed building and residential value held by a particular metacorp.                                                       |
| `units_per_prop` | Numeric | Total estimated units divided by the property count. This is a measure of what scale of property a given owner invests in. |
| `val_per_prop`   | Numeric | Total value divided by property count. A measure of how valuable a given metacorps properties are.                         |
| `val_per_area`   | Numeric | Value per square foot. Another measure of how valuable a metacorps properties are.                                         |
| `company_count`  | Integer | How many unique companies appear within a given metacorp.                                                                  |

### `metacorps_cosine`

Each row represents a cosine-deduplication-identified 'metacorp', or group of companies that we've identified as related.

| Field     | Type    | Description                               |
|-----------|---------|-------------------------------------------|
| `id` (PK) | Integer | Unique identifier.                        |
| `name`    | String  | Most common company name within metacorp. |

#### Summary Fields

Currently included only when `load_results()` is run with `summarize=TRUE`.

| Field            | Type    | Description                                                                                                                |
|---------------------|------------------|---------------------------------|
| `prop_count`     | Integer | Number of properties (i.e., `sites` rows) linked to a given metacorp.                                                      |
| `unit_count`     | Numeric | Estimated number of units linked to a given metacorp.                                                                      |
| `area`           | Integer | Summed building area held by a particular metacorp (where 'building area' means the larger of `res_area` and `bld_area`).  |
| `val`            | Integer | Summed building and residential value held by a particular metacorp.                                                       |
| `units_per_prop` | Numeric | Total estimated units divided by the property count. This is a measure of what scale of property a given owner invests in. |
| `val_per_prop`   | Numeric | Total value divided by property count. A measure of how valuable a given metacorps properties are.                         |
| `val_per_area`   | Numeric | Value per square foot. Another measure of how valuable a metacorps properties are.                                         |

### `parcels_point` (🌐)

Each row is a `POINT()` representation of a parcel in the MassGIS parcels database.

| Field                                   | Type           | Description                                                              |
|--------------------|----------------|-------------------------------------|
| `loc_id` (PK)                           | Integer        | Unique identifier.                                                       |
| `muni_id` (FK to `munis`)               | String         | Unique identifier of municipality.                                       |
| `block_group_id` (FK to `block_groups`) | String         | Unique identifier of block group that contains parcel.                   |
| `tract_id` (FK to `tracts`)             | String         | Unique identifier of census tract that contains tract.                   |
| `geometry`                              | Point Geometry | Point representation of parcel, found using `sf::st_point_on_surface()`. |

### `addresses`

Each row is a unique address (including parsed ranges) found in any of `assessors`, `sites`, `owners`, `companies`, or `owners`. Constructed, in part, using the statewide and Boston address layers as a reference dataset.

| Field                            | Type    | Description                                                                                        |
|--------------|--------------|---------------------------------------------|
| `id` (PK)                    | Integer | Unique identifier.                                                                                 |
| `addr`                           | String  | Complete number, street name, type string, often reconstructed from address ranges, PO Boxes, etc. |
| `start`                          | Number  | For ranges, start of address range. For single-number addresses, that single number.               |
| `end`                            | Number  | For ranges, end of address range. For single-number addresses, that single number.                 |
| `body`                           | String  | Street name and address type.                                                                      |
| `even`                           | Boolean | Whether an address is even or odd.                                                                 |
| `muni`                           | String  | Name of municipality.                                                                              |
| `postal`                         | String  | Postal code. For US addresses, a ZIP code.                                                         |
| `state`                          | String  | State (or, for international addresses, a region).                                                 |
| `loc_id` (FK to `parcels_point`) | String  | Unique identifier of parcel.                                                                       |

## Boundaries

These are loaded using `load_results()` if `load_boundaries = TRUE`.

### `munis` (🌐)

| Field          | Type                    | Description                                                              |
|---------------|---------------|------------------------------------------|
| `muni_id` (PK) | Integer                 | Unique identifier.                                                       |
| `muni`         | String                  | Name of municipality.                                                    |
| `hns`          | Boolean                 | If `TRUE`, municipality is one of the Healthy Neighborhoods Study areas. |
| `mapc`         | Boolean                 | If `TRUE`, municipality is part of the MAPC region.                      |
| `geometry`     | Geometry (MultiPolygon) | Municipal boundary.                                                      |

### `block_groups` (🌐)

Each row is a Massachusetts block group from the most recent vintage available in `tigris`. Currently, 2022.

| Field      | Type                    | Description                                   |
|----------------|-------------------|-------------------------------------|
| `id` (PK)  | Integer                 | Unique identifier (i.e., the 12-digit GEOID). |
| `geometry` | Geometry (MultiPolygon) | Block group boundary.                         |

### `tracts` (🌐)

Each row is a Massachusetts census tract from the most recent vintage available in `tigris`. Currently, 2022.

| Field      | Type                    | Description                                   |
|----------------|-------------------|-------------------------------------|
| `id` (PK)  | Integer                 | Unique identifier (i.e., the 11-digit GEOID). |
| `geometry` | Geometry (MultiPolygon) | Block group boundary.                         |

### `zips` (🌐)

Each row is a ZIP code boundary some of which intersects with Massachusetts. (ZIPS can cross state lines).

| Field      | Type                    | Description                                   |
|----------------|-------------------|-------------------------------------|
| `id` (PK)  | Integer                 | Unique identifier (i.e., the 11-digit GEOID). |
| `geometry` | Geometry (MultiPolygon) | Block group boundary.                         |
