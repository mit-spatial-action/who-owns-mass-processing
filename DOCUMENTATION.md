# Parcel Processing

1. For all parcel locations, fill `state` column with `"MA"` and country with `"US"`. (All parcels are in MA and in the US.)

3. Standardize parcel addresses.

  1. standardize street types (RD > ROAD, etc.)
  2. Standardize directions (NW > NORTHWEST, etc) in `muni` and `addr`.
  3. Standardize (i.e., remove) extraneous leading zeroes in addresses in `addr`.
  4. Standardize instances of Massachusetts variants > MA in `addr`.
  5. Standardize postal format in `postal`.
  6. Standardize municipality names (this basically means correct common errors, collected mostly through manual observation) in `muni`.
  
  
TODO:

+ deal with dropping condo land.
+ 

When one of the multi-