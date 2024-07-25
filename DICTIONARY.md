# Data Dictionary

+ Author: Eric Robsky Huntley, PhD
+ Last Updated: July 2, 2024

This data dictionary provides documentation for files output by data processing scripts for Who Owns Massachusetts.

## Data Sources

+ OpenCorporates. 2024. Massachusetts Bulk Data Delivery. Sharing restricted by license agreement.
+ MassGIS. 2024. [Property Tax Parcels](https://www.mass.gov/info-details/massgis-data-property-tax-parcels).
+ MassGIS. 2023. [Geographic Placenames](https://www.mass.gov/info-details/massgis-data-geographic-place-names). October.
+ MassGIS. 2024. [Municipalities](https://www.mass.gov/info-details/massgis-data-municipalities).
+ US Census Bureau TIGER/Line. ZIP Codes. 2010. Fetched using [Tidycensus](https://walker-data.com/tidycensus/index.html).
+ MassGIS. 2024. [Master Address Data](https://www.mass.gov/info-details/massgis-data-master-address-data).

## Production Owner Table

```yaml
{
  "id": '001', // metacorp id
  "name": 'CAMBERVILLE PROPERTIES, LLC',
  "corps": [
    {
      "name": 'CAMBERVILLE PROPERTIES, LLC',
      "address": '61 SIXTH ST',
      "city": 'CAMBRIDGE', 
      "state": 'MA',
      "postal": '021141'
    },
    {
      "name": 'CAMBERVILLE PROPERTIES II, LLC',
      "address": '61 SIXTH ST', 
      "city": 'CAMBRIDGE', 
      "state": 'MA',
      "postal": '021141'
    }
  ],
  "props": [
    {
      "name": 'CAMBERVILLE PROPERTIES II, LLC'
      "address": 
    },
  ],
  "evictions": {
    "np": 8,
    "nc": 3,
    "c": 1
  }
}
```

## Mapbox Simplified Parcels

```yaml
{
  "type": "Feature",
  "properties": {
    "id": "F_768178_2960083",
    "addr": "61 SIXTH ST, CAMBRIDGE, MA 021141",
    "metacorp_ids": ['001'],
    "owner_names": ['CAMBERVILLE PROPERTIES, LLC', 'EL-DIAN MANAGEMENT LLC']
  },
  "geometry": {
    "type": "Point",
    "coordinates": [125.6, 10.1]
  }
}
```

## Owner

```yaml
{
  "id": '001',
  "name": 'CAMBERVILLE PROPERTIES, LLC',
  "agents": [''],
  "addresses": ['017', '037'],
  "type": 'LLC'
  "metacorp": '001'
}
```

## Person

```yaml
{
  "id": '001',
  "name": 'Applesauce McGoo',
  "address": ['000', '027']
}
```

## Filing

```yaml
{
  "plaintiff_ids": ['076'], // FK to owner.
  "attorney_ids": ['125'], // FK to person.
  "arrears": 25000,
  "type": 'np',
  "parcel_id": 'F_768178_2960083' // FK to parcel
}
```

| Month    | Savings |
| -------- | ------- |
| January  | $250    |
| February | $80     |
| March    | $420    |

## Parcel

```yaml
{
  "id": "F_768178_2960083",
  "addr": "61 SIXTH ST",
  "city": 'CAMBRIDGE',
  "state": 'MA',
  "zip": '02141',
  "ls_price": 19911015,
  "ls_date": '1991-10-15',
  "units": 1,
  "bld_area": 2364,
  "res_area": 2996,
  "bldg_val": 401600,
  "land_val": 941800,
  "total_val": 1343400,
  "use_code_orig": "109",
  "use_code": "109"
}
```