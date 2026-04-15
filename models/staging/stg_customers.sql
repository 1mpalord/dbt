{{ config(materialized='view') }}

SELECT 
    "CustomerKey" AS customer_id,
    "Customer ID" AS customer_alt_id,
    "Customer" AS full_name,
    "City" AS city_name,
    "State-Province" AS state_province,
    "Country-Region" AS country_region,
    "Postal Code" AS zip_code
FROM {{ source('adventureworks', 'Customers') }}
-- Filter out the "Not Applicable" placeholder for cleaner analysis
WHERE "CustomerKey" <> -1