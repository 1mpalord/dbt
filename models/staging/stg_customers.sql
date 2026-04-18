{{ config(materialized='view') }}

SELECT 
    "CustomerKey" AS customer_id,
    "Customer_ID" AS customer_alt_id,
    "Customer" AS full_name,
    "City" AS city_name,
    "State_Province" AS state_province,
    "Country_Region" AS country_region,
    "Postal_Code" AS zip_code
FROM {{ source('adventureworks', 'Customers') }}
-- Filter out the "Not Applicable" placeholder for cleaner analysis
WHERE "CustomerKey" <> -1