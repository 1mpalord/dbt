{{ config(materialized='view') }}

SELECT 
    ResellerKey AS reseller_id,
    "Reseller ID" AS reseller_alt_id,
    "Business Type" AS business_type,
    Reseller AS reseller_name,
    City AS city_name,
    "State-Province" AS state_province,
    "Country-Region" AS country_region
FROM {{ source('adventureworks', 'Reseller') }}
-- Filtering the placeholder
WHERE ResellerKey <> -1