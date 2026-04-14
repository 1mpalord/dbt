{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_resellers') }}
)

SELECT 
    *,
    -- 1. Descriptive: Partner Persona
    -- Helps the AI understand the 'scale' of the reseller
    CASE 
        WHEN business_type = 'Warehouse' THEN 'High Volume Partner'
        WHEN business_type = 'Value Added Reseller' THEN 'Solution Partner'
        ELSE 'Retail Specialist'
    END AS partner_segment,

    -- 2. Location Context for AI Narration
    reseller_name || ' based in ' || city_name || ', ' || country_region AS partner_profile

FROM base