{{ config(materialized='view') }}

SELECT 
    SalesTerritoryKey AS territory_id,
    Region AS region_name,
    Country AS country_name,
    "Group" AS territory_group, -- "Group" is often a reserved word in SQL, so we quote it
    
    -- Descriptive Tagging for the AI
    CASE 
        WHEN Region = 'Corporate HQ' THEN 'Internal'
        ELSE 'Sales Region'
    END AS region_type

FROM {{ source('adventureworks', 'SalesTerritory') }}