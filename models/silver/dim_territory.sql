{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_territories') }}
)

SELECT 
    territory_id,
    region_name,
    country_name,
    territory_group,
    
    -- 1. Market Tiering (Descriptive)
    -- Helps the AI prioritize which regions to mention in an Executive Summary
    CASE 
        WHEN territory_group = 'North America' THEN 'Primary Market'
        WHEN territory_group = 'Europe' THEN 'Secondary Market'
        ELSE 'Emerging Market'
    END AS market_priority,

    -- 2. Performance Persona (Context for ElevenLabs)
    -- Tells the AI Agent how to "speak" about the region
    CASE 
        WHEN country_name = 'United States' THEN 'Domestic'
        ELSE 'International'
    END AS regional_persona,

    -- 3. Search Index (For the n8n AI Agent)
    -- A single string the AI can scan to find the right ID
    LOWER(region_name || ' ' || country_name || ' ' || territory_group) AS search_metadata

FROM base
WHERE region_type = 'Sales Region'