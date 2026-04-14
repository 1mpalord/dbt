{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT 
    *,
    -- 1. Descriptive: Full Address for AI Narration
    -- Makes it easy for the AI to say "Our customer Jon Yang in Queensland..."
    full_name || ' from ' || city_name || ', ' || country_region AS customer_bio,

    -- 2. Predictive: Market Segment
    -- Categorize by country to allow the AI to group behaviors
    CASE 
        WHEN country_region IN ('Australia', 'United Kingdom', 'Canada') THEN 'International'
        WHEN country_region = 'United States' THEN 'Domestic'
        ELSE 'Other'
    END AS customer_market_segment,

    -- 3. Behavioral: Personalization Key
    -- If you ever add emails or phone numbers, this is where they link.
    -- For now, we create a "Greeting" for ElevenLabs
    'Hi ' || SPLIT_PART(full_name, ' ', 1) || '!' AS ai_greeting

FROM base