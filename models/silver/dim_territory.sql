{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_territories') }}
),

territory_sales AS (
    SELECT 
        territory_id,
        SUM(total_sales) AS total_revenue,
        SUM((unit_price * quantity) - total_cost) AS total_profit,
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT customer_id) AS unique_customers
    FROM {{ ref('stg_sales') }}
    GROUP BY territory_id
)

SELECT 
    b.territory_id,
    b.region_name,
    b.country_name,
    b.territory_group,
    
    -- Market tiering
    CASE 
        WHEN b.territory_group = 'North America' THEN 'Primary Market'
        WHEN b.territory_group = 'Europe' THEN 'Secondary Market'
        ELSE 'Emerging Market'
    END AS market_priority,

    -- Regional persona
    CASE 
        WHEN b.country_name = 'United States' THEN 'Domestic'
        ELSE 'International'
    END AS regional_persona,

    -- Pre-aggregated performance (LLM can directly compare territories)
    COALESCE(ts.total_revenue, 0) AS total_revenue,
    COALESCE(ts.total_profit, 0) AS total_profit,
    COALESCE(ts.total_transactions, 0) AS total_transactions,
    COALESCE(ts.unique_customers, 0) AS unique_customers,

    -- Territory performance tier
    CASE
        WHEN COALESCE(ts.total_revenue, 0) >= 5000000 THEN 'Top Performer'
        WHEN COALESCE(ts.total_revenue, 0) >= 1000000 THEN 'Strong'
        WHEN COALESCE(ts.total_revenue, 0) >= 100000 THEN 'Growing'
        ELSE 'Developing'
    END AS performance_tier,

    -- AI search metadata
    LOWER(b.region_name || ' | ' || b.country_name || ' | ' || b.territory_group) AS search_metadata

FROM base b
LEFT JOIN territory_sales ts ON b.territory_id = ts.territory_id
WHERE b.region_type = 'Sales Region'