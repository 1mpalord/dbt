{{ config(materialized='table') }}

-- City Performance Command Center
-- Unifies both B2C (Customer) and B2B (Reseller) sales at the City level
-- Includes territory context so you can query "Best cities in the Southwest"

WITH b2c_city_sales AS (
    SELECT 
        c.city_name,
        c.state_province,
        c.country_region AS country_name,
        t.region_name,
        t.territory_group,
        'B2C (Internet)' AS business_model,
        f.fiscal_year,
        SUM(f.revenue) AS revenue,
        SUM(f.profit) AS profit,
        SUM(f.quantity) AS units_sold,
        COUNT(DISTINCT f.order_number) AS orders,
        COUNT(DISTINCT f.customer_id) AS unique_buyers
    FROM {{ ref('fct_sales') }} f
    JOIN {{ ref('dim_customer') }} c ON f.customer_id = c.customer_id
    JOIN {{ ref('dim_territory') }} t ON f.territory_id = t.territory_id
    WHERE f.customer_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

b2b_city_sales AS (
    SELECT 
        r.city_name,
        r.state_province,
        r.country_region AS country_name,
        t.region_name,
        t.territory_group,
        'B2B (Reseller)' AS business_model,
        f.fiscal_year,
        SUM(f.revenue) AS revenue,
        SUM(f.profit) AS profit,
        SUM(f.quantity) AS units_sold,
        COUNT(DISTINCT f.order_number) AS orders,
        COUNT(DISTINCT f.reseller_id) AS unique_buyers
    FROM {{ ref('fct_sales') }} f
    JOIN {{ ref('dim_reseller') }} r ON f.reseller_id = r.reseller_id
    JOIN {{ ref('dim_territory') }} t ON f.territory_id = t.territory_id
    WHERE f.reseller_id IS NOT NULL AND f.reseller_id > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

combined_cities AS (
    SELECT * FROM b2c_city_sales
    UNION ALL
    SELECT * FROM b2b_city_sales
),

city_lifetime AS (
    SELECT
        city_name,
        state_province,
        country_name,
        region_name,
        territory_group,
        SUM(revenue) AS lifetime_revenue,
        SUM(profit) AS lifetime_profit,
        SUM(orders) AS lifetime_orders,
        SUM(unique_buyers) AS lifetime_unique_buyers
    FROM combined_cities
    GROUP BY 1, 2, 3, 4, 5
),

latest_year AS (
    SELECT MAX(fiscal_year) AS max_year FROM {{ ref('fct_sales') }}
),

city_latest_year AS (
    SELECT
        c.city_name,
        c.state_province,
        SUM(c.revenue) AS latest_year_revenue,
        SUM(c.profit) AS latest_year_profit,
        SUM(CASE WHEN c.business_model = 'B2C (Internet)' THEN c.revenue ELSE 0 END) AS latest_year_b2c_revenue,
        SUM(CASE WHEN c.business_model = 'B2B (Reseller)' THEN c.revenue ELSE 0 END) AS latest_year_b2b_revenue
    FROM combined_cities c
    CROSS JOIN latest_year ly
    WHERE c.fiscal_year = ly.max_year
    GROUP BY c.city_name, c.state_province
),

city_previous_year AS (
    SELECT
        c.city_name,
        c.state_province,
        SUM(c.revenue) AS prev_year_revenue
    FROM combined_cities c
    CROSS JOIN latest_year ly
    WHERE c.fiscal_year = ly.max_year - 1
    GROUP BY c.city_name, c.state_province
)

SELECT
    -- Geography
    cl.city_name,
    cl.state_province,
    cl.country_name,
    cl.region_name,
    cl.territory_group,
    
    -- Identifier
    cl.city_name || ', ' || cl.state_province || ' (' || cl.country_name || ')' AS full_city_display,

    -- Lifetime Performance
    cl.lifetime_revenue,
    cl.lifetime_profit,
    cl.lifetime_orders,
    cl.lifetime_unique_buyers,
    
    -- Profitability Efficiency
    CASE 
        WHEN cl.lifetime_revenue > 0 
        THEN ROUND(CAST(cl.lifetime_profit / cl.lifetime_revenue AS NUMERIC), 2)
        ELSE 0 
    END AS lifetime_margin,

    -- Latest Year Performance
    COALESCE(cly.latest_year_revenue, 0) AS latest_year_revenue,
    COALESCE(cly.latest_year_profit, 0) AS latest_year_profit,
    COALESCE(cly.latest_year_b2c_revenue, 0) AS latest_year_b2c_revenue,
    COALESCE(cly.latest_year_b2b_revenue, 0) AS latest_year_b2b_revenue,
    
    -- Channel Dominance
    CASE 
        WHEN COALESCE(cly.latest_year_b2b_revenue, 0) > COALESCE(cly.latest_year_b2c_revenue, 0) * 1.5 THEN 'B2B Hub'
        WHEN COALESCE(cly.latest_year_b2c_revenue, 0) > COALESCE(cly.latest_year_b2b_revenue, 0) * 1.5 THEN 'B2C Hub'
        ELSE 'Balanced Market'
    END AS channel_dominance,

    -- YoY Growth
    COALESCE(cpy.prev_year_revenue, 0) AS prev_year_revenue,
    CASE 
        WHEN COALESCE(cpy.prev_year_revenue, 0) > 0 
        THEN ROUND((COALESCE(cly.latest_year_revenue, 0) - cpy.prev_year_revenue) / cpy.prev_year_revenue * 100, 2)
        ELSE NULL 
    END AS yoy_growth_pct,

    -- AI Narrative
    cl.city_name || ' (' || cl.region_name || ') has generated $' || CAST(ROUND(cl.lifetime_revenue) AS VARCHAR) || ' historically. ' 
    || 'Latest year revenue: $' || CAST(COALESCE(ROUND(cly.latest_year_revenue), 0) AS VARCHAR) || '. '
    || 'It behaves as a ' || CASE 
        WHEN COALESCE(cly.latest_year_b2b_revenue, 0) > COALESCE(cly.latest_year_b2c_revenue, 0) * 1.5 THEN 'B2B Hub'
        WHEN COALESCE(cly.latest_year_b2c_revenue, 0) > COALESCE(cly.latest_year_b2b_revenue, 0) * 1.5 THEN 'B2C Hub'
        ELSE 'Balanced Market'
    END || '.'
    AS city_narrative

FROM city_lifetime cl
LEFT JOIN city_latest_year cly 
    ON cl.city_name = cly.city_name AND cl.state_province = cly.state_province
LEFT JOIN city_previous_year cpy 
    ON cl.city_name = cpy.city_name AND cl.state_province = cpy.state_province
