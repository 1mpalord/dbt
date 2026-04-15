{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_sales AS (
    SELECT 
        customer_id,
        COUNT(*) AS total_transactions,
        SUM(total_sales) AS lifetime_revenue,
        SUM((unit_price * quantity) - total_cost) AS lifetime_profit,
        MIN(effective_date_key) AS first_purchase_date_key,
        MAX(effective_date_key) AS last_purchase_date_key,
        COUNT(DISTINCT effective_date_key) AS distinct_purchase_days,
        AVG(total_sales) AS avg_order_value
    FROM {{ ref('stg_sales') }}
    GROUP BY customer_id
)

SELECT 
    b.customer_id,
    b.customer_alt_id,
    b.full_name,
    b.city_name,
    b.state_province,
    b.country_region,
    b.zip_code,

    -- Descriptive: Full location string for AI narration
    b.full_name || ' from ' || b.city_name || ', ' || b.state_province || ', ' || b.country_region AS customer_bio,

    -- Market segmentation
    CASE 
        WHEN b.country_region IN ('Australia', 'United Kingdom', 'Canada', 'France', 'Germany') THEN 'International'
        WHEN b.country_region = 'United States' THEN 'Domestic'
        ELSE 'Other'
    END AS market_segment,

    -- Behavioral: Personalization greeting
    'Hi ' || SPLIT_PART(b.full_name, ' ', 1) || '!' AS ai_greeting,

    -- Aggregated purchase behavior (pre-joined for LLM convenience)
    COALESCE(cs.total_transactions, 0) AS total_transactions,
    COALESCE(cs.lifetime_revenue, 0) AS lifetime_revenue,
    COALESCE(cs.lifetime_profit, 0) AS lifetime_profit,
    COALESCE(cs.avg_order_value, 0) AS avg_order_value,
    cs.first_purchase_date_key,
    cs.last_purchase_date_key,

    -- Customer value tier (LLM can directly use this for personalization)
    CASE
        WHEN COALESCE(cs.lifetime_revenue, 0) >= 10000 THEN 'Platinum'
        WHEN COALESCE(cs.lifetime_revenue, 0) >= 5000 THEN 'Gold'
        WHEN COALESCE(cs.lifetime_revenue, 0) >= 1000 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_value_tier,

    -- Purchase frequency category
    CASE
        WHEN COALESCE(cs.total_transactions, 0) >= 20 THEN 'Power Buyer'
        WHEN COALESCE(cs.total_transactions, 0) >= 5 THEN 'Regular'
        WHEN COALESCE(cs.total_transactions, 0) >= 1 THEN 'Occasional'
        ELSE 'Never Purchased'
    END AS purchase_frequency,

    -- Retention status
    CASE 
        WHEN cs.last_purchase_date_key >= 20210601 THEN 'Active'
        WHEN cs.last_purchase_date_key >= 20200101 THEN 'At Risk'
        WHEN cs.last_purchase_date_key IS NOT NULL THEN 'Churned'
        ELSE 'Prospect'
    END AS retention_status,

    -- AI Search: single text blob for semantic search
    LOWER(
        b.full_name || ' | ' || b.city_name || ' | ' || b.state_province || ' | ' || b.country_region
        || ' | tier:' || CASE
            WHEN COALESCE(cs.lifetime_revenue, 0) >= 10000 THEN 'platinum'
            WHEN COALESCE(cs.lifetime_revenue, 0) >= 5000 THEN 'gold'
            WHEN COALESCE(cs.lifetime_revenue, 0) >= 1000 THEN 'silver'
            ELSE 'bronze'
        END
    ) AS search_metadata

FROM base b
LEFT JOIN customer_sales cs ON b.customer_id = cs.customer_id