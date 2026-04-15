{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_resellers') }}
),

reseller_sales AS (
    SELECT 
        s.territory_id,
        o.channel_type,
        SUM(s.total_sales) AS total_revenue,
        SUM((s.unit_price * s.quantity) - s.total_cost) AS total_profit,
        COUNT(*) AS total_transactions
    FROM {{ ref('stg_sales') }} s
    JOIN {{ ref('stg_sales_orders') }} o ON s.sales_order_line_id = o.sales_order_line_id
    WHERE o.channel_type = 'Reseller'
    GROUP BY s.territory_id, o.channel_type
)

SELECT 
    b.reseller_id,
    b.reseller_alt_id,
    b.reseller_name,
    b.business_type,
    b.city_name,
    b.state_province,
    b.country_region,

    -- Partner segmentation
    CASE 
        WHEN b.business_type = 'Warehouse' THEN 'High Volume Partner'
        WHEN b.business_type = 'Value Added Reseller' THEN 'Solution Partner'
        WHEN b.business_type = 'Specialty Bike Shop' THEN 'Specialist Partner'
        ELSE 'Retail Partner'
    END AS partner_segment,

    -- Partner tier based on business type
    CASE
        WHEN b.business_type = 'Warehouse' THEN 'Tier 1 - Strategic'
        WHEN b.business_type = 'Value Added Reseller' THEN 'Tier 2 - Growth'
        ELSE 'Tier 3 - Standard'
    END AS partner_tier,

    -- Location for AI narration
    b.reseller_name || ' (' || b.business_type || ') based in ' || b.city_name || ', ' || b.country_region AS partner_profile,

    -- AI search metadata
    LOWER(
        b.reseller_name || ' | ' || b.business_type || ' | ' || b.city_name || ' | ' || b.country_region
    ) AS search_metadata

FROM base b