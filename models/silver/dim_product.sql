{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT 
    *,
    -- 1. Descriptive: Profit Potential
    (list_price - standard_cost) AS potential_unit_profit,
    (list_price - standard_cost) / NULLIF(list_price, 0) AS markup_percentage,

    -- 2. Predictive: Recommendation Categories
    -- Helps the AI suggest "Add-ons" (Accessories) vs "Core Buys" (Bikes/Components)
    CASE 
        WHEN category_name = 'Accessories' THEN 'Add-on'
        WHEN category_name = 'Clothing' THEN 'Apparel'
        ELSE 'Core Product'
    END AS product_role,

    -- 3. Descriptive: Price Tiering
    CASE 
        WHEN list_price > 1000 THEN 'Premium'
        WHEN list_price > 200 THEN 'Mid-Range'
        ELSE 'Entry-Level'
    END AS price_segment

FROM base