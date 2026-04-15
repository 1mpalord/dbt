{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_products') }}
),

product_sales AS (
    SELECT 
        product_id,
        SUM(total_sales) AS total_revenue,
        SUM((unit_price * quantity) - total_cost) AS total_profit,
        SUM(quantity) AS total_units_sold,
        COUNT(*) AS total_transactions,
        AVG(total_sales) AS avg_sale_value
    FROM {{ ref('stg_sales') }}
    GROUP BY product_id
)

SELECT 
    b.product_id,
    b.product_sku,
    b.product_name,
    b.standard_cost,
    b.list_price,
    b.product_color,
    b.product_model,
    b.subcategory_name,
    b.category_name,

    -- Profit potential per unit
    (b.list_price - b.standard_cost) AS unit_profit_potential,
    CASE 
        WHEN b.list_price > 0 
        THEN ROUND((b.list_price - b.standard_cost) / b.list_price, 4) 
        ELSE 0 
    END AS markup_ratio,

    -- Product role for recommendation engine
    CASE 
        WHEN b.category_name = 'Accessories' THEN 'Add-on'
        WHEN b.category_name = 'Clothing' THEN 'Apparel'
        WHEN b.category_name = 'Components' THEN 'Component'
        ELSE 'Core Product'
    END AS product_role,

    -- Price tiering
    CASE
        WHEN b.list_price > 3000 THEN 'Ultra-Premium'
        WHEN b.list_price > 1000 THEN 'Premium'
        WHEN b.list_price > 200 THEN 'Mid-Range'
        WHEN b.list_price > 20 THEN 'Entry-Level'
        ELSE 'Budget'
    END AS price_segment,

    -- Pre-aggregated sales performance (LLM can directly rank products)
    COALESCE(ps.total_revenue, 0) AS total_revenue,
    COALESCE(ps.total_profit, 0) AS total_profit,
    COALESCE(ps.total_units_sold, 0) AS total_units_sold,
    COALESCE(ps.total_transactions, 0) AS total_transactions,
    COALESCE(ps.avg_sale_value, 0) AS avg_sale_value,

    -- Product popularity ranking
    CASE
        WHEN COALESCE(ps.total_units_sold, 0) >= 500 THEN 'Best Seller'
        WHEN COALESCE(ps.total_units_sold, 0) >= 100 THEN 'Popular'
        WHEN COALESCE(ps.total_units_sold, 0) >= 10 THEN 'Moderate'
        WHEN COALESCE(ps.total_units_sold, 0) >= 1 THEN 'Niche'
        ELSE 'No Sales'
    END AS popularity_tier,

    -- AI search metadata
    LOWER(
        b.product_name || ' | ' || b.category_name || ' | ' || b.subcategory_name 
        || ' | ' || COALESCE(b.product_color, 'no color') || ' | ' || COALESCE(b.product_model, 'no model')
        || ' | sku:' || b.product_sku
    ) AS search_metadata

FROM base b
LEFT JOIN product_sales ps ON b.product_id = ps.product_id