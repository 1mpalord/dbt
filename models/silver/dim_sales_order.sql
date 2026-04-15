{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_sales_orders') }}
),

order_metrics AS (
    SELECT 
        sales_order_line_id,
        total_sales,
        quantity
    FROM {{ ref('stg_sales') }}
)

SELECT 
    b.sales_order_line_id,
    b.order_number,
    b.order_line_item,
    b.channel_type,
    b.sales_order_id,

    -- Order complexity: items per order
    COUNT(*) OVER (PARTITION BY b.order_number) AS items_in_order,

    -- Total order value (all lines in this order)
    SUM(om.total_sales) OVER (PARTITION BY b.order_number) AS total_order_value,

    -- Business segment classification
    CASE 
        WHEN b.channel_type = 'Reseller' THEN 'B2B'
        WHEN b.channel_type = 'Internet' THEN 'B2C'
        ELSE 'Direct'
    END AS business_segment,

    -- Order size classification
    CASE 
        WHEN COUNT(*) OVER (PARTITION BY b.order_number) > 10 THEN 'Large Order'
        WHEN COUNT(*) OVER (PARTITION BY b.order_number) > 5 THEN 'Medium Order'
        ELSE 'Small Order'
    END AS order_size_category,

    -- Wholesale flag
    CASE 
        WHEN COUNT(*) OVER (PARTITION BY b.order_number) > 5 THEN true
        ELSE false
    END AS is_wholesale_candidate,

    -- AI search metadata
    LOWER(
        b.order_number || ' | ' || b.channel_type || ' | line ' || CAST(b.order_line_item AS VARCHAR)
    ) AS search_metadata

FROM base b
LEFT JOIN order_metrics om ON b.sales_order_line_id = om.sales_order_line_id