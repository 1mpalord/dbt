{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_sales_orders') }}
)

SELECT 
    *,
    -- 1. Descriptive: Order Complexity
    -- Counts how many lines are in this specific order
    COUNT(*) OVER (PARTITION BY order_number) AS items_in_order,

    -- 2. Behavioral: Channel Tagging
    -- Helps the AI Agent distinguish between B2B (Reseller) and B2C (Internet)
    CASE 
        WHEN channel_type = 'Reseller' THEN 'B2B'
        WHEN channel_type = 'Internet' THEN 'B2C'
        ELSE 'Direct'
    END AS business_segment,

    -- 3. Predictive: Large Order Flag
    -- If an order has > 5 items, it might be a wholesale signal for the AI
    CASE 
        WHEN COUNT(*) OVER (PARTITION BY order_number) > 5 THEN 1 
        ELSE 0 
    END AS is_wholesale_candidate

FROM base