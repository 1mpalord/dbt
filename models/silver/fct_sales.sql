{{ config(materialized='table') }}

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
)

SELECT 
    -- 1. Explicitly name the columns the YAML is looking for
    sales_order_line_id, 
    product_id,
    customer_id,
    territory_id,
    effective_date_key,
    quantity,
    unit_price,
    total_cost,
    total_sales,

    -- 2. Calculated Metrics
    (unit_price * quantity) - total_cost AS total_profit,
    ((unit_price * quantity) - total_cost) / NULLIF(total_sales, 0) AS profit_margin

FROM sales