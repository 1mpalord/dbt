{{ config(materialized='table') }}

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
)

SELECT 
    *,
    -- REQUIREMENT: Descriptive/Predictive Metric
    (unit_price * quantity) - total_cost AS total_profit,
    ((unit_price * quantity) - total_cost) / NULLIF(total_sales, 0) AS profit_margin
FROM sales