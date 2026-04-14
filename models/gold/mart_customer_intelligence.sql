{{ config(materialized='table') }}

SELECT 
    c.customer_id,
    c.full_name,
    c.ai_greeting,
    c.customer_market_segment,
    COUNT(f.sales_order_id) AS total_orders,
    SUM(f.total_profit) AS lifetime_value,
    -- Predictive: Churn Risk (If they haven't bought in a long time)
    CASE 
        WHEN MAX(f.effective_date_key) < '2025-01-01' THEN 'At Risk'
        ELSE 'Active'
    END AS retention_status,
    -- Predictive: Next Best Action
    CASE 
        WHEN SUM(f.total_profit) > 5000 THEN 'Upsell Premium'
        ELSE 'Send Discount'
    END AS recommended_action
FROM {{ ref('fct_sales') }} f
JOIN {{ ref('dim_customer') }} c ON f.customer_id = c.customer_id
GROUP BY 1, 2, 3, 4