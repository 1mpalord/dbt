{{ config(materialized='table') }}

SELECT 
    f.effective_date_key,
    t.territory_group,
    t.country_name,
    p.category_name,
    p.product_role,
    s.channel_type,
    SUM(f.total_sales) AS revenue,
    SUM(f.total_profit) AS profit,
    AVG(f.profit_margin) AS margin,
    COUNT(DISTINCT s.order_number) AS order_volumee
FROM {{ ref('fct_sales') }} f
JOIN {{ ref('dim_territory') }} t ON f.territory_id = t.territory_id
JOIN {{ ref('dim_product') }} p ON f.product_id = p.product_id
JOIN {{ ref('dim_sales_order') }} s ON f.sales_order_line_id = s.sales_order_line_id
GROUP BY 1, 2, 3, 4, 5, 6