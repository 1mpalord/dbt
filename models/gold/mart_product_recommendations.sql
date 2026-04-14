{{ config(materialized='table') }}

SELECT 
    p.category_name,
    p.subcategory_name,
    p.price_segment,
    AVG(f.profit_margin) AS avg_category_margin,
    -- AI Context: Suggests a high-margin "Add-on" for this category
    'Based on your interest in ' || p.subcategory_name || ', consider our top-rated accessories.' AS ai_recommendation_script
FROM {{ ref('fct_sales') }} f
JOIN {{ ref('dim_product') }} p ON f.product_id = p.product_id
GROUP BY 1, 2, 3