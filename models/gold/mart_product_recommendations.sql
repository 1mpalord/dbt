{{ config(materialized='table') }}

-- Product Intelligence: comprehensive product performance + recommendation engine
-- Combines product catalog with sales performance, cross-sell data, and AI scripts

WITH product_base AS (
    SELECT * FROM {{ ref('dim_product') }}
),

-- Find which products are commonly bought together (market basket analysis)
product_pairs AS (
    SELECT 
        a.product_id AS product_a,
        b.product_id AS product_b,
        COUNT(DISTINCT a.order_number) AS co_purchase_count
    FROM {{ ref('fct_sales') }} a
    JOIN {{ ref('fct_sales') }} b 
        ON a.order_number = b.order_number 
        AND a.product_id < b.product_id
    GROUP BY a.product_id, b.product_id
    HAVING COUNT(DISTINCT a.order_number) >= 3
),

-- Get the top co-purchased product for each product
top_cross_sell AS (
    SELECT DISTINCT ON (pp.product_a)
        pp.product_a AS product_id,
        pb.product_name AS cross_sell_product,
        pb.category_name AS cross_sell_category,
        pp.co_purchase_count
    FROM product_pairs pp
    JOIN product_base pb ON pp.product_b = pb.product_id
    ORDER BY pp.product_a, pp.co_purchase_count DESC
),

-- Channel performance per product
channel_split AS (
    SELECT 
        product_id,
        SUM(CASE WHEN channel_type = 'Internet' THEN revenue ELSE 0 END) AS online_revenue,
        SUM(CASE WHEN channel_type = 'Reseller' THEN revenue ELSE 0 END) AS reseller_revenue,
        COUNT(DISTINCT customer_id) AS unique_buyers
    FROM {{ ref('fct_sales') }}
    GROUP BY product_id
),

-- Category-level benchmarks for comparative analysis
category_benchmarks AS (
    SELECT 
        category_name,
        AVG(total_revenue) AS avg_category_revenue,
        AVG(total_units_sold) AS avg_category_units
    FROM product_base
    WHERE total_units_sold > 0
    GROUP BY category_name
)

SELECT 
    -- Product identity
    pb.product_id,
    pb.product_sku,
    pb.product_name,
    pb.product_model,
    pb.category_name,
    pb.subcategory_name,
    pb.product_color,
    pb.product_role,
    pb.price_segment,

    -- Pricing
    pb.standard_cost,
    pb.list_price,
    pb.unit_profit_potential,
    pb.markup_ratio,

    -- Sales performance
    pb.total_revenue,
    pb.total_profit,
    pb.total_units_sold,
    pb.total_transactions,
    pb.avg_sale_value,
    pb.popularity_tier,

    -- Channel analysis
    COALESCE(cs.online_revenue, 0) AS online_revenue,
    COALESCE(cs.reseller_revenue, 0) AS reseller_revenue,
    COALESCE(cs.unique_buyers, 0) AS unique_buyers,
    CASE 
        WHEN COALESCE(cs.online_revenue, 0) > COALESCE(cs.reseller_revenue, 0) THEN 'Online Dominant'
        WHEN COALESCE(cs.reseller_revenue, 0) > COALESCE(cs.online_revenue, 0) THEN 'Reseller Dominant'
        ELSE 'Balanced'
    END AS channel_dominance,

    -- Market basket: Cross-sell recommendation
    COALESCE(tcs.cross_sell_product, 'No data') AS top_cross_sell_product,
    COALESCE(tcs.cross_sell_category, 'No data') AS cross_sell_category,
    COALESCE(tcs.co_purchase_count, 0) AS cross_sell_strength,

    -- Comparative performance vs category average
    CASE 
        WHEN cb.avg_category_revenue > 0 
        THEN ROUND(pb.total_revenue / cb.avg_category_revenue, 2) 
        ELSE 0 
    END AS revenue_vs_category_avg,

    CASE
        WHEN pb.total_revenue > cb.avg_category_revenue * 1.5 THEN 'Star Performer'
        WHEN pb.total_revenue > cb.avg_category_revenue THEN 'Above Average'
        WHEN pb.total_revenue > cb.avg_category_revenue * 0.5 THEN 'Below Average'
        WHEN pb.total_units_sold > 0 THEN 'Underperformer'
        ELSE 'No Sales'
    END AS category_performance_rank,

    -- AI recommendation script (dynamic)
    CASE
        WHEN pb.popularity_tier = 'Best Seller' AND tcs.cross_sell_product IS NOT NULL
            THEN 'This is a best-seller. Customers who buy ' || pb.product_name || ' often also purchase ' || tcs.cross_sell_product || '. Consider bundling.'
        WHEN pb.popularity_tier IN ('Best Seller', 'Popular')
            THEN pb.product_name || ' is a top performer in ' || pb.subcategory_name || ' with ' || CAST(pb.total_units_sold AS VARCHAR) || ' units sold.'
        WHEN pb.markup_ratio > 0.5 AND pb.popularity_tier IN ('Moderate', 'Niche')
            THEN pb.product_name || ' has excellent margins (' || CAST(ROUND(pb.markup_ratio * 100) AS VARCHAR) || '%) but low sales. Consider promoting.'
        WHEN pb.popularity_tier = 'No Sales'
            THEN pb.product_name || ' has never sold. Review pricing or consider discontinuing.'
        ELSE 'Based on interest in ' || pb.subcategory_name || ', recommend ' || pb.product_name || ' (' || pb.price_segment || ').'
    END AS ai_recommendation_script,

    -- Product narrative for RAG
    pb.product_name || ' (' || pb.category_name || ' > ' || pb.subcategory_name || ') '
    || 'priced at $' || CAST(pb.list_price AS VARCHAR)
    || ', ' || pb.popularity_tier || ' with $' || CAST(pb.total_revenue AS VARCHAR) || ' lifetime revenue'
    || '. ' || pb.price_segment || ' tier, ' || pb.product_role || '.'
    AS product_narrative

FROM product_base pb
LEFT JOIN channel_split cs ON pb.product_id = cs.product_id
LEFT JOIN top_cross_sell tcs ON pb.product_id = tcs.product_id
LEFT JOIN category_benchmarks cb ON pb.category_name = cb.category_name