{{ config(materialized='table') }}

-- Customer Intelligence: The ultimate customer 360° view for AI personalization
-- Combines dim_customer profile data with fct_sales behavioral data

WITH customer_base AS (
    SELECT * FROM {{ ref('dim_customer') }}
),

customer_orders AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT order_number) AS distinct_orders,
        COUNT(DISTINCT fiscal_year) AS years_active,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        SUM(revenue) AS total_revenue,
        SUM(profit) AS total_profit,
        AVG(revenue) AS avg_transaction_value,
        SUM(CASE WHEN channel_type = 'Internet' THEN revenue ELSE 0 END) AS online_revenue,
        SUM(CASE WHEN channel_type = 'Reseller' THEN revenue ELSE 0 END) AS reseller_revenue
    FROM {{ ref('fct_sales') }}
    GROUP BY customer_id
),

-- Find each customer's most-purchased category
customer_top_category AS (
    SELECT DISTINCT ON (f.customer_id)
        f.customer_id,
        p.category_name AS favorite_category,
        p.subcategory_name AS favorite_subcategory,
        COUNT(*) AS category_purchases
    FROM {{ ref('fct_sales') }} f
    JOIN {{ ref('dim_product') }} p ON f.product_id = p.product_id
    GROUP BY f.customer_id, p.category_name, p.subcategory_name
    ORDER BY f.customer_id, COUNT(*) DESC
)

SELECT 
    -- Identity
    cb.customer_id,
    cb.full_name,
    cb.customer_bio,
    cb.ai_greeting,
    cb.city_name,
    cb.state_province,
    cb.country_region,
    cb.market_segment,

    -- Value metrics
    cb.customer_value_tier,
    cb.purchase_frequency,
    cb.retention_status,
    COALESCE(co.total_revenue, 0) AS lifetime_revenue,
    COALESCE(co.total_profit, 0) AS lifetime_profit,
    COALESCE(co.distinct_orders, 0) AS total_orders,
    COALESCE(co.avg_transaction_value, 0) AS avg_order_value,

    -- Time metrics
    co.first_order_date,
    co.last_order_date,
    COALESCE(co.years_active, 0) AS years_active,

    -- Channel affinity
    COALESCE(co.online_revenue, 0) AS online_revenue,
    COALESCE(co.reseller_revenue, 0) AS reseller_revenue,
    CASE 
        WHEN COALESCE(co.online_revenue, 0) > COALESCE(co.reseller_revenue, 0) THEN 'Online-First'
        WHEN COALESCE(co.reseller_revenue, 0) > COALESCE(co.online_revenue, 0) THEN 'Store-First'
        ELSE 'Balanced'
    END AS channel_preference,

    -- Product affinity
    COALESCE(ctc.favorite_category, 'Unknown') AS favorite_category,
    COALESCE(ctc.favorite_subcategory, 'Unknown') AS favorite_subcategory,

    -- Next Best Action (enriched)
    CASE 
        WHEN cb.retention_status = 'Churned' AND cb.customer_value_tier IN ('Platinum', 'Gold') 
            THEN 'Win-Back Campaign: High-value customer lost. Offer exclusive discount.'
        WHEN cb.retention_status = 'At Risk' AND cb.customer_value_tier IN ('Platinum', 'Gold') 
            THEN 'Retention Alert: VIP customer going quiet. Send personalized outreach.'
        WHEN cb.retention_status = 'At Risk' 
            THEN 'Re-Engagement: Send targeted email with new arrivals in ' || COALESCE(ctc.favorite_category, 'their preferred category') || '.'
        WHEN cb.retention_status = 'Active' AND cb.customer_value_tier = 'Platinum' 
            THEN 'VIP Treatment: Invite to exclusive preview or loyalty program upgrade.'
        WHEN cb.retention_status = 'Active' AND COALESCE(co.total_revenue, 0) > 5000 
            THEN 'Upsell Premium: Customer has strong purchase history. Recommend premium upgrades.'
        WHEN cb.retention_status = 'Active' 
            THEN 'Cross-Sell: Suggest complementary products from ' || COALESCE(ctc.favorite_category, 'related categories') || '.'
        ELSE 'Nurture: First-time buyer flow. Send welcome sequence.'
    END AS recommended_action,

    -- AI narrative for RAG
    cb.full_name || ' is a ' || cb.customer_value_tier || '-tier ' || cb.retention_status 
    || ' customer from ' || cb.country_region 
    || ' who has spent $' || CAST(COALESCE(co.total_revenue, 0) AS VARCHAR) 
    || ' across ' || CAST(COALESCE(co.distinct_orders, 0) AS VARCHAR) || ' orders'
    || '. Their favorite category is ' || COALESCE(ctc.favorite_category, 'unknown')
    || '.' AS customer_narrative

FROM customer_base cb
LEFT JOIN customer_orders co ON cb.customer_id = co.customer_id
LEFT JOIN customer_top_category ctc ON cb.customer_id = ctc.customer_id