{{ config(materialized='table') }}

-- Reseller Intelligence: B2B Strategic Account Management
-- Deep dive into wholesale partner performance, loyalty, and growth

WITH reseller_base AS (
    SELECT * FROM {{ ref('dim_reseller') }}
),

reseller_annual_sales AS (
    SELECT 
        f.reseller_id,
        f.fiscal_year,
        COUNT(DISTINCT f.order_number) AS annual_orders,
        SUM(f.revenue) AS annual_revenue,
        SUM(f.profit) AS annual_profit,
        SUM(f.quantity) AS annual_units
    FROM {{ ref('fct_sales') }} f
    WHERE f.reseller_id IS NOT NULL AND f.reseller_id > 0
    GROUP BY f.reseller_id, f.fiscal_year
),

-- Aggregate lifetime performance and latest activity
reseller_summary AS (
    SELECT 
        f.reseller_id,
        MIN(f.order_date) AS first_order_date,
        MAX(f.order_date) AS last_order_date,
        COUNT(DISTINCT f.order_number) AS total_orders,
        SUM(f.revenue) AS lifetime_revenue,
        SUM(f.profit) AS lifetime_profit,
        AVG(f.revenue) AS avg_deal_size,
        COUNT(DISTINCT f.product_id) AS unique_products_bought
    FROM {{ ref('fct_sales') }} f
    WHERE f.reseller_id IS NOT NULL AND f.reseller_id > 0
    GROUP BY f.reseller_id
),

-- YoY Growth Calculation (comparing latest available year to previous year)
latest_year AS (
    SELECT MAX(fiscal_year) AS max_year FROM {{ ref('fct_sales') }}
),

reseller_growth AS (
    SELECT 
        ty.reseller_id,
        ty.annual_revenue AS current_year_revenue,
        ly.annual_revenue AS previous_year_revenue,
        CASE 
            WHEN COALESCE(ly.annual_revenue, 0) > 0 
            THEN ROUND((ty.annual_revenue - ly.annual_revenue) / ly.annual_revenue * 100, 2)
            ELSE NULL 
        END AS yoy_growth_pct
    FROM reseller_annual_sales ty
    CROSS JOIN latest_year
    LEFT JOIN reseller_annual_sales ly 
        ON ty.reseller_id = ly.reseller_id 
        AND ly.fiscal_year = latest_year.max_year - 1
    WHERE ty.fiscal_year = latest_year.max_year
),

-- Find the top performing category for each reseller
reseller_top_category AS (
    SELECT DISTINCT ON (f.reseller_id)
        f.reseller_id,
        p.category_name AS top_category,
        SUM(f.revenue) AS category_revenue
    FROM {{ ref('fct_sales') }} f
    JOIN {{ ref('dim_product') }} p ON f.product_id = p.product_id
    WHERE f.reseller_id IS NOT NULL AND f.reseller_id > 0
    GROUP BY f.reseller_id, p.category_name
    ORDER BY f.reseller_id, SUM(f.revenue) DESC
)

SELECT 
    -- Identity & Segmentation
    rb.reseller_id,
    rb.reseller_name,
    rb.business_type,
    rb.city_name,
    rb.country_region,
    rb.partner_segment,
    rb.partner_tier,

    -- Lifetime Value
    COALESCE(rs.lifetime_revenue, 0) AS lifetime_revenue,
    COALESCE(rs.lifetime_profit, 0) AS lifetime_profit,
    COALESCE(rs.total_orders, 0) AS lifetime_orders,
    COALESCE(rs.avg_deal_size, 0) AS avg_deal_size,
    
    -- Engagement & Breadth
    COALESCE(rs.unique_products_bought, 0) AS distinct_products_stocked,
    rs.first_order_date,
    rs.last_order_date,
    
    -- Recent Performance & Growth
    COALESCE(rg.current_year_revenue, 0) AS latest_year_revenue,
    rg.previous_year_revenue,
    rg.yoy_growth_pct,

    -- Product Synergy
    COALESCE(rtc.top_category, 'None') AS primary_category_focus,

    -- Strategic Health Assessment
    CASE
        WHEN rg.yoy_growth_pct > 20 THEN '🚀 High Growth Partner'
        WHEN rg.yoy_growth_pct > 0 THEN '📈 Steady Expansion'
        WHEN rg.previous_year_revenue IS NOT NULL AND rg.current_year_revenue IS NULL THEN '🔻 Churned Partner'
        WHEN rg.yoy_growth_pct <= -20 THEN '⚠️ Severe Contraction'
        WHEN rg.yoy_growth_pct <= 0 THEN '📉 Declining Volume'
        ELSE '🆕 New/Onboarding'
    END AS partner_health_status,

    -- AI-Powered Strategic Account Management Action
    CASE
        WHEN rs.last_order_date < '2021-01-01' THEN 'Win-Back: Reach out with new product line catalog and bulk rebate offers.'
        WHEN rg.yoy_growth_pct <= -20 THEN 'Intervention: Schedule QBR (Quarterly Business Review) to identify supply issues or competitor threats.'
        WHEN rb.partner_tier = 'Tier 1 - Strategic' AND rg.yoy_growth_pct > 15 THEN 'VIP Nurture: Invite to advisory board. Partner is scaling rapidly.'
        WHEN rs.unique_products_bought < 5 THEN 'Expansion: Partner only buys niche items. Send generic catalog to cross-sell ' || COALESCE(rtc.top_category, 'other') || '.'
        WHEN rg.yoy_growth_pct > 0 THEN 'Status Quo: Maintain regular check-ins. Growth is solid.'
        ELSE 'Audit: Review account history for engagement opportunities.'
    END AS strategic_action_plan,

    -- AI Context Narrative
    rb.reseller_name || ' (' || rb.business_type || ') in ' || rb.country_region 
    || ' is a ' || rb.partner_tier || ' partner. '
    || 'Lifetime revenue: $' || CAST(COALESCE(rs.lifetime_revenue, 0) AS VARCHAR) || '. '
    || 'Health: ' || CASE
        WHEN rg.yoy_growth_pct > 20 THEN 'High Growth'
        WHEN rg.yoy_growth_pct > 0 THEN 'Steady Expansion'
        WHEN rg.yoy_growth_pct <= 0 THEN 'Declining'
        ELSE 'Unknown/New'
    END || '. Focuses primarily on ' || COALESCE(rtc.top_category, 'unknown') || '.'
    AS partner_narrative

FROM reseller_base rb
LEFT JOIN reseller_summary rs ON rb.reseller_id = rs.reseller_id
LEFT JOIN reseller_growth rg ON rb.reseller_id = rg.reseller_id
LEFT JOIN reseller_top_category rtc ON rb.reseller_id = rtc.reseller_id
WHERE rs.lifetime_revenue IS NOT NULL
