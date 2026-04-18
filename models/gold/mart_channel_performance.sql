{{ config(materialized='table') }}

-- Channel Performance Matrix: B2B vs B2C deep comparison
-- Answers questions like "which channel is more profitable?" or "where should we invest?"

WITH channel_yearly AS (
    SELECT 
        f.fiscal_year,
        f.channel_type,
        CASE 
            WHEN f.channel_type = 'Reseller' THEN 'B2B'
            WHEN f.channel_type = 'Internet' THEN 'B2C'
            ELSE 'Other'
        END AS business_model,
        SUM(f.revenue) AS revenue,
        SUM(f.profit) AS profit,
        AVG(f.profit_margin_ratio) AS avg_margin,
        COUNT(DISTINCT f.order_number) AS order_count,
        SUM(f.quantity) AS units_sold,
        COUNT(DISTINCT f.customer_id) AS unique_customers,
        AVG(f.revenue) AS avg_transaction_value,
        SUM(f.revenue) / NULLIF(COUNT(DISTINCT f.customer_id), 0) AS revenue_per_customer
    FROM {{ ref('fct_sales') }} f
    GROUP BY 1, 2, 3
),

-- Previous year for growth calculation
prev_year_channel AS (
    SELECT 
        fiscal_year + 1 AS compare_year,
        channel_type,
        revenue AS prev_revenue,
        profit AS prev_profit,
        order_count AS prev_orders,
        unique_customers AS prev_customers
    FROM channel_yearly
),

-- Yearly totals for share calculation
yearly_total AS (
    SELECT 
        fiscal_year,
        SUM(revenue) AS total_revenue,
        SUM(profit) AS total_profit
    FROM channel_yearly
    GROUP BY fiscal_year
),

-- Top category per channel per year
channel_top_category AS (
    SELECT DISTINCT ON (f.fiscal_year, f.channel_type)
        f.fiscal_year,
        f.channel_type,
        p.category_name AS top_category,
        SUM(f.revenue) AS category_revenue
    FROM {{ ref('fct_sales') }} f
    JOIN {{ ref('dim_product') }} p ON f.product_id = p.product_id
    GROUP BY f.fiscal_year, f.channel_type, p.category_name
    ORDER BY f.fiscal_year, f.channel_type, SUM(f.revenue) DESC
)

SELECT 
    cy.fiscal_year,
    cy.channel_type,
    cy.business_model,

    -- Core metrics
    cy.revenue,
    cy.profit,
    cy.avg_margin,
    cy.order_count,
    cy.units_sold,
    cy.unique_customers,
    cy.avg_transaction_value,
    cy.revenue_per_customer,

    -- Channel share of total
    CASE 
        WHEN yt.total_revenue > 0 
        THEN ROUND((cy.revenue / yt.total_revenue * 100)::NUMERIC, 2) 
        ELSE 0 
    END AS revenue_share_pct,

    CASE 
        WHEN yt.total_profit > 0 
        THEN ROUND((cy.profit / yt.total_profit * 100)::NUMERIC, 2) 
        ELSE 0 
    END AS profit_share_pct,

    -- YoY growth
    CASE 
        WHEN COALESCE(pyc.prev_revenue, 0) > 0 
        THEN ROUND(((cy.revenue - pyc.prev_revenue) / pyc.prev_revenue * 100)::NUMERIC, 2) 
        ELSE NULL 
    END AS revenue_yoy_growth_pct,

    CASE 
        WHEN COALESCE(pyc.prev_customers, 0) > 0 
        THEN ROUND((cy.unique_customers - pyc.prev_customers)::NUMERIC / pyc.prev_customers * 100, 2) 
        ELSE NULL 
    END AS customer_yoy_growth_pct,

    -- Best category for this channel
    COALESCE(ctc.top_category, 'Unknown') AS leading_category,

    -- Channel health assessment
    CASE
        WHEN cy.avg_margin > 0.4 AND cy.unique_customers > 100 
            THEN 'Strong & Profitable'
        WHEN cy.avg_margin > 0.3 
            THEN 'Healthy'
        WHEN cy.unique_customers > 200 AND cy.avg_margin < 0.2 
            THEN 'High Volume / Low Margin - Review Pricing'
        ELSE 'Developing'
    END AS channel_health,

    -- AI narrative
    cy.business_model || ' (' || cy.channel_type || ') in ' || CAST(cy.fiscal_year AS VARCHAR) || ': '
    || '$' || CAST(cy.revenue AS VARCHAR) || ' revenue '
    || '(' || CAST(
        CASE WHEN yt.total_revenue > 0 THEN ROUND((cy.revenue / yt.total_revenue * 100)::NUMERIC, 2) ELSE 0 END
        AS VARCHAR
    ) || '% share), '
    || CAST(cy.unique_customers AS VARCHAR) || ' customers, '
    || 'avg margin ' || CAST(ROUND(cy.avg_margin * 100, 1) AS VARCHAR) || '%'
    AS channel_narrative

FROM channel_yearly cy
LEFT JOIN prev_year_channel pyc 
    ON cy.fiscal_year = pyc.compare_year AND cy.channel_type = pyc.channel_type
LEFT JOIN yearly_total yt ON cy.fiscal_year = yt.fiscal_year
LEFT JOIN channel_top_category ctc 
    ON cy.fiscal_year = ctc.fiscal_year AND cy.channel_type = ctc.channel_type
