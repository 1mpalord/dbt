{{ config(materialized='table') }}

-- Executive Pulse: The CEO/CFO dashboard in a single table
-- Every row = one (period × territory × category × channel) combination
-- Pre-computed with YoY growth, contribution %, and trend signals

WITH current_period AS (
    SELECT 
        fiscal_year,
        fiscal_quarter,
        order_month,
        t.territory_group,
        t.country_name,
        t.market_priority,
        p.category_name,
        p.product_role,
        f.channel_type,
        SUM(f.revenue) AS revenue,
        SUM(f.profit) AS profit,
        AVG(f.profit_margin_ratio) AS avg_margin,
        COUNT(DISTINCT f.order_number) AS order_count,
        SUM(f.quantity) AS units_sold,
        COUNT(DISTINCT f.customer_id) AS unique_customers,
        AVG(f.revenue) AS avg_deal_size
    FROM {{ ref('fct_sales') }} f
    JOIN {{ ref('dim_territory') }} t ON f.territory_id = t.territory_id
    JOIN {{ ref('dim_product') }} p ON f.product_id = p.product_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

-- Yearly totals for contribution % calculations
yearly_totals AS (
    SELECT 
        fiscal_year,
        SUM(revenue) AS year_total_revenue,
        SUM(profit) AS year_total_profit
    FROM current_period
    GROUP BY fiscal_year
),

-- Previous year data for YoY growth
prev_year AS (
    SELECT 
        fiscal_year + 1 AS compare_year,
        territory_group,
        category_name,
        channel_type,
        SUM(revenue) AS prev_revenue,
        SUM(profit) AS prev_profit,
        SUM(order_count) AS prev_orders
    FROM current_period
    GROUP BY 1, 2, 3, 4
)

SELECT 
    -- Time dimensions
    cp.fiscal_year,
    cp.fiscal_quarter,
    cp.order_month,

    -- Geography
    cp.territory_group,
    cp.country_name,
    cp.market_priority,

    -- Product
    cp.category_name,
    cp.product_role,

    -- Channel
    cp.channel_type,

    -- Core metrics
    cp.revenue,
    cp.profit,
    cp.avg_margin,
    cp.order_count,
    cp.units_sold,
    cp.unique_customers,
    cp.avg_deal_size,

    -- Contribution analysis (what % of total revenue does this segment represent)
    CASE 
        WHEN yt.year_total_revenue > 0 
        THEN ROUND(cp.revenue / yt.year_total_revenue * 100, 2) 
        ELSE 0 
    END AS revenue_contribution_pct,

    CASE 
        WHEN yt.year_total_profit > 0 
        THEN ROUND(CAST((cp.profit / yt.year_total_profit * 100) AS NUMERIC), 2)
        ELSE 0 
    END AS profit_contribution_pct,

    -- Year-over-Year growth rates
    CASE 
        WHEN COALESCE(py.prev_revenue, 0) > 0 
        THEN ROUND((cp.revenue - py.prev_revenue) / py.prev_revenue * 100, 2) 
        ELSE NULL 
    END AS revenue_yoy_growth_pct,

    CASE 
        WHEN COALESCE(py.prev_profit, 0) > 0 
        THEN ROUND((cp.profit - py.prev_profit) / py.prev_profit * 100, 2) 
        ELSE NULL 
    END AS profit_yoy_growth_pct,

    CASE 
        WHEN COALESCE(py.prev_orders, 0) > 0 
        THEN ROUND((cp.order_count - py.prev_orders)::NUMERIC / py.prev_orders * 100, 2) 
        ELSE NULL 
    END AS orders_yoy_growth_pct,

    -- Trend signal for AI reasoning
    CASE
        WHEN COALESCE(py.prev_revenue, 0) = 0 THEN 'New Segment'
        WHEN cp.revenue > py.prev_revenue * 1.2 THEN '🚀 Strong Growth (>20%)'
        WHEN cp.revenue > py.prev_revenue THEN '📈 Growing'
        WHEN cp.revenue > py.prev_revenue * 0.8 THEN '📉 Declining'
        ELSE '🔻 Significant Decline (>20%)'
    END AS revenue_trend_signal,

    -- Profitability health check
    CASE
        WHEN cp.avg_margin > 0.5 THEN 'Excellent Margins'
        WHEN cp.avg_margin > 0.3 THEN 'Healthy Margins'
        WHEN cp.avg_margin > 0.1 THEN 'Thin Margins'
        ELSE 'Margin Pressure'
    END AS margin_health,

    -- AI narrative
    cp.category_name || ' in ' || cp.territory_group || ' (' || cp.channel_type || '): '
    || '$' || CAST(cp.revenue AS VARCHAR) || ' revenue, '
    || '$' || CAST(cp.profit AS VARCHAR) || ' profit, '
    || CAST(cp.order_count AS VARCHAR) || ' orders in ' || CAST(cp.fiscal_year AS VARCHAR)
    AS segment_narrative

FROM current_period cp
LEFT JOIN yearly_totals yt ON cp.fiscal_year = yt.fiscal_year
LEFT JOIN prev_year py 
    ON cp.fiscal_year = py.compare_year 
    AND cp.territory_group = py.territory_group 
    AND cp.category_name = py.category_name 
    AND cp.channel_type = py.channel_type