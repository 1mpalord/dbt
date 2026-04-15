{{ config(materialized='table') }}

-- Territory War Room: Geographic performance intelligence
-- Every row = one territory with comprehensive revenue, growth, and customer metrics
-- Designed for "where should we invest?" and "which market is winning?" questions

WITH territory_base AS (
    SELECT * FROM {{ ref('dim_territory') }}
),

territory_yearly AS (
    SELECT 
        territory_id,
        fiscal_year,
        SUM(revenue) AS yearly_revenue,
        SUM(profit) AS yearly_profit,
        COUNT(DISTINCT customer_id) AS yearly_customers,
        COUNT(DISTINCT order_number) AS yearly_orders,
        SUM(quantity) AS yearly_units
    FROM {{ ref('fct_sales') }}
    GROUP BY territory_id, fiscal_year
),

-- Get max year per territory for latest period analysis
latest_year AS (
    SELECT 
        territory_id,
        MAX(fiscal_year) AS max_year
    FROM territory_yearly
    GROUP BY territory_id
),

territory_latest AS (
    SELECT ty.*
    FROM territory_yearly ty
    JOIN latest_year ly ON ty.territory_id = ly.territory_id AND ty.fiscal_year = ly.max_year
),

territory_prev AS (
    SELECT ty.*
    FROM territory_yearly ty
    JOIN latest_year ly ON ty.territory_id = ly.territory_id AND ty.fiscal_year = ly.max_year - 1
),

-- Channel breakdown per territory
territory_channels AS (
    SELECT 
        territory_id,
        SUM(CASE WHEN channel_type = 'Internet' THEN revenue ELSE 0 END) AS online_revenue,
        SUM(CASE WHEN channel_type = 'Reseller' THEN revenue ELSE 0 END) AS reseller_revenue
    FROM {{ ref('fct_sales') }}
    GROUP BY territory_id
),

-- Top product category per territory
territory_top_category AS (
    SELECT DISTINCT ON (f.territory_id)
        f.territory_id,
        p.category_name AS top_category,
        SUM(f.revenue) AS category_revenue
    FROM {{ ref('fct_sales') }} f
    JOIN {{ ref('dim_product') }} p ON f.product_id = p.product_id
    GROUP BY f.territory_id, p.category_name
    ORDER BY f.territory_id, SUM(f.revenue) DESC
),

-- Global totals for market share
global_totals AS (
    SELECT SUM(total_revenue) AS global_revenue FROM territory_base
)

SELECT 
    -- Territory identity
    tb.territory_id,
    tb.region_name,
    tb.country_name,
    tb.territory_group,
    tb.market_priority,
    tb.regional_persona,

    -- Lifetime performance
    tb.total_revenue AS lifetime_revenue,
    tb.total_profit AS lifetime_profit,
    tb.total_transactions AS lifetime_transactions,
    tb.unique_customers,
    tb.performance_tier,

    -- Market share
    CASE 
        WHEN gt.global_revenue > 0 
        THEN ROUND(tb.total_revenue / gt.global_revenue * 100, 2) 
        ELSE 0 
    END AS market_share_pct,

    -- Latest year performance
    COALESCE(tl.yearly_revenue, 0) AS latest_year_revenue,
    COALESCE(tl.yearly_profit, 0) AS latest_year_profit,
    COALESCE(tl.yearly_customers, 0) AS latest_year_customers,
    COALESCE(tl.yearly_orders, 0) AS latest_year_orders,

    -- YoY growth
    CASE 
        WHEN COALESCE(tp.yearly_revenue, 0) > 0 
        THEN ROUND((COALESCE(tl.yearly_revenue, 0) - tp.yearly_revenue) / tp.yearly_revenue * 100, 2) 
        ELSE NULL 
    END AS revenue_yoy_growth_pct,

    CASE 
        WHEN COALESCE(tp.yearly_customers, 0) > 0 
        THEN ROUND((COALESCE(tl.yearly_customers, 0) - tp.yearly_customers)::NUMERIC / tp.yearly_customers * 100, 2) 
        ELSE NULL 
    END AS customer_yoy_growth_pct,

    -- Channel mix
    COALESCE(tc.online_revenue, 0) AS online_revenue,
    COALESCE(tc.reseller_revenue, 0) AS reseller_revenue,
    CASE 
        WHEN COALESCE(tc.online_revenue, 0) + COALESCE(tc.reseller_revenue, 0) > 0 
        THEN ROUND(COALESCE(tc.online_revenue, 0)::NUMERIC / (COALESCE(tc.online_revenue, 0) + COALESCE(tc.reseller_revenue, 0)) * 100, 2)
        ELSE 0 
    END AS online_mix_pct,

    -- Product affinity
    COALESCE(ttc.top_category, 'Unknown') AS top_selling_category,

    -- Revenue per customer (efficiency metric)
    CASE 
        WHEN tb.unique_customers > 0 
        THEN ROUND(tb.total_revenue / tb.unique_customers, 2) 
        ELSE 0 
    END AS revenue_per_customer,

    -- Strategic assessment
    CASE
        WHEN COALESCE(tl.yearly_revenue, 0) > COALESCE(tp.yearly_revenue, 0) * 1.2 AND tb.performance_tier = 'Top Performer'
            THEN '🌟 Champion: Top performer with strong growth. Protect and expand.'
        WHEN COALESCE(tl.yearly_revenue, 0) > COALESCE(tp.yearly_revenue, 0) * 1.2
            THEN '🚀 Rising Star: Strong growth trajectory. Increase investment.'
        WHEN tb.performance_tier = 'Top Performer' AND COALESCE(tl.yearly_revenue, 0) < COALESCE(tp.yearly_revenue, 0)
            THEN '⚠️ Fading Giant: Large market but declining. Investigate root cause.'
        WHEN tb.performance_tier IN ('Growing', 'Developing')
            THEN '🌱 Opportunity: Untapped potential. Consider market development.'
        ELSE '📊 Stable: Maintain current strategy.'
    END AS strategic_assessment,

    -- AI narrative
    tb.region_name || ' (' || tb.country_name || '): '
    || tb.performance_tier || ' with $' || CAST(tb.total_revenue AS VARCHAR) || ' lifetime revenue '
    || 'and ' || CAST(tb.unique_customers AS VARCHAR) || ' customers. '
    || 'Market share: ' || CAST(
        CASE WHEN gt.global_revenue > 0 THEN ROUND(tb.total_revenue / gt.global_revenue * 100, 2) ELSE 0 END
        AS VARCHAR
    ) || '%.'
    AS territory_narrative

FROM territory_base tb
LEFT JOIN territory_latest tl ON tb.territory_id = tl.territory_id
LEFT JOIN territory_prev tp ON tb.territory_id = tp.territory_id
LEFT JOIN territory_channels tc ON tb.territory_id = tc.territory_id
LEFT JOIN territory_top_category ttc ON tb.territory_id = ttc.territory_id
CROSS JOIN global_totals gt
