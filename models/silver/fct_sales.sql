{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

date_context AS (
    SELECT
        date_key,
        date_actual,
        day_of_week_name,
        calendar_month_name  AS month_name,
        fiscal_quarter,
        fiscal_year,
        season,
        period_label,
        is_weekend
    FROM {{ ref('dim_date') }}
),

order_context AS (
    SELECT * FROM {{ ref('stg_sales_orders') }}
)

SELECT 
    -- Keys
    s.sales_order_line_id,
    s.product_id,
    s.customer_id,
    s.reseller_id,
    s.territory_id,
    s.effective_date_key,

    -- Order context (denormalized for LLM single-table queries)
    o.order_number,
    o.channel_type,

    -- Time context (denormalized so LLM can filter by human-readable dates)
    d.date_actual AS order_date,
    d.day_of_week_name AS day_of_week,
    d.month_name AS order_month,
    d.fiscal_quarter,
    d.fiscal_year,
    d.season,
    d.period_label,
    d.is_weekend,

    -- Raw measures
    s.quantity,
    s.unit_price,
    s.total_cost,
    s.total_sales AS revenue,

    -- Calculated metrics
    (s.unit_price * s.quantity) - s.total_cost AS profit,
    CASE 
        WHEN s.total_sales > 0 
        THEN ROUND(((s.unit_price * s.quantity) - s.total_cost) / s.total_sales, 4) 
        ELSE 0 
    END AS profit_margin_ratio,

    -- LLM-friendly: Profit tier classification
    CASE
        WHEN ((s.unit_price * s.quantity) - s.total_cost) > 500 THEN 'High Profit'
        WHEN ((s.unit_price * s.quantity) - s.total_cost) > 100 THEN 'Medium Profit'
        WHEN ((s.unit_price * s.quantity) - s.total_cost) > 0 THEN 'Low Profit'
        ELSE 'Loss-Making'
    END AS profit_tier,

    -- LLM-friendly: Order size classification
    CASE
        WHEN s.quantity >= 10 THEN 'Bulk Order'
        WHEN s.quantity >= 3 THEN 'Multi-Unit'
        ELSE 'Single Unit'
    END AS order_size_category,

    -- LLM-friendly: Revenue band
    CASE
        WHEN s.total_sales >= 5000 THEN 'Enterprise Deal'
        WHEN s.total_sales >= 1000 THEN 'Mid-Market Transaction'
        WHEN s.total_sales >= 100 THEN 'Standard Sale'
        ELSE 'Micro Transaction'
    END AS deal_size_tier,

    -- AI Search: single text blob for semantic search
    'Sale of ' || CAST(s.quantity AS VARCHAR) || ' units at $' || CAST(s.unit_price AS VARCHAR) 
    || ' via ' || COALESCE(o.channel_type, 'Unknown') 
    || ' on ' || COALESCE(CAST(d.date_actual AS VARCHAR), 'unknown date')
    AS sale_narrative

FROM base s
LEFT JOIN date_context d ON s.effective_date_key = d.date_key
LEFT JOIN order_context o ON s.sales_order_line_id = o.sales_order_line_id