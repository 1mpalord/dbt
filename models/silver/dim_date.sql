{{ config(materialized='table') }}

WITH base AS (
    SELECT * FROM {{ ref('stg_date') }}
)

SELECT
    -- Primary key
    date_key,

    -- Calendar dates
    date_actual,
    EXTRACT(YEAR  FROM date_actual)::INTEGER    AS calendar_year,
    EXTRACT(MONTH FROM date_actual)::INTEGER    AS calendar_month_num,
    EXTRACT(DAY   FROM date_actual)::INTEGER    AS day_of_month,
    TO_CHAR(date_actual, 'Month')              AS calendar_month_name,
    TO_CHAR(date_actual, 'Day')               AS day_of_week_name,
    EXTRACT(DOW  FROM date_actual)::INTEGER    AS day_of_week_num,  -- 0=Sun, 6=Sat
    EXTRACT(DOY  FROM date_actual)::INTEGER    AS day_of_year,

    -- Weekend flag (useful for LLM "weekday vs weekend" questions)
    CASE WHEN EXTRACT(DOW FROM date_actual) IN (0, 6) THEN true ELSE false END AS is_weekend,

    -- Month start / end flags for period analysis
    CASE WHEN date_actual = DATE_TRUNC('month', date_actual) THEN true ELSE false END AS is_month_start,
    CASE WHEN date_actual = (DATE_TRUNC('month', date_actual) + INTERVAL '1 month - 1 day')::DATE
         THEN true ELSE false END AS is_month_end,

    -- Fiscal dimensions (from source)
    fiscal_year,
    fiscal_year_label,
    fiscal_quarter,
    fiscal_quarter_label,
    month_key,
    month_label,
    full_date_label,

    -- Quarter number for sorting
    CASE fiscal_quarter
        WHEN 'Q1' THEN 1
        WHEN 'Q2' THEN 2
        WHEN 'Q3' THEN 3
        WHEN 'Q4' THEN 4
    END AS fiscal_quarter_num,

    -- LLM-friendly: combined period description
    fiscal_year_label || ' ' || fiscal_quarter AS period_label,  -- e.g. 'FY2018 Q1'

    -- Seasonal classification (Northern Hemisphere)
    CASE EXTRACT(MONTH FROM date_actual)::INTEGER
        WHEN 12 THEN 'Winter' WHEN 1 THEN 'Winter' WHEN 2 THEN 'Winter'
        WHEN 3  THEN 'Spring' WHEN 4 THEN 'Spring' WHEN 5 THEN 'Spring'
        WHEN 6  THEN 'Summer' WHEN 7 THEN 'Summer' WHEN 8 THEN 'Summer'
        ELSE 'Autumn'
    END AS season

FROM base
