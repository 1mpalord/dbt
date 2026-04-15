{{ config(materialized='view') }}

SELECT
    -- Key for joining to fct_sales.effective_date_key
    "DateKey"           AS date_key,

    -- Raw calendar date (stored as text in source, e.g. '7/1/2017')
    CAST("Date" AS DATE) AS date_actual,

    -- Fiscal dimensions (sourced as strings, e.g. 'FY2018', 'FY2018 Q1')
    "Fiscal Year"       AS fiscal_year_label,       -- e.g. 'FY2018'
    CAST(
        RIGHT("Fiscal Year", 4) AS INTEGER
    )                   AS fiscal_year,              -- e.g. 2018

    "Fiscal Quarter"    AS fiscal_quarter_label,     -- e.g. 'FY2018 Q1'
    RIGHT("Fiscal Quarter", 2) AS fiscal_quarter,    -- e.g. 'Q1'

    -- Human-readable fields
    "Month"             AS month_label,              -- e.g. '2017 Jul'
    "Full Date"         AS full_date_label,          -- e.g. '2017 Jul, 01'
    "MonthKey"          AS month_key                 -- e.g. 201707

FROM {{ source('adventureworks', 'Date') }}
