{{ config(materialized='view') }}

SELECT 
    SalesOrderLineKey AS sales_order_line_id,
    "Sales Order" AS order_number,
    "Sales Order Line" AS order_line_item,
    Channel AS channel_type,
    -- Extracting the numeric ID for sorting/indexing
    CAST(REPLACE("Sales Order", 'SO', '') AS INTEGER) AS sales_order_id
FROM {{ source('adventureworks', 'SalesOrder') }}