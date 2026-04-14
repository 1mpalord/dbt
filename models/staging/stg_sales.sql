{{ config(materialized='view') }}

SELECT 
    SalesOrderLineKey AS order_id,
    ProductKey AS product_id,
    CustomerKey AS customer_id,
    Order Quantity AS quantity,
    Unit Price AS unit_price,
    Total Product Cost AS total_cost,
    Sales Amount AS total_sales,
    -- REQUIREMENT: Handle empty/null ShipDateKey
    COALESCE(ShipDateKey, OrderDateKey) AS effective_date_key
FROM {{ source('adventureworks', 'Sales') }}