SELECT 
    "SalesOrderLineKey" AS sales_order_line_id, -- Alias happens here!
    "ProductKey" AS product_id,
    "CustomerKey" AS customer_id,
    "ResellerKey" AS reseller_id,
    "SalesTerritoryKey" AS territory_id,
    "OrderDateKey" AS effective_date_key,
    "Order Quantity" AS quantity,
    -- (The currency cleaning logic we discussed)
    CAST(REPLACE(REPLACE("Unit Price", '$', ''), ',', '') AS DECIMAL(18,2)) AS unit_price,
    CAST(REPLACE(REPLACE("Total Product Cost", '$', ''), ',', '') AS DECIMAL(18,2)) AS total_cost,
    CAST(REPLACE(REPLACE("Sales Amount", '$', ''), ',', '') AS DECIMAL(18,2)) AS total_sales
FROM {{ source('adventureworks', 'Sales') }}