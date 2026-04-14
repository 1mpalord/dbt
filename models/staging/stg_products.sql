{{ config(materialized='view') }}

SELECT 
    ProductKey AS product_id,
    SKU AS product_sku,
    Product AS product_name,
    -- Handle currency formatting if it's imported as text
    CAST(REPLACE(REPLACE("Standard Cost", '$', ''), ',', '') AS DECIMAL(18,2)) AS standard_cost,
    CAST(REPLACE(REPLACE("List Price", '$', ''), ',', '') AS DECIMAL(18,2)) AS list_price,
    Color AS product_color,
    Model AS product_model,
    Subcategory AS subcategory_name,
    Category AS category_name
FROM {{ source('adventureworks', 'Products') }}