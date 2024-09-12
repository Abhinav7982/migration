WITH raw_data AS (
    SELECT DISTINCT * 
    FROM {{ ref('bronze_table') }}   --here we are removing the duplicate by using distinct
), 
silver_table AS (
    SELECT 
        Invoice, 
        StockCode,  
        Quantity, 
        InvoiceDate, 
        Price,
        (Quantity * Price) AS TotalValue,  -- Calculate total value for the line item
        Country
    FROM raw_data
    WHERE Country = 'United Kingdom'
      AND Quantity > 0  -- Remove invalid quantities
      AND Price > 6  -- Keep only rows with price greater than 6
)

SELECT * 
FROM silver_table;
