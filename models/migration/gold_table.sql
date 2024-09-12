WITH aggregated_data AS (
    SELECT 
        Invoice, 
        SUM(TotalValue) AS TotalInvoiceValue,  -- Total value of all items in each invoice
        COUNT(DISTINCT StockCode) AS NumberOfItems,  -- Number of distinct items in each invoice
        MIN(InvoiceDate) AS InvoiceDate,  -- Assuming a single date per invoice
        MAX(InvoiceDate) AS LastUpdatedDate  -- Last updated date of the invoice
    FROM {{ ref('silver_table') }}  -- Directly reference silver_table
    GROUP BY Invoice
),

-- Further aggregation to get total sales by day
daily_sales AS (
    SELECT 
        SUM(TotalInvoiceValue) AS TotalDailySales,
        COUNT(DISTINCT Invoice) AS NumberOfInvoices
    FROM aggregated_data
    WHERE InvoiceDate IS NOT NULL  -- Exclude rows where InvoiceDate is NULL
    GROUP BY DATE(InvoiceDate)
)

SELECT * 
FROM daily_sales;
